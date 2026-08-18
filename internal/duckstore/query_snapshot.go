// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
)

// The query-source snapshot: one consistent view of everything a store query
// reads, taken once per query and held for the read's lifetime. Before it,
// the pieces were gathered independently — the window list under one lock,
// each lease under another, the delta connection resolved last with a retry
// — so a generation roll in between could hand the query a connection from
// one generation next to a window set and generation list from another. The
// snapshot closes that gap, and the read it feeds is widened past it in two
// phases: the rolled generations' window plans are read through the pins it
// holds, and the serving boundary — which windows each generation may still
// contribute, and which windows the read leases — is decided under the
// windows' read locks (withQuerySources), where no consumption can commit
// underneath the decision.

// querySource is one store file a query reads, in the shape the SQL builders
// address it: the file's kind and identity (which generation, which archive
// window), the tier table it contributes and under what qualifier, and the
// time range it is allowed to contribute. The active delta generation and
// the archive windows answer [from, to) in full — exactly what the retired
// qualifier-per-source shape expressed — while a rolled-off generation
// contributes one descriptor per archive window consumption has not yet
// taken from it, bounded by that window's own range.
type querySource struct {
	kind  fileKind  // fileKindDelta for a delta generation, fileKindArchive for a window
	gen   int64     // delta: the generation identity; archive: zero
	key   windowKey // archive: the window identity; a rolled delta: the window whose range it contributes; the active delta: zero
	table string    // the tier table this source contributes
	alias string    // SQL qualifier on the query's connection: "" is the delta — the connection's own database — else the ATTACH alias
	from  int64     // first second the source may contribute
	to    int64     // one past the last second the source may contribute
}

// tableRef is the source's table as addressed on the query's connection: the
// plain tier table for the delta (its database is the connection's own), the
// alias-qualified table for an attached archive window.
func (src querySource) tableRef() string {
	if src.alias == "" {
		return src.table
	}
	return src.alias + "." + src.table
}

// queryWindow is one leased archive window of a snapshot: the descriptor the
// builder will address it by (carrying its attach alias), plus the file's
// path and the lease holding it against retention.
type queryWindow struct {
	src   querySource
	path  string
	lease *Lease
}

// queryGen is one rolled-off delta generation of a snapshot: pinned against
// consumption's unlink, with the archive windows of the queried tier its rows
// span that overlap the query's range — the candidates for serving. Which
// candidates it actually contributes is decided later, under those windows'
// read locks, against the consumption records: a window that durably records
// the generation already holds its rows, and serving both would count them
// twice.
type queryGen struct {
	gen     int64
	pin     *DeltaPin
	path    string
	windows []int64 // candidate window starts, ascending; consumption-undecided
	alias   string  // the read-only attach's SQL qualifier, unique to the query
}

// querySnapshot is one consistent view of everything a store query reads: the
// active delta generation with a connection already checked out of its pool,
// every delta generation present at that moment — the active one pinned, the
// rolled-off ones pinned as serving candidates — and every served archive
// window of the queried tier overlapping the range, resolved under one store
// lock, so no roll can interleave and pair a connection from one generation
// with a view of another. The pins and the window leases hold for the
// snapshot's lifetime, so neither consumption nor retention can remove a file
// underneath the read. Release returns all of it.
type querySnapshot struct {
	conn    *sql.Conn
	gen     int64         // the generation conn serves — the active one when the snapshot was taken
	deltas  []int64       // every delta generation present then, ascending; those below gen are rolled off
	pin     *DeltaPin     // holds the snapshot generation's file against ConsumeGeneration's unlink
	rolled  []queryGen    // the generations below gen, ascending, pinned as serving candidates
	windows []queryWindow // the leased windows, in served order (tier, window start)
}

// acquireQuerySnapshot takes one consistent query view for a range: a
// connection checked out of the active generation's pool, the generation
// list, a pin on the active generation and on every rolled-off one, and a
// lease on every served window of the tier overlapping [from, to). The
// connection is checked out first and the rest is then resolved under one
// store lock that also verifies the generation did not roll in between — an
// interleaved roll discards the connection and the attempt repeats, so the
// returned snapshot's connection always serves exactly the generation it
// reports.
func (s *Store) acquireQuerySnapshot(ctx context.Context, tier string, from, to int64) (*querySnapshot, error) {
	// A roll between attempts is rare and each attempt reads the new state,
	// so a handful covers any realistic interleaving; more means something
	// else is wrong and the query fails loudly rather than spin.
	const attempts = 5
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		s.mu.RLock()
		db, gen := s.delta, s.gen
		s.mu.RUnlock()
		if db == nil {
			return nil, fmt.Errorf("duck-store: store is closed")
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			// A roll closes the pool it just replaced; the retry reads the
			// new one. Any other failure fails again next attempt just the
			// same, so the error is carried through rather than classified.
			lastErr = fmt.Errorf("duck-store: store query connection: %w", err)
			continue
		}

		s.mu.Lock()
		if s.delta != db || s.gen != gen {
			s.mu.Unlock()
			// The generation rolled between the checkout and this view, so
			// the connection serves a generation the rest of the snapshot
			// would not describe; discard both and start over on the new one.
			lastErr = fmt.Errorf("duck-store: generation rolled under the store query")
			_ = conn.Close()
			continue
		}
		snap := &querySnapshot{conn: conn, gen: gen, deltas: append([]int64(nil), s.deltas...)}
		snap.pin = s.acquireDeltaPinLocked(gen)
		for _, g := range snap.deltas {
			if g == gen {
				continue
			}
			snap.rolled = append(snap.rolled, queryGen{
				gen:  g,
				path: filepath.Join(s.cfg.Dir, deltaFileName(g)),
				// present in this locked view, so the pin cannot be nil here
				pin: s.acquireDeltaPinLocked(g),
			})
		}
		for _, wf := range s.windows {
			if wf.Tier != tier || wf.WindowStart >= to || wf.WindowStart+tierWindowSecs[tier] <= from {
				continue
			}
			k := windowKey{tier: wf.Tier, start: wf.WindowStart}
			l := s.acquireWindowLeaseLocked(k)
			if l == nil {
				continue // cannot happen under the lock; kept for the shared helper's contract
			}
			snap.windows = append(snap.windows, queryWindow{
				src: querySource{
					kind:  fileKindArchive,
					key:   k,
					table: TierTable(wf.Tier),
					from:  from,
					to:    to,
				},
				path:  wf.Path,
				lease: l,
			})
		}
		s.mu.Unlock()
		return snap, nil
	}
	return nil, lastErr
}

// release returns everything the snapshot holds: the windows' leases, every
// generation's pin and the connection. After it, consumption may unlink a
// generation's file and retention may unlink any window's.
func (q *querySnapshot) release() {
	for i := range q.windows {
		q.windows[i].lease.Release()
	}
	for i := range q.rolled {
		q.rolled[i].pin.Release()
	}
	q.pin.Release()
	_ = q.conn.Close()
}

// errQuerySnapshotInvalidated reports that the store moved under a query
// snapshot between its acquisition and the serving boundary in a way the
// boundary itself cannot absorb — the snapshot's own generation was consumed
// into a window the query reads. withQuerySources retries on a fresh
// snapshot; readers never see it.
var errQuerySnapshotInvalidated = errors.New("duck-store: query snapshot invalidated by consumption")

// resolveRolledWindows reads every rolled generation's window plan for the
// queried tier and keeps the windows overlapping [from, to): the candidates
// the generation might serve. The plans are read through the snapshot's own
// connection, over the same read-only attach the read itself will address —
// never a standalone handle: DuckDB serializes opens of one file through its
// process-global instance cache, so a per-query open-and-close of every rolled
// generation would churn that cache against consumption's own opens and wedge
// the two (queries holding generation pins blocked in the open, consumption
// waiting on the pins before its unlink). The attach bypasses the cache
// entirely, and the pins keep the file unlinked-but-addressable meanwhile; a
// rolled generation is immutable, so its plan cannot change under the read.
func (q *querySnapshot) resolveRolledWindows(ctx context.Context, tier string, from, to int64) error {
	win := tierWindowSecs[tier]
	for i := range q.rolled {
		rg := &q.rolled[i]
		rows, err := q.conn.QueryContext(ctx, fmt.Sprintf(
			"SELECT DISTINCT time - time %% %d FROM %s.%s ORDER BY 1", win, rg.alias, tierTables[tier]))
		if err != nil {
			return fmt.Errorf("duck-store: read generation %d for store query: %w", rg.gen, err)
		}
		for rows.Next() {
			var start int64
			if err := rows.Scan(&start); err != nil {
				rows.Close()
				return fmt.Errorf("duck-store: read generation %d for store query: %w", rg.gen, err)
			}
			if start < to && start+win > from {
				rg.windows = append(rg.windows, start)
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("duck-store: read generation %d for store query: %w", rg.gen, err)
		}
		rows.Close()
	}
	return nil
}

// resolveServingBoundary decides, under one store lock held together with the
// read locks of every window the query touches, which candidate windows each
// rolled generation serves — and whether the snapshot's own generation can
// still serve its full range. Holding both is what makes the decision durable
// for the read's lifetime: a consumption of any window involved needs that
// window's write lock first, so it cannot commit between this decision and
// the read's end, and every row is served either from its generation or from
// the window that durably recorded it — never both, never neither.
//
// A candidate whose window records the generation contributes nothing from
// the generation's file; the window — served by the very transaction that
// recorded it, which is what this same hold sees — is leased into the read
// instead (leaseWindowLocked), so the rows stay visible exactly once even
// when the consumption committed between the snapshot and this decision. A
// candidate whose window carries an eviction tombstone (s.evicted: consumed,
// then unlinked by retention) contributes nothing either: those rows are gone
// by policy, and serving them from the generation would undo the eviction.
//
// The snapshot's own generation has no boundary of its own — its connection
// serves [from, to) outright — so a roll followed by a consumption since the
// snapshot would land its rows in a window the query also reads and count
// them twice. A record of it in any read window means exactly that happened;
// false tells the caller to retry on a fresh snapshot, where the generation
// is rolled and the boundary above applies to it like any other.
func (s *Store) resolveServingBoundary(snap *querySnapshot, tier string, from, to int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range snap.rolled {
		rg := &snap.rolled[i]
		kept := rg.windows[:0]
		for _, start := range rg.windows {
			k := windowKey{tier: tier, start: start}
			if _, done := s.consumed[k][rg.gen]; done {
				snap.leaseWindowLocked(s, k, from, to)
				continue
			}
			if _, evicted := s.evicted[k]; evicted {
				continue
			}
			kept = append(kept, start)
		}
		rg.windows = kept
	}
	for i := range snap.windows {
		if _, done := s.consumed[snap.windows[i].src.key][snap.gen]; done {
			return false
		}
	}
	return true
}

// leaseWindowLocked adds a served window to the snapshot's read set — leasing
// it against retention — unless it is already there. Callers hold s.mu and
// the window's read lock, under which the served entry cannot change, so a
// miss means the window is not served at all and there is nothing to lease.
func (q *querySnapshot) leaseWindowLocked(s *Store, k windowKey, from, to int64) {
	for i := range q.windows {
		if q.windows[i].src.key == k {
			return
		}
	}
	for _, wf := range s.windows {
		if wf.Tier != k.tier || wf.WindowStart != k.start {
			continue
		}
		l := s.acquireWindowLeaseLocked(k)
		if l == nil {
			return // cannot happen under the lock; kept for the shared helper's contract
		}
		q.windows = append(q.windows, queryWindow{
			src: querySource{
				kind:  fileKindArchive,
				key:   k,
				table: TierTable(k.tier),
				from:  from,
				to:    to,
			},
			path:  wf.Path,
			lease: l,
		})
		return
	}
}

// rolledSources expands the rolled generations' surviving windows into
// descriptors: one per window, bounded by the window's own range cut to the
// query's, under the generation's one attach alias. The generations ascend,
// and each one's windows ascend within it.
func (q *querySnapshot) rolledSources(tier string, from, to int64) []querySource {
	win := tierWindowSecs[tier]
	var srcs []querySource
	for i := range q.rolled {
		rg := &q.rolled[i]
		if len(rg.windows) == 0 {
			continue
		}
		sort.Slice(rg.windows, func(a, b int) bool { return rg.windows[a] < rg.windows[b] })
		for _, start := range rg.windows {
			srcs = append(srcs, querySource{
				kind:  fileKindDelta,
				gen:   rg.gen,
				key:   windowKey{tier: tier, start: start},
				table: TierTable(tier),
				alias: rg.alias,
				from:  max(from, start),
				to:    min(to, start+win),
			})
		}
	}
	return srcs
}

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
	"fmt"
)

// The query-source snapshot: one consistent view of everything a store query
// reads, taken once per query and held for the read's lifetime. Before it,
// the pieces were gathered independently — the window list under one lock,
// each lease under another, the delta connection resolved last with a retry
// — so a generation roll in between could hand the query a connection from
// one generation next to a window set and generation list from another. The
// snapshot closes that gap and is the seam later work widens: making
// rolled-but-unconsumed generations query sources is a matter of adding
// descriptors resolved from this same view.

// querySource is one store file a query reads, in the shape the SQL builders
// address it: the file's kind and identity (which generation, which archive
// window), the tier table it contributes and under what qualifier, and the
// time range it is allowed to contribute. Today every source carries the
// query's own range — the active delta and the archive windows both answer
// [from, to) in full — so the descriptor set is exactly what the retired
// qualifier-per-source shape expressed; a source that may contribute only
// part of a range, or a different table, is expressed by narrowing these
// fields, not by changing the builders.
type querySource struct {
	kind  fileKind  // fileKindDelta for a delta generation, fileKindArchive for a window
	gen   int64     // delta: the generation identity; archive: zero
	key   windowKey // archive: the window identity; delta: zero
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

// querySnapshot is one consistent view of everything a store query reads: the
// active delta generation with a connection already checked out of its pool,
// every delta generation present at that moment, and every served archive
// window of the queried tier overlapping the range — resolved under one store
// lock, so no roll can interleave and pair a connection from one generation
// with a view of another. The generation is pinned and the windows leased for
// the snapshot's lifetime, so neither consumption nor retention can remove a
// file underneath the read. Release returns all of it.
type querySnapshot struct {
	conn    *sql.Conn
	gen     int64         // the generation conn serves — the active one when the snapshot was taken
	deltas  []int64       // every delta generation present then, ascending; those below gen are rolled off
	pin     *DeltaPin     // holds the generation's file against ConsumeGeneration's unlink
	windows []queryWindow // the leased windows, in served order (tier, window start)
}

// acquireQuerySnapshot takes one consistent query view for a range: a
// connection checked out of the active generation's pool, the generation
// list, a pin on the active generation and a lease on every served window of
// the tier overlapping [from, to). The connection is checked out first and
// the rest is then resolved under one store lock that also verifies the
// generation did not roll in between — an interleaved roll discards the
// connection and the attempt repeats, so the returned snapshot's connection
// always serves exactly the generation it reports.
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

// release returns everything the snapshot holds: the windows' leases, the
// generation's pin and the connection. After it, consumption may unlink the
// generation's file and retention may unlink any window's.
func (q *querySnapshot) release() {
	for i := range q.windows {
		q.windows[i].lease.Release()
	}
	q.pin.Release()
	_ = q.conn.Close()
}

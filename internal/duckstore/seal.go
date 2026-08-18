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
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
)

// Sealing rewrites an archive window's several compaction runs into one
// collapsed, (metric, time)-sorted run and reopens the file read-only. It is
// the one CPU burst in the design, so it runs once per window lifetime, at
// window end plus the historic window — 48 hours past the point any late row
// can still arrive for that window, which is what makes the sealed collapse
// final. From the seal on, the window's contents never change again:
// the sealed marker in the window file's own metadata refuses every later
// append, and the store opens the file read-only from then on.
//
// The rewrite and the marker commit as one transaction in the window file, so
// a crash mid-seal rolls back to the unsealed several-run state and the next
// pass simply retries; a crash after the commit is indistinguishable from a
// completed seal, because the marker rides along with the rewrite.
//
// Crossing the seal time is necessary but not sufficient: the writer's ingest
// guard and the seal boundary meet exactly, so a row accepted in the last
// conforming second can sit in a delta generation that consumption has not
// taken when the window comes due. Sealing it then would doom that row —
// consumeWindow's sealed branch can never place it. A pass therefore seals
// only behind the barrier (sealBarrier): a coordinated roll-and-drain that
// establishes, durably, that no generation — the active one included — can
// still contribute a row to any window the pass is about to seal.
//
// A pass also tends the windows that are not going anywhere yet: the
// intra-window re-collapse. A window stays unsealed — still accepting rows —
// for its whole length plus the historic window, while compaction appends a
// fresh partial run every pass, so an unsealed window's table accumulates
// runs for up to two days (1s tier) or a month (1h tier) before the seal
// folds them. The sweep after the sealing half rewrites the windows
// compaction appended to, through the very statement the seal uses minus the
// marker, whenever a window's physical rows exceed a factor of its collapsed
// count — so no unsealed window holds more than that factor times its
// collapsed rows for longer than one sealer interval.

// DefaultSealerInterval is how often the sealer wakes to look for windows past
// their seal time. Sealing is rare — one rewrite per window lifetime, 48
// hours after the window closed — so the cadence is relaxed.
const DefaultSealerInterval = 30 * time.Second

// DefaultRecollapseFactor is the re-collapse trigger: how many physical rows
// per collapsed row an unsealed archive window may accumulate before the
// sealer's sweep folds them back into one run. The trade it picks is
// amortized — each rewrite processes at most factor times the collapsed rows
// while at least factor-1 times them were appended since the previous one,
// so the steady-state cost is O(factor/(factor-1)) row-rewrites per appended
// row, 4/3 at this default — against the read and disk cost of an unsealed
// window holding up to factor times its collapsed rows between sweeps. It is
// a constant, not a flag: the amortization argument, not a deployment
// property, picks it.
const DefaultRecollapseFactor = 4

// SealerConfig configures a Sealer.
type SealerConfig struct {
	// Interval is how often a sealing pass looks for due windows; the default
	// is DefaultSealerInterval.
	Interval time.Duration

	// NowFunc supplies the clock the seal boundary is judged by. Defaults to
	// time.Now.
	NowFunc func() time.Time

	// Metrics receives each pass's timing (MaintenanceSealing) and one
	// MaintenanceWindow per window the pass sealed or re-collapsed. Optional.
	Metrics MetricsRecorder

	// RecollapseFactor is how many physical rows per collapsed row an
	// unsealed archive window may accumulate before the pass's re-collapse
	// sweep rewrites it; see DefaultRecollapseFactor. Values below one take
	// the default.
	RecollapseFactor int

	// DrainFault, when set, is consulted at each crash point of every
	// generation the seal barrier consumes — the ConsumeOptions.Fault
	// protocol — so a non-nil error fails the barrier, and the pass with it,
	// exactly where a compaction that cannot finish would stall. It exists
	// for tests; production leaves it nil.
	DrainFault func(CrashPoint) error

	// Logf receives pass failures. Defaults to log.Printf.
	Logf func(format string, args ...any)
}

// Sealer drives a store's sealing passes. It is one goroutine working one
// window at a time at a relaxed cadence, and it holds nothing the write path
// needs — the per-window maintenance lock it shares with compaction's appends
// is never taken by ingestion. The one coordination sealing now performs with
// the writer is the barrier's roll: the generation switch waits for the round
// already in flight and rounds submitted behind it wait for the switch, a
// one-round pause a pass costs only while a window actually came due.
type Sealer struct {
	store *Store
	cfg   SealerConfig
	clock maintenanceClock // liveness: time since the last successful pass

	mu sync.Mutex // one pass at a time
}

// NewSealer returns the store's sealer. Run it until the process ends; the
// store stays usable without one, leaving due windows unsealed.
func NewSealer(s *Store, cfg SealerConfig) *Sealer {
	if cfg.Interval <= 0 {
		cfg.Interval = DefaultSealerInterval
	}
	if cfg.RecollapseFactor <= 0 {
		cfg.RecollapseFactor = DefaultRecollapseFactor
	}
	if cfg.NowFunc == nil {
		cfg.NowFunc = time.Now
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	return &Sealer{store: s, cfg: cfg, clock: newMaintenanceClock()}
}

// Run seals until ctx is done: one pass immediately (a restarted process
// first finishes whatever windows came due while it was down), then one pass
// per Interval. A failed pass is logged and retried by the next; the seal
// transaction makes that safe. It returns nil when ctx is done.
func (sl *Sealer) Run(ctx context.Context) error {
	return runMaintenanceLoop(ctx, MaintenanceSealing, sl.cfg.Interval, sl.cfg.Logf, sl.SealOnce)
}

// SealOnce seals every served window that has crossed its seal time. Windows
// that no longer exist on disk at their seal time — retention unlinked them
// in the meantime — are skipped: sealing rewrites, it never resurrects.
func (sl *Sealer) SealOnce(ctx context.Context) error {
	start := time.Now()
	err := sl.sealOnce(ctx)
	if err == nil {
		sl.clock.markSuccess()
	}
	recordMaintenancePass(sl.cfg.Metrics, MaintenanceSealing, start, err)
	return err
}

// Liveness reports the sealer's liveness input: time since its last
// successful pass, counted from its creation until the first one lands.
func (sl *Sealer) Liveness() MaintenanceLiveness {
	return MaintenanceLiveness{Kind: MaintenanceSealing, SinceLastPass: sl.clock.since}
}

func (sl *Sealer) sealOnce(ctx context.Context) error {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	now := sl.cfg.NowFunc().Unix()
	var due []windowKey
	for _, wf := range sl.store.Windows() {
		if wf.Sealed || !windowSealDue(wf.Tier, wf.WindowStart, now) {
			continue
		}
		if _, err := os.Stat(wf.Path); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue // the window left before its seal; nothing to rewrite
			}
			return fmt.Errorf("duck-store: seal %s: %w", wf.Path, err)
		}
		due = append(due, windowKey{tier: wf.Tier, start: wf.WindowStart})
	}
	if len(due) > 0 {
		if err := sl.store.sealBarrier(ctx, due, sl.cfg.DrainFault); err != nil {
			return err
		}
		for _, k := range due {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := sl.store.SealWindow(ctx, k.tier, k.start); err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					continue // the window left between the barrier and the rewrite
				}
				return fmt.Errorf("duck-store: seal %s: %w",
					filepath.Join(sl.store.cfg.Dir, archiveSubdir, archiveFileName(k.tier, k.start)), err)
			}
			recordMaintenanceWindow(sl.cfg.Metrics, WindowSealed, k.tier)
		}
	}
	// The other half of the pass tends the windows not going anywhere yet:
	// folding the partial rows compaction keeps appending to them.
	return sl.recollapseSweep(ctx)
}

// recollapseSweep re-collapses the archive windows compaction appended to
// since the previous pass: each candidate is measured, and the ones holding
// more physical rows than RecollapseFactor times their collapsed count are
// rewritten in place through the seal's own statement, minus the marker. The
// sweep drains the candidate set before opening any file — an append that
// lands while it works cannot hold the window's write lock, so it either
// marked before the drain and is being checked, or marks after it and is the
// next pass's candidate; no append is missed either way. A window whose check
// fails is re-armed before the pass gives up, so the next pass retries it.
func (sl *Sealer) recollapseSweep(ctx context.Context) error {
	for _, k := range sl.store.takeRecollapseCandidates() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		recollapsed, err := sl.store.RecollapseWindow(ctx, k.tier, k.start, sl.cfg.RecollapseFactor)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue // the window left between the sweep and the rewrite
			}
			sl.store.markRecollapse(k) // the next pass retries this window
			return err
		}
		if recollapsed {
			recordMaintenanceWindow(sl.cfg.Metrics, WindowRecollapsed, k.tier)
		}
	}
	return nil
}

// windowSealDue reports whether a window has crossed its seal time: window
// end plus the historic window, past which no conforming sender can produce a
// row for it anymore.
func windowSealDue(tier string, windowStart, nowUnix int64) bool {
	return nowUnix >= windowStart+tierWindowSecs[tier]+data_model.MaxHistoricWindow
}

// sealBarrier establishes the durable precondition for sealing the due
// windows: by the time it returns, no delta generation — the active one
// included — can still contribute a row to any of them, so the rewrites that
// follow cannot strand a conformingly-accepted row in a generation whose
// window is already frozen. Two things must hold, and the barrier delivers
// both with one coordinated roll-and-drain:
//
// (a) No writer round accepted before the boundary can still commit a row
// into a generation the drain cannot see. The roll below is the handshake:
// the writer switches generations only between rounds, so every round already
// accepted commits into the rolled generation before the roll returns, and
// every later round runs its ingest guard at a time the pass already found
// the windows due — where the guard rejects their rows outright.
//
// (b) Every rolled generation contributing to a due window has durably
// recorded consumption. After the roll, each such generation is immutable
// with its plan fixed, so its contribution is decidable exactly (its windows,
// read under a pin so a concurrent consume cannot unlink it mid-read), and
// consuming it lands its rows in the archive before any rewrite starts.
//
// The generation list the drain walks is resolved under one store lock
// (deltaState — the same consistency the query snapshot gives reads), taken
// after the roll: every generation below the then-active one was sealed by
// some roll at or before the barrier's, so all of them are eligible and none
// can be missed. A generation the compactor finishes under the barrier is
// skipped — its rows are already durably recorded — while a genuine consume
// failure fails the pass, leaving every due window unsealed for the retry.
func (s *Store) sealBarrier(ctx context.Context, due []windowKey, fault func(CrashPoint) error) error {
	// The roll costs one generation switch and makes the pending-row question
	// decidable: everything that can ever land in a due window is now in a
	// rolled, immutable generation.
	if err := s.RollGeneration(); err != nil {
		return fmt.Errorf("duck-store: seal barrier roll: %w", err)
	}
	dueSet := make(map[windowKey]struct{}, len(due))
	for _, k := range due {
		dueSet[k] = struct{}{}
	}
	opts := ConsumeOptions{AppendWindow: collapseWindowRows, Fault: fault}
	gens, active := s.deltaState()
	for _, gen := range gens {
		if gen == active {
			break // gens ascend; this one and anything after took no pre-boundary rows
		}
		contributes, err := s.generationContributesToDue(gen, dueSet)
		if err != nil {
			return err
		}
		if !contributes {
			continue
		}
		if err := s.ConsumeGeneration(ctx, gen, opts); err != nil {
			if deltaFileGone(filepath.Join(s.cfg.Dir, deltaFileName(gen))) {
				continue // the compactor finished this generation under the barrier; its windows hold the rows
			}
			return fmt.Errorf("duck-store: seal barrier: %w", err)
		}
	}
	return nil
}

// generationContributesToDue reports whether a rolled generation's archive
// window plan intersects the set the barrier is about to seal. The plan is
// read through a pin (lease.go), so the compactor cannot unlink the file
// mid-read; the pin is back before the caller consumes the generation, since
// consumption itself waits for pins to clear. A generation that vanished
// before or during the check contributes nothing: its rows are already in the
// archive.
func (s *Store) generationContributesToDue(gen int64, due map[windowKey]struct{}) (bool, error) {
	pin := s.AcquireDeltaPin(gen)
	if pin == nil {
		return false, nil // not present anymore: consumed or gone under the barrier
	}
	defer pin.Release()
	path := filepath.Join(s.cfg.Dir, deltaFileName(gen))
	windows, err := generationWindows(path, s.cfg.Resources)
	if err != nil {
		return false, fmt.Errorf("duck-store: seal barrier: plan generation %d: %w", gen, err)
	}
	for k := range windows {
		if _, ok := due[k]; ok {
			return true, nil
		}
	}
	return false, nil
}

// deltaFileGone reports whether a delta generation's file left the disk — the
// barrier's way to tell a generation the compactor finished under it from one
// its own consume genuinely failed to land.
func deltaFileGone(path string) bool {
	_, err := os.Stat(path)
	return errors.Is(err, fs.ErrNotExist)
}

// SealWindow rewrites one archive window's runs into a single collapsed run
// and seals the file: the rewrite, the sealed marker and nothing else land
// in one transaction, and the file is reopened read-only — the access mode
// every open from the seal on uses. Sealing an already-sealed window is a
// no-op, so a retried pass after a crash between the commit and the in-memory
// bookkeeping completes quietly. The window must have crossed its seal time
// and cleared the barrier (sealBarrier) — a sealing pass establishes both; a
// direct caller takes responsibility for them itself.
func (s *Store) SealWindow(ctx context.Context, tier string, windowStart int64) error {
	path := filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(tier, windowStart))
	table := tierTables[tier]

	// Serialize against compaction's appends and retention's unlinks of the
	// same file through the window's own write lock (window_locks.go) — not
	// the store-global archive lock it replaces: a rewrite and an append must
	// never interleave on one file, while sealing one window no longer fences
	// work on every other. The existence check below must not race an unlink
	// either — a read-write open of a freshly unlinked path would create a
	// fresh empty database there. The check runs under the lock, where
	// unlinks are serialized away. Ingestion never takes this lock, so a seal
	// can never delay a write.
	unlock := s.lockWindowWrite(windowKey{tier: tier, start: windowStart})
	defer unlock()

	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("duck-store: window %s: %w", path, fs.ErrNotExist)
		}
		return fmt.Errorf("duck-store: seal %s: %w", path, err)
	}
	db, err := openStoreFile(path, false, s.cfg.Resources)
	if err != nil {
		return fmt.Errorf("duck-store: seal %s: %w", path, err)
	}
	sealed, err := readSealed(db)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("duck-store: read %s of %s: %w", SealedTable, path, err)
	}
	if sealed {
		_ = db.Close()
		s.markWindowSealed(tier, windowStart)
		return nil
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("duck-store: seal %s: %w", path, err)
	}
	if err := s.rewriteWindowRuns(ctx, conn, tier, table, windowStart, true); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return fmt.Errorf("duck-store: seal %s: %w", path, err)
	}
	_ = conn.Close()

	// The rewrite is committed; fold it into the file itself so the window's
	// several runs are reclaimed on disk too. Best-effort: a failure leaves a
	// correct, sealed, merely uncompact file.
	if _, err := db.Exec("CHECKPOINT"); err != nil {
		s.cfg.Logf("[error] duck-store: checkpoint %s after sealing: %v", path, err)
	}
	_ = db.Close()

	// Reopen read-only and take the marker from the file itself: the mode the
	// store serves the window in from now on, proven by the open.
	ro, err := openStoreFile(path, true, s.cfg.Resources)
	if err != nil {
		return fmt.Errorf("duck-store: reopen %s read-only after sealing: %w", path, err)
	}
	sealed, err = readSealed(ro)
	_ = ro.Close()
	if err != nil {
		return fmt.Errorf("duck-store: read %s of %s after sealing: %w", SealedTable, path, err)
	}
	if !sealed {
		return fmt.Errorf("duck-store: %s reopened without its sealed marker", path)
	}
	s.markWindowSealed(tier, windowStart)
	s.cfg.Logf("[info] duck-store: sealed %s: runs rewritten into one, file read-only from now on", path)
	return nil
}

// rewriteWindowRuns is the seal's and the re-collapse's shared transaction:
// collapse the window's rows — the same one-statement collapse compaction
// runs over the delta, reading the window's own table — into a fresh table,
// swap it in under the original name and, when sealing, plant the sealed
// marker, all committing or rolling back together. The transaction is the
// writer's protocol — explicit BEGIN TRANSACTION / COMMIT through the
// connection — and the collapse's fold UDFs are registered on the connection
// before it opens, since DuckDB UDFs live per-connection.
func (s *Store) rewriteWindowRuns(ctx context.Context, conn *sql.Conn, tier, table string, windowStart int64, seal bool) error {
	if err := registerFoldUDFs(conn); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		return err
	}
	fail := func(err error) error {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		return err
	}
	fresh := table + "_sealed"
	if !seal {
		fresh = table + "_recollapsed"
	}
	if _, err := conn.ExecContext(ctx, tierTableDDL(fresh)); err != nil {
		return fail(fmt.Errorf("create %s: %w", fresh, err))
	}
	// The window's rows are already tier-truncated, so the collapse reads
	// the plain time column — the statement stays the one the tier's own
	// table always produced.
	if _, err := conn.ExecContext(ctx,
		collapseInsert(fresh, "main", table, "time"), windowStart, windowStart+tierWindowSecs[tier]); err != nil {
		return fail(fmt.Errorf("collapse %s into %s: %w", table, fresh, err))
	}
	if _, err := conn.ExecContext(ctx, "DROP TABLE "+table); err != nil {
		return fail(fmt.Errorf("drop %s: %w", table, err))
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", fresh, table)); err != nil {
		return fail(fmt.Errorf("rename %s to %s: %w", fresh, table, err))
	}
	if seal {
		// The marker rides in the rewrite's transaction: same file, so neither
		// can exist without the other.
		if _, err := conn.ExecContext(ctx, "INSERT INTO "+SealedTable+" VALUES (true)"); err != nil {
			return fail(fmt.Errorf("plant the marker in %s: %w", SealedTable, err))
		}
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fail(err)
	}
	return nil
}

// RecollapseWindow rewrites one unsealed archive window's accumulated partial
// runs in place: the same collapse statement the seal plants its marker with,
// minus the marker, so the window keeps accepting rows afterwards. The stored
// contents come out decode-equivalent — every column the collapse folds is
// safe to fold again — merely physically denser: one collapsed, (metric,
// time)-sorted run per key. It reports whether it rewrote. A window holding
// no more than factor times its collapsed count is left alone, and so is a
// sealed window, whose contents must never change again; a window whose file
// is gone reports fs.ErrNotExist. The whole check-and-rewrite holds the
// window's own write lock (window_locks.go), serializing against
// compaction's appends, the sealer's rewrite and retention's unlink of the
// same file while work on every other window proceeds in parallel. The file
// is not reopened read-only afterwards: an unsealed window stays writable —
// exactly the state it serves in.
func (s *Store) RecollapseWindow(ctx context.Context, tier string, windowStart int64, factor int) (bool, error) {
	if factor <= 0 {
		factor = DefaultRecollapseFactor
	}
	k := windowKey{tier: tier, start: windowStart}
	path := filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(tier, windowStart))
	table := tierTables[tier]

	unlock := s.lockWindowWrite(k)
	defer unlock()

	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, fmt.Errorf("duck-store: window %s: %w", path, fs.ErrNotExist)
		}
		return false, fmt.Errorf("duck-store: recollapse %s: %w", path, err)
	}
	db, err := openStoreFile(path, false, s.cfg.Resources)
	if err != nil {
		return false, fmt.Errorf("duck-store: recollapse %s: %w", path, err)
	}
	defer db.Close()

	// The marker decides, not the served list's in-memory flag: a window
	// sealed between the sweep's snapshot and this lock already holds its
	// final, folded state, and rewriting it again is at best wasted work —
	// at worst it churnes a file nothing may change again.
	sealed, err := readSealed(db)
	if err != nil {
		return false, fmt.Errorf("duck-store: read %s of %s: %w", SealedTable, path, err)
	}
	if sealed {
		return false, nil
	}

	physical, collapsed, err := countCollapseGroups(db, table)
	if err != nil {
		return false, fmt.Errorf("duck-store: count %s in %s: %w", table, path, err)
	}
	if physical <= int64(factor)*collapsed {
		return false, nil
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return false, fmt.Errorf("duck-store: recollapse %s: %w", path, err)
	}
	err = s.rewriteWindowRuns(ctx, conn, tier, table, windowStart, false)
	_ = conn.Close()
	if err != nil {
		return false, fmt.Errorf("duck-store: recollapse %s: %w", path, err)
	}

	// Fold the rewrite's dead blocks into the file itself — half the point of
	// re-collapsing is the disk. Best-effort, the way the seal's is: a failure
	// leaves a correct, merely uncompact file.
	if _, err := db.Exec("CHECKPOINT"); err != nil {
		s.cfg.Logf("[error] duck-store: checkpoint %s after re-collapsing: %v", path, err)
	}
	s.cfg.Logf("[info] duck-store: re-collapsed %s: %d partial rows folded into %d", path, physical, collapsed)
	return true, nil
}

// markRecollapseLocked adds k to the re-collapse candidate set; callers hold
// s.mu. Every window an append lands in is a candidate — compaction's
// consumeWindow marks on commit — and an open seeds the unsealed windows it
// recovers from disk, because a restarted process owes them a check no append
// of its own will trigger.
func (s *Store) markRecollapseLocked(k windowKey) {
	if s.recollapsePending == nil {
		s.recollapsePending = map[windowKey]struct{}{}
	}
	s.recollapsePending[k] = struct{}{}
}

// markRecollapse is markRecollapseLocked taking s.mu itself.
func (s *Store) markRecollapse(k windowKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markRecollapseLocked(k)
}

// takeRecollapseCandidates drains the re-collapse candidate set in the
// canonical window order. Draining under one lock, before any file is opened,
// is what keeps the sweep correct against concurrent appends: an append
// landing while the sweep works cannot hold its window's write lock, so it
// either marked before the drain and is being checked, or marks after it and
// is the next pass's candidate — no append is missed either way.
func (s *Store) takeRecollapseCandidates() []windowKey {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.recollapsePending) == 0 {
		return nil
	}
	ks := sortedWindowKeys(s.recollapsePending)
	s.recollapsePending = map[windowKey]struct{}{}
	return ks
}

// markWindowSealed flips the served window's in-memory sealed flag.
func (s *Store) markWindowSealed(tier string, windowStart int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.windows {
		if w := &s.windows[i]; w.Tier == tier && w.WindowStart == windowStart {
			w.Sealed = true
		}
	}
}

// readSealed reports whether a store file carries its sealed marker.
func readSealed(db *sql.DB) (bool, error) {
	var sealed int
	if err := db.QueryRow("SELECT count(*) FROM " + SealedTable).Scan(&sealed); err != nil {
		return false, err
	}
	return sealed > 0, nil
}

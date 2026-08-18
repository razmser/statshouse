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

// DefaultSealerInterval is how often the sealer wakes to look for windows past
// their seal time. Sealing is rare — one rewrite per window lifetime, 48
// hours after the window closed — so the cadence is relaxed.
const DefaultSealerInterval = 30 * time.Second

// SealerConfig configures a Sealer.
type SealerConfig struct {
	// Interval is how often a sealing pass looks for due windows; the default
	// is DefaultSealerInterval.
	Interval time.Duration

	// NowFunc supplies the clock the seal boundary is judged by. Defaults to
	// time.Now.
	NowFunc func() time.Time

	// Metrics receives each pass's timing (MaintenanceSealing) and one
	// MaintenanceWindow per window the pass sealed. Optional.
	Metrics MetricsRecorder

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
	if len(due) == 0 {
		return nil
	}
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
	if err := s.rewriteWindowRuns(ctx, conn, tier, table, windowStart); err != nil {
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

// rewriteWindowRuns is the seal's transaction: collapse the window's rows —
// the same one-statement collapse compaction runs over the delta, reading the
// window's own table — into a fresh table, swap it in under the original name,
// and plant the sealed marker, all committing or rolling back together. The
// transaction is the writer's protocol — explicit BEGIN TRANSACTION / COMMIT
// through the connection — and the collapse's fold UDFs are registered on the
// connection before it opens, since DuckDB UDFs live per-connection.
func (s *Store) rewriteWindowRuns(ctx context.Context, conn *sql.Conn, tier, table string, windowStart int64) error {
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
	sealedTable := table + "_sealed"
	if _, err := conn.ExecContext(ctx, tierTableDDL(sealedTable)); err != nil {
		return fail(fmt.Errorf("create %s: %w", sealedTable, err))
	}
	// The window's rows are already tier-truncated, so the collapse reads
	// the plain time column — the statement stays the one the tier's own
	// table always produced.
	if _, err := conn.ExecContext(ctx,
		collapseInsert(sealedTable, "main", table, "time"), windowStart, windowStart+tierWindowSecs[tier]); err != nil {
		return fail(fmt.Errorf("collapse %s into %s: %w", table, sealedTable, err))
	}
	if _, err := conn.ExecContext(ctx, "DROP TABLE "+table); err != nil {
		return fail(fmt.Errorf("drop %s: %w", table, err))
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s RENAME TO %s", sealedTable, table)); err != nil {
		return fail(fmt.Errorf("rename %s to %s: %w", sealedTable, table, err))
	}
	// The marker rides in the rewrite's transaction: same file, so neither
	// can exist without the other.
	if _, err := conn.ExecContext(ctx, "INSERT INTO "+SealedTable+" VALUES (true)"); err != nil {
		return fail(fmt.Errorf("plant the marker in %s: %w", SealedTable, err))
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return fail(err)
	}
	return nil
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

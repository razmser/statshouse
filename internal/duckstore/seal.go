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
// safe to freeze. From the seal on, the window's contents never change again:
// the sealed marker in the window file's own metadata refuses every later
// append, and the store opens the file read-only from then on.
//
// The rewrite and the marker commit as one transaction in the window file, so
// a crash mid-seal rolls back to the unsealed several-run state and the next
// pass simply retries; a crash after the commit is indistinguishable from a
// completed seal, because the marker rides along with the rewrite.

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

	// Logf receives pass failures. Defaults to log.Printf.
	Logf func(format string, args ...any)
}

// Sealer drives a store's sealing passes. It is one goroutine working one
// window at a time at a relaxed cadence, and it never holds anything the
// write path needs — the archive maintenance lock it shares with compaction's
// appends is never taken by ingestion — so sealing runs at lowest priority
// and can never delay an insert round.
type Sealer struct {
	store *Store
	cfg   SealerConfig

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
	return &Sealer{store: s, cfg: cfg}
}

// Run seals until ctx is done: one pass immediately (a restarted process
// first finishes whatever windows came due while it was down), then one pass
// per Interval. A failed pass is logged and retried by the next; the seal
// transaction makes that safe. It returns nil when ctx is done.
func (sl *Sealer) Run(ctx context.Context) error {
	if err := sl.SealOnce(ctx); err != nil && ctx.Err() == nil {
		sl.cfg.Logf("[error] duck-store: sealing pass: %v", err)
	}
	t := time.NewTicker(sl.cfg.Interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := sl.SealOnce(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				sl.cfg.Logf("[error] duck-store: sealing pass: %v", err)
			}
		}
	}
}

// SealOnce seals every served window that has crossed its seal time. Windows
// that no longer exist on disk at their seal time — retention unlinked them
// in the meantime — are skipped: sealing rewrites, it never resurrects.
func (sl *Sealer) SealOnce(ctx context.Context) error {
	start := time.Now()
	err := sl.sealOnce(ctx)
	recordMaintenancePass(sl.cfg.Metrics, MaintenanceSealing, start, err)
	return err
}

func (sl *Sealer) sealOnce(ctx context.Context) error {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	now := sl.cfg.NowFunc().Unix()
	for _, wf := range sl.store.Windows() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if wf.Sealed || !windowSealDue(wf.Tier, wf.WindowStart, now) {
			continue
		}
		if err := sl.store.SealWindow(ctx, wf.Tier, wf.WindowStart); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue // the window left before its seal; nothing to rewrite
			}
			return fmt.Errorf("duck-store: seal %s: %w", wf.Path, err)
		}
		recordMaintenanceWindow(sl.cfg.Metrics, WindowSealed, wf.Tier)
	}
	return nil
}

// windowSealDue reports whether a window has crossed its seal time: window
// end plus the historic window, past which no conforming sender can produce a
// row for it anymore.
func windowSealDue(tier string, windowStart, nowUnix int64) bool {
	return nowUnix >= windowStart+tierWindowSecs[tier]+data_model.MaxHistoricWindow
}

// SealWindow rewrites one archive window's runs into a single collapsed run
// and freezes the file: the rewrite, the sealed marker and nothing else land
// in one transaction, and the file is reopened read-only — the access mode
// every open from the seal on uses. Sealing an already-sealed window is a
// no-op, so a retried pass after a crash between the commit and the in-memory
// bookkeeping completes quietly.
func (s *Store) SealWindow(ctx context.Context, tier string, windowStart int64) error {
	path := filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(tier, windowStart))
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("duck-store: window %s: %w", path, fs.ErrNotExist)
		}
		return fmt.Errorf("duck-store: seal %s: %w", path, err)
	}
	table := tierTables[tier]

	// Serialize against compaction's appends to the same file: a rewrite and
	// an append must never interleave. Ingestion never takes this lock, so a
	// seal can never delay a write.
	s.archiveMu.Lock()
	defer s.archiveMu.Unlock()

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
// reading the window's own table, the same statement compaction runs over the
// delta — into a fresh table, swap it in under the original name, and plant
// the sealed marker, all committing or rolling back together.
func (s *Store) rewriteWindowRuns(ctx context.Context, conn *sql.Conn, tier, table string, windowStart int64) error {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		_ = tx.Rollback()
		return err
	}
	groups, err := queryCollapsedGroups(tx, "main", table, windowStart, windowStart+tierWindowSecs[tier])
	if err != nil {
		return fail(fmt.Errorf("collapse %s: %w", table, err))
	}
	sealedTable := table + "_sealed"
	if _, err := tx.Exec(tierTableDDL(sealedTable)); err != nil {
		return fail(fmt.Errorf("create %s: %w", sealedTable, err))
	}
	if err := insertCollapsedGroups(tx, sealedTable, groups); err != nil {
		return fail(fmt.Errorf("fill %s: %w", sealedTable, err))
	}
	if _, err := tx.Exec("DROP TABLE " + table); err != nil {
		return fail(fmt.Errorf("drop %s: %w", table, err))
	}
	if _, err := tx.Exec(fmt.Sprintf("ALTER TABLE %s RENAME TO %s", sealedTable, table)); err != nil {
		return fail(fmt.Errorf("rename %s to %s: %w", sealedTable, table, err))
	}
	// The marker rides in the rewrite's transaction: same file, so neither
	// can exist without the other.
	if _, err := tx.Exec("INSERT INTO " + SealedTable + " VALUES (true)"); err != nil {
		return fail(fmt.Errorf("plant the marker in %s: %w", SealedTable, err))
	}
	return tx.Commit()
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

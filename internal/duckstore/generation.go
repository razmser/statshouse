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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// deltaSrcAlias is the name consume transactions attach the delta generation
// file under, so the append reads its rows straight out of DuckDB instead of
// pulling them through Go.
const deltaSrcAlias = "delta_src"

// windowTmpSuffix marks an archive window file mid-creation. A window is
// created under the temporary name and renamed into place, because the
// consume path creates one only when the final path is absent: a crash
// between creating the file and committing its tables would otherwise leave
// a tableless leftover at the final path that no later pass repairs — the
// conditional create skips it and every read of it fails — wedging
// consumption of that window forever.
const windowTmpSuffix = ".tmp"

// createArchiveWindow creates an empty archive window file at path
// atomically: the database is built under a temporary name — removing any
// stale temporary a crashed earlier attempt left — and renamed into place,
// so the final path only ever exists complete. Delta generations do not need
// this: their creation is unconditional and idempotent, so a leftover
// mid-creation file is simply completed by the next attempt.
func createArchiveWindow(path, tier string, st stamp, res ResourcesConfig) error {
	tmp := path + windowTmpSuffix
	_ = os.Remove(tmp)
	_ = os.Remove(tmp + ".wal")
	if err := createFile(tmp, []string{tierTables[tier]}, st, res); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	// createFile checkpoints and closes the database before returning, so the
	// renamed file holds its schema on its own; a write-ahead log next to the
	// temporary name can only be a stale leftover.
	_ = os.Remove(tmp + ".wal")
	return nil
}

// tierWindowSecs is each tier's archive window length: the file boundary
// consumption routes rows by, retention unlinks whole files at, and sealing
// rewrites one of. The 1s tier's hour is the tickets' provisional starting
// point; the numbers are revisited when compaction is wired up.
var tierWindowSecs = map[string]int64{
	Tier1s: 3600,
	Tier1m: 86400,
	Tier1h: 30 * 86400,
}

// windowKey identifies one archive window file.
type windowKey struct {
	tier  string
	start int64 // unix seconds
}

// CrashPoint names one commit point of the consume protocol. ConsumeOptions'
// Fault sees them in this order: for every archive window in the plan,
// CrashBeforeAppend and then CrashAfterAppendBeforeCommit; once every window
// has committed, CrashAfterCommitBeforeUnlink. They exist so tests can kill
// the protocol exactly where a process crash would land.
type CrashPoint int

const (
	// CrashBeforeAppend is reached before a window's transaction opens, with
	// nothing of that window on disk yet.
	CrashBeforeAppend CrashPoint = iota
	// CrashAfterAppendBeforeCommit is reached after the window's rows are
	// appended but before the transaction — rows plus consumption record —
	// commits.
	CrashAfterAppendBeforeCommit
	// CrashAfterCommitBeforeUnlink is reached after every window has
	// committed, before the delta generation file is unlinked.
	CrashAfterCommitBeforeUnlink
)

func (p CrashPoint) String() string {
	switch p {
	case CrashBeforeAppend:
		return "before append"
	case CrashAfterAppendBeforeCommit:
		return "after append, before commit"
	case CrashAfterCommitBeforeUnlink:
		return "after commit, before unlink"
	}
	return fmt.Sprintf("crash point %d", int(p))
}

// ConsumeOptions tunes ConsumeGeneration.
type ConsumeOptions struct {
	// AppendWindow appends one window's share of the generation's rows to the
	// tier table inside one open transaction on conn — driven by BEGIN
	// TRANSACTION / COMMIT through conn.ExecContext, the writer's protocol —
	// so the append statement's rows land in the same transaction as the
	// consumption record and commit with it or roll back with it. conn is
	// already bound to the window file's own database with the delta
	// generation attached read-only as deltaSrcAlias, and carries the fold
	// UDFs registered for the collapse. The default copies the window's rows
	// verbatim; compaction passes its collapse statement instead.
	AppendWindow func(ctx context.Context, conn *sql.Conn, tier string, windowStart, windowEnd int64) error

	// Fault, when set, is consulted at each CrashPoint; a non-nil error
	// aborts the consume exactly there, standing in for a process crash:
	// the transaction in flight rolls back, committed work stays, and
	// nothing is cleaned up that a crash would not. Production leaves it
	// nil.
	Fault func(CrashPoint) error
}

// fault runs the crash-injection hook for one point.
func (o ConsumeOptions) fault(p CrashPoint) error {
	if o.Fault == nil {
		return nil
	}
	if err := o.Fault(p); err != nil {
		return fmt.Errorf("duck-store: consume crashed %s: %w", p, err)
	}
	return nil
}

// RollGeneration finishes writes to the active delta generation and starts a
// fresh one, leaving the old file on disk untouched — a generation is never
// truncated in place. From the roll on, the writer appends only to the new
// generation, so a generation handed to ConsumeGeneration is never written to
// again and no reader ever has a file changed underneath it. Rolls also
// advance the generation number past anything already quarantined, so a
// quarantined file is never shadowed.
func (s *Store) RollGeneration() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.gen + 1
	path := filepath.Join(s.cfg.Dir, deltaFileName(next))
	// A file left by a failed earlier roll is complete (createFile is
	// all-or-nothing) and stamped by this binary, so it is reused as-is.
	if err := createFile(path, allTierTables(), s.currentStamp(), s.cfg.Resources); err != nil {
		return fmt.Errorf("duck-store: roll: %w", err)
	}
	db, err := openStoreFile(path, false, s.cfg.Resources)
	if err != nil {
		return fmt.Errorf("duck-store: roll: %w", err)
	}
	if w := s.writer; w != nil {
		if err := w.SwitchDelta(db); err != nil {
			_ = db.Close()
			return fmt.Errorf("duck-store: roll onto generation %d: %w", next, err)
		}
	}
	// The switch is the moment the rolled-off generation stops accepting
	// writes; the backlog's age is counted from here.
	s.rolledOff[s.gen] = time.Now()
	old := s.delta
	s.delta = db
	s.gen = next
	s.deltas = append(s.deltas, next)
	if old != nil {
		_ = old.Close() // the writer's connection went back with the switch
	}
	return nil
}

// ConsumeGeneration moves a rolled-off delta generation's rows into the
// archive windows they belong to and then unlinks the generation's file —
// consumption is the only way a delta file ever disappears. It is the
// consumer's job to call it for one generation at a time.
//
// Each window's append and its "consumed generation N" record commit as one
// transaction on the window file's own database, so a crash mid-consume can
// only repeat work, never double-count it: windows that already recorded the
// generation are skipped, the rest are appended, and the unlink happens once
// every window in the generation's plan is recorded. OpenStore performs the
// same check when the store reopens, so a generation left behind by a crash
// between the last commit and the unlink is unlinked without appending
// anything again.
//
// Until its consumption completes, a sealed generation is input to this
// protocol rather than a query source: its rows re-enter queries from the
// archive windows as they commit, which is what keeps a query that unions the
// active delta with the windows from counting a row in both places.
func (s *Store) ConsumeGeneration(ctx context.Context, gen int64, opts ConsumeOptions) error {
	s.mu.RLock()
	active := s.gen
	s.mu.RUnlock()
	if gen >= active {
		return fmt.Errorf("duck-store: generation %d is the active delta or newer; roll before consuming", gen)
	}
	deltaPath := filepath.Join(s.cfg.Dir, deltaFileName(gen))
	windows, err := generationWindows(deltaPath, s.cfg.Resources)
	if err != nil {
		return fmt.Errorf("duck-store: consume generation %d: %w", gen, err)
	}
	for _, k := range sortedWindowKeys(windows) {
		if err := s.consumeWindow(ctx, gen, deltaPath, k, opts); err != nil {
			return err
		}
	}
	if err := opts.fault(CrashAfterCommitBeforeUnlink); err != nil {
		return err
	}
	s.mu.Lock()
	s.deltas = removeGeneration(s.deltas, gen)
	delete(s.rolledOff, gen) // the backlog no longer knows this generation
	s.mu.Unlock()
	if !s.unlinkDelta(deltaPath, gen) {
		return fmt.Errorf("duck-store: generation %d is consumed but %s could not be unlinked", gen, deltaPath)
	}
	return nil
}

// consumeWindow lands one archive window's share of the generation: the
// append and the consumption record in a single transaction on the window
// file, skipped entirely when an earlier attempt already committed it. The
// whole window maintenance holds archiveMu, so the append can never interleave
// with the sealer's rewrite of the same file, and a sealed window — whose
// contents must never change again — records the generation without
// appending, so one unplaceable row cannot wedge consumption.
func (s *Store) consumeWindow(ctx context.Context, gen int64, deltaPath string, k windowKey, opts ConsumeOptions) error {
	s.archiveMu.Lock()
	defer s.archiveMu.Unlock()
	if err := opts.fault(CrashBeforeAppend); err != nil {
		return err
	}
	path := filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(k.tier, k.start))
	if _, err := os.Stat(path); os.IsNotExist(err) {
		if err := createArchiveWindow(path, k.tier, s.currentStamp(), s.cfg.Resources); err != nil {
			return fmt.Errorf("duck-store: consume generation %d: %w", gen, err)
		}
	}
	db, err := openStoreFile(path, false, s.cfg.Resources)
	if err != nil {
		return fmt.Errorf("duck-store: consume generation %d into %s: %w", gen, path, err)
	}
	defer db.Close()

	// A window that already recorded this generation holds its rows; resuming
	// must not append them a second time.
	recorded, err := readConsumed(db)
	if err != nil {
		return fmt.Errorf("duck-store: read %s of %s: %w", ConsumedTable, path, err)
	}
	if _, done := recorded[gen]; done {
		return nil
	}

	// A sealed window is immutable: its rows were rewritten into one run past the
	// historic window, so only a sender violating that window could land here
	// — the writer's ingest guard drops such rows, and this is the backstop.
	// The rows can never be appended, and failing here would wedge this
	// generation and every later one on rows that are unplaceable by
	// construction, so they are dropped loudly instead: an error log, a
	// metric, and the consumption record in its own transaction, so the
	// generation still completes and its other windows still land.
	sealed, err := readSealed(db)
	if err != nil {
		return fmt.Errorf("duck-store: read %s of %s: %w", SealedTable, path, err)
	}
	if sealed {
		s.cfg.Logf("[error] duck-store: %s is sealed: dropping generation %d rows for a window past the historic window", path, gen)
		recordMaintenanceWindow(s.cfg.Metrics, WindowLateDropped, k.tier)
		conn, err := db.Conn(ctx)
		if err != nil {
			return fmt.Errorf("duck-store: connection to %s: %w", path, err)
		}
		defer conn.Close()
		if _, err := conn.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
			return fmt.Errorf("duck-store: record generation %d in %s: %w", gen, path, err)
		}
		if _, err := conn.ExecContext(ctx, "INSERT INTO "+ConsumedTable+" VALUES ($1)", gen); err != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			return fmt.Errorf("duck-store: record generation %d in %s: %w", gen, path, err)
		}
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
			return fmt.Errorf("duck-store: commit generation %d record in %s: %w", gen, path, err)
		}
		s.mu.Lock()
		if s.consumed[k] == nil {
			s.consumed[k] = map[int64]struct{}{}
		}
		s.consumed[k][gen] = struct{}{}
		s.mu.Unlock()
		return nil
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("duck-store: connection to %s: %w", path, err)
	}
	defer conn.Close()
	// The collapse statement folds both aggregate-state columns through scalar
	// UDFs, and DuckDB UDFs live on the connection: register them before this
	// connection's first statement, so whichever AppendWindow runs — the
	// collapsing one today — finds them in place.
	if err := registerFoldUDFs(conn); err != nil {
		return fmt.Errorf("duck-store: consume generation %d into %s: %w", gen, path, err)
	}
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS %s (READ_ONLY)", sqlString(deltaPath), deltaSrcAlias)); err != nil {
		return fmt.Errorf("duck-store: attach %s to consume generation %d: %w", deltaPath, gen, err)
	}
	defer func() { _, _ = conn.ExecContext(context.Background(), "DETACH "+deltaSrcAlias) }()

	// The writer's transaction protocol, not database/sql's: an explicit BEGIN
	// TRANSACTION / COMMIT through the connection, so the append's Appender —
	// which reaches the raw driver connection underneath conn.Raw — flushes
	// into the very transaction the consumption record and the commit land in
	// (the writer's tx-probe test pins that appender flushes join the
	// connection's open transaction). Rollbacks run on a background context by
	// necessity: the consume's own context may be exactly what died.
	if _, err := conn.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		return fmt.Errorf("duck-store: consume generation %d into %s: %w", gen, path, err)
	}
	rollback := func() {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
	}
	appendWindow := opts.AppendWindow
	if appendWindow == nil {
		appendWindow = copyWindowRows
	}
	if err := appendWindow(ctx, conn, k.tier, k.start, k.start+tierWindowSecs[k.tier]); err != nil {
		rollback()
		return fmt.Errorf("duck-store: append generation %d to %s: %w", gen, path, err)
	}
	if err := opts.fault(CrashAfterAppendBeforeCommit); err != nil {
		rollback()
		return err
	}
	// The record rides in the append's transaction: same file, so the two
	// commit together and neither can exist without the other.
	if _, err := conn.ExecContext(ctx, "INSERT INTO "+ConsumedTable+" VALUES ($1)", gen); err != nil {
		rollback()
		return fmt.Errorf("duck-store: record generation %d in %s: %w", gen, path, err)
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		rollback()
		return fmt.Errorf("duck-store: commit generation %d into %s: %w", gen, path, err)
	}

	s.mu.Lock()
	s.recordWindowLocked(k, path)
	if s.consumed[k] == nil {
		s.consumed[k] = map[int64]struct{}{}
	}
	s.consumed[k][gen] = struct{}{}
	s.mu.Unlock()
	return nil
}

// copyWindowRows is the default append: the window's rows verbatim, the way
// they sit in the delta. Compaction passes its collapsing AppendWindow
// instead; correctness never depends on the collapse, because read-time
// GROUP BY folds whatever rows it finds.
func copyWindowRows(ctx context.Context, conn *sql.Conn, tier string, windowStart, windowEnd int64) error {
	table := tierTables[tier]
	_, err := conn.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %s SELECT * FROM %s.%s WHERE time >= $1 AND time < $2", table, deltaSrcAlias, table),
		windowStart, windowEnd)
	return err
}

// generationWindows derives, from the generation's own rows, the archive
// windows it must be consumed into: per tier, every distinct (already
// tier-truncated) time mapped through the tier's window length. The plan is a
// function of the generation's immutable contents, so a crashed consumption
// and its resume always agree on it. A generation with no rows plans no
// windows and is consumed by the unlink alone.
func generationWindows(deltaPath string, res ResourcesConfig) (map[windowKey]struct{}, error) {
	db, err := openStoreFile(deltaPath, true, res)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return deltaWindows(db)
}

// deltaWindows is generationWindows over an open handle.
func deltaWindows(db *sql.DB) (map[windowKey]struct{}, error) {
	windows := map[windowKey]struct{}{}
	for _, tier := range tiers {
		rows, err := db.Query("SELECT DISTINCT time FROM " + tierTables[tier])
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var t int64
			if err := rows.Scan(&t); err != nil {
				rows.Close()
				return nil, err
			}
			windows[windowKey{tier: tier, start: t - t%tierWindowSecs[tier]}] = struct{}{}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return windows, nil
}

// generationRecorded reports whether every archive window in the generation's
// plan already records it as consumed — the state a crash between the last
// window's commit and the unlink leaves behind. OpenStore uses it to finish
// the unlink. A missing window file counts as unrecorded, so consumption
// resumes onto it rather than dropping its rows.
func (s *Store) generationRecorded(db *sql.DB, gen int64) (bool, error) {
	windows, err := deltaWindows(db)
	if err != nil {
		return false, err
	}
	for k := range windows {
		if _, ok := s.consumed[k][gen]; !ok {
			return false, nil
		}
	}
	return true, nil
}

// unlinkDeltaIfConsumed finishes the unlink of a generation that OpenStore
// found fully recorded, returning whether it did. A generation that cannot be
// planned stays: consumption resumes on it rather than risk dropping rows.
func (s *Store) unlinkDeltaIfConsumed(gen int64) bool {
	path := filepath.Join(s.cfg.Dir, deltaFileName(gen))
	db, err := openStoreFile(path, true, s.cfg.Resources)
	if err != nil {
		s.cfg.Logf("[error] duck-store: cannot reopen %s to check its consumption: %v", path, err)
		return false
	}
	recorded, err := s.generationRecorded(db, gen)
	db.Close()
	if err != nil {
		s.cfg.Logf("[error] duck-store: cannot plan the consumption of %s: %v", path, err)
		return false
	}
	if !recorded {
		return false
	}
	// The rows already live in the archive windows, so the generation is
	// stopped from serving whether or not the unlink itself succeeds.
	s.unlinkDelta(path, gen)
	return true
}

// unlinkDelta removes a delta generation file once its rows are durably
// recorded in archive windows; the write-ahead log goes with it.
func (s *Store) unlinkDelta(path string, gen int64) bool {
	if err := os.Remove(path); err != nil {
		s.cfg.Logf("[error] duck-store: cannot unlink consumed generation %d file %s: %v", gen, path, err)
		return false
	}
	_ = os.Remove(path + ".wal")
	s.cfg.Logf("[info] duck-store: unlinked %s: generation %d is recorded as consumed in its archive windows", path, gen)
	return true
}

// readConsumed returns the delta generations a window file records as
// already held.
func readConsumed(db *sql.DB) (map[int64]struct{}, error) {
	rows, err := db.Query("SELECT generation FROM " + ConsumedTable)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	gens := map[int64]struct{}{}
	for rows.Next() {
		var gen int64
		if err := rows.Scan(&gen); err != nil {
			return nil, err
		}
		gens[gen] = struct{}{}
	}
	return gens, rows.Err()
}

// recordWindowLocked adds a window file to the served set, keeping the
// tier/window-start order. Callers hold s.mu.
func (s *Store) recordWindowLocked(k windowKey, path string) {
	for _, w := range s.windows {
		if w.Tier == k.tier && w.WindowStart == k.start {
			return
		}
	}
	s.windows = append(s.windows, WindowFile{Tier: k.tier, WindowStart: k.start, Path: path})
	sort.Slice(s.windows, func(i, j int) bool { return lessWindow(s.windows[i], s.windows[j]) })
}

func lessWindow(a, b WindowFile) bool {
	if a.Tier != b.Tier {
		return tierOrder(a.Tier) < tierOrder(b.Tier)
	}
	return a.WindowStart < b.WindowStart
}

// sortedWindowKeys returns the plan's windows in consume order: tier, then
// window start.
func sortedWindowKeys(windows map[windowKey]struct{}) []windowKey {
	keys := make([]windowKey, 0, len(windows))
	for k := range windows {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].tier != keys[j].tier {
			return tierOrder(keys[i].tier) < tierOrder(keys[j].tier)
		}
		return keys[i].start < keys[j].start
	})
	return keys
}

// removeGeneration drops one generation from the ascending list.
func removeGeneration(gens []int64, gen int64) []int64 {
	for i, g := range gens {
		if g == gen {
			return append(gens[:i], gens[i+1:]...)
		}
	}
	return gens
}

// sqlString quotes a path for inclusion in a statement.
func sqlString(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

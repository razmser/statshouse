// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
)

// sealTestStore builds a store whose archive windows hold two compaction runs
// per key — the several-run state sealing exists to rewrite — with the
// sealer's frozen clock parked far enough past every window's seal time that
// all of them are due.
func sealTestStore(t *testing.T) (*Store, *Sealer) {
	t.Helper()
	s, w := newTestWriter(t)
	c := NewCompactor(s, CompactorConfig{})
	writeCollapseFixture(t, s, w) // run one
	require.NoError(t, c.CompactOnce(context.Background()))
	writeCollapseFixture(t, s, w) // run two, same windows
	require.NoError(t, c.CompactOnce(context.Background()))
	require.NoError(t, w.Close())

	var far int64
	for _, wf := range s.Windows() {
		if due := wf.WindowStart + tierWindowSecs[wf.Tier] + data_model.MaxHistoricWindow; due > far {
			far = due
		}
	}
	sealNow := time.Unix(far+1, 0)
	return s, NewSealer(s, SealerConfig{NowFunc: func() time.Time { return sealNow }})
}

// findWindow returns the served window entry for one tier and window start.
func findWindow(t *testing.T, s *Store, tier string, windowStart int64) WindowFile {
	t.Helper()
	for _, wf := range s.Windows() {
		if wf.Tier == tier && wf.WindowStart == windowStart {
			return wf
		}
	}
	t.Fatalf("no served %s window at %d", tier, windowStart)
	return WindowFile{}
}

// TestSealRewritesRunsIntoOnePreservingDecodedContents drives the whole seal:
// a window holding two collapsed runs comes out holding one row per key —
// one sorted run — decoding to exactly its pre-seal contents, and the store
// keeps reporting it sealed across a restart.
func TestSealRewritesRunsIntoOnePreservingDecodedContents(t *testing.T) {
	s, sealer := sealTestStore(t)
	now := uint32(writerNow.Unix())

	// the pre-seal decoded contents, read per tier before anything rewrites
	want := map[string]map[decodedKey]*decodedGroup{}
	for _, tier := range tiers {
		wf := findWindow(t, s, tier, testWindowStart(tier, int64(now)-5))
		db, err := openStoreFile(wf.Path, true, ResourcesConfig{})
		require.NoError(t, err)
		rows := scanTableRows(t, db, tierTables[tier])
		require.NoError(t, db.Close())
		decoded := decodeRows(t, rows)
		for _, g := range decoded {
			require.Equal(t, 2, g.rows, "%s: the fixture must land two runs per key before the seal", tier)
		}
		want[tier] = decoded
	}

	require.NoError(t, sealer.SealOnce(context.Background()))

	for _, wf := range s.Windows() {
		require.True(t, wf.Sealed, "%s window %d must be sealed", wf.Tier, wf.WindowStart)
		db, err := openStoreFile(wf.Path, true, ResourcesConfig{})
		require.NoError(t, err)
		rows := scanTableRows(t, db, tierTables[wf.Tier])
		// one row per key: the runs were rewritten into one...
		require.Len(t, rows, len(want[wf.Tier]), "%s: one row per key after the seal", wf.Tier)
		requireSameDecoded(t, want[wf.Tier], decodeRows(t, rows))
		// ...and that one run is physically sorted
		var lastM int32 = -1 << 30
		var lastT int64 = -1 << 62
		for _, r := range rows {
			require.LessOrEqual(t, lastM, r.metric)
			if r.metric == lastM {
				require.LessOrEqual(t, lastT, r.time)
			}
			lastM, lastT = r.metric, r.time
		}
		require.NoError(t, db.Close())
	}

	// a retried pass after a crash between the commit and the in-memory
	// bookkeeping completes quietly: sealing an already-sealed window is the
	// documented no-op, and the rewrite must not run a second time
	require.NoError(t, sealer.SealOnce(context.Background()))
	for _, wf := range s.Windows() {
		require.True(t, wf.Sealed, "%s window %d must stay sealed on the retried pass", wf.Tier, wf.WindowStart)
		db, err := openStoreFile(wf.Path, true, ResourcesConfig{})
		require.NoError(t, err)
		rows := scanTableRows(t, db, tierTables[wf.Tier])
		require.Len(t, rows, len(want[wf.Tier]), "%s: the retried pass must not rewrite again", wf.Tier)
		require.NoError(t, db.Close())
	}

	// the marker is the file's own: a restart serves the window sealed
	dir := s.cfg.Dir
	require.NoError(t, s.Close())
	s2, _ := openTestStore(t, dir)
	for _, wf := range s2.Windows() {
		require.True(t, wf.Sealed, "%s window %d must stay sealed after a restart", wf.Tier, wf.WindowStart)
	}
}

// TestSealedFileRejectsWrites proves the seal at both levels: the read-only
// handle the store reopens through refuses an INSERT, and the consume
// protocol drops a late round for a sealed window loudly instead of
// corrupting the window — or wedging consumption on rows it can never place.
func TestSealedFileRejectsWrites(t *testing.T) {
	s, sealer := sealTestStore(t)
	now := uint32(writerNow.Unix())
	require.NoError(t, sealer.SealOnce(context.Background()))

	// the read-only reopen rejects writes itself
	wf := findWindow(t, s, Tier1s, testWindowStart(Tier1s, int64(now)-5))
	db, err := openStoreFile(wf.Path, true, ResourcesConfig{})
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (metric, time) VALUES ($1, $2)", TierTable(Tier1s)), int32(1), int64(1))
	require.Error(t, err, "a read-only open must reject writes")
	require.NoError(t, db.Close())

	// a late row for the sealed window — possible only from a sender racing
	// the historic window — is dropped by the consume protocol, loudly, and
	// the generation still completes
	w, err := NewWriter(s, WriterConfig{NowFunc: func() time.Time { return writerNow }})
	require.NoError(t, err)
	late := partialRow(t, testMetricID, now-5)
	late.Count, late.Sum = 7, 70
	require.NoError(t, w.WriteRound(context.Background(), []Row{late}))
	require.NoError(t, w.Close())

	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(context.Background()))
	require.Equal(t, []int64{s.ActiveDeltaGeneration()}, s.DeltaGenerations(),
		"the generation holding the unplaceable row must still be consumed")

	// the dropped append left the window exactly as the seal wrote it: the
	// fixture's two keys, one row each
	db, err = openStoreFile(wf.Path, true, ResourcesConfig{})
	require.NoError(t, err)
	defer db.Close()
	var rows int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM "+TierTable(Tier1s)).Scan(&rows))
	require.Equal(t, 2, rows, "the sealed window must not gain a run")
}

// TestSealOnceHonoursHistoricWindowBoundary pins the boundary itself: nothing
// seals before window end plus the historic window, the first tier to cross
// its boundary seals and later ones do not, and a window that no longer exists
// at its seal time is skipped rather than resurrected or failed.
func TestSealOnceHonoursHistoricWindowBoundary(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())
	writeCollapseFixture(t, s, w)
	require.NoError(t, w.Close())
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(context.Background()))

	ts := int64(now) - 5
	due1s := testWindowStart(Tier1s, ts) + tierWindowSecs[Tier1s] + data_model.MaxHistoricWindow
	require.NoError(t, NewSealer(s, SealerConfig{NowFunc: func() time.Time { return time.Unix(due1s-1, 0) }}).SealOnce(context.Background()))
	for _, wf := range s.Windows() {
		require.False(t, wf.Sealed, "nothing may seal before window end plus the historic window")
	}

	require.NoError(t, NewSealer(s, SealerConfig{NowFunc: func() time.Time { return time.Unix(due1s, 0) }}).SealOnce(context.Background()))
	require.True(t, findWindow(t, s, Tier1s, testWindowStart(Tier1s, ts)).Sealed, "the 1s window crosses its boundary first")
	for _, tier := range []string{Tier1m, Tier1h} {
		require.False(t, findWindow(t, s, tier, testWindowStart(tier, ts)).Sealed, "%s window ends later and must stay unsealed", tier)
	}

	// a window that left before its seal time (retention unlinked it) is
	// skipped: sealing rewrites, it never resurrects
	gone := findWindow(t, s, Tier1m, testWindowStart(Tier1m, ts))
	require.NoError(t, os.Remove(gone.Path))
	sealer := NewSealer(s, SealerConfig{NowFunc: func() time.Time {
		return time.Unix(int64(now)+40*86400, 0) // past every boundary
	}})
	require.NoError(t, sealer.SealOnce(context.Background()))
	require.True(t, findWindow(t, s, Tier1h, testWindowStart(Tier1h, ts)).Sealed, "the surviving window still seals")
	require.NoFileExists(t, gone.Path)
}

// TestSealerRunsAlongsideIngestion drives the sealer against a compactor and a
// writer hammering rounds, with one window already past its seal time: the
// rounds keep flowing (sealing never runs ahead of ingestion), every
// acknowledged round lands exactly once, and the due window gets sealed.
func TestSealerRunsAlongsideIngestion(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())

	// rows 47 hours old: inside the historic-window ingest guard, in windows
	// the sealer's clock finds due — written and consumed before the loops
	// start
	const oldAge = 47 * 3600
	old := partialRow(t, testMetricID, now-oldAge)
	old.Count, old.Sum = 2, 20
	require.NoError(t, w.WriteRound(context.Background(), []Row{old}))
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(context.Background()))
	oldWindow := testWindowStart(Tier1s, int64(now)-oldAge)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// now+2h: past the old 1s window's boundary, nowhere near the fresh ones'
	sealClock := time.Unix(int64(now)+2*3600, 0)
	sealer := NewSealer(s, SealerConfig{Interval: 10 * time.Millisecond, NowFunc: func() time.Time { return sealClock }})
	compactor := NewCompactor(s, CompactorConfig{Interval: 10 * time.Millisecond})
	sealDone := make(chan struct{})
	compactDone := make(chan struct{})
	go func() { defer close(sealDone); _ = sealer.Run(ctx) }()
	go func() { defer close(compactDone); _ = compactor.Run(ctx) }()

	const rounds = 50
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			row := partialRow(t, testMetricID2, now-uint32(i%10))
			row.Count, row.Sum = 1, 2
			if err := w.WriteRound(context.Background(), []Row{row}); err != nil {
				t.Errorf("round %d failed: %v", i, err)
				return
			}
		}
	}()
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("ingestion stalled behind sealing")
	}

	cancel()
	<-sealDone
	<-compactDone
	require.NoError(t, w.Close())
	require.NoError(t, compactor.CompactOnce(context.Background())) // flush what is left
	require.Equal(t, []int64{s.ActiveDeltaGeneration()}, s.DeltaGenerations(),
		"every sealed generation must be consumed")

	// the due window got sealed while ingestion ran
	require.True(t, findWindow(t, s, Tier1s, oldWindow).Sealed, "the window past its seal time must be sealed")

	// every acknowledged round lands exactly once across the delta and the 1s
	// windows, and the sealed old window kept its value
	var count, c float64
	require.NoError(t, s.Delta().QueryRow(
		`SELECT coalesce(sum(count), 0) FROM s1 WHERE metric = $1`, testMetricID2).Scan(&c))
	count += c
	for _, wf := range s.Windows() {
		if wf.Tier != Tier1s {
			continue
		}
		db, err := openStoreFile(wf.Path, true, ResourcesConfig{})
		require.NoError(t, err)
		require.NoError(t, db.QueryRow(
			`SELECT coalesce(sum(count), 0) FROM s1 WHERE metric = $1`, testMetricID2).Scan(&c))
		_ = db.Close()
		count += c
	}
	require.EqualValues(t, rounds, count, "every acknowledged round must land exactly once")

	db, err := openStoreFile(filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(Tier1s, oldWindow)), true, ResourcesConfig{})
	require.NoError(t, err)
	require.NoError(t, db.QueryRow(
		`SELECT coalesce(sum(count), 0) FROM s1 WHERE metric = $1`, testMetricID).Scan(&c))
	require.NoError(t, db.Close())
	require.EqualValues(t, 2, c, "the sealed window's own value must survive the loops")
}

// TestSealRewriteRollsBackAppendedRowsWithMarker fails the seal's transaction
// AFTER the sealed table is filled through the appender — the moment a flush
// escaping the transaction would leak rows — and proves the rollback leaves
// the window exactly as it was: no sealed table, no marker, every original row
// intact. With the fault removed, the seal completes and lands the rewrite
// and the marker together.
func TestSealRewriteRollsBackAppendedRowsWithMarker(t *testing.T) {
	s, w := newTestWriter(t)
	writeCollapseFixture(t, s, w)
	require.NoError(t, w.Close())
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(context.Background()))

	start := testWindowStart(Tier1s, int64(uint32(writerNow.Unix()))-5)
	wf := findWindow(t, s, Tier1s, start)
	ro, err := openStoreFile(wf.Path, true, ResourcesConfig{})
	require.NoError(t, err)
	before := scanTableRows(t, ro, tierTables[Tier1s])
	require.NoError(t, ro.Close())

	// a marker table that cannot take the marker row makes the rewrite fail
	// after its fill: the sealed table's rows are appended by then, and the
	// one-value INSERT no longer matches the table's widened column count
	rw, err := openStoreFile(wf.Path, false, ResourcesConfig{})
	require.NoError(t, err)
	_, err = rw.Exec("ALTER TABLE " + SealedTable + " ADD COLUMN probe INTEGER")
	require.NoError(t, err)
	require.NoError(t, rw.Close())

	err = s.SealWindow(context.Background(), Tier1s, start)
	require.Error(t, err, "the seal must fail when the marker cannot land")
	require.Contains(t, err.Error(), "plant the marker")

	// the rollback discarded the whole rewrite: no sealed table, no marker,
	// and the window's rows exactly as they were
	db, err := openStoreFile(wf.Path, true, ResourcesConfig{})
	require.NoError(t, err)
	require.Equal(t, before, scanTableRows(t, db, tierTables[Tier1s]),
		"the rolled-back rewrite must leave the original run untouched")
	var sealedTables int
	require.NoError(t, db.QueryRow(
		"SELECT count(*) FROM duckdb_tables() WHERE table_name = $1", tierTables[Tier1s]+"_sealed").Scan(&sealedTables))
	require.Zero(t, sealedTables, "the appender-filled sealed table must not survive the rollback")
	sealed, err := readSealed(db)
	require.NoError(t, err)
	require.False(t, sealed)
	require.NoError(t, db.Close())
	require.False(t, findWindow(t, s, Tier1s, start).Sealed, "a failed seal must not mark the window sealed")

	// with the fault removed the seal completes: the rewrite and the marker
	// commit together
	rw2, err := openStoreFile(wf.Path, false, ResourcesConfig{})
	require.NoError(t, err)
	_, err = rw2.Exec("ALTER TABLE " + SealedTable + " DROP COLUMN probe")
	require.NoError(t, err)
	require.NoError(t, rw2.Close())

	require.NoError(t, s.SealWindow(context.Background(), Tier1s, start))
	require.True(t, findWindow(t, s, Tier1s, start).Sealed)
	db2, err := openStoreFile(wf.Path, true, ResourcesConfig{})
	require.NoError(t, err)
	defer db2.Close()
	after := scanTableRows(t, db2, tierTables[Tier1s])
	require.Len(t, after, len(decodeRows(t, before)), "one row per key: the runs were rewritten into one")
	requireSameDecoded(t, decodeRows(t, before), decodeRows(t, after))
	sealed, err = readSealed(db2)
	require.NoError(t, err)
	require.True(t, sealed)
}

// TestWindowSealDue pins the boundary arithmetic: window end plus the historic
// window, per tier.
func TestWindowSealDue(t *testing.T) {
	const start = int64(1740000000)
	for _, tier := range tiers {
		end := start + tierWindowSecs[tier]
		due := end + data_model.MaxHistoricWindow
		require.False(t, windowSealDue(tier, start, due-1), "%s: one second early must not seal", tier)
		require.True(t, windowSealDue(tier, start, due), "%s: window end plus the historic window seals", tier)
	}
}

// TestNewSealerDefaults checks the config defaults land.
func TestNewSealerDefaults(t *testing.T) {
	sl := NewSealer(&Store{}, SealerConfig{})
	require.Equal(t, DefaultSealerInterval, sl.cfg.Interval)
	require.Equal(t, DefaultRecollapseFactor, sl.cfg.RecollapseFactor)
	require.NotNil(t, sl.cfg.NowFunc)
	require.NotNil(t, sl.cfg.Logf)
}

// sealDueClock returns a clock parked far enough past a 1s window's seal time
// that the window is due, on the same shape the boundary tests use.
func sealDueClock(windowStart int64) time.Time {
	return time.Unix(windowStart+tierWindowSecs[Tier1s]+data_model.MaxHistoricWindow, 0)
}

// windowMetricCount sums one metric's count over one archive window file.
func windowMetricCount(t *testing.T, wf WindowFile, metric int32) float64 {
	t.Helper()
	db, err := openStoreFile(wf.Path, true, ResourcesConfig{})
	require.NoError(t, err)
	defer db.Close()
	var c float64
	require.NoError(t, db.QueryRow(
		"SELECT coalesce(sum(count), 0) FROM "+TierTable(wf.Tier)+" WHERE metric = $1", metric).Scan(&c))
	return c
}

// TestSealBarrierHoldsWindowWithPendingGeneration is the barrier's core
// contract: a served window that has come due, but whose latest rows still
// sit in an unconsumed generation, does not seal — a drain that cannot
// complete fails the pass and leaves the rows exactly where they were — and
// does seal, with the rows landed, once nothing holds the generation back.
func TestSealBarrierHoldsWindowWithPendingGeneration(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNowUnix)
	ctx := context.Background()

	// The due window enters the served set through an ordinary compaction
	// pass; its rows (count 3) are durably in the archive.
	const oldAge = 47 * 3600
	first := partialRow(t, testMetricID, now-oldAge)
	first.Count, first.Sum = 3, 30
	require.NoError(t, w.WriteRound(ctx, []Row{first}))
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(ctx))
	oldWindow := testWindowStart(Tier1s, int64(now)-oldAge)
	require.EqualValues(t, 3, windowMetricCount(t, findWindow(t, s, Tier1s, oldWindow), testMetricID))

	// More rows for the same window — still inside the guard under the
	// writer's frozen clock — land in a generation consumption has not taken:
	// the exact shape the barrier exists for.
	second := partialRow(t, testMetricID, now-oldAge)
	second.Count, second.Sum = 4, 40
	require.NoError(t, w.WriteRound(ctx, []Row{second}))
	require.NoError(t, s.RollGeneration())

	// The drain fails: the pass must refuse to seal, the pending generation
	// must survive with its rows, and the window must stay as it was.
	stalled := errors.New("drain stalled")
	blocked := NewSealer(s, SealerConfig{
		NowFunc:    func() time.Time { return sealDueClock(oldWindow) },
		DrainFault: func(CrashPoint) error { return stalled },
	})
	require.ErrorIs(t, blocked.SealOnce(ctx), stalled)
	require.False(t, findWindow(t, s, Tier1s, oldWindow).Sealed,
		"a window with a pending generation must not seal")
	require.Equal(t, []int64{1, 2, 3}, s.DeltaGenerations(),
		"the failed drain must leave the pending generation holding its rows")
	require.EqualValues(t, 3, windowMetricCount(t, findWindow(t, s, Tier1s, oldWindow), testMetricID),
		"the refused seal must not change the window")

	// With the drain healthy the same pass lands the rows and seals: the
	// barrier consumes the contributor itself instead of trusting a separate
	// compactor to have done it.
	require.NoError(t, NewSealer(s, SealerConfig{NowFunc: func() time.Time { return sealDueClock(oldWindow) }}).
		SealOnce(ctx))
	wf := findWindow(t, s, Tier1s, oldWindow)
	require.True(t, wf.Sealed, "the window must seal once its generation is consumed")
	require.Equal(t, []int64{2, 3, 4}, s.DeltaGenerations(),
		"the barrier must drain the contributing generation; non-contributors wait for the compactor")
	require.EqualValues(t, 7, windowMetricCount(t, wf, testMetricID),
		"the pending rows must land in the window, not drop")
}

// TestSealBarrierPlanFailureFailsThePass pins the barrier's other error
// exit: a rolled generation whose window plan cannot be read — an
// undecidable contribution — fails the whole pass and leaves the due window
// unsealed, rather than sealing on an assumption. Repairing the generation
// lets the next pass drain it and seal with the rows landed.
func TestSealBarrierPlanFailureFailsThePass(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNowUnix)
	ctx := context.Background()

	const oldAge = 47 * 3600
	first := partialRow(t, testMetricID, now-oldAge)
	first.Count, first.Sum = 3, 30
	require.NoError(t, w.WriteRound(ctx, []Row{first}))
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(ctx))
	oldWindow := testWindowStart(Tier1s, int64(now)-oldAge)

	second := partialRow(t, testMetricID, now-oldAge)
	second.Count, second.Sum = 4, 40
	require.NoError(t, w.WriteRound(ctx, []Row{second}))
	gen := s.ActiveDeltaGeneration() // the generation the boundary rows sit in
	require.NoError(t, s.RollGeneration())

	// the plan read fails: the generation's file is not openable, so whether
	// it contributes to the due window cannot be decided
	path := filepath.Join(s.cfg.Dir, deltaFileName(gen))
	aside := path + ".aside"
	require.NoError(t, os.Rename(path, aside))

	err := NewSealer(s, SealerConfig{NowFunc: func() time.Time { return sealDueClock(oldWindow) }}).
		SealOnce(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "plan generation", "the unreadable plan must fail the pass")
	require.False(t, findWindow(t, s, Tier1s, oldWindow).Sealed,
		"a window must not seal while a contributor's plan is undecidable")

	// repaired, the same pass drains the generation and seals holding its rows
	require.NoError(t, os.Rename(aside, path))
	require.NoError(t, NewSealer(s, SealerConfig{NowFunc: func() time.Time { return sealDueClock(oldWindow) }}).
		SealOnce(ctx))
	wf := findWindow(t, s, Tier1s, oldWindow)
	require.True(t, wf.Sealed)
	require.EqualValues(t, 7, windowMetricCount(t, wf, testMetricID),
		"the pending rows must land in the window once the plan reads again")
}

// TestSealBarrierLandsBoundaryRowInsteadOfLosingIt reproduces the loss the
// barrier closes. Pre-fix, a row accepted in the last conforming second sat
// in a pending generation when its window came due; the pass sealed on
// wall-clock age alone, and the later consume dropped the row through the
// sealed branch with only a WindowLateDropped to show. With the barrier the
// pass drains first, so the row lands and the window seals holding it — and
// the sealed branch keeps firing only for a sender whose clock genuinely
// violates the boundary.
func TestSealBarrierLandsBoundaryRowInsteadOfLosingIt(t *testing.T) {
	rec := &recordingMetrics{}
	var logs []string
	s, err := OpenStore(StoreConfig{
		Dir:               t.TempDir(),
		StatshouseVersion: testStatshouseVersion,
		Logf:              func(format string, args ...any) { logs = append(logs, fmt.Sprintf(format, args...)) },
		Metrics:           rec,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	w, err := NewWriter(s, WriterConfig{NowFunc: func() time.Time { return writerNow }})
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	now := uint32(writerNowUnix)
	ctx := context.Background()

	// The boundary row's window enters the served set first...
	const oldAge = 47 * 3600
	first := partialRow(t, testMetricID, now-oldAge)
	first.Count, first.Sum = 3, 30
	require.NoError(t, w.WriteRound(ctx, []Row{first}))
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(ctx))
	oldWindow := testWindowStart(Tier1s, int64(now)-oldAge)

	// ...then the boundary row itself: accepted by the guard under the
	// writer's clock, pending in a rolled generation when the window comes
	// due. Pre-fix this is the loss; with the barrier it is a drain.
	second := partialRow(t, testMetricID, now-oldAge)
	second.Count, second.Sum = 4, 40
	require.NoError(t, w.WriteRound(ctx, []Row{second}))
	require.NoError(t, s.RollGeneration())

	require.NoError(t, NewSealer(s, SealerConfig{
		NowFunc: func() time.Time { return sealDueClock(oldWindow) },
		Metrics: rec,
	}).SealOnce(ctx))

	wf := findWindow(t, s, Tier1s, oldWindow)
	require.True(t, wf.Sealed)
	require.EqualValues(t, 7, windowMetricCount(t, wf, testMetricID),
		"the row accepted at the boundary must land in the sealed window")
	require.Zero(t, countWindowEvents(rec, WindowLateDropped),
		"a conformingly accepted row must never take the late-drop path")

	// The backstop: the same row written after the seal — possible only for a
	// sender whose clock still sits before the boundary — is dropped loudly,
	// with the metric that now means exactly that.
	violating := partialRow(t, testMetricID, now-oldAge)
	violating.Count, violating.Sum = 5, 50
	require.NoError(t, w.WriteRound(ctx, []Row{violating}))
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(ctx))
	require.Equal(t, []int64{s.ActiveDeltaGeneration()}, s.DeltaGenerations(),
		"the generation holding the violating row must still complete")
	require.Equal(t, 1, countWindowEvents(rec, WindowLateDropped),
		"the violating sender alone must take the late-drop path")
	require.EqualValues(t, 7, windowMetricCount(t, wf, testMetricID),
		"the dropped append must not change the sealed window")
	droppedLog := fmt.Sprintf(
		"[error] duck-store: %s is sealed: dropping generation",
		filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(Tier1s, oldWindow)))
	var logged bool
	for _, l := range logs {
		logged = logged || strings.Contains(l, droppedLog)
	}
	require.True(t, logged, "the loud drop must reach the operator log: want %q in %v", droppedLog, logs)
}

// TestSealBarrierProgressesAgainstBlockedCompactor pins the barrier's
// liveness against a compaction pass that is itself stuck: work parked on one
// window's lock neither stops the sealer draining other generations and
// sealing unrelated windows, nor deadlocks the sealer when the park is on the
// very window due for sealing — the barrier waits the park out and still
// completes, tolerating the parked pass finishing the same generation under
// it.
func TestSealBarrierProgressesAgainstBlockedCompactor(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNowUnix)
	ctx := context.Background()

	// parkFault builds a consume fault that signals the moment it holds its
	// first window's write lock, then blocks until released — the exact state
	// of a compaction pass stuck mid-consume.
	parkFault := func(entered chan<- struct{}, release <-chan struct{}) func(CrashPoint) error {
		var once sync.Once
		return func(p CrashPoint) error {
			if p == CrashBeforeAppend {
				once.Do(func() { close(entered) })
				<-release
			}
			return nil
		}
	}
	consumeParked := func(gen int64, fault func(CrashPoint) error) chan error {
		done := make(chan error, 1)
		go func() {
			done <- s.ConsumeGeneration(ctx, gen, ConsumeOptions{AppendWindow: collapseWindowRows, Fault: fault})
		}()
		return done
	}
	// writeRolled writes one row and leaves it pending in a rolled
	// generation, returning that generation.
	writeRolled := func(row Row) int64 {
		require.NoError(t, w.WriteRound(ctx, []Row{row}))
		gen := s.ActiveDeltaGeneration()
		require.NoError(t, s.RollGeneration())
		return gen
	}

	// A due window, served by an ordinary compaction pass, and a pending
	// generation whose rows aim only at a fresh window.
	const oldAge = 47 * 3600
	first := partialRow(t, testMetricID, now-oldAge)
	first.Count, first.Sum = 3, 30
	require.NoError(t, w.WriteRound(ctx, []Row{first}))
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(ctx))
	dueWindow := testWindowStart(Tier1s, int64(now)-oldAge)
	fresh := partialRow(t, testMetricID2, now-5)
	fresh.Count, fresh.Sum = 1, 2
	freshGen := writeRolled(fresh)
	sealer := NewSealer(s, SealerConfig{NowFunc: func() time.Time { return sealDueClock(dueWindow) }})

	// The stuck pass holds the fresh window's lock while the sealer seals the
	// due one: the barrier walks the pending generation, sees it contributes
	// to no due window, and leaves it alone — sealing progresses regardless
	// of the compactor's fate.
	entered, release := make(chan struct{}), make(chan struct{})
	parked := consumeParked(freshGen, parkFault(entered, release))
	<-entered
	require.NoError(t, sealer.SealOnce(ctx),
		"a stuck pass on another window must not stop sealing")
	require.True(t, findWindow(t, s, Tier1s, dueWindow).Sealed,
		"the due window must seal while the compactor is blocked elsewhere")
	close(release)
	require.NoError(t, <-parked)

	// Now the stuck pass sits on the very window due for sealing, consuming a
	// pending generation that contributes to it. The barrier must wait it
	// out — no seal while the park holds — and complete once the pass lets
	// go, even though the parked pass finished the same generation under the
	// barrier mid-wait.
	laterTs := now - oldAge - 3600
	laterFirst := partialRow(t, testMetricID, laterTs)
	laterFirst.Count, laterFirst.Sum = 5, 50
	require.NoError(t, w.WriteRound(ctx, []Row{laterFirst}))
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(ctx))
	laterWindow := testWindowStart(Tier1s, int64(laterTs))
	laterSecond := partialRow(t, testMetricID, laterTs)
	laterSecond.Count, laterSecond.Sum = 2, 20
	laterGen := writeRolled(laterSecond)

	entered2, release2 := make(chan struct{}), make(chan struct{})
	parked2 := consumeParked(laterGen, parkFault(entered2, release2))
	<-entered2

	sealDone := make(chan error, 1)
	go func() { sealDone <- sealer.SealOnce(ctx) }()
	select {
	case err := <-sealDone:
		t.Fatalf("the sealer finished (%v) while the stuck pass still holds the window", err)
	case <-time.After(150 * time.Millisecond):
	}
	close(release2)
	require.NoError(t, <-parked2)
	select {
	case err := <-sealDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the barrier must finish once the stuck pass lets go — no deadlock")
	}
	wf := findWindow(t, s, Tier1s, laterWindow)
	require.True(t, wf.Sealed)
	require.EqualValues(t, 7, windowMetricCount(t, wf, testMetricID),
		"the row must land exactly once whatever side of the race consumed it")
}

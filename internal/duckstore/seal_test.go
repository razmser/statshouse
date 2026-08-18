// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
	require.NotNil(t, sl.cfg.NowFunc)
	require.NotNil(t, sl.cfg.Logf)
}

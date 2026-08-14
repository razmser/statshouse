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
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// testWindowStart maps a row time to its tier's archive window start, the way
// consumption routes rows.
func testWindowStart(tier string, ts int64) int64 {
	return ts - ts%tierWindowSecs[tier]
}

// TestRollGenerationSwitchesWriterAndNeverWritesTheOldFile checks the roll:
// the old generation keeps its file and rows, writes go to the new one only,
// and the store reports both generations.
func TestRollGenerationSwitchesWriterAndNeverWritesTheOldFile(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())
	require.NoError(t, w.WriteRound(context.Background(), []Row{testRow(testMetricID, now)}))

	require.NoError(t, s.RollGeneration())
	require.EqualValues(t, 1, s.ActiveDeltaGeneration())
	require.Equal(t, []int64{0, 1}, s.DeltaGenerations())

	require.NoError(t, w.WriteRound(context.Background(), []Row{testRow(testMetricID2, now)}))

	// the new rows are in the rolled-to generation and nowhere else
	require.Equal(t, 1, tierCount(t, s, Tier1s, testMetricID2))
	gen0, err := openStoreFile(filepath.Join(s.cfg.Dir, deltaFileName(0)), true, ResourcesConfig{})
	require.NoError(t, err)
	defer gen0.Close()
	var oldNew, oldKept int
	require.NoError(t, gen0.QueryRow(`SELECT count(*) FROM s1 WHERE metric = $1`, testMetricID2).Scan(&oldNew))
	require.NoError(t, gen0.QueryRow(`SELECT count(*) FROM s1 WHERE metric = $1`, testMetricID).Scan(&oldKept))
	require.Zero(t, oldNew, "a sealed generation must never be written to again")
	require.Equal(t, 1, oldKept, "the sealed generation keeps the rows written before the roll")

	// a second writer cannot attach to a store that has one
	_, err = NewWriter(s, WriterConfig{NowFunc: func() time.Time { return writerNow }})
	require.Error(t, err)
}

// TestRollGenerationWithoutWriter checks the store-side roll on its own: a
// store with no writer still advances generations and serves the new one.
func TestRollGenerationWithoutWriter(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir())
	require.NoError(t, s.RollGeneration())
	require.NoError(t, s.RollGeneration())
	require.EqualValues(t, 2, s.ActiveDeltaGeneration())
	require.Equal(t, []int64{0, 1, 2}, s.DeltaGenerations())
	requireDeltaServes(t, s)
}

// TestRollGenerationDuringConcurrentRounds drives rolls against a submitter
// hammering rounds and proves every acknowledged round landed exactly once:
// no round is lost or doubled by a roll, and none is split across generations.
func TestRollGenerationDuringConcurrentRounds(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())

	const rounds = 60
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			row := testRow(testMetricID, now-uint32(i%30))
			row.Count = 1
			if err := w.WriteRound(context.Background(), []Row{row}); err != nil {
				t.Errorf("round %d failed: %v", i, err)
				return
			}
		}
	}()
	for r := 0; r < 3; r++ {
		require.NoError(t, s.RollGeneration())
	}
	wg.Wait()

	// the count of acknowledged rounds is conserved across every generation;
	// the active one is read through the store's handle, the sealed ones
	// read-only
	var total float64
	for _, gen := range s.DeltaGenerations() {
		db := s.Delta()
		if gen != s.ActiveDeltaGeneration() {
			var err error
			db, err = openStoreFile(filepath.Join(s.cfg.Dir, deltaFileName(gen)), true, ResourcesConfig{})
			require.NoError(t, err)
		}
		var c float64
		require.NoError(t, db.QueryRow(`SELECT sum(count) FROM s1 WHERE metric = $1`, testMetricID).Scan(&c))
		if gen != s.ActiveDeltaGeneration() {
			require.NoError(t, db.Close())
		}
		total += c
	}
	require.EqualValues(t, rounds, total, "every acknowledged round must land exactly once, in exactly one generation")
}

// TestConsumeGenerationRefusesActiveGeneration pins the guard: what is being
// written is never consumption input.
func TestConsumeGenerationRefusesActiveGeneration(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir())
	err := s.ConsumeGeneration(context.Background(), s.ActiveDeltaGeneration(), ConsumeOptions{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "roll before consuming")
}

// consumeFixtureTotals is the decoded total each metric must show, per tier,
// once the fixture's data has settled: nothing lost, nothing counted twice.
type consumeTotals struct {
	count, sum, min, max, sumsquare float64
}

// writeConsumeFixture fills generation 0 with rows spanning two 1s-tier
// archive windows, rolls to generation 1 and writes one row there, then
// closes everything — the on-disk state a process that died after the roll
// leaves. It returns the totals every metric must reach, identical in each
// tier because every row lands in all three.
func writeConsumeFixture(t *testing.T, dir string) map[string]consumeTotals {
	t.Helper()
	s, _ := openTestStore(t, dir)
	w, err := NewWriter(s, WriterConfig{NowFunc: func() time.Time { return writerNow }})
	require.NoError(t, err)

	now := uint32(writerNow.Unix())
	older := testRow(testMetricID, now-3700) // lands in the previous 1s-tier window
	older.Count, older.Sum = 2, 20
	newerA := testRow(testMetricID2, now-100) // lands in the current 1s-tier window...
	newerA.Count, newerA.Sum = 3, 30
	newerB := testRow(testMetricID2, now) // ...twice, so one window holds several rows
	newerB.Count, newerB.Sum = 4, 40
	require.NoError(t, w.WriteRound(context.Background(), []Row{older, newerA, newerB}))

	require.NoError(t, s.RollGeneration())
	active := testRow(testMetricID2+1, now) // stays in the active generation
	active.Count, active.Sum = 5, 50
	require.NoError(t, w.WriteRound(context.Background(), []Row{active}))

	require.NoError(t, w.Close())
	require.NoError(t, s.Close())

	perMetric := map[int32]consumeTotals{
		testMetricID:      {count: 2, sum: 20, min: 1.5, max: 9.75, sumsquare: 101.25},
		testMetricID2:     {count: 7, sum: 70, min: 1.5, max: 9.75, sumsquare: 202.5},
		testMetricID2 + 1: {count: 5, sum: 50, min: 1.5, max: 9.75, sumsquare: 101.25},
	}
	want := map[string]consumeTotals{}
	for _, tier := range tiers {
		for m, tt := range perMetric {
			want[fmt.Sprintf("%s/%d", tierTables[tier], m)] = tt
		}
	}
	return want
}

// readerTotals sums the decoded aggregates over everything a query reads —
// the active delta generation plus every served archive window — mirroring
// the read path's UNION ALL and GROUP BY. Values must come out exactly once
// across that whole view.
func readerTotals(t *testing.T, s *Store) map[string]consumeTotals {
	t.Helper()
	got := map[string]consumeTotals{}
	add := func(db *sql.DB, table string) {
		rows, err := db.Query(fmt.Sprintf(
			`SELECT metric, sum(count), sum(sum), min(min), max(max), sum(sumsquare) FROM %s GROUP BY metric`, table))
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			var m int32
			var tt consumeTotals
			require.NoError(t, rows.Scan(&m, &tt.count, &tt.sum, &tt.min, &tt.max, &tt.sumsquare))
			key := fmt.Sprintf("%s/%d", table, m)
			all := got[key]
			all.count += tt.count
			all.sum += tt.sum
			if all.min == 0 || tt.min < all.min {
				all.min = tt.min
			}
			if tt.max > all.max {
				all.max = tt.max
			}
			all.sumsquare += tt.sumsquare
			got[key] = all
		}
		require.NoError(t, rows.Err())
	}
	// the active generation is read through the store's own handle: DuckDB
	// refuses a read-only open of a file the process already holds read-write
	for _, tier := range tiers {
		add(s.Delta(), tierTables[tier])
	}
	for _, wf := range s.Windows() {
		db, err := openStoreFile(wf.Path, true, ResourcesConfig{})
		require.NoError(t, err)
		add(db, tierTables[wf.Tier])
		require.NoError(t, db.Close())
	}
	return got
}

// crashAt builds a Fault that crashes the consume at its nth encounter of
// point, standing in for a process dying exactly there.
func crashAt(point CrashPoint, n int) func(CrashPoint) error {
	seen := 0
	return func(p CrashPoint) error {
		if p != point {
			return nil
		}
		seen++
		if seen == n {
			return fmt.Errorf("simulated crash")
		}
		return nil
	}
}

// TestConsumeCrashPointsKeepValuesExactlyOnce drives a crash at every commit
// point of the consume protocol and proves the two invariants: no loss and no
// double count of decoded values. A crashed consumption leaves a generation
// that restart resumes; a generation whose every window recorded it is
// unlinked by restart without appending anything again.
func TestConsumeCrashPointsKeepValuesExactlyOnce(t *testing.T) {
	now := writerNow.Unix()
	current1sWindow := testWindowStart(Tier1s, now)
	previous1sWindow := testWindowStart(Tier1s, now-3700)

	for _, tc := range []struct {
		name    string
		fault   func(CrashPoint) error
		resumed bool // the generation survives restart and consumption resumes
	}{
		{"crash before any append", crashAt(CrashBeforeAppend, 1), true},
		{"crash after an append before its commit", crashAt(CrashAfterAppendBeforeCommit, 2), true},
		{"crash after every commit before the unlink", crashAt(CrashAfterCommitBeforeUnlink, 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			want := writeConsumeFixture(t, dir)

			// the consumption crashes; committed work stays, the transaction
			// in flight rolls back
			s, _ := openTestStore(t, dir)
			err := s.ConsumeGeneration(context.Background(), 0, ConsumeOptions{Fault: tc.fault})
			require.Error(t, err)
			require.Contains(t, err.Error(), "simulated crash")
			require.NoError(t, s.Close())

			// restart recovers from whatever the crash left
			s2, _ := openTestStore(t, dir)
			defer func() { _ = s2.Close() }()

			if tc.resumed {
				require.Equal(t, []int64{0, 1}, s2.DeltaGenerations(),
					"an unconsumed generation must be resumed, not dropped")
			} else {
				require.Equal(t, []int64{1}, s2.DeltaGenerations(),
					"a generation recorded as consumed in every window must be unlinked on restart")
				require.NoFileExists(t, filepath.Join(dir, deltaFileName(0)))
				require.Len(t, s2.Windows(), 4, "two 1s windows, one 1m window, one 1h window")
				require.Equal(t, want, readerTotals(t, s2))
				return
			}

			if tc.name == "crash before any append" {
				require.Empty(t, s2.Windows(), "nothing may be committed before the first append")
			}

			if tc.name == "crash after an append before its commit" {
				// the first window committed; the second one's file exists —
				// it is created before the transaction — but holds neither
				// rows nor the record, so what it serves is nothing
				wins := s2.Windows()
				require.Len(t, wins, 2)
				require.Equal(t, Tier1s, wins[0].Tier)
				require.EqualValues(t, previous1sWindow, wins[0].WindowStart)
				require.Equal(t, Tier1s, wins[1].Tier)
				require.EqualValues(t, current1sWindow, wins[1].WindowStart)

				aborted := filepath.Join(dir, archiveSubdir, archiveFileName(Tier1s, current1sWindow))
				db, err := openStoreFile(aborted, true, ResourcesConfig{})
				require.NoError(t, err)
				var rows int
				require.NoError(t, db.QueryRow(`SELECT count(*) FROM s1`).Scan(&rows))
				require.Zero(t, rows, "an uncommitted append must leave no rows")
				recorded, err := readConsumed(db)
				require.NoError(t, err)
				require.Empty(t, recorded, "an uncommitted append must leave no consumption record")
				require.NoError(t, db.Close())

				// mid-consume, what is served is served exactly once: the
				// committed window's rows and the active generation's rows;
				// the rows still sealed in the old generation re-enter only
				// as their windows commit
				partial := map[string]consumeTotals{}
				for _, tier := range tiers {
					partial[fmt.Sprintf("%s/%d", tierTables[tier], testMetricID2+1)] =
						want[fmt.Sprintf("%s/%d", tierTables[tier], testMetricID2+1)]
				}
				partial[fmt.Sprintf("%s/%d", tierTables[Tier1s], testMetricID)] =
					want[fmt.Sprintf("%s/%d", tierTables[Tier1s], testMetricID)]
				require.Equal(t, partial, readerTotals(t, s2))
			}

			// resuming the consumption completes it: every value lands in a
			// window exactly once and the generation is unlinked
			require.NoError(t, s2.ConsumeGeneration(context.Background(), 0, ConsumeOptions{}))
			require.Equal(t, []int64{1}, s2.DeltaGenerations())
			require.NoFileExists(t, filepath.Join(dir, deltaFileName(0)))
			require.Len(t, s2.Windows(), 4)
			require.Equal(t, want, readerTotals(t, s2))
		})
	}
}

// TestConsumeGenerationEmptyGenerationUnlinksAlone checks the degenerate
// consume: a rolled generation that never received a row is consumed by the
// unlink alone, creating no windows.
func TestConsumeGenerationEmptyGenerationUnlinksAlone(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTestStore(t, dir)
	require.NoError(t, s.RollGeneration())
	require.NoError(t, s.ConsumeGeneration(context.Background(), 0, ConsumeOptions{}))

	require.Equal(t, []int64{1}, s.DeltaGenerations())
	require.NoFileExists(t, filepath.Join(dir, deltaFileName(0)))
	require.Empty(t, s.Windows())
}

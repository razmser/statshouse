// Copyright 2025 V Kontate LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// recollapseSealer returns a sealer whose clock is parked at the writer's
// frozen now, so no window is ever due and a pass's whole work is the
// re-collapse sweep.
func recollapseSealer(s *Store, rec MetricsRecorder, factor int) *Sealer {
	return NewSealer(s, SealerConfig{
		NowFunc:          func() time.Time { return writerNow },
		Metrics:          rec,
		RecollapseFactor: factor,
	})
}

// writeFixturePass writes one collapse fixture and lands it through one
// compaction pass: one more partial run per key in each of the fixture's
// windows.
func writeFixturePass(t *testing.T, s *Store, w *Writer) {
	t.Helper()
	writeCollapseFixture(t, s, w)
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(context.Background()))
}

// recollapseWindowRows counts one tier window file's physical rows.
func recollapseWindowRows(t *testing.T, s *Store, tier string, windowStart int64) int {
	t.Helper()
	path := filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(tier, windowStart))
	db, err := openStoreFile(path, true, ResourcesConfig{})
	require.NoError(t, err)
	defer db.Close()
	var n int
	require.NoError(t, db.QueryRow("SELECT count(*) FROM "+tierTables[tier]).Scan(&n))
	return n
}

// TestCountCollapseGroupsKeysOnTheFullRowKey pins the trigger's measure
// directly: rows differing only outside the collapse's group key count as one
// collapsed row, and every distinct key counts separately.
func TestCountCollapseGroupsKeysOnTheFullRowKey(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNowUnix)
	sameKey := partialRow(t, testMetricID, now-5)
	sameKey.Count = 1
	otherKey := partialRow(t, testMetricID, now-5)
	otherKey.Count = 2 // same key, different aggregates: still one group
	otherSecond := partialRow(t, testMetricID, now-6)
	otherMetric := partialRow(t, testMetricID2, now-5)
	require.NoError(t, w.WriteRound(context.Background(), []Row{sameKey, otherKey, otherSecond, otherMetric}))

	physical, collapsed, err := countCollapseGroups(s.Delta(), tierTables[Tier1s])
	require.NoError(t, err)
	require.EqualValues(t, 4, physical)
	require.EqualValues(t, 3, collapsed)
}

// TestRecollapseMatchesSingleCompaction is the fix's core promise: repeated
// compaction into one window — each pass appending a fresh collapsed run for
// every key — followed by the sweep's re-collapse yields the same physical
// shape and the same decoded values as a single compaction of the same rows,
// across every tier, and the sweep reports what it did without re-arming
// itself.
func TestRecollapseMatchesSingleCompaction(t *testing.T) {
	const passes = 5
	now := uint32(writerNowUnix)
	ts := int64(now) - 5

	// repeated: one fixture per generation, a compaction between each
	sRep, wRep := newTestWriter(t)
	for i := 0; i < passes; i++ {
		writeFixturePass(t, sRep, wRep)
	}
	require.NoError(t, wRep.Close())

	// single: the same five fixtures land in one generation, one compaction
	sOne, wOne := newTestWriter(t)
	for i := 0; i < passes; i++ {
		writeCollapseFixture(t, sOne, wOne)
	}
	require.NoError(t, NewCompactor(sOne, CompactorConfig{}).CompactOnce(context.Background()))
	require.NoError(t, wOne.Close())

	for _, tier := range tiers {
		start := testWindowStart(tier, ts)
		require.Equal(t, 2*passes, recollapseWindowRows(t, sRep, tier, start),
			"%s: each pass must have appended a run per key before the sweep", tier)
		require.Equal(t, 2, recollapseWindowRows(t, sOne, tier, start),
			"%s: the single compaction holds one row per key", tier)
	}

	rec := &recordingMetrics{}
	require.NoError(t, recollapseSealer(sRep, rec, DefaultRecollapseFactor).SealOnce(context.Background()))
	require.Equal(t, len(tiers), countWindowEvents(rec, WindowRecollapsed),
		"one recollapse event per tier window past the factor")

	for _, tier := range tiers {
		start := testWindowStart(tier, ts)
		require.Equal(t, 2, recollapseWindowRows(t, sRep, tier, start),
			"%s: the re-collapsed window must hold one row per key, like a single compaction", tier)

		one := openStoreFileRows(t, sOne, tier, start)
		rep := openStoreFileRows(t, sRep, tier, start)
		require.Len(t, rep, len(one), "%s: the same keys must survive", tier)
		requireSameDecoded(t, decodeRows(t, one), decodeRows(t, rep))
	}

	// the sweep drains: a second pass finds nothing to do, because the
	// rewrite itself marks no window — only an append does
	rec2 := &recordingMetrics{}
	require.NoError(t, recollapseSealer(sRep, rec2, DefaultRecollapseFactor).SealOnce(context.Background()))
	require.Zero(t, countWindowEvents(rec2, WindowRecollapsed), "an idle sweep must not rewrite")
	for _, tier := range tiers {
		require.Equal(t, 2, recollapseWindowRows(t, sRep, tier, testWindowStart(tier, ts)))
	}
}

// TestRecollapseSweepFailureRearmsEveryUncheckedWindow pins the sweep's
// give-up contract: a pass that fails on one candidate must re-arm every
// candidate it did not get to check — the failing one and the whole unvisited
// remainder — so a quiet window's fold is owed to the next pass, not to a
// future append or a restart. Without the remainder re-arm, one transient
// failure on the first candidate silently disarmed every later one until
// seal.
func TestRecollapseSweepFailureRearmsEveryUncheckedWindow(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNowUnix)
	ts := int64(now) - 5
	for i := 0; i < 5; i++ { // every tier's window past the default factor
		writeFixturePass(t, s, w)
	}
	require.NoError(t, w.Close())

	pending := func() map[windowKey]struct{} {
		s.mu.Lock()
		defer s.mu.Unlock()
		out := make(map[windowKey]struct{}, len(s.recollapsePending))
		for k := range s.recollapsePending {
			out[k] = struct{}{}
		}
		return out
	}
	require.Len(t, pending(), len(tiers), "every tier's window must be a candidate")

	// fail the sweep on its FIRST candidate: a directory at the 1s window's
	// path passes the stat but fails the open, and the sweep visits windows
	// in tier order, so 1m and 1h are the unvisited remainder
	start1s := testWindowStart(Tier1s, ts)
	path1s := filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(Tier1s, start1s))
	aside := path1s + ".aside"
	require.NoError(t, os.Rename(path1s, aside))
	require.NoError(t, os.Mkdir(path1s, 0o755))

	err := recollapseSealer(s, &recordingMetrics{}, DefaultRecollapseFactor).SealOnce(context.Background())
	require.Error(t, err, "the unopenable window must fail the pass")

	still := pending()
	for _, tier := range tiers {
		_, ok := still[windowKey{tier: tier, start: testWindowStart(tier, ts)}]
		require.True(t, ok, "%s: a give-up pass must re-arm every unchecked window", tier)
	}

	// repair the file: the next pass must reach every re-armed candidate
	require.NoError(t, os.Remove(path1s))
	require.NoError(t, os.Rename(aside, path1s))
	rec := &recordingMetrics{}
	require.NoError(t, recollapseSealer(s, rec, DefaultRecollapseFactor).SealOnce(context.Background()))
	require.Equal(t, len(tiers), countWindowEvents(rec, WindowRecollapsed),
		"the repaired sweep must fold every re-armed window")
	for _, tier := range tiers {
		require.Equal(t, 2, recollapseWindowRows(t, s, tier, testWindowStart(tier, ts)),
			"%s: one row per key after the repaired sweep", tier)
	}
}

// openStoreFileRows scans one tier window file's rows read-only.
func openStoreFileRows(t *testing.T, s *Store, tier string, windowStart int64) []rawRow {
	t.Helper()
	path := filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(tier, windowStart))
	db, err := openStoreFile(path, true, ResourcesConfig{})
	require.NoError(t, err)
	defer db.Close()
	return scanTableRows(t, db, tierTables[tier])
}

// TestRecollapseNeverTouchesSealedWindow proves the exclusion both directly
// and through the sweep: a sealed window refuses the re-collapse at a factor
// of one — where anything foldable would rewrite — while the same call on an
// unsealed window does rewrite, and the sweep skips the sealed window without
// reporting it.
func TestRecollapseNeverTouchesSealedWindow(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNowUnix)
	ts := int64(now) - 5
	writeFixturePass(t, s, w)
	writeFixturePass(t, s, w) // two runs per key in every window
	require.NoError(t, w.Close())

	start1s := testWindowStart(Tier1s, ts)
	start1m := testWindowStart(Tier1m, ts)
	require.Equal(t, 4, recollapseWindowRows(t, s, Tier1s, start1s))
	require.Equal(t, 4, recollapseWindowRows(t, s, Tier1m, start1m))

	// seal the 1s window alone (SealWindow is the raw primitive; a direct
	// caller takes responsibility for the precondition), so its runs are
	// already folded and frozen
	require.NoError(t, s.SealWindow(context.Background(), Tier1s, start1s))
	require.True(t, findWindow(t, s, Tier1s, start1s).Sealed)
	require.Equal(t, 2, recollapseWindowRows(t, s, Tier1s, start1s), "the seal itself folds the runs")

	// factor one rewrites anything foldable; the sealed window still refuses
	recollapsed, err := s.RecollapseWindow(context.Background(), Tier1s, start1s, 1)
	require.NoError(t, err)
	require.False(t, recollapsed, "a sealed window must never be re-collapsed")
	require.Equal(t, 2, recollapseWindowRows(t, s, Tier1s, start1s), "the refusal must leave the file untouched")
	require.True(t, findWindow(t, s, Tier1s, start1s).Sealed, "the refusal must not unseal the window")

	// the control: the same call on an unsealed window does rewrite, so the
	// refusal above is the seal and not the shape of the data
	recollapsed, err = s.RecollapseWindow(context.Background(), Tier1m, start1m, 1)
	require.NoError(t, err)
	require.True(t, recollapsed, "an unsealed window past the factor must rewrite")
	require.Equal(t, 2, recollapseWindowRows(t, s, Tier1m, start1m))

	// through the sweep: the compactions left every window a candidate, but
	// the sealed 1s window is skipped without an event and the already
	// re-collapsed 1m window holds nothing foldable — only the 1h window acts
	rec := &recordingMetrics{}
	require.NoError(t, recollapseSealer(s, rec, 1).SealOnce(context.Background()))
	require.Equal(t, 1, countWindowEvents(rec, WindowRecollapsed))
	for _, e := range rec.windows {
		if e.kind == WindowRecollapsed {
			require.Equal(t, Tier1h, e.tier, "only the untouched 1h window may report a recollapse")
		}
	}
	require.Equal(t, 2, recollapseWindowRows(t, s, Tier1s, start1s), "the sweep must not touch the sealed window")
	require.True(t, findWindow(t, s, Tier1s, start1s).Sealed)
	require.Equal(t, 2, recollapseWindowRows(t, s, Tier1h, testWindowStart(Tier1h, ts)))
}

// TestRecollapseTriggerFactorRespected pins the boundary arithmetic: a window
// holds exactly the factor's worth of partial rows without rewriting, and the
// first row past it folds everything back to the one collapsed row.
func TestRecollapseTriggerFactorRespected(t *testing.T) {
	const factor = 4
	s, w := newTestWriter(t)
	now := uint32(writerNowUnix)
	ts := int64(now) - 5
	ctx := context.Background()
	start1s := testWindowStart(Tier1s, ts)

	writeRowPass := func() {
		row := partialRow(t, testMetricID, now-5)
		row.Count, row.Sum = 1, 1
		require.NoError(t, w.WriteRound(ctx, []Row{row}))
		require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(ctx))
	}

	// four passes hold four partial rows of the one key — at the factor, not
	// past it — and no check rewrites them
	for i := 1; i <= factor; i++ {
		writeRowPass()
		require.Equal(t, i, recollapseWindowRows(t, s, Tier1s, start1s))
		recollapsed, err := s.RecollapseWindow(ctx, Tier1s, start1s, factor)
		require.NoError(t, err)
		require.False(t, recollapsed, "%d physical rows against %d collapsed is at the factor, not past it", i, 1)
		require.Equal(t, i, recollapseWindowRows(t, s, Tier1s, start1s), "an untriggered check must not rewrite")
	}

	// the fifth crosses the factor and folds back to the one collapsed row
	writeRowPass()
	recollapsed, err := s.RecollapseWindow(ctx, Tier1s, start1s, factor)
	require.NoError(t, err)
	require.True(t, recollapsed, "the first row past the factor must trigger")
	require.Equal(t, 1, recollapseWindowRows(t, s, Tier1s, start1s), "the rewrite must leave one row per key")
	require.EqualValues(t, factor+1, windowMetricCount(t, findWindow(t, s, Tier1s, start1s), testMetricID),
		"the folded row must carry every partial row's count")
}

// TestRecollapseSweepChecksWindowsRecoveredFromDisk pins the restart case:
// the open seeds the unsealed windows it recovers as re-collapse candidates,
// so a process that inherits a store full of partial runs folds them on its
// first pass even though no append of its own has marked anything.
func TestRecollapseSweepChecksWindowsRecoveredFromDisk(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(StoreConfig{Dir: dir, StatshouseVersion: testStatshouseVersion})
	require.NoError(t, err)
	w, err := NewWriter(s, WriterConfig{NowFunc: func() time.Time { return writerNow }})
	require.NoError(t, err)
	for i := 0; i < 5; i++ {
		writeFixturePass(t, s, w)
	}
	require.NoError(t, w.Close())
	require.NoError(t, s.Close())

	s2, _ := openTestStore(t, dir) // the restart: only the open's seeding can feed the sweep
	rec := &recordingMetrics{}
	require.NoError(t, recollapseSealer(s2, rec, DefaultRecollapseFactor).SealOnce(context.Background()))
	require.Equal(t, len(tiers), countWindowEvents(rec, WindowRecollapsed),
		"the recovered unsealed windows must be sweep candidates")

	ts := int64(uint32(writerNowUnix)) - 5
	for _, tier := range tiers {
		wf := findWindow(t, s2, tier, testWindowStart(tier, ts))
		require.False(t, wf.Sealed, "%s: a recollapse must not seal anything", tier)
		require.Equal(t, 2, recollapseWindowRows(t, s2, tier, testWindowStart(tier, ts)),
			"%s: one row per key after the recovered sweep", tier)
	}
}

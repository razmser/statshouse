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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// agedWindowsFixture writes one row at each age — seconds back from the
// frozen writer clock, all inside the three-day ingest guard — and compacts
// them, so each tier holds one archive window per distinct age bucket: three
// hourly 1s windows, three daily 1m windows and (50 h all within one 30-day
// span) a single 1h window.
func agedWindowsFixture(t *testing.T, ages ...int64) *Store {
	t.Helper()
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())
	rows := make([]Row, 0, len(ages))
	for i, age := range ages {
		row := partialRow(t, testMetricID+int32(i), now-uint32(age))
		row.Count, row.Sum = 1, 1
		rows = append(rows, row)
	}
	require.NoError(t, w.WriteRound(context.Background(), rows))
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(context.Background()))
	return s
}

// farFutureClock is a store clock parked sixty days past the frozen writer
// clock, far past the 52 h and 33 d retentions but never past the unbounded
// 1h one.
func farFutureClock() func() time.Time {
	now := writerNowUnix
	return func() time.Time { return time.Unix(now+60*86400, 0) }
}

// specDefaultRetainer is the spec's retention defaults under a chosen clock,
// the configuration a plain --storage-backend=duck aggregator runs with.
func specDefaultRetainer(s *Store, now func() time.Time) *Retainer {
	cfg := DefaultRetentionConfig()
	cfg.NowFunc = now
	return NewRetainer(s, cfg)
}

// windowPath is a window's archive file path inside the store.
func windowPath(s *Store, tier string, windowStart int64) string {
	return filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(tier, windowStart))
}

// TestRetainerUnlinksExpiredWindows drives the spec defaults: sixty days on,
// every 1s window (52 h retention) and every 1m window (33 d) is unlinked —
// file and served entry both — while the 1h window (unbounded) survives.
func TestRetainerUnlinksExpiredWindows(t *testing.T) {
	s := agedWindowsFixture(t, 5, 26*3600, 50*3600)
	now := writerNowUnix
	h1 := testWindowStart(Tier1h, now-5)

	retainer := specDefaultRetainer(s, farFutureClock())
	require.NoError(t, retainer.RetainOnce(context.Background()))

	survivors := s.Windows()
	require.Len(t, survivors, 1, "only the unbounded 1h window may survive")
	require.Equal(t, Tier1h, survivors[0].Tier)
	require.Equal(t, h1, survivors[0].WindowStart)
	require.FileExists(t, survivors[0].Path)

	// the expired files left the disk, not just the served set
	entries, err := os.ReadDir(filepath.Join(s.cfg.Dir, archiveSubdir))
	require.NoError(t, err)
	for _, e := range entries {
		tier, _, ok := parseArchiveFileName(e.Name())
		require.True(t, ok && tier == Tier1h, "no expired window file may remain: %s", e.Name())
	}

	st := retainer.Stats()
	require.EqualValues(t, 6, st.ExpiredUnlinked, "three 1s and three 1m windows must be unlinked")
	require.Zero(t, st.EarlyEvicted)
	require.Zero(t, st.LeaseDeferred)
}

// TestRetainerRetentionIsPerTierConfigurable proves each tier's retention is
// its own flag-shaped knob: with only the 1h tier bounded, sixty days on,
// exactly the 1h window goes while the unbounded tiers keep every window.
func TestRetainerRetentionIsPerTierConfigurable(t *testing.T) {
	s := agedWindowsFixture(t, 5, 26*3600, 50*3600)
	now := writerNowUnix
	h1 := testWindowStart(Tier1h, now-5)

	retainer := NewRetainer(s, RetentionConfig{
		Retention1h: 10 * 24 * time.Hour,
		NowFunc:     farFutureClock(),
	})
	require.NoError(t, retainer.RetainOnce(context.Background()))

	require.NoFileExists(t, windowPath(s, Tier1h, h1), "the bounded 1h window must be unlinked")
	for _, wf := range s.Windows() {
		require.NotEqual(t, Tier1h, wf.Tier, "only the 1h window may be gone")
		require.FileExists(t, wf.Path)
	}
	require.EqualValues(t, 1, retainer.Stats().ExpiredUnlinked)
}

// TestRetainerLeaseDefersUnlink drives the file lease: an expired window a
// reader holds survives the pass that expired it, the pass after the reader
// finishes takes it, and a window that is no longer served leases nothing.
func TestRetainerLeaseDefersUnlink(t *testing.T) {
	s := agedWindowsFixture(t, 5, 26*3600, 50*3600)
	now := writerNowUnix
	leased := testWindowStart(Tier1s, now-26*3600)

	l := s.AcquireWindowLease(Tier1s, leased)
	require.NotNil(t, l, "the window is served, so the lease must be granted")

	retainer := specDefaultRetainer(s, farFutureClock())
	require.NoError(t, retainer.RetainOnce(context.Background()))

	// the leased window survived the pass that expired it
	require.FileExists(t, windowPath(s, Tier1s, leased))
	served := false
	for _, wf := range s.Windows() {
		if wf.Tier == Tier1s && wf.WindowStart == leased {
			served = true
		}
	}
	require.True(t, served, "a leased window stays served until its reader finishes")
	require.EqualValues(t, 1, retainer.Stats().LeaseDeferred)

	// the reader finishes, and the next pass takes the window
	l.Release()
	l.Release() // idempotent
	require.NoError(t, retainer.RetainOnce(context.Background()))
	require.NoFileExists(t, windowPath(s, Tier1s, leased))

	// a window that is no longer served leases nothing: retention got there
	require.Nil(t, s.AcquireWindowLease(Tier1s, leased))
}

// TestRetainerLowWatermarkEvictsOldestFirst proves the safety net: with
// nothing past retention, free space below the watermark evicts the oldest
// windows by window end, earliest data first, until space recovers — and
// every eviction is reported as early.
func TestRetainerLowWatermarkEvictsOldestFirst(t *testing.T) {
	s := agedWindowsFixture(t, 5, 26*3600, 50*3600)

	const watermark = uint64(1 << 20)
	const keep = 2 // free space recovers once this many windows remain
	free := func(dir string) (uint64, error) {
		if len(s.Windows()) > keep {
			return watermark - 1, nil
		}
		return watermark * 2, nil
	}

	// the expected evictees: the oldest windows by window end
	sorted := append([]WindowFile(nil), s.Windows()...)
	sort.Slice(sorted, func(i, j int) bool { return windowEnd(sorted[i]) < windowEnd(sorted[j]) })
	evicted, survived := sorted[:len(sorted)-keep], sorted[len(sorted)-keep:]

	retainer := NewRetainer(s, RetentionConfig{
		FreeSpaceWatermark: watermark,
		FreeSpace:          free,
		NowFunc:            func() time.Time { return writerNow }, // nothing is past retention under this clock
	})
	require.NoError(t, retainer.RetainOnce(context.Background()))

	require.Len(t, s.Windows(), keep)
	for _, wf := range evicted {
		require.NoFileExists(t, wf.Path, "the oldest windows must be evicted first")
	}
	for _, wf := range survived {
		require.FileExists(t, wf.Path, "the newest windows must survive the early eviction")
	}
	st := retainer.Stats()
	require.EqualValues(t, len(evicted), st.EarlyEvicted, "every early eviction must be reported")
	require.Zero(t, st.ExpiredUnlinked, "nothing was past retention; every unlink is an early eviction")
	require.Zero(t, st.LeaseDeferred)
}

// TestRetainerRunsAlongsideIngestion drives the retainer against a compactor
// and a writer hammering rounds, with one window already past its retention:
// the rounds keep flowing (retention never runs ahead of ingestion), every
// acknowledged round lands exactly once, and the expired window goes.
func TestRetainerRunsAlongsideIngestion(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNowUnix)

	// a row 50 hours old: its 1s window is past the 52 h retention under the
	// retainer's clock, its 1m and 1h windows are not — written and consumed
	// before the loops start
	const oldAge = 50 * 3600
	old := partialRow(t, testMetricID, now-oldAge)
	old.Count, old.Sum = 2, 20
	require.NoError(t, w.WriteRound(context.Background(), []Row{old}))
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(context.Background()))
	oldWindow := testWindowStart(Tier1s, int64(now)-oldAge)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// the spec defaults under a clock four hours past the writer clock: past
	// the old 1s window's boundary, nowhere near the fresh windows'
	cfg := DefaultRetentionConfig()
	cfg.Interval = 10 * time.Millisecond
	cfg.NowFunc = func() time.Time { return time.Unix(int64(now)+4*3600, 0) }
	retainer := NewRetainer(s, cfg)
	compactor := NewCompactor(s, CompactorConfig{Interval: 10 * time.Millisecond})
	retDone := make(chan struct{})
	compactDone := make(chan struct{})
	go func() { defer close(retDone); _ = retainer.Run(ctx) }()
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
		t.Fatal("ingestion stalled behind retention")
	}

	cancel()
	<-retDone
	<-compactDone
	require.NoError(t, w.Close())
	require.NoError(t, compactor.CompactOnce(context.Background())) // flush what is left

	// the expired window went while ingestion ran
	require.NoFileExists(t, windowPath(s, Tier1s, oldWindow))
	require.GreaterOrEqual(t, retainer.Stats().ExpiredUnlinked, int64(1), "the expired window must be unlinked")

	// every acknowledged round lands exactly once across the delta and the
	// surviving windows — retention cost the fresh data nothing
	var count float64
	var c float64
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
			fmt.Sprintf(`SELECT coalesce(sum(count), 0) FROM %s WHERE metric = $1`, TierTable(Tier1s)),
			testMetricID2).Scan(&c))
		_ = db.Close()
		count += c
	}
	require.EqualValues(t, rounds, count, "every acknowledged round must land exactly once")
}

// TestRetainerUnboundedRetentionKeepsEverything pins the zero-retention
// meaning: no clock, however far ahead, unlinks an unbounded tier's windows.
func TestRetainerUnboundedRetentionKeepsEverything(t *testing.T) {
	s := agedWindowsFixture(t, 5, 26*3600, 50*3600)
	before := s.Windows()

	retainer := NewRetainer(s, RetentionConfig{NowFunc: farFutureClock()})
	require.NoError(t, retainer.RetainOnce(context.Background()))

	require.Len(t, s.Windows(), len(before), "every window is unbounded and must stay")
	for _, wf := range s.Windows() {
		require.FileExists(t, wf.Path)
	}
	require.Zero(t, retainer.Stats().ExpiredUnlinked)
}

// TestWindowExpired pins the boundary arithmetic: a window expires at window
// end plus the retention, not one second before, and zero retention never
// expires it.
func TestWindowExpired(t *testing.T) {
	const start = int64(1740000000)
	for _, tier := range tiers {
		retention := 52 * time.Hour
		end := start + tierWindowSecs[tier]
		require.False(t, windowExpired(tier, start, end+int64(retention/time.Second)-1, retention),
			"%s: one second before window end plus retention must not expire", tier)
		require.True(t, windowExpired(tier, start, end+int64(retention/time.Second), retention),
			"%s: window end plus retention expires", tier)
		require.False(t, windowExpired(tier, start, start+10*365*86400, 0),
			"%s: zero retention keeps windows forever", tier)
	}
}

// TestNewRetainerDefaults checks the config defaults land.
func TestNewRetainerDefaults(t *testing.T) {
	r := NewRetainer(&Store{}, RetentionConfig{})
	require.Equal(t, DefaultRetainerInterval, r.cfg.Interval)
	require.NotNil(t, r.cfg.NowFunc)
	require.NotNil(t, r.cfg.Logf)
	require.NotNil(t, r.cfg.FreeSpace)
}

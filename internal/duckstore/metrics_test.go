// Copyright 2025 V Kontakte LLC
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

// recordingMetrics captures every event a MetricsRecorder receives, so tests
// can assert each duck-store event emits its metric. Locked, because the
// interface promises implementations are safe for concurrent use — the size
// sampler's goroutine appends while a test's assertions read.
type recordingMetrics struct {
	mu sync.Mutex

	passes     []recordedPass
	windows    []recordedWindow
	quarantine []recordedQuarantine
	queries    []recordedQuery
	sizes      []recordedSize
}

type recordedPass struct {
	kind MaintenanceKind
	err  error
	dur  time.Duration
}

type recordedWindow struct {
	kind WindowEventKind
	tier string
}

type recordedQuarantine struct {
	axis  QuarantineAxis
	count int
}

type recordedQuery struct {
	verb QueryVerb
	err  error
	dur  time.Duration
}

type recordedSize struct {
	location SizeLocation
	used     int64
	free     int64
}

func (m *recordingMetrics) MaintenancePass(kind MaintenanceKind, err error, dur time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.passes = append(m.passes, recordedPass{kind: kind, err: err, dur: dur})
}

func (m *recordingMetrics) MaintenanceWindow(kind WindowEventKind, tier string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.windows = append(m.windows, recordedWindow{kind: kind, tier: tier})
}

func (m *recordingMetrics) QuarantinedFiles(axis QuarantineAxis, count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.quarantine = append(m.quarantine, recordedQuarantine{axis: axis, count: count})
}

func (m *recordingMetrics) StoreQuery(verb QueryVerb, err error, dur time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queries = append(m.queries, recordedQuery{verb: verb, err: err, dur: dur})
}

func (m *recordingMetrics) StoreSize(location SizeLocation, used, free int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sizes = append(m.sizes, recordedSize{location: location, used: used, free: free})
}

// snapshotSizes returns a copy of the recorded sizes, safe to read while the
// sampler still runs.
func (m *recordingMetrics) snapshotSizes() []recordedSize {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]recordedSize(nil), m.sizes...)
}

// requireOnePass asserts exactly one pass of one maintenance kind was
// reported, and returns it.
func requireOnePass(t *testing.T, m *recordingMetrics, kind MaintenanceKind) recordedPass {
	t.Helper()
	require.Len(t, m.passes, 1, "one %s pass must be reported", kind)
	got := m.passes[0]
	require.Equal(t, kind, got.kind)
	require.GreaterOrEqual(t, got.dur, time.Duration(0))
	return got
}

// TestCompactorReportsPassTiming proves every compaction pass is reported —
// successful and failed alike — with the maintenance kind and the pass's
// outcome, so the timing metric's status split is observable.
func TestCompactorReportsPassTiming(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		s, w := newTestWriter(t)
		m := &recordingMetrics{}
		c := NewCompactor(s, CompactorConfig{Metrics: m})

		require.NoError(t, c.CompactOnce(context.Background()))
		pass := requireOnePass(t, m, MaintenanceCompaction)
		require.NoError(t, pass.err)
		require.Empty(t, m.windows, "an idle pass touches no windows")
		_ = w.Close()
	})

	t.Run("failed", func(t *testing.T) {
		s, w := newTestWriter(t)
		row := partialRow(t, testMetricID, uint32(writerNowUnix)-5)
		row.Count, row.Sum = 1, 1
		require.NoError(t, w.WriteRound(context.Background(), []Row{row}))

		m := &recordingMetrics{}
		c := NewCompactor(s, CompactorConfig{Metrics: m})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.Error(t, c.CompactOnce(ctx))
		pass := requireOnePass(t, m, MaintenanceCompaction)
		require.Error(t, pass.err, "a failed pass must be reported as errored")
	})
}

// TestSealerReportsPassAndSealedWindows proves the sealer reports its pass
// timing plus one sealed event per window it seals, with the window's tier.
func TestSealerReportsPassAndSealedWindows(t *testing.T) {
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

	m := &recordingMetrics{}
	sealer := NewSealer(s, SealerConfig{
		NowFunc: func() time.Time { return time.Unix(far+1, 0) },
		Metrics: m,
	})
	require.NoError(t, sealer.SealOnce(context.Background()))
	pass := requireOnePass(t, m, MaintenanceSealing)
	require.NoError(t, pass.err)

	// the fixture's one age bucket yields one window per tier, and each
	// sealed window is reported with its tier
	require.Len(t, m.windows, 3)
	kinds := map[WindowEventKind]int{}
	tiers := map[string]bool{}
	for _, ev := range m.windows {
		require.Equal(t, WindowSealed, ev.kind)
		kinds[ev.kind]++
		tiers[ev.tier] = true
	}
	require.Equal(t, 3, kinds[WindowSealed])
	require.Equal(t, map[string]bool{Tier1s: true, Tier1m: true, Tier1h: true}, tiers)
}

// TestRetainerReportsWindowEvents proves retention's three window outcomes —
// unlink by retention, early eviction by the free-space watermark, and a
// lease-deferred unlink — each emit their event, alongside the pass timing.
func TestRetainerReportsWindowEvents(t *testing.T) {
	t.Run("unlinks_by_retention", func(t *testing.T) {
		s := agedWindowsFixture(t, 5, 26*3600, 50*3600)

		m := &recordingMetrics{}
		cfg := DefaultRetentionConfig()
		cfg.NowFunc = farFutureClock()
		cfg.Metrics = m
		retainer := NewRetainer(s, cfg)

		require.NoError(t, retainer.RetainOnce(context.Background()))
		pass := requireOnePass(t, m, MaintenanceRetention)
		require.NoError(t, pass.err)

		// sixty days on, every 1s window (52 h retention) and every 1m
		// window (33 d) is gone with one unlinked event each; the 1h tier
		// is unbounded and untouched
		require.Len(t, m.windows, 6)
		counts := map[string]int{}
		for _, ev := range m.windows {
			require.Equal(t, WindowUnlinked, ev.kind)
			counts[ev.tier]++
		}
		require.Equal(t, map[string]int{Tier1s: 3, Tier1m: 3}, counts)
	})

	t.Run("early_evicts_for_free_space", func(t *testing.T) {
		s := agedWindowsFixture(t, 5, 26*3600, 50*3600)

		const watermark = uint64(1 << 20)
		const keep = 2
		m := &recordingMetrics{}
		retainer := NewRetainer(s, RetentionConfig{
			FreeSpaceWatermark: watermark,
			FreeSpace: func(dir string) (uint64, error) {
				if len(s.Windows()) > keep {
					return watermark - 1, nil
				}
				return watermark * 2, nil
			},
			NowFunc: func() time.Time { return writerNow }, // nothing is past retention
			Metrics: m,
		})

		require.NoError(t, retainer.RetainOnce(context.Background()))
		pass := requireOnePass(t, m, MaintenanceRetention)
		require.NoError(t, pass.err)

		require.Len(t, s.Windows(), keep)
		require.Len(t, m.windows, 5, "every early eviction must be reported as such")
		for _, ev := range m.windows {
			require.Equal(t, WindowEarlyEvicted, ev.kind)
		}
	})

	t.Run("lease_defers_unlink", func(t *testing.T) {
		s := agedWindowsFixture(t, 5, 26*3600, 50*3600)
		leased := testWindowStart(Tier1s, writerNowUnix-26*3600)
		l := s.AcquireWindowLease(Tier1s, leased)
		require.NotNil(t, l)

		m := &recordingMetrics{}
		cfg := DefaultRetentionConfig()
		cfg.NowFunc = farFutureClock()
		cfg.Metrics = m
		retainer := NewRetainer(s, cfg)

		require.NoError(t, retainer.RetainOnce(context.Background()))
		pass := requireOnePass(t, m, MaintenanceRetention)
		require.NoError(t, pass.err, "a lease defers, it does not fail the pass")

		var deferred int
		for _, ev := range m.windows {
			if ev.kind == WindowLeaseDeferred {
				deferred++
				require.Equal(t, Tier1s, ev.tier, "only the leased window defers")
			} else {
				require.Equal(t, WindowUnlinked, ev.kind)
			}
		}
		require.Equal(t, 1, deferred)
	})
}

// TestOpenStoreReportsQuarantinedFilesPerAxis proves each quarantined file is
// counted on the axis that excluded it: the three version axes and the
// unreadable catch-all.
func TestOpenStoreReportsQuarantinedFilesPerAxis(t *testing.T) {
	for _, tc := range []struct {
		axis string
		want QuarantineAxis
		file func(t *testing.T, dir string)
	}{
		{
			axis: "schema",
			want: QuarantineSchema,
			file: func(t *testing.T, dir string) {
				bad := currentTestStamp(t)
				bad.schemaVersion = SchemaVersion + 1
				createTestFile(t, filepath.Join(dir, deltaFileName(0)), allTierTables(), bad, nil)
			},
		},
		{
			axis: "storage",
			want: QuarantineStorage,
			file: func(t *testing.T, dir string) {
				bad := currentTestStamp(t)
				bad.storageVersion = "v0.0.0-someotherduckdb"
				createTestFile(t, filepath.Join(dir, deltaFileName(0)), allTierTables(), bad, nil)
			},
		},
		{
			axis: "statshouse",
			want: QuarantineStatshouse,
			file: func(t *testing.T, dir string) {
				bad := currentTestStamp(t)
				bad.statshouseVersion = "some-other-binary"
				createTestFile(t, filepath.Join(dir, deltaFileName(0)), allTierTables(), bad, nil)
			},
		},
		{
			axis: "unreadable",
			want: QuarantineUnreadable,
			file: func(t *testing.T, dir string) {
				require.NoError(t, os.WriteFile(filepath.Join(dir, deltaFileName(0)), []byte("not a duckdb file at all"), 0o644))
			},
		},
	} {
		t.Run(tc.axis, func(t *testing.T) {
			dir := t.TempDir()
			tc.file(t, dir)

			m := &recordingMetrics{}
			s, err := OpenStore(StoreConfig{
				Dir:               dir,
				StatshouseVersion: testStatshouseVersion,
				Metrics:           m,
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = s.Close() })

			require.Len(t, m.quarantine, 1, "the open must report the axis it excluded files on")
			require.Equal(t, tc.want, m.quarantine[0].axis)
			require.Equal(t, 1, m.quarantine[0].count)
		})
	}

	t.Run("valid_files_report_nothing", func(t *testing.T) {
		m := &recordingMetrics{}
		openTestStoreWithMetrics := func() {
			s, err := OpenStore(StoreConfig{
				Dir:               t.TempDir(),
				StatshouseVersion: testStatshouseVersion,
				Metrics:           m,
			})
			require.NoError(t, err)
			_ = s.Close()
		}
		openTestStoreWithMetrics()
		require.Empty(t, m.quarantine)
	})
}

// TestRenderersReportQueryEvents proves both query verbs report one event per
// call — success and failure alike — carrying the verb and outcome: the event
// stream is the query load.
func TestRenderersReportQueryEvents(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	row := Row{Metric: testMetricID, Time: uint32(b1 + 5), Tags: tag0(11), Count: 3, Sum: 9}
	require.NoError(t, w.WriteRound(context.Background(), []Row{row}))

	m := &recordingMetrics{}
	s.cfg.Metrics = m

	// series: one ok, one failed
	r := renderSeriesSorted(t, s, 1, seriesReq(testMetricID, twoMappedKinds,
		[]int32{int32(data_model.DigestCount)}, []int32{0}, b1, b1+60, 60))
	require.Len(t, r.count, 1)
	q := seriesReq(testMetricID, twoMappedKinds, []int32{int32(data_model.DigestCount)}, []int32{0}, b1, b1+60, 0)
	requireBadRequest(t, renderSeriesErr(t, s, 1, q), "step_sec")

	// tag values: one ok, one failed
	resp := renderTagValues(t, s, tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+60, 60))
	require.Len(t, resp.Tag, 1)
	tv := tagValuesReq(testMetricID, twoMappedKinds, format.MaxTags, b1, b1+60, 60)
	_, err := s.RenderTagValues(context.Background(), tv)
	require.Error(t, err)

	require.Len(t, m.queries, 4)
	expect := []struct {
		verb QueryVerb
		err  bool
	}{
		{QuerySeries, false},
		{QuerySeries, true},
		{QueryTagValues, false},
		{QueryTagValues, true},
	}
	for i, e := range expect {
		got := m.queries[i]
		require.Equal(t, e.verb, got.verb)
		require.GreaterOrEqual(t, got.dur, time.Duration(0))
		if e.err {
			require.Error(t, got.err)
		} else {
			require.NoError(t, got.err)
		}
	}
}

// TestSampleStoreSizeMeasuresBothLocations proves a size sample reports both
// locations with the database-size pragma's numbers: the delta grows past
// zero once rows land, and the archive appears once compaction builds
// windows.
func TestSampleStoreSizeMeasuresBothLocations(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNowUnix)
	m := &recordingMetrics{}

	// rows in the active delta: the delta location must measure non-zero
	row := partialRow(t, testMetricID, now-5)
	row.Count, row.Sum = 1, 1
	require.NoError(t, w.WriteRound(context.Background(), []Row{row}))
	s.SampleStoreSize(m)
	require.Len(t, m.sizes, 2)
	require.Equal(t, SizeDelta, m.sizes[0].location)
	require.Greater(t, m.sizes[0].used, int64(0), "rows landed in the delta, so used bytes must be non-zero")
	require.Equal(t, SizeArchive, m.sizes[1].location)
	require.Zero(t, m.sizes[1].used, "no archive windows exist yet")

	// compaction moves the rows into archive windows: both locations report
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(context.Background()))
	m.sizes = nil
	s.SampleStoreSize(m)
	require.Len(t, m.sizes, 2)
	require.Equal(t, SizeDelta, m.sizes[0].location)
	require.Equal(t, SizeArchive, m.sizes[1].location)
	require.Greater(t, m.sizes[1].used, int64(0), "windows hold the compacted rows, so used bytes must be non-zero")
}

// TestRunSizeSamplerSamplesImmediatelyAndStops proves the sampler's protocol:
// one sample lands before Run returns control to its caller's expectations
// (recorded synchronously through the recorder), and a canceled context ends
// the loop.
func TestRunSizeSamplerSamplesImmediatelyAndStops(t *testing.T) {
	s, _ := newTestWriter(t)

	ctx, cancel := context.WithCancel(context.Background())
	m := &recordingMetrics{sizes: nil}
	go func() { _ = RunSizeSampler(ctx, s, m, time.Hour) }()

	// the immediate sample reports both locations; then the long interval
	// keeps the loop quiet until the cancel
	require.Eventually(t, func() bool { return len(m.snapshotSizes()) == 2 }, 5*time.Second, 10*time.Millisecond)
	cancel()
	// nothing more arrives: wait out a fraction of the interval
	time.Sleep(50 * time.Millisecond)
	require.Len(t, m.snapshotSizes(), 2)
}

// TestBuiltinMetricRowsFlowUnchanged proves builtin metrics — whose IDs are
// negative, __src_ingestion_status among them — travel the duck write path
// exactly like user metrics: the row that lands is the row written, ID, tags
// and aggregates byte for byte.
func TestBuiltinMetricRowsFlowUnchanged(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60

	rows := []Row{
		{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Time:   uint32(b1 + 5),
			Tags:   tag0(format.TagValueIDSrcIngestionStatusErrZeroCounter),
			Count:  2,
			Sum:    6,
		},
		{
			Metric: format.BuiltinMetricIDIngestionStatus,
			Time:   uint32(b1 + 41),
			Tags:   tag0(format.TagValueIDSrcIngestionStatusWarnMapTagSetTwice),
			Count:  1,
			Sum:    3,
		},
	}
	require.NoError(t, w.WriteRound(context.Background(), rows))

	// the read path the API serves: group by the status tag, exact counts
	// (rows arrive sorted by tag value: warn first, err second)
	r := renderSeriesSorted(t, s, 1, seriesReq(format.BuiltinMetricIDIngestionStatus, twoMappedKinds,
		[]int32{int32(data_model.DigestCount), int32(data_model.DigestSum)}, []int32{0}, b1, b1+60, 60))
	require.Len(t, r.count, 2)
	require.Equal(t, []int64{b1, b1}, r.time)
	require.Equal(t, []int64{
		format.TagValueIDSrcIngestionStatusWarnMapTagSetTwice,
		format.TagValueIDSrcIngestionStatusErrZeroCounter,
	}, r.tags[0], "the ingestion-status tag values must round-trip unchanged")
	require.Equal(t, []float64{1, 2}, r.count)
	require.Equal(t, []float64{3, 6}, r.sum)

	// and the tag-values verb sees the same unmapped-empty string halves
	resp := renderTagValues(t, s, tagValuesReq(format.BuiltinMetricIDIngestionStatus, twoMappedKinds, 0, b1, b1+60, 60))
	require.Equal(t, []int64{
		format.TagValueIDSrcIngestionStatusErrZeroCounter,
		format.TagValueIDSrcIngestionStatusWarnMapTagSetTwice,
	}, resp.Tag)
	require.Equal(t, []float64{2, 1}, resp.Count)
}

// TestRenderTagValuesNilRecorderIsQuiet proves a nil recorder — every config
// field is optional — changes no behavior: queries succeed as usual.
func TestRenderTagValuesNilRecorderIsQuiet(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	row := Row{Metric: testMetricID, Time: uint32(b1 + 5), Tags: tag0(11), Count: 1}
	require.NoError(t, w.WriteRound(context.Background(), []Row{row}))
	require.Nil(t, s.cfg.Metrics)

	resp := renderTagValues(t, s, tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+60, 60))
	require.Len(t, resp.Tag, 1)
}

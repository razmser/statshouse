// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package aggregator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/metajournal"
)

// sinkNowUnix is the frozen clock the duck sink tests run under — the writer's
// age guard needs a NowFunc, which openDuckStore cannot inject.
const (
	sinkNowUnix      = int64(1740000000 + 100) // % 60 == 40: truncation is observable
	sinkTestMetricID = int32(10)
)

var sinkNow = time.Unix(sinkNowUnix, 0)

// newTestDuckSink opens a store and writer directly, the way openDuckStore
// does but under the frozen clock, keeping the store handle for assertions.
func newTestDuckSink(t *testing.T, cfg duckstore.WriterConfig) (*duckstore.Store, *duckSink) {
	t.Helper()
	s, err := duckstore.OpenStore(duckstore.StoreConfig{Dir: t.TempDir(), Logf: t.Logf})
	require.NoError(t, err)
	cfg.NowFunc = func() time.Time { return sinkNow }
	w, err := duckstore.NewWriter(s, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = w.Close()
		_ = s.Close()
	})
	return s, newDuckSink(w)
}

// sinkTestInsertRow builds one fully populated insertRow, the conveyor-side
// counterpart of duckstore's testRow: every aggregate, both aggregate states,
// both host encodings and tags in both encodings.
func sinkTestInsertRow(metric int32, ts uint32) insertRow {
	row := insertRow{
		key:         data_model.Key{Timestamp: ts, Metric: metric},
		top:         data_model.TagUnion{S: "duck top"},
		count:       3,
		min:         1.5,
		max:         9.75,
		sum:         21,
		sumSquare:   101.25,
		percentiles: []byte{1, 2, 3, 4},
		unique:      []byte{5, 6, 7},
		minHost:     hostPair{tag: data_model.TagUnion{I: 7}, value: 0.5},
		maxHost:     hostPair{tag: data_model.TagUnion{S: "max host"}, value: 0.5},
	}
	row.key.Tags[0] = 11
	row.key.STags[1] = "raw tag"
	row.key.Tags[2] = 13
	row.key.STags[2] = "ignored when id set"
	return row
}

// sinkTierCount counts the rows one metric has in one tier.
func sinkTierCount(t *testing.T, s *duckstore.Store, tier string, metric int32) int {
	t.Helper()
	var n int
	require.NoError(t, s.Delta().QueryRow(
		fmt.Sprintf(`SELECT count(*) FROM %s WHERE metric = $1`, duckstore.TierTable(tier)),
		metric).Scan(&n))
	return n
}

// TestDuckSinkRoundLandsInStore drives one append/send cycle end to end and
// checks the conversion: the resolved row's tags, string top, hosts, aggregate
// states and aggregates must land decoded in the store — once, in the delta's
// 1s table at its raw second, with no coarser-tier table in the file (those
// tiers derive at compaction and read time) — and the per-row size accounting
// must match the ClickHouse encoder's.
func TestDuckSinkRoundLandsInStore(t *testing.T) {
	s, sink := newTestDuckSink(t, duckstore.WriterConfig{})
	ts := uint32(sinkNowUnix - 37) // inside the guard, second 63 of its minute
	row := sinkTestInsertRow(sinkTestMetricID, ts)

	require.Equal(t, rowBinarySize(&row), sink.AppendRow(&row))
	require.Equal(t, rowBinarySize(&row), sink.RoundSize(), "one row's size must be the round's size")

	status, exception, _, err := sink.Send(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Zero(t, exception)

	// the row lands once, at its raw second, and no coarser-tier table
	// exists in the delta
	require.Equal(t, 1, sinkTierCount(t, s, duckstore.Tier1s, sinkTestMetricID), "the 1s tier must hold the row")
	var gotTS int64
	require.NoError(t, s.Delta().QueryRow(
		fmt.Sprintf(`SELECT time FROM %s WHERE metric = $1`, duckstore.TierTable(duckstore.Tier1s)),
		sinkTestMetricID).Scan(&gotTS))
	require.EqualValues(t, ts, gotTS, "1s time is the row's own second")
	var coarse int
	require.NoError(t, s.Delta().QueryRow(
		`SELECT count(*) FROM duckdb_tables() WHERE table_name IN ('s1m', 's1h')`).Scan(&coarse))
	require.Zero(t, coarse, "the delta must hold the 1s table alone")

	// the conversion: tags in both encodings, the string top in slot 47, and
	// an id with a string keeping only its id half
	var tag0, tag2, tag47 int32
	var stag1, stag2, stag47 string
	var count, min, max, sum, sumsquare float64
	require.NoError(t, s.Delta().QueryRow(
		`SELECT tag0, stag1, tag2, stag2, tag47, stag47, count, min, max, sum, sumsquare FROM s1 WHERE metric = $1`,
		sinkTestMetricID).Scan(&tag0, &stag1, &tag2, &stag2, &tag47, &stag47, &count, &min, &max, &sum, &sumsquare))
	require.EqualValues(t, 11, tag0)
	require.Equal(t, "raw tag", stag1)
	require.EqualValues(t, 13, tag2)
	require.Empty(t, stag2, "a tag with an id must not store its string half")
	require.Zero(t, tag47, "the string top must land through its own columns")
	require.Equal(t, "duck top", stag47)
	require.EqualValues(t, 3, count)
	require.EqualValues(t, 1.5, min)
	require.EqualValues(t, 9.75, max)
	require.EqualValues(t, 21, sum)
	require.EqualValues(t, 101.25, sumsquare)

	// hosts and aggregate states are stored verbatim, the skewed state values included
	var minHost, maxHost int32
	var minShost, maxShost string
	var minHostValue, maxHostValue float64
	var percentiles, uniq []byte
	require.NoError(t, s.Delta().QueryRow(
		`SELECT min_host, min_shost, min_host_value, max_host, max_shost, max_host_value, percentiles, uniq_state FROM s1 WHERE metric = $1`,
		sinkTestMetricID).Scan(&minHost, &minShost, &minHostValue, &maxHost, &maxShost, &maxHostValue, &percentiles, &uniq))
	require.EqualValues(t, 7, minHost)
	require.Empty(t, minShost)
	require.Zero(t, maxHost)
	require.Equal(t, "max host", maxShost)
	require.Equal(t, 0.5, minHostValue, "the argMin state's skewed payload must survive the conversion")
	require.Equal(t, 0.5, maxHostValue)
	require.Equal(t, []byte{1, 2, 3, 4}, percentiles)
	require.Equal(t, []byte{5, 6, 7}, uniq)
}

// TestDuckSinkSendMapsFailure maps a writer failure onto the conveyor's
// quadruple — zero status and the error, the shape that keeps contributors
// unacked — and proves a later round succeeds again.
func TestDuckSinkSendMapsFailure(t *testing.T) {
	s, sink := newTestDuckSink(t, duckstore.WriterConfig{
		FlushFault: func(round int64) error {
			if round != 1 {
				return nil // only the first round fails
			}
			return fmt.Errorf("round %d: simulated disk failure", round)
		},
	})
	row := sinkTestInsertRow(sinkTestMetricID, uint32(sinkNowUnix))
	sink.AppendRow(&row)

	status, exception, _, err := sink.Send(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "duck-store insert round failed")
	require.Contains(t, err.Error(), "simulated disk failure")
	require.Zero(t, status)
	require.Zero(t, exception)
	require.Equal(t, 0, sinkTierCount(t, s, duckstore.Tier1s, sinkTestMetricID), "the failed round must not land")

	// the conveyor would reset and retry the round through a fresh append
	sink.Reset()
	row2 := sinkTestInsertRow(sinkTestMetricID+1, uint32(sinkNowUnix))
	sink.AppendRow(&row2)
	status, exception, _, err = sink.Send(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Zero(t, exception)
	require.Equal(t, 1, sinkTierCount(t, s, duckstore.Tier1s, sinkTestMetricID+1))
}

// TestDuckSinkRoundSizeAndReset checks the size accounting and the reset
// contract across multiple rows and rounds.
func TestDuckSinkRoundSizeAndReset(t *testing.T) {
	_, sink := newTestDuckSink(t, duckstore.WriterConfig{})
	now := uint32(sinkNowUnix)

	row1 := sinkTestInsertRow(sinkTestMetricID, now)
	row2 := sinkTestInsertRow(sinkTestMetricID+1, now)
	n1 := sink.AppendRow(&row1)
	n2 := sink.AppendRow(&row2)
	require.Equal(t, rowBinarySize(&row1), n1)
	require.Equal(t, rowBinarySize(&row2), n2)
	require.Equal(t, n1+n2, sink.RoundSize())

	sink.Reset()
	require.Zero(t, sink.RoundSize())

	// a reset round is a successful no-op write, and new rows accumulate from zero
	row3 := sinkTestInsertRow(sinkTestMetricID, now)
	n3 := sink.AppendRow(&row3)
	require.Equal(t, n1, n3)
	require.Equal(t, n3, sink.RoundSize())
	status, _, _, err := sink.Send(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
}

// TestDuckSinkCopiesAggregateStateBytes guards the duck sink's side of the
// scratch contract: AppendRow must copy the aggregate-state bytes, because the conveyor reuses
// the scratch they were encoded into before Send ever runs.
func TestDuckSinkCopiesAggregateStateBytes(t *testing.T) {
	s, sink := newTestDuckSink(t, duckstore.WriterConfig{})
	row := sinkTestInsertRow(sinkTestMetricID, uint32(sinkNowUnix))
	sink.AppendRow(&row)

	// the conveyor's next resolve overwrites the scratch under the same slices
	for i := range row.percentiles {
		row.percentiles[i] = 0xAA
	}
	for i := range row.unique {
		row.unique[i] = 0xBB
	}

	_, _, _, err := sink.Send(context.Background())
	require.NoError(t, err)
	var percentiles, uniq []byte
	require.NoError(t, s.Delta().QueryRow(
		`SELECT percentiles, uniq_state FROM s1 WHERE metric = $1`, sinkTestMetricID).
		Scan(&percentiles, &uniq))
	require.Equal(t, []byte{1, 2, 3, 4}, percentiles, "AppendRow must have copied the percentiles")
	require.Equal(t, []byte{5, 6, 7}, uniq, "AppendRow must have copied the uniques")
}

// TestOpenDuckStore covers the plumbing: the dir must be set, and a set dir
// yields a handle that produces working sinks and closes cleanly. A nil
// metrics agent is allowed — the store and its maintenance run without
// observability in that case.
func TestOpenDuckStore(t *testing.T) {
	t.Run("empty_dir_is_rejected", func(t *testing.T) {
		_, err := openDuckStore(ConfigAggregator{}, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "store directory is not set")
	})

	t.Run("opens_and_closes", func(t *testing.T) {
		handle, err := openDuckStore(ConfigAggregator{DuckStoreDir: t.TempDir()}, nil)
		require.NoError(t, err)
		require.NotNil(t, handle)

		sink, ok := handle.NewSink().(*duckSink)
		require.True(t, ok, "the handle's sinks must be duck sinks")
		_, _, _, err = sink.Send(context.Background())
		require.NoError(t, err, "an empty round is a no-op success")

		require.NoError(t, handle.Close())
	})
}

// TestDuckMetricsTagMappings proves the duck*Tag helpers name every event
// value the store emits with the constant its metric's value comments
// document, and collapse anything unknown to 0 — the value no comment names.
func TestDuckMetricsTagMappings(t *testing.T) {
	require.Equal(t, int32(format.TagValueIDStatusOK), duckStatusTag(nil))
	require.Equal(t, int32(format.TagValueIDStatusError), duckStatusTag(errors.New("boom")))

	require.Equal(t, int32(format.TagValueIDDuckMaintenanceCompaction), duckMaintenanceTag(duckstore.MaintenanceCompaction))
	require.Equal(t, int32(format.TagValueIDDuckMaintenanceSealing), duckMaintenanceTag(duckstore.MaintenanceSealing))
	require.Equal(t, int32(format.TagValueIDDuckMaintenanceRetention), duckMaintenanceTag(duckstore.MaintenanceRetention))
	require.Zero(t, duckMaintenanceTag(duckstore.MaintenanceKind("other")))

	require.Equal(t, int32(format.TagValueIDDuckWindowSealed), duckWindowEventTag(duckstore.WindowSealed))
	require.Equal(t, int32(format.TagValueIDDuckWindowUnlinked), duckWindowEventTag(duckstore.WindowUnlinked))
	require.Equal(t, int32(format.TagValueIDDuckWindowEarlyEvicted), duckWindowEventTag(duckstore.WindowEarlyEvicted))
	require.Equal(t, int32(format.TagValueIDDuckWindowLeaseDeferred), duckWindowEventTag(duckstore.WindowLeaseDeferred))
	require.Zero(t, duckWindowEventTag(duckstore.WindowEventKind("other")))

	require.Equal(t, int32(format.TagValueIDDuckTier1s), duckTierTag(duckstore.Tier1s))
	require.Equal(t, int32(format.TagValueIDDuckTier1m), duckTierTag(duckstore.Tier1m))
	require.Equal(t, int32(format.TagValueIDDuckTier1h), duckTierTag(duckstore.Tier1h))
	require.Zero(t, duckTierTag("no such tier"))

	require.Equal(t, int32(format.TagValueIDDuckQuarantineDeltaSchema), duckQuarantineAxisTag(duckstore.QuarantineDeltaSchema))
	require.Equal(t, int32(format.TagValueIDDuckQuarantineArchiveSchema), duckQuarantineAxisTag(duckstore.QuarantineArchiveSchema))
	require.Equal(t, int32(format.TagValueIDDuckQuarantineStorage), duckQuarantineAxisTag(duckstore.QuarantineStorage))
	require.Equal(t, int32(format.TagValueIDDuckQuarantineStatshouse), duckQuarantineAxisTag(duckstore.QuarantineStatshouse))
	require.Equal(t, int32(format.TagValueIDDuckQuarantineUnreadable), duckQuarantineAxisTag(duckstore.QuarantineUnreadable))
	require.Zero(t, duckQuarantineAxisTag(duckstore.QuarantineAxis("other")))

	require.Equal(t, int32(format.TagValueIDDuckQuerySeries), duckQueryVerbTag(duckstore.QuerySeries))
	require.Equal(t, int32(format.TagValueIDDuckQueryTagValues), duckQueryVerbTag(duckstore.QueryTagValues))
	require.Zero(t, duckQueryVerbTag(duckstore.QueryVerb("other")))

	require.Equal(t, int32(format.TagValueIDDuckSizeDelta), duckSizeLocationTag(duckstore.SizeDelta))
	require.Equal(t, int32(format.TagValueIDDuckSizeArchive), duckSizeLocationTag(duckstore.SizeArchive))
	require.Zero(t, duckSizeLocationTag(duckstore.SizeLocation("other")))
}

// duckValueCounterCall and duckCounterCall record one forwarded event: the
// builtin metric it landed in, the full ordered tag slice and the numbers.
type duckValueCounterCall struct {
	meta  *format.MetricMetaValue
	tags  []int32
	value float64
	count float64
}

type duckCounterCall struct {
	meta *format.MetricMetaValue
	tags []int32
	n    float64
}

type capturingDuckSink struct {
	valueCounters []duckValueCounterCall
	counters      []duckCounterCall
}

func (c *capturingDuckSink) AddValueCounter(_ uint32, meta *format.MetricMetaValue, tags []int32, value, count float64) {
	c.valueCounters = append(c.valueCounters, duckValueCounterCall{meta, tags, value, count})
}

func (c *capturingDuckSink) AddCounter(_ uint32, meta *format.MetricMetaValue, tags []int32, n float64) {
	c.counters = append(c.counters, duckCounterCall{meta, tags, n})
}

// TestDuckMetricsForwardsEvents proves the forwarding itself — which builtin
// metric every store event lands in, with which ordered tag literals and
// which value/count — not just the duck*Tag mappings: a transposed tag or a
// swapped value/count here misattributes the operator surface documented in
// docs/duck-store.md, silently, because nothing else exercises these calls.
func TestDuckMetricsForwardsEvents(t *testing.T) {
	sink := &capturingDuckSink{}
	m := &duckMetrics{sh: sink}

	m.MaintenancePass(duckstore.MaintenanceCompaction, nil, 2500*time.Millisecond)
	m.MaintenancePass(duckstore.MaintenanceSealing, errors.New("boom"), 500*time.Millisecond)
	require.Equal(t, []duckValueCounterCall{
		{format.BuiltinMetricMetaDuckMaintenanceTime,
			[]int32{0, format.TagValueIDDuckMaintenanceCompaction, format.TagValueIDStatusOK}, 2.5, 1},
		{format.BuiltinMetricMetaDuckMaintenanceTime,
			[]int32{0, format.TagValueIDDuckMaintenanceSealing, format.TagValueIDStatusError}, 0.5, 1},
	}, sink.valueCounters)

	m.MaintenanceWindow(duckstore.WindowSealed, duckstore.Tier1m)
	require.Equal(t, []duckCounterCall{
		{format.BuiltinMetricMetaDuckWindows,
			[]int32{0, format.TagValueIDDuckWindowSealed, format.TagValueIDDuckTier1m}, 1},
	}, sink.counters)

	m.QuarantinedFiles(duckstore.QuarantineStorage, 3)
	require.Equal(t, []duckCounterCall{
		{format.BuiltinMetricMetaDuckWindows,
			[]int32{0, format.TagValueIDDuckWindowSealed, format.TagValueIDDuckTier1m}, 1},
		{format.BuiltinMetricMetaDuckQuarantinedFiles,
			[]int32{0, format.TagValueIDDuckQuarantineStorage}, 3},
	}, sink.counters)

	sink.valueCounters = nil
	m.StoreQuery(duckstore.QueryTagValues, errors.New("boom"), 500*time.Millisecond)
	require.Equal(t, []duckValueCounterCall{
		{format.BuiltinMetricMetaDuckQueryTime,
			[]int32{0, format.TagValueIDDuckQueryTagValues, format.TagValueIDStatusError}, 0.5, 1},
	}, sink.valueCounters)

	sink.valueCounters = nil
	m.StoreSize(duckstore.SizeArchive, 100, 40)
	require.Equal(t, []duckValueCounterCall{
		{format.BuiltinMetricMetaDuckStoreSize,
			[]int32{0, format.TagValueIDDuckSizeArchive, format.TagValueIDDuckSizeUsed}, 100, 1},
		{format.BuiltinMetricMetaDuckStoreSize,
			[]int32{0, format.TagValueIDDuckSizeArchive, format.TagValueIDDuckSizeFree}, 40, 1},
	}, sink.valueCounters)

	sink.valueCounters = nil
	m.StoreBacklog(2, 90*time.Second)
	m.MaintenanceAge(duckstore.MaintenanceSealing, 30*time.Second)
	require.Equal(t, []duckValueCounterCall{
		{format.BuiltinMetricMetaDuckBacklog,
			[]int32{0, format.TagValueIDDuckBacklogGenerations}, 2, 1},
		{format.BuiltinMetricMetaDuckBacklog,
			[]int32{0, format.TagValueIDDuckBacklogOldestAgeSeconds}, 90, 1},
		{format.BuiltinMetricMetaDuckMaintenanceAge,
			[]int32{0, format.TagValueIDDuckMaintenanceSealing}, 30, 1},
	}, sink.valueCounters)
}

// TestNewInsertSinkRoutesByBackend pins the write seam's routing: an
// aggregator holding a duck-store handle produces the store's sinks, and one
// without it produces the ClickHouse inserter.
func TestNewInsertSinkRoutesByBackend(t *testing.T) {
	sinkFromHandle := &stubDuckSink{}
	a := &Aggregator{duckStore: stubDuckHandle{sink: sinkFromHandle}}
	got := a.newInsertSink(nil)
	require.Same(t, sinkFromHandle, got, "a non-nil duck store must produce its sinks")

	a = &Aggregator{}
	require.IsType(t, &clickhouseSink{}, a.newInsertSink(nil), "without duck-store the sink is the ClickHouse inserter")
}

type stubDuckSink struct{}

func (stubDuckSink) AppendRow(*insertRow) int { return 0 }
func (stubDuckSink) Send(context.Context) (int, int, time.Duration, error) {
	return 0, 0, 0, nil
}
func (stubDuckSink) RoundSize() int { return 0 }
func (stubDuckSink) Reset()         {}

type stubDuckHandle struct {
	sink InsertSink
}

func (h stubDuckHandle) NewSink() InsertSink { return h.sink }
func (stubDuckHandle) QueryExecutor(*metajournal.MetricsStorage, int32) storeQueryExecutor {
	return nil
}
func (stubDuckHandle) Close() error { return nil }

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	// the month-LOD validator needs IANA zones even on a tzdata-less runner
	"testing"
	_ "time/tzdata"

	"github.com/hrissan/tdigest"
	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
)

// The series renderer's tests. Everything is asserted by decoded value —
// never on the built SQL — and the seeded rows go through the real writer, so
// the renderer reads exactly what ingestion lands.

// twoMappedKinds is the layout most tests use: tag0 and tag1 mapped, so both
// the id and the string half exist.
var twoMappedKinds = []int32{tagKindMapped, tagKindMapped}

// seriesReq builds a storeQuerySeries over one metric with the given layout,
// selectors, grouping and LOD window (UTC, utc_offset 0 unless overridden).
func seriesReq(metric int32, kinds []int32, what, by []int32, from, to, step int64) tlstatshouse.StoreQuerySeries {
	return tlstatshouse.StoreQuerySeries{
		Base: tlstatshouse.StoreQueryBase{
			MetricId:  metric,
			TagLayout: tlstatshouse.StoreTagLayout{Kinds: kinds},
			Lod:       tlstatshouse.StoreLod{FromSec: from, ToSec: to, StepSec: step},
		},
		What: what,
		By:   by,
	}
}

// seriesRows is the response flattened across batches into parallel vectors,
// the shape assertions are convenient against. Only requested columns are
// filled, mirroring the response's conditional vectors.
type seriesRows struct {
	time         []int64
	tags         [][]int64
	stags        [][]string
	count        []float64
	min          []float64
	max          []float64
	sum          []float64
	sumsquare    []float64
	cardinality  []float64
	percentiles  []string
	uniqState    []string
	minHostValue []float64
	minHostTag   []int32
	minHostStag  []string
	maxHostValue []float64
	maxHostTag   []int32
	maxHostStag  []string
}

// flattenSeries concatenates the response's batches, checking each batch's
// vectors are row-complete.
func flattenSeries(t *testing.T, resp tlstatshouse.StoreSeriesResponse) seriesRows {
	t.Helper()
	var r seriesRows
	for _, b := range resp.Batches {
		require.Equal(t, int(b.Rows), len(b.Time), "batch rows vs time vector")
		for i, v := range b.Tag {
			require.Equal(t, int(b.Rows), len(v), "batch rows vs tag vector %d", i)
		}
		r.time = append(r.time, b.Time...)
		for i := range b.Tag {
			for len(r.tags) <= i {
				r.tags = append(r.tags, nil)
				r.stags = append(r.stags, nil)
			}
			r.tags[i] = append(r.tags[i], b.Tag[i]...)
			r.stags[i] = append(r.stags[i], b.Stag[i]...)
		}
		if b.IsSetCount() {
			r.count = append(r.count, b.Count...)
		}
		if b.IsSetMin() {
			r.min = append(r.min, b.Min...)
		}
		if b.IsSetMax() {
			r.max = append(r.max, b.Max...)
		}
		if b.IsSetSum() {
			r.sum = append(r.sum, b.Sum...)
		}
		if b.IsSetSumsquare() {
			r.sumsquare = append(r.sumsquare, b.Sumsquare...)
		}
		if b.IsSetCardinality() {
			r.cardinality = append(r.cardinality, b.Cardinality...)
		}
		if b.IsSetPercentiles() {
			r.percentiles = append(r.percentiles, b.Percentiles...)
		}
		if b.IsSetUniqState() {
			r.uniqState = append(r.uniqState, b.UniqState...)
		}
		if b.IsSetMinHostValue() {
			r.minHostValue = append(r.minHostValue, b.MinHostValue...)
			r.minHostTag = append(r.minHostTag, b.MinHostTag...)
			r.minHostStag = append(r.minHostStag, b.MinHostStag...)
		}
		if b.IsSetMaxHostValue() {
			r.maxHostValue = append(r.maxHostValue, b.MaxHostValue...)
			r.maxHostTag = append(r.maxHostTag, b.MaxHostTag...)
			r.maxHostStag = append(r.maxHostStag, b.MaxHostStag...)
		}
	}
	return r
}

// renderSeriesSorted runs one series query with sort_asc, so the flattened
// rows arrive ordered by (time, tags) and assertions can index them directly.
func renderSeriesSorted(t *testing.T, s *Store, shardNum int32, args tlstatshouse.StoreQuerySeries) seriesRows {
	t.Helper()
	args.SetSortAsc(true)
	resp, err := s.RenderSeries(context.Background(), shardNum, args)
	require.NoError(t, err)
	require.Equal(t, shardNum, resp.ShardNum)
	return flattenSeries(t, resp)
}

// renderSeriesErr runs one series query and returns its error.
func renderSeriesErr(t *testing.T, s *Store, shardNum int32, args tlstatshouse.StoreQuerySeries) error {
	t.Helper()
	_, err := s.RenderSeries(context.Background(), shardNum, args)
	return err
}

// requireBadRequest asserts err is the structured bad_request and its
// description names the problem.
func requireBadRequest(t *testing.T, err error, contains string) {
	t.Helper()
	require.Error(t, err)
	code, ok := ErrorCode(err)
	require.True(t, ok, "not a structured store error: %v", err)
	require.Equal(t, ErrCodeBadRequest, code)
	require.Contains(t, err.Error(), contains)
}

// TestRenderSeriesExactAggregates seeds partial rows sharing buckets and tags
// and checks every plain aggregate folds to the exact value, per group. The
// stddev selector is covered by its three underlying columns, which is all the
// response carries; cardinality counts the stored rows of each group.
func TestRenderSeriesExactAggregates(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60 // minute-aligned
	rows := []Row{
		{Metric: testMetricID, Time: uint32(b1 + 5), Tags: tag0(11), Count: 3, Min: 1.5, Max: 9.75, Sum: 21, SumSquare: 101.25},
		{Metric: testMetricID, Time: uint32(b1 + 41), Tags: tag0(11), Count: 2, Min: 0.25, Max: 5, Sum: 10, SumSquare: 26.25},
		{Metric: testMetricID, Time: uint32(b1 + 59), Tags: tag0(12), Count: 1, Min: 4, Max: 4, Sum: 4, SumSquare: 16},
		{Metric: testMetricID, Time: uint32(b1 + 60), Tags: tag0(11), Count: 7, Min: 1, Max: 3, Sum: 14, SumSquare: 4},
	}
	require.NoError(t, w.WriteRound(context.Background(), rows))

	what := []int32{int32(data_model.DigestCount), int32(data_model.DigestSum),
		int32(data_model.DigestMin), int32(data_model.DigestMax), int32(data_model.DigestStdDev)}
	r := renderSeriesSorted(t, s, 1, seriesReq(testMetricID, twoMappedKinds, what, []int32{0}, b1, b1+120, 60))
	require.Len(t, r.time, 3)
	require.Equal(t, []int64{b1, b1, b1 + 60}, r.time)
	require.Equal(t, []int64{11, 12, 11}, r.tags[0])
	require.Equal(t, []string{"", "", ""}, r.stags[0], "no unmapped string values were written")
	require.Equal(t, []float64{5, 1, 7}, r.count)
	require.Equal(t, []float64{31, 4, 14}, r.sum)
	require.Equal(t, []float64{0.25, 4, 1}, r.min)
	require.Equal(t, []float64{9.75, 4, 3}, r.max)
	require.Equal(t, []float64{127.5, 16, 4}, r.sumsquare, "stddev's inputs travel exactly")

	// cardinality counts stored rows per group — two partial rows, not one
	r = renderSeriesSorted(t, s, 1, seriesReq(testMetricID, twoMappedKinds,
		[]int32{int32(data_model.DigestCardinality)}, []int32{0}, b1, b1+120, 60))
	require.Equal(t, []float64{2, 1, 1}, r.cardinality)
}

// tag0 returns the tag array of a row with tag0 set to v.
func tag0(v int32) [format.MaxTags]int32 {
	var tags [format.MaxTags]int32
	tags[0] = v
	return tags
}

// TestRenderSeriesEmptyWhat covers the tag-only series shape the
// cache-invalidation poll uses: no aggregate vectors at all, only time and
// the grouped tags.
func TestRenderSeriesEmptyWhat(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 3},
	}))

	resp, err := s.RenderSeries(context.Background(), 1,
		seriesReq(testMetricID, twoMappedKinds, nil, []int32{0}, b1, b1+60, 60))
	require.NoError(t, err)
	require.Len(t, resp.Batches, 1)
	b := resp.Batches[0]
	require.Equal(t, int32(1), b.Rows)
	require.Equal(t, []int64{b1}, b.Time)
	require.Equal(t, []int64{11}, b.Tag[0])
	require.Equal(t, uint32(0), b.FieldsMask, "no aggregate was requested")
	require.Empty(t, b.Count)
	require.Empty(t, b.Sum)
}

// TestRenderSeriesQuoteHeavyFilterValues proves parameterization behaviourally:
// unmapped string values and RE2 patterns full of quotes and backslashes
// filter correctly instead of breaking the statement. If any request value
// were interpolated into the SQL text, these cases would not even execute.
func TestRenderSeriesQuoteHeavyFilterValues(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	nasty := "o'brien\"\\x'--" // quote, backslash, quote, SQL comment marker
	rows := []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 1},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 1},
	}
	rows[0].STags[1] = nasty
	rows[1].STags[1] = "plain"
	require.NoError(t, w.WriteRound(context.Background(), rows))

	byTag1 := func(filters []tlstatshouse.StoreTagFilter) tlstatshouse.StoreQuerySeries {
		q := seriesReq(testMetricID, twoMappedKinds, []int32{int32(data_model.DigestCount)}, []int32{1}, b1, b1+60, 60)
		q.Base.FilterIn = filters
		return q
	}

	// exact string value with every quoting hazard in it
	var f tlstatshouse.StoreTagFilter
	f.TagIndex = 1
	f.SetValues([]string{nasty})
	r := renderSeriesSorted(t, s, 1, byTag1([]tlstatshouse.StoreTagFilter{f}))
	require.Len(t, r.time, 1, "only the nasty-valued row matches")
	require.Equal(t, []string{nasty}, r.stags[0])

	// an RE2 pattern with quotes, a backslash escape and an anchor
	f = tlstatshouse.StoreTagFilter{TagIndex: 1}
	f.SetRe2(`^o'brien"\\x.*--$`)
	r = renderSeriesSorted(t, s, 1, byTag1([]tlstatshouse.StoreTagFilter{f}))
	require.Len(t, r.time, 1, "the pattern matches only the nasty value")
	require.Equal(t, []string{nasty}, r.stags[0])

	// NOT IN with the same value excludes exactly that row
	q := seriesReq(testMetricID, twoMappedKinds, []int32{int32(data_model.DigestCount)}, []int32{1}, b1, b1+60, 60)
	q.Base.FilterNotIn = []tlstatshouse.StoreTagFilter{f}
	q.SetSortAsc(true)
	resp, err := s.RenderSeries(context.Background(), 1, q)
	require.NoError(t, err)
	require.Equal(t, []string{"plain"}, flattenSeries(t, resp).stags[0])

	// mapped ids filter normally alongside
	f = tlstatshouse.StoreTagFilter{TagIndex: 0}
	f.SetMapped([]int64{12})
	q = seriesReq(testMetricID, twoMappedKinds,
		[]int32{int32(data_model.DigestCount)}, []int32{0}, b1, b1+60, 60)
	q.Base.FilterIn = []tlstatshouse.StoreTagFilter{f}
	r = renderSeriesSorted(t, s, 1, q)
	require.Equal(t, []int64{12}, r.tags[0])
}

// TestRenderSeriesRe2FilterReplacesValues pins the builder's arm precedence on
// a filter carrying both string values and an RE2 pattern. Every PromQL
// negative-regex matcher arrives exactly this way (the engine enumerates the
// known non-matching values into Values and sets Re2), and the pattern arm
// must REPLACE the values arm, as the ClickHouse builder's `else if` does — a
// values arm alongside NOT-match would exclude the enumerated rows the
// pattern itself keeps.
func TestRenderSeriesRe2FilterReplacesValues(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	rows := []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 1},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 1},
	}
	rows[0].STags[1] = "foo"   // a known non-matching value the pattern keeps
	rows[1].STags[1] = "bar.x" // the only value the pattern rejects
	require.NoError(t, w.WriteRound(context.Background(), rows))

	// the !~ shape: Values enumerates the non-matching values, Re2 the pattern
	f := tlstatshouse.StoreTagFilter{TagIndex: 1}
	f.SetValues([]string{"foo"})
	f.SetRe2(`^bar`)
	q := seriesReq(testMetricID, twoMappedKinds, []int32{int32(data_model.DigestCount)}, []int32{1}, b1, b1+60, 60)
	q.Base.FilterNotIn = []tlstatshouse.StoreTagFilter{f}
	r := renderSeriesSorted(t, s, 1, q)
	require.Equal(t, []string{"foo"}, r.stags[0],
		"the pattern arm alone decides: the enumerated non-matching value survives, as under ClickHouse")

	// the =~ shape: skipping the values arm changes nothing, every enumerated
	// value matches the pattern anyway
	f = tlstatshouse.StoreTagFilter{TagIndex: 1}
	f.SetValues([]string{"foo"})
	f.SetRe2(`^foo`)
	q = seriesReq(testMetricID, twoMappedKinds, []int32{int32(data_model.DigestCount)}, []int32{1}, b1, b1+60, 60)
	q.Base.FilterIn = []tlstatshouse.StoreTagFilter{f}
	r = renderSeriesSorted(t, s, 1, q)
	require.Equal(t, []string{"foo"}, r.stags[0])
}

// withFilterIn returns the request with its IN filters set.
func withFilterIn(q tlstatshouse.StoreQuerySeries, filters ...tlstatshouse.StoreTagFilter) tlstatshouse.StoreQuerySeries {
	q.Base.FilterIn = filters
	return q
}

// TestRenderSeriesEmptyFilterArm pins the empty-tag arm: a filter with only
// Empty set matches rows that lack the tag entirely, on both the IN and the
// NOT IN side, for a mapped tag (id zero AND string empty).
func TestRenderSeriesEmptyFilterArm(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 1},
		{Metric: testMetricID, Time: uint32(b1), Count: 2}, // tag0 empty
	}))

	var f tlstatshouse.StoreTagFilter
	f.TagIndex = 0
	f.SetEmpty(true)

	r := renderSeriesSorted(t, s, 1, withFilterIn(seriesReq(testMetricID, twoMappedKinds,
		[]int32{int32(data_model.DigestCount)}, []int32{0}, b1, b1+60, 60), f))
	require.Equal(t, []float64{2}, r.count, "the empty-tag row alone matches Empty")

	q := seriesReq(testMetricID, twoMappedKinds, []int32{int32(data_model.DigestCount)}, []int32{0}, b1, b1+60, 60)
	q.Base.FilterNotIn = []tlstatshouse.StoreTagFilter{f}
	q.SetSortAsc(true)
	resp, err := s.RenderSeries(context.Background(), 1, q)
	require.NoError(t, err)
	require.Equal(t, []float64{1}, flattenSeries(t, resp).count, "NOT Empty keeps the tagged row")
}

// TestRenderSeriesHostsByIdentifyingValue seeds partial rows whose skewed
// state values disagree with the true extremes, so a correct host column must
// pick the host of the smallest/largest SKEW — the value-weighted sample
// ClickHouse's argMin/argMax states produce — not the host of the extreme
// row, and must serve that skew back as the value. max_host switches to the
// max-count pair exactly when `what` does not include max.
func TestRenderSeriesHostsByIdentifyingValue(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	rows := []Row{
		{
			Metric: testMetricID, Time: uint32(b1), Tags: tag0(11),
			Count: 3, Min: 1.5, Max: 9.75, Sum: 21,
			MinHost:      HostPair{Tag: HostTag{ID: 7}, Value: 0.4},
			MaxHost:      HostPair{Tag: HostTag{ID: 7}, Value: 5},
			MaxCountHost: HostPair{Tag: HostTag{ID: 100}, Value: 2},
		},
		{
			Metric: testMetricID, Time: uint32(b1), Tags: tag0(11),
			Count: 9, Min: 0.5, Max: 5, Sum: 9,
			MinHost:      HostPair{Tag: HostTag{ID: 9}, Value: 0.9},  // holds the true min, larger skew
			MaxHost:      HostPair{Tag: HostTag{S: "big"}, Value: 8}, // holds the smaller max, larger skew
			MaxCountHost: HostPair{Tag: HostTag{S: "chost"}, Value: 1},
		},
	}
	require.NoError(t, w.WriteRound(context.Background(), rows))

	// min_host always rides its own skew; max_host rides max's because `what`
	// has max
	withMax := seriesReq(testMetricID, twoMappedKinds,
		[]int32{int32(data_model.DigestCount), int32(data_model.DigestMin), int32(data_model.DigestMax)}, nil, b1, b1+60, 60)
	withMax.SetMinHost(true)
	withMax.SetMaxHost(true)
	r := renderSeriesSorted(t, s, 1, withMax)
	require.Len(t, r.minHostValue, 1)
	require.Equal(t, 0.4, r.minHostValue[0], "the winning skew is served back, argMinMergeState's payload")
	require.Equal(t, int32(7), r.minHostTag[0], "the host of the smallest skew, not of the smallest min")
	require.Equal(t, "", r.minHostStag[0])
	require.Equal(t, 8.0, r.maxHostValue[0])
	require.Equal(t, "big", r.maxHostStag[0], "the host of the largest skew, not of the largest max")
	require.Equal(t, int32(0), r.maxHostTag[0])

	// without max in `what`, max_host switches to the max-count pair
	withoutMax := seriesReq(testMetricID, twoMappedKinds, []int32{int32(data_model.DigestCount)}, nil, b1, b1+60, 60)
	withoutMax.SetMaxHost(true)
	r = renderSeriesSorted(t, s, 1, withoutMax)
	require.Equal(t, 2.0, r.maxHostValue[0], "the max-count pair's own skew is served")
	require.Equal(t, int32(100), r.maxHostTag[0], "the winning host is the id one")
	require.Equal(t, "", r.maxHostStag[0])
}

// TestRenderSeriesFoldedStates checks the two sketch columns come back as one
// folded state per row, matching a direct Go merge of the same inputs by
// decoded value.
func TestRenderSeriesFoldedStates(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	rows := []Row{
		{
			Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 1,
			Percentiles: pctState(t, 1, 2, 3), Unique: uniqState(t, 1, 2, 3),
		},
		{
			Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 1,
			Percentiles: pctState(t, 7, 8), Unique: uniqState(t, 3, 4),
		},
	}
	require.NoError(t, w.WriteRound(context.Background(), rows))

	r := renderSeriesSorted(t, s, 1, seriesReq(testMetricID, twoMappedKinds,
		[]int32{int32(data_model.DigestPercentile), int32(data_model.DigestUnique)}, []int32{0}, b1, b1+60, 60))
	require.Len(t, r.percentiles, 1)
	require.Len(t, r.uniqState, 1)

	got := decodePct(t, []byte(r.percentiles[0]))
	want := tdigestOf(t, 1, 2, 3, 7, 8)
	for _, q := range []float64{0.01, 0.25, 0.5, 0.75, 0.99} {
		require.InDelta(t, want.Quantile(q), got.Quantile(q), 1e-6, "quantile %v", q)
	}
	uniq := decodeUniq(t, []byte(r.uniqState[0]))
	require.Equal(t, uint64(4), uniq.Size(true),
		"1,2,3 and 3,4 fold to four distinct")
}

// TestRenderSeriesDeltaPlusArchiveWindow writes rows, rolls the generation,
// consumes it into archive windows and writes fresh rows into the new delta:
// one query over the range must union the window and the delta, seeing every
// row exactly once, and the freshly written rows immediately. Afterwards no
// query alias may linger on the delta instance.
func TestRenderSeriesDeltaPlusArchiveWindow(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 1},
		{Metric: testMetricID, Time: uint32(b1 + 60), Tags: tag0(11), Count: 2},
	}))

	gen := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	require.NoError(t, s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{}))
	require.NotEmpty(t, s.Windows(), "consumption created archive windows")

	// fresh rows in the new delta land after the roll, with no flush or
	// maintenance step between write and read
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1 + 120), Tags: tag0(11), Count: 4},
	}))

	what := []int32{int32(data_model.DigestCount)}
	r := renderSeriesSorted(t, s, 3, seriesReq(testMetricID, twoMappedKinds, what, []int32{0}, b1, b1+180, 60))
	require.Equal(t, []int64{b1, b1 + 60, b1 + 120}, r.time)
	require.Equal(t, []float64{1, 2, 4}, r.count, "archive window and delta contribute their own buckets")

	// the same query again — attaches and detaches repeat cleanly
	r = renderSeriesSorted(t, s, 3, seriesReq(testMetricID, twoMappedKinds, what, []int32{0}, b1, b1+180, 60))
	require.Equal(t, []float64{1, 2, 4}, r.count)

	// a range covering only the window reads the window alone
	r = renderSeriesSorted(t, s, 3, seriesReq(testMetricID, twoMappedKinds, what, []int32{0}, b1, b1+60, 60))
	require.Equal(t, []float64{1}, r.count)

	var lingering int
	require.NoError(t, s.Delta().QueryRow(
		`SELECT count(*) FROM duckdb_databases() WHERE database_name LIKE 'q%'`).Scan(&lingering))
	require.Zero(t, lingering, "query aliases must be detached after the read")
}

// TestRenderSeriesRowLimit pins the LIMIT row_limit+1 contract: a query that
// produced more rows than the limit fails with row_limit and returns nothing,
// and a query exactly at the limit succeeds.
func TestRenderSeriesRowLimit(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	var rows []Row
	for i := 0; i < 5; i++ {
		rows = append(rows, Row{Metric: testMetricID, Time: uint32(b1 + 60*int64(i)), Count: 1})
	}
	require.NoError(t, w.WriteRound(context.Background(), rows))

	what := []int32{int32(data_model.DigestCount)}
	q := seriesReq(testMetricID, twoMappedKinds, what, nil, b1, b1+300, 60)
	q.Base.RowLimit = 4
	err := renderSeriesErr(t, s, 1, q)
	require.True(t, IsCode(err, ErrCodeRowLimit), "got %v", err)
	require.Contains(t, err.Error(), "at least 5")

	q.Base.RowLimit = 5
	resp, err := s.RenderSeries(context.Background(), 1, q)
	require.NoError(t, err)
	require.Len(t, flattenSeries(t, resp).time, 5, "exactly at the limit succeeds")
}

// TestRenderSeriesShardTagFromLiteral groups by the shard pseudo-tag and the
// stored tag0: the shard column comes from the literal the caller supplied,
// not from storage, and carries no string half.
func TestRenderSeriesShardTagFromLiteral(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 1},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 1},
	}))

	r := renderSeriesSorted(t, s, 17, seriesReq(testMetricID, twoMappedKinds,
		[]int32{int32(data_model.DigestCount)}, []int32{format.ShardTagIndex, 0}, b1, b1+60, 60))
	require.Equal(t, []int64{17, 17}, r.tags[0], "the shard column is the literal")
	require.Equal(t, []int64{11, 12}, r.tags[1])
	require.Empty(t, r.stags[0], "the shard entry has no string half")
	require.Len(t, r.stags[1], 2)
}

// TestRenderSeriesSortOrder checks the two explicit orders; a request with
// neither must still answer, its order its own choice.
func TestRenderSeriesSortOrder(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 1},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 1},
		{Metric: testMetricID, Time: uint32(b1 + 60), Tags: tag0(10), Count: 1},
	}))

	base := seriesReq(testMetricID, twoMappedKinds, []int32{int32(data_model.DigestCount)}, []int32{0}, b1, b1+120, 60)

	desc := base
	desc.SetSortDesc(true)
	resp, err := s.RenderSeries(context.Background(), 1, desc)
	require.NoError(t, err)
	r := flattenSeries(t, resp)
	require.Equal(t, []int64{b1 + 60, b1, b1}, r.time)
	require.Equal(t, []int64{10, 12, 11}, r.tags[0])

	asc := base
	asc.SetSortAsc(true)
	resp, err = s.RenderSeries(context.Background(), 1, asc)
	require.NoError(t, err)
	r = flattenSeries(t, resp)
	require.Equal(t, []int64{b1, b1, b1 + 60}, r.time)
	require.Equal(t, []int64{11, 12, 10}, r.tags[0])
}

// TestRenderSeriesRaw64GroupAndFilter covers the raw64 layout: grouping and
// filtering by the whole 64-bit value rebuilt from the two stored halves,
// including negative values.
func TestRenderSeriesRaw64GroupAndFilter(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	// kinds: tag0 raw64 over (tag0 lo, tag1 hi), tag1 raw32
	kinds := []int32{tagKindRaw64, tagKindRaw32}
	rows := []Row{
		{Metric: testMetricID, Time: uint32(b1), Count: 1}, // value 0
		{Metric: testMetricID, Time: uint32(b1), Count: 1}, // value -2
		{Metric: testMetricID, Time: uint32(b1), Count: 1}, // value 5
		{Metric: testMetricID, Time: uint32(b1), Count: 1}, // value 1<<32
	}
	rows[1].Tags[0], rows[1].Tags[1] = -2, -1 // hi<<32|lo = -2
	rows[2].Tags[0] = 5
	rows[3].Tags[0], rows[3].Tags[1] = 0, 1 // 1<<32
	require.NoError(t, w.WriteRound(context.Background(), rows))

	what := []int32{int32(data_model.DigestCount)}
	r := renderSeriesSorted(t, s, 1, seriesReq(testMetricID, kinds, what, []int32{0}, b1, b1+60, 60))
	require.Equal(t, []int64{-2, 0, 5, 1 << 32}, r.tags[0], "whole 64-bit values, negatives included")

	// the mapped arm of a raw64 filter matches the whole value
	f := tlstatshouse.StoreTagFilter{TagIndex: 0}
	f.SetMapped([]int64{-2, 1 << 32})
	r = renderSeriesSorted(t, s, 1, withFilterIn(seriesReq(testMetricID, kinds, what, []int32{0}, b1, b1+60, 60), f))
	require.Equal(t, []int64{-2, 1 << 32}, r.tags[0])

	// the empty arm of a raw64 filter is the zero value
	f = tlstatshouse.StoreTagFilter{TagIndex: 0}
	f.SetEmpty(true)
	r = renderSeriesSorted(t, s, 1, withFilterIn(seriesReq(testMetricID, kinds, what, []int32{0}, b1, b1+60, 60), f))
	require.Equal(t, []int64{0}, r.tags[0])
}

// TestRenderSeriesMonthBuckets checks the one genuinely timezone-dependent
// step: calendar months in a named zone. Rows are inserted directly (the
// writer's ingestion guard would drop months this old) into the 1h tier the
// step reads.
func TestRenderSeriesMonthBuckets(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir())
	// 2023-05-02 Moscow and 2023-11-15 Moscow: local months May and November 2023
	_, err := s.Delta().Exec(
		"INSERT INTO s1h (metric, time, count) VALUES ($1, $2, 1), ($1, $3, 2)",
		testMetricID, int64(1683000000), int64(1699999200))
	require.NoError(t, err)

	q := seriesReq(testMetricID, twoMappedKinds, []int32{int32(data_model.DigestCount)}, nil,
		1682899200, 1700000000, monthLodStep)
	q.Base.Lod.Location = "Europe/Moscow"
	r := renderSeriesSorted(t, s, 1, q)
	require.Equal(t, []int64{1682899200, 1698796800}, r.time, "local month starts as unix seconds")
	require.Equal(t, []float64{1, 2}, r.count)
}

// TestRenderSeriesBatchSplitting shrinks the batch target so a handful of rows
// spans several batches, and checks the split keeps every batch row-complete
// and the concatenation ordered.
func TestRenderSeriesBatchSplitting(t *testing.T) {
	old := seriesBatchTargetBytes
	seriesBatchTargetBytes = 50 // one time+count row estimates 16 bytes
	t.Cleanup(func() { seriesBatchTargetBytes = old })

	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	var rows []Row
	for i := 0; i < 6; i++ {
		rows = append(rows, Row{Metric: testMetricID, Time: uint32(b1 + 60*int64(i)), Count: 1})
	}
	require.NoError(t, w.WriteRound(context.Background(), rows))

	q := seriesReq(testMetricID, twoMappedKinds, []int32{int32(data_model.DigestCount)}, nil, b1, b1+360, 60)
	q.SetSortAsc(true)
	resp, err := s.RenderSeries(context.Background(), 1, q)
	require.NoError(t, err)
	require.Greater(t, len(resp.Batches), 1, "the shrunken target must split the rows")
	var times []int64
	for _, b := range resp.Batches {
		require.NotZero(t, b.Rows)
		require.Equal(t, int(b.Rows), len(b.Time))
		require.Equal(t, int(b.Rows), len(b.Count))
		times = append(times, b.Time...)
	}
	require.Equal(t, []int64{b1, b1 + 60, b1 + 120, b1 + 180, b1 + 240, b1 + 300}, times)
}

// TestRenderSeriesMetricFilter covers the three metric predicates: an exact
// id, an IN list and a NOT IN list.
func TestRenderSeriesMetricFilter(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Count: 1},
		{Metric: testMetricID2, Time: uint32(b1), Count: 1},
	}))

	what := []int32{int32(data_model.DigestCount)}
	r := renderSeriesSorted(t, s, 1, seriesReq(testMetricID, twoMappedKinds, what, nil, b1, b1+60, 60))
	require.Equal(t, []float64{1}, r.count, "exact id sees only its metric")

	q := seriesReq(0, twoMappedKinds, what, nil, b1, b1+60, 60)
	q.Base.SetMetricIn([]int32{testMetricID2})
	r = renderSeriesSorted(t, s, 1, q)
	require.Equal(t, []float64{1}, r.count, "the IN list selects the other metric")

	q = seriesReq(0, twoMappedKinds, what, nil, b1, b1+60, 60)
	q.Base.SetMetricNotIn([]int32{testMetricID2})
	r = renderSeriesSorted(t, s, 1, q)
	require.Equal(t, []float64{1}, r.count, "the NOT IN list keeps the first")
}

// TestRenderSeriesValidation walks the malformed-request table: each entry
// must fail as bad_request naming what was wrong, before any storage is
// touched.
func TestRenderSeriesValidation(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir())
	b1 := (writerNowUnix - 7200) / 60 * 60
	count := []int32{int32(data_model.DigestCount)}
	cases := []struct {
		name    string
		mutate  func(q *tlstatshouse.StoreQuerySeries)
		problem string
	}{
		{"step not a LOD step", func(q *tlstatshouse.StoreQuerySeries) { q.Base.Lod.StepSec = 7 },
			"step_sec 7 is not a LOD step"},
		{"negative step", func(q *tlstatshouse.StoreQuerySeries) { q.Base.Lod.StepSec = -60 },
			"step_sec -60 is not a LOD step"},
		{"what kind unspecified", func(q *tlstatshouse.StoreQuerySeries) { q.What = []int32{0} },
			"what kind 0 is not a digest selector"},
		{"what kind last", func(q *tlstatshouse.StoreQuerySeries) {
			q.What = []int32{int32(data_model.DigestLast)}
		}, "is not a digest selector"},
		{"both sort flags", func(q *tlstatshouse.StoreQuerySeries) {
			q.SetSortDesc(true)
			q.SetSortAsc(true)
		}, "sort_desc and sort_asc are both set"},
		{"by outside layout", func(q *tlstatshouse.StoreQuerySeries) { q.By = []int32{16} },
			"by tag 16 is outside the tag layout of 2 kinds"},
		{"by negative", func(q *tlstatshouse.StoreQuerySeries) { q.By = []int32{-2} },
			"by tag -2 is outside the tag layout"},
		{"filter outside layout", func(q *tlstatshouse.StoreQuerySeries) {
			f := tlstatshouse.StoreTagFilter{TagIndex: 9}
			f.SetMapped([]int64{1})
			q.Base.FilterIn = []tlstatshouse.StoreTagFilter{f}
		}, "filter on tag 9 is outside the tag layout"},
		{"unknown layout kind", func(q *tlstatshouse.StoreQuerySeries) {
			q.Base.TagLayout.Kinds = []int32{5, 0}
		}, "tag layout kind 5 at index 0 is not mapped, raw32 or raw64"},
		{"layout longer than storage", func(q *tlstatshouse.StoreQuerySeries) {
			q.Base.TagLayout.Kinds = make([]int32, format.MaxTags+1)
		}, "more than the 48 tags stored"},
		{"raw64 without a high half", func(q *tlstatshouse.StoreQuerySeries) {
			q.Base.TagLayout.Kinds = []int32{tagKindMapped, tagKindRaw64}
			q.By = []int32{1}
		}, "raw64 tag 1 has no high half"},
		{"raw64 filter without a high half", func(q *tlstatshouse.StoreQuerySeries) {
			q.Base.TagLayout.Kinds = []int32{tagKindMapped, tagKindRaw64}
			f := tlstatshouse.StoreTagFilter{TagIndex: 1}
			f.SetMapped([]int64{1})
			q.Base.FilterIn = []tlstatshouse.StoreTagFilter{f}
		}, "raw64 filter on tag 1 has no high half"},
		{"month step with a bad location", func(q *tlstatshouse.StoreQuerySeries) {
			q.Base.Lod.StepSec = monthLodStep
			q.Base.Lod.Location = "Not/AZone"
		}, "is not an IANA time zone"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := seriesReq(testMetricID, twoMappedKinds, count, []int32{0}, b1, b1+60, 60)
			tc.mutate(&q)
			requireBadRequest(t, renderSeriesErr(t, s, 1, q), tc.problem)
		})
	}
}

// TestRenderSeriesConcurrentQueries runs two series queries against the same
// archive window simultaneously: the shared read-only attach must let them
// both read, and both must detach afterwards.
func TestRenderSeriesConcurrentQueries(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 1},
	}))
	gen := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	require.NoError(t, s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{}))

	q := seriesReq(testMetricID, twoMappedKinds, []int32{int32(data_model.DigestCount)}, []int32{0}, b1, b1+60, 60)
	type outcome struct {
		resp tlstatshouse.StoreSeriesResponse
		err  error
	}
	done := make(chan outcome, 2)
	for i := 0; i < 2; i++ {
		go func() {
			resp, err := s.RenderSeries(context.Background(), 1, q)
			done <- outcome{resp: resp, err: err}
		}()
	}
	for i := 0; i < 2; i++ {
		out := <-done
		require.NoError(t, out.err)
		require.Equal(t, []float64{1}, flattenSeries(t, out.resp).count)
	}
	var lingering int
	require.NoError(t, s.Delta().QueryRow(
		`SELECT count(*) FROM duckdb_databases() WHERE database_name LIKE 'q%'`).Scan(&lingering))
	require.Zero(t, lingering)
}

// tdigestOf builds one digest from all the values, the direct-merge oracle
// the folded-state assertions compare against.
func tdigestOf(t *testing.T, values ...float64) *tdigest.TDigest {
	t.Helper()
	return decodePct(t, pctState(t, values...))
}

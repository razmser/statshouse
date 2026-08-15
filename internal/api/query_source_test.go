// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/hrissan/tdigest"
	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
)

// chQueryCapture records what a chQuerySource handed to doSelect.
type chQueryCapture struct {
	meta  chutil.QueryMetaInto
	body  string
	query ch.Query
}

// testMetricWithTags builds a user metric with RestoreCachedInfo applied, so
// tag Index and raw64 flags are set. Index 0 is the environment slot
// RestoreCachedInfo clears; pass the interesting tags from index 1 on.
func testMetricWithTags(t *testing.T, name string, tags ...format.MetricMetaTag) *format.MetricMetaValue {
	m := &format.MetricMetaValue{
		Name:     name,
		MetricID: 1002,
	}
	m.Tags = append([]format.MetricMetaTag{{}}, tags...)
	require.NoError(t, m.RestoreCachedInfo())
	return m
}

// appendTestValue appends row i's synthetic value to a result column,
// deriving the value from the column's position p, so decode assertions can
// predict every field.
func appendTestValue(data proto.ColResult, p int, i int) {
	switch c := data.(type) {
	case *proto.ColInt64:
		c.Append(int64(100*p + i))
	case *proto.ColInt32:
		c.Append(int32(100*p + i))
	case *proto.ColUInt32:
		c.Append(uint32(100*p + i))
	case *proto.ColFloat64:
		c.Append(float64(200*p + i))
	case *proto.ColStr:
		c.Append(fmt.Sprintf("%d-%d", p, i))
	case *chutil.ColTDigest:
		td := tdigest.New()
		td.Add(float64(300*p+i), 1)
		*c = append(*c, td)
	case *chutil.ColArgMinStringFloat32:
		*c = append(*c, data_model.ArgMinStringFloat32{ArgMinMaxStringFloat32: data_model.ArgMinMaxStringFloat32{
			AsString: fmt.Sprintf("min-%d-%d", p, i),
			Val:      float32(i),
		}})
	case *chutil.ColArgMaxStringFloat32:
		*c = append(*c, data_model.ArgMaxStringFloat32{ArgMinMaxStringFloat32: data_model.ArgMinMaxStringFloat32{
			AsString: fmt.Sprintf("max-%d-%d", p, i),
			Val:      float32(i),
		}})
	case *chutil.ColUnique:
		*c = append(*c, data_model.ChUnique{})
	}
}

// feedTestRows appends rows to every result column of the query and delivers
// them as one result block, standing in for the ClickHouse network round trip.
func feedTestRows(ctx context.Context, query ch.Query, rows int) error {
	results, ok := query.Result.(proto.Results)
	if !ok {
		return fmt.Errorf("unexpected query result type %T", query.Result)
	}
	for p, rc := range results {
		// the wire protocol decodes every block into a reset column, so
		// row indices restart at 0 per block
		rc.Data.Reset()
		for i := 0; i < rows; i++ {
			appendTestValue(rc.Data, p, i)
		}
	}
	return query.OnResult(ctx, proto.Block{Rows: rows})
}

// The ClickHouse source must generate exactly the SQL the pre-seam code
// generated for the same request: the seam lifts a queryBuilder into a
// semantic request and the CH implementation lowers it back, and no field may
// be lost in either direction.
func TestCHQuerySourceSeriesSQLMatchesPreSeamBuilder(t *testing.T) {
	const settings = " SETTINGS optimize_aggregation_in_order=1"

	metricTags := testMetricWithTags(t, "api_query_source_metric", format.MetricMetaTag{Name: "project"})
	metricRaw64 := testMetricWithTags(t, "api_query_source_raw64", format.MetricMetaTag{Name: "counter_id", RawKind: "uint64"})

	cases := []struct {
		name   string
		pq     *queryBuilder
		lod    data_model.LOD
		golden string // full expected SQL, for requests with a known-good text
	}{
		{
			name: "filters",
			pq: func() *queryBuilder {
				pq := &queryBuilder{
					metric: metric,
					user:   "test-user",
					what: tsWhat{
						data_model.DigestSelector{What: data_model.DigestCardinality},
						data_model.DigestSelector{What: data_model.DigestMax},
					},
					utcOffset: utcOffset,
				}
				pq.filterIn.Append(1, data_model.NewTagValue("one", 1), data_model.NewTagValue("two", 2))
				pq.filterNotIn.AppendValue(0, "staging")
				return pq
			}(),
			lod: getLodForV6(t, 100_000, 2_000_000, 2_000_000, 3),
			// the pre-seam golden from sql_test.go, unchanged through the seam
			golden: `SELECT toInt64(toStartOfInterval(time+10800,INTERVAL 14400 second))-10800 AS _time,toFloat64(sum(1)) AS _val0,toFloat64(max(max)) AS _val1 FROM statshouse_v6_1h WHERE time>=86397 AND time<2001597 AND index_type=0 AND pre_tag=0 AND pre_stag='' AND metric=1000 AND (tag1 IN (1,2) OR stag1 IN ('one','two')) AND (0=0 AND stag0 NOT IN ('staging')) GROUP BY _time LIMIT 10000000 SETTINGS optimize_aggregation_in_order=1`,
		},
		{
			name: "group by tag and shard",
			pq: &queryBuilder{
				metric: metricTags,
				user:   "test-user",
				what:   tsWhat{data_model.DigestSelector{What: data_model.DigestCount}},
				by:     []int{1, format.ShardTagIndex},
			},
			lod: getLodForV6(t, 10_000, 20_000, 20_000, 3),
		},
		{
			name: "table view sort descending",
			pq: &queryBuilder{
				metric: metricTags,
				user:   "test-user",
				what:   tsWhat{data_model.DigestSelector{What: data_model.DigestCount}},
				by:     []int{1},
				sort:   sortDescending,
				play:   3, // play/cache key fields stay behind the seam
			},
			lod: getLodForV6(t, 10_000, 20_000, 20_000, 3),
		},
		{
			name: "min and max host",
			pq: &queryBuilder{
				metric:     metricTags,
				user:       "test-user",
				what:       tsWhat{data_model.DigestSelector{What: data_model.DigestMax}},
				minMaxHost: [2]bool{true, true},
			},
			lod: getLodForV6(t, 10_000, 20_000, 20_000, 3),
		},
		{
			name: "raw64 tag group by",
			pq: &queryBuilder{
				metric: metricRaw64,
				user:   "test-user",
				what:   tsWhat{data_model.DigestSelector{What: data_model.DigestCount}},
				by:     []int{1},
			},
			lod: getLodForV6(t, 10_000, 20_000, 20_000, 3),
		},
		{
			name: "multi metric",
			pq: &queryBuilder{
				user: "test-user",
				what: tsWhat{data_model.DigestSelector{What: data_model.DigestCount}},
				filterIn: data_model.TagFilters{
					Metrics: []*format.MetricMetaValue{
						{MetricID: 1000},
						{MetricID: 1001},
					},
				},
			},
			lod: getLodForV6(t, 10_000, 20_000, 20_000, 3),
		},
		{
			// the cache-invalidation poll's shape: empty what (timestamps and
			// grouped tags only), group by tag1, builtin metric -61. The SQL
			// differs from the poll's retired hand-written query — extra always
			// empty stag1 in GROUP BY, maxSeriesRows LIMIT, settings suffix —
			// but returns the same (time, key1) rows; lock the new text.
			name: "poll empty what",
			pq: &queryBuilder{
				user:   "cache-update",
				metric: format.BuiltinMetricMetaContributorsLog,
				by:     []int{1},
			},
			lod: data_model.LOD{
				FromSec: 1000,
				ToSec:   2000,
				StepSec: _1s,
				Version: Version6,
				Metric:  format.BuiltinMetricMetaContributorsLog,
				Location: location,
			},
			golden: `SELECT toInt64(toStartOfInterval(time+0,INTERVAL 1 second))-0 AS _time,tag1,stag1 FROM statshouse_v6_1s_dist WHERE time>=1000 AND time<2000 AND index_type=0 AND pre_tag=0 AND pre_stag='' AND metric=-61 GROUP BY _time,tag1,stag1 LIMIT 10000000 SETTINGS optimize_aggregation_in_order=1`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &requestHandler{Handler: &Handler{
				HandlerOptions: HandlerOptions{location: location},
				selectSettings: settings,
				DisableCHAddr:  []string{"host1"},
			}}
			expected, err := tc.pq.buildSeriesQuery(tc.lod, settings)
			require.NoError(t, err)

			var capture chQueryCapture
			src := chQuerySource{selectFn: func(_ context.Context, _ *requestHandler, meta chutil.QueryMetaInto, query ch.Query) error {
				capture.meta, capture.body, capture.query = meta, query.Body, query
				return nil
			}}
			err = src.querySeries(context.Background(), h, seriesQueryFromBuilder(tc.pq), tc.lod, func(tsSelectRow) error { return nil })
			require.NoError(t, err)

			require.Equal(t, expected.body, capture.body)
			if tc.golden != "" {
				require.Equal(t, tc.golden, capture.body)
			}
			require.Equal(t, tc.pq.user, capture.meta.User)
			require.Equal(t, tc.pq.metric, capture.meta.Metric)
			require.Equal(t, tc.pq.metric.Sharded(), capture.meta.Sharded)
			require.Equal(t, tc.lod.Table(tc.pq.metric.Sharded()), capture.meta.Table)
			require.Equal(t, tc.lod.IsFast(), capture.meta.IsFast)
			require.Equal(t, expected.isLight(), capture.meta.IsLight)
			require.Equal(t, expected.isHardware(), capture.meta.IsHardware)
			require.Equal(t, []string{"host1"}, capture.meta.DisableCHAddrs)
		})
	}
}

func TestCHQuerySourceTagValuesSQLMatchesPreSeamBuilder(t *testing.T) {
	const settings = " SETTINGS optimize_aggregation_in_order=1"
	metricTags := testMetricWithTags(t, "api_query_source_metric", format.MetricMetaTag{Name: "project"})
	lod := getLodForV6(t, 10_000, 20_000, 20_000, 3)

	cases := []struct {
		name string
		q    *tagValuesDataQuery
	}{
		{
			name: "values",
			q: &tagValuesDataQuery{
				user:       "test-user",
				metric:     metricTags,
				tag:        metricTags.Tags[1],
				numResults: 100,
			},
		},
		{
			name: "ids only",
			q: &tagValuesDataQuery{
				user:       "test-user",
				metric:     metricTags,
				tag:        metricTags.Tags[1],
				numResults: math.MaxInt - 1,
				idsOnly:    true,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := &requestHandler{Handler: &Handler{
				HandlerOptions: HandlerOptions{location: location},
				selectSettings: settings,
			}}
			expected := tc.q.queryBuilder()
			var expectedQuery *tagValuesQuery
			if tc.q.idsOnly {
				expectedQuery = expected.buildTagValueIDsQuery(lod, settings)
			} else {
				expectedQuery = expected.buildTagValuesQuery(lod, settings)
			}

			var capture chQueryCapture
			src := chQuerySource{selectFn: func(_ context.Context, _ *requestHandler, meta chutil.QueryMetaInto, query ch.Query) error {
				capture.meta, capture.body = meta, query.Body
				return nil
			}}
			err := src.queryTagValues(context.Background(), h, tc.q, lod, func(selectRow) error { return nil })
			require.NoError(t, err)

			require.Equal(t, expectedQuery.body, capture.body)
			require.Equal(t, tc.q.user, capture.meta.User)
			require.Equal(t, tc.q.metric, capture.meta.Metric)
			require.True(t, capture.meta.IsLight)
			require.Equal(t, lod.Table(tc.q.metric.Sharded()), capture.meta.Table)
		})
	}
}

// Feeding synthetic result blocks through the captured ch.Query must decode
// to the same rows the pre-seam OnResult loops produced.
func TestCHQuerySourceSeriesDecode(t *testing.T) {
	metricTags := testMetricWithTags(t, "api_query_source_metric", format.MetricMetaTag{Name: "project"})
	h := &requestHandler{Handler: &Handler{HandlerOptions: HandlerOptions{location: location}}}
	q := &seriesDataQuery{
		user:   "test-user",
		metric: metricTags,
		what: tsWhat{
			data_model.DigestSelector{What: data_model.DigestSum},
			data_model.DigestSelector{What: data_model.DigestMax},
			data_model.DigestSelector{What: data_model.DigestPercentile},
			data_model.DigestSelector{What: data_model.DigestUnique},
		},
		by:         []int{1, format.ShardTagIndex},
		minMaxHost: [2]bool{true, true},
	}
	lod := getLodForV6(t, 100_000, 2_000_000, 2_000_000, 3)

	var capture chQueryCapture
	src := chQuerySource{selectFn: func(_ context.Context, _ *requestHandler, meta chutil.QueryMetaInto, query ch.Query) error {
		capture.meta = meta
		// two blocks: rows must stream through in order
		if err := feedTestRows(context.Background(), query, 2); err != nil {
			return err
		}
		return feedTestRows(context.Background(), query, 3)
	}}
	var rows []tsSelectRow
	err := src.querySeries(context.Background(), h, q, lod, func(row tsSelectRow) error {
		rows = append(rows, row)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, rows, 5)

	// column layout: _time(0), sum(1), max(2), percentile(3), unique(4),
	// _minHost(5), _maxHost(6), tag1(7), stag1(8), _shard_num(9); row indices
	// restart at 0 in each result block
	local := []int{0, 1, 0, 1, 2}
	for i, row := range rows {
		j := local[i]
		require.Equal(t, int64(j), row.time, "row %d", i)
		require.Equal(t, float64(200+j), row.sum, "row %d", i)
		require.Equal(t, float64(400+j), row.max, "row %d", i)
		require.InDelta(t, float64(900+j), row.percentile.Quantile(0.5), 1e-9, "row %d", i)
		require.Equal(t, data_model.ChUnique{}, row.unique, "row %d", i)
		require.Equal(t, fmt.Sprintf("min-5-%d", j), row.minHostStr.AsString, "row %d", i)
		require.Equal(t, float32(j), row.minHostStr.Val, "row %d", i)
		require.Equal(t, fmt.Sprintf("max-6-%d", j), row.maxHostStr.AsString, "row %d", i)
		require.Equal(t, int64(700+j), row.tag[1], "row %d", i)
		require.Equal(t, fmt.Sprintf("8-%d", j), row.stag[1], "row %d", i)
		require.Equal(t, uint32(900+j), row.shardNum, "row %d", i)
		require.Equal(t, q.what, row.what, "row %d", i)
	}
	// percentile and unique digests make the query heavy
	require.False(t, capture.meta.IsLight)
}

// Point rows decode without time and string tags, exactly as the pre-seam
// loadPoint did.
func TestCHQuerySourcePointDecode(t *testing.T) {
	h := &requestHandler{Handler: &Handler{HandlerOptions: HandlerOptions{location: location}}}
	q := &seriesDataQuery{
		user:   "test-user",
		metric: metric,
		what:   tsWhat{data_model.DigestSelector{What: data_model.DigestCount}},
		point:  true,
	}
	lod := getLodForV6(t, 100_000, 2_000_000, 2_000_000, 3)

	src := chQuerySource{selectFn: func(_ context.Context, _ *requestHandler, _ chutil.QueryMetaInto, query ch.Query) error {
		return feedTestRows(context.Background(), query, 2)
	}}
	var rows []tsSelectRow
	err := src.querySeries(context.Background(), h, q, lod, func(row tsSelectRow) error {
		rows = append(rows, row)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, rows, 2)
	// column layout: _time(0), count(1); point decode reads values only
	for i, row := range rows {
		require.Zero(t, row.time, "row %d", i)
		require.Equal(t, float64(200+i), row.count, "row %d", i)
		require.Equal(t, tsTags{}, row.tsTags, "row %d", i)
	}
}

func TestCHQuerySourceTagValuesDecode(t *testing.T) {
	metricTags := testMetricWithTags(t, "api_query_source_metric", format.MetricMetaTag{Name: "project"})
	h := &requestHandler{Handler: &Handler{HandlerOptions: HandlerOptions{location: location}}}
	lod := getLodForV6(t, 10_000, 20_000, 20_000, 3)

	t.Run("values", func(t *testing.T) {
		q := &tagValuesDataQuery{
			user:       "test-user",
			metric:     metricTags,
			tag:        metricTags.Tags[1],
			numResults: 10,
		}
		src := chQuerySource{selectFn: func(_ context.Context, _ *requestHandler, _ chutil.QueryMetaInto, query ch.Query) error {
			return feedTestRows(context.Background(), query, 2)
		}}
		var rows []selectRow
		err := src.queryTagValues(context.Background(), h, q, lod, func(row selectRow) error {
			rows = append(rows, row)
			return nil
		})
		require.NoError(t, err)
		// column layout: tag1(0), stag1(1), _count(2)
		require.Equal(t, []selectRow{
			{valID: 0, val: "1-0", cnt: 400},
			{valID: 1, val: "1-1", cnt: 401},
		}, rows)
	})

	t.Run("ids only", func(t *testing.T) {
		q := &tagValuesDataQuery{
			user:       "test-user",
			metric:     metricTags,
			tag:        metricTags.Tags[1],
			numResults: math.MaxInt - 1,
			idsOnly:    true,
		}
		src := chQuerySource{selectFn: func(_ context.Context, _ *requestHandler, _ chutil.QueryMetaInto, query ch.Query) error {
			return feedTestRows(context.Background(), query, 2)
		}}
		var rows []selectRow
		err := src.queryTagValues(context.Background(), h, q, lod, func(row selectRow) error {
			rows = append(rows, row)
			return nil
		})
		require.NoError(t, err)
		// column layout: tag1(0), _count(1); no string column in ids mode
		require.Equal(t, []selectRow{
			{valID: 0, val: "", cnt: 200},
			{valID: 1, val: "", cnt: 201},
		}, rows)
	})
}

// fakeQuerySource records the semantic requests a handler makes and feeds
// canned rows back.
type fakeQuerySource struct {
	seriesQ    *seriesDataQuery
	seriesLOD  data_model.LOD
	seriesRows []tsSelectRow

	tagQ    *tagValuesDataQuery
	tagLOD  data_model.LOD
	tagRows []selectRow

	seriesCalls int
	tagCalls    int
}

func (f *fakeQuerySource) querySeries(_ context.Context, _ *requestHandler, q *seriesDataQuery, lod data_model.LOD, onRow func(tsSelectRow) error) error {
	f.seriesCalls++
	f.seriesQ, f.seriesLOD = q, lod
	for _, r := range f.seriesRows {
		if err := onRow(r); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeQuerySource) queryTagValues(_ context.Context, _ *requestHandler, q *tagValuesDataQuery, lod data_model.LOD, onRow func(selectRow) error) error {
	f.tagCalls++
	f.tagQ, f.tagLOD = q, lod
	for _, r := range f.tagRows {
		if err := onRow(r); err != nil {
			return err
		}
	}
	return nil
}

func TestLoadPointsRoutesThroughQuerySource(t *testing.T) {
	fake := &fakeQuerySource{}
	h := &requestHandler{Handler: &Handler{
		HandlerOptions: HandlerOptions{location: location},
		querySource:    fake,
	}}
	pq := &queryBuilder{
		user: "test-user",
		metric: metric,
		what:  tsWhat{data_model.DigestSelector{What: data_model.DigestCount}},
	}
	lod := data_model.LOD{FromSec: 1000, ToSec: 1010, StepSec: 5, Version: Version6, Metric: metric, Location: location}
	fake.seriesRows = []tsSelectRow{
		{time: 1005, tsValues: tsValues{count: 5}},
		{time: 1000, tsValues: tsValues{count: 1}},
		{time: 1005, tsValues: tsValues{count: 6}},
		{time: 1000, tsValues: tsValues{count: 2}},
	}

	ret := make([][]tsSelectRow, 2)
	n, err := loadPoints(context.Background(), h, pq, lod, ret, 0)
	require.NoError(t, err)
	require.Equal(t, 4, n)

	// rows scatter into buckets by timestamp, preserving order
	require.Equal(t, []tsSelectRow{fake.seriesRows[1], fake.seriesRows[3]}, ret[0])
	require.Equal(t, []tsSelectRow{fake.seriesRows[0], fake.seriesRows[2]}, ret[1])

	// the semantic request carries every SQL-relevant field of the builder
	require.Equal(t, &seriesDataQuery{
		user:   "test-user",
		metric: metric,
		what:   tsWhat{data_model.DigestSelector{What: data_model.DigestCount}},
	}, fake.seriesQ)
	require.Equal(t, lod, fake.seriesLOD)
	require.Equal(t, 1, fake.seriesCalls)
	require.Zero(t, fake.tagCalls)
}

func TestLoadPointRoutesThroughQuerySource(t *testing.T) {
	fake := &fakeQuerySource{}
	h := &requestHandler{Handler: &Handler{
		HandlerOptions: HandlerOptions{location: location},
		querySource:    fake,
	}}
	pq := &queryBuilder{
		user:   "test-user",
		metric: metric,
		what:   tsWhat{data_model.DigestSelector{What: data_model.DigestCount}},
		point:  true,
	}
	lod := data_model.LOD{FromSec: 1000, ToSec: 1010, StepSec: 5, Version: Version6, Metric: metric, Location: location}
	row := tsSelectRow{time: 1000, tsTags: tsTags{shardNum: 3}, tsValues: tsValues{count: 7}}
	fake.seriesRows = []tsSelectRow{row}

	rows, err := loadPoint(context.Background(), h, pq, lod)
	require.NoError(t, err)
	// point rows carry values and tags only, exactly as before the seam
	require.Equal(t, []pSelectRow{{tsTags: row.tsTags, tsValues: row.tsValues}}, rows)

	require.Equal(t, &seriesDataQuery{
		user:   "test-user",
		metric: metric,
		what:   tsWhat{data_model.DigestSelector{What: data_model.DigestCount}},
		point:  true,
	}, fake.seriesQ)
}

// The once-per-second cache-invalidation poll is now a tag-only series query
// through the read seam.
func TestInvalidateCacheThroughQuerySource(t *testing.T) {
	now := time.Now().Unix()
	pollRow := func(inserted, changed int64) tsSelectRow {
		var r tsSelectRow
		r.time = inserted
		r.tag[1] = changed
		return r
	}
	fake := &fakeQuerySource{seriesRows: []tsSelectRow{
		pollRow(now-10, now-100),
		pollRow(now-5, now-100), // same changed second, newer insert time
		pollRow(now-5, now-50),
	}}
	hh := &Handler{
		HandlerOptions: HandlerOptions{location: time.Local},
		querySource:    fake,
		pointsCache:    newPointsCache(1000, 0, nil, time.Now),
	}
	hh.cache2 = newCache2(hh, 0, nil)
	defer hh.cache2.shutdown().Wait()

	from, seen := hh.invalidateCache(context.Background(), now, nil)

	// the poll is a tag-only series query on the contributors-log metric
	require.Equal(t, 1, fake.seriesCalls)
	require.Equal(t, &seriesDataQuery{
		user:   "cache-update",
		metric: format.BuiltinMetricMetaContributorsLog,
		by:     []int{1},
	}, fake.seriesQ)
	lod := fake.seriesLOD
	require.Equal(t, int64(_1s), lod.StepSec)
	require.Equal(t, Version6, lod.Version)
	require.Equal(t, format.BuiltinMetricMetaContributorsLog, lod.Metric)
	require.Equal(t, time.Local, lod.Location)
	require.GreaterOrEqual(t, lod.FromSec, now-int64(invalidateLinger/time.Second))
	require.LessOrEqual(t, lod.FromSec, time.Now().Unix()-int64(invalidateLinger/time.Second))
	require.GreaterOrEqual(t, lod.ToSec, now+int64(cacheInvalidateLookAhead/time.Second))

	// rows ratchet `from` and build the seen set
	require.Equal(t, now-5, from)
	require.Equal(t, map[cacheInvalidateLogRow]struct{}{
		{T: now - 10, At: now - 100}: {},
		{T: now - 5, At: now - 100}:  {},
		{T: now - 5, At: now - 50}:   {},
	}, seen)

	// the changed seconds reach the point cache at 1s granularity
	require.Contains(t, hh.pointsCache.invalidatedAtNano.seconds[2], now-100)
	require.Contains(t, hh.pointsCache.invalidatedAtNano.seconds[2], now-50)

	// a second poll over the same rows invalidates nothing new
	size := len(hh.pointsCache.invalidatedAtNano.seconds[2])
	from2, seen2 := hh.invalidateCache(context.Background(), from, seen)
	require.Equal(t, from, from2)
	require.Equal(t, seen, seen2)
	require.Equal(t, 2, fake.seriesCalls)
	require.Len(t, hh.pointsCache.invalidatedAtNano.seconds[2], size)
}

func TestNewQuerySourceSelection(t *testing.T) {
	require.Equal(t, chQuerySource{}, newQuerySource(duckstore.BackendClickHouse))

	duck := newQuerySource(duckstore.BackendDuck)
	require.ErrorIs(t, duck.querySeries(context.Background(), nil, &seriesDataQuery{}, data_model.LOD{}, func(tsSelectRow) error { return nil }), errDuckQuerySourcePending)
	require.ErrorIs(t, duck.queryTagValues(context.Background(), nil, &tagValuesDataQuery{}, data_model.LOD{}, func(selectRow) error { return nil }), errDuckQuerySourcePending)

	// handlers without a configured source fall back to ClickHouse
	require.Equal(t, chQuerySource{}, (&requestHandler{}).querySource())
	require.Equal(t, chQuerySource{}, (&requestHandler{Handler: &Handler{}}).querySource())
}

// Copyright 2025 V Kontate LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/VKCOM/tl/pkg/rpc"
	"github.com/hrissan/tdigest"
	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

// The fan-out tests run against simulated multi-shard sources: fake
// storeShardClients returning canned columnar responses, so every merge,
// cap, retry and pruning rule is exercised without a live aggregator.

// fakeStoreShard is a storeShardClient backed by test closures. Every request
// it served is recorded; the closures run under fanoutCall's concurrency, so
// access is mutex-guarded.
type fakeStoreShard struct {
	shard uint32
	where string

	mu            sync.Mutex
	seriesFn      func(ctx context.Context, args tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error)
	tagValuesFn   func(ctx context.Context, args tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error)
	seriesArgs    []tlstatshouse.StoreQuerySeries
	tagValuesArgs []tlstatshouse.StoreQueryTagValues
}

func (f *fakeStoreShard) shardNum() uint32 { return f.shard }
func (f *fakeStoreShard) addr() string     { return f.where }

func (f *fakeStoreShard) querySeries(ctx context.Context, args tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
	f.mu.Lock()
	f.seriesArgs = append(f.seriesArgs, args)
	fn := f.seriesFn
	f.mu.Unlock()
	if fn == nil {
		return tlstatshouse.StoreSeriesResponse{}, nil
	}
	return fn(ctx, args)
}

func (f *fakeStoreShard) queryTagValues(ctx context.Context, args tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
	f.mu.Lock()
	f.tagValuesArgs = append(f.tagValuesArgs, args)
	fn := f.tagValuesFn
	f.mu.Unlock()
	if fn == nil {
		return tlstatshouse.StoreTagValuesResponse{}, nil
	}
	return fn(ctx, args)
}

func (f *fakeStoreShard) seriesCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.seriesArgs)
}

// fanoutTestSource builds a source over the fakes, sorted by shard number
// exactly as the address parsing produces.
func fanoutTestSource(fakes ...*fakeStoreShard) *duckQuerySource {
	slices.SortFunc(fakes, func(a, b *fakeStoreShard) int { return int(a.shard) - int(b.shard) })
	clients := make([]storeShardClient, len(fakes))
	numShards := 0
	for i, f := range fakes {
		clients[i] = f
		if int(f.shard) > numShards {
			numShards = int(f.shard)
		}
	}
	return &duckQuerySource{clients: clients, numShards: numShards}
}

func fanoutFakes(n int) []*fakeStoreShard {
	fakes := make([]*fakeStoreShard, n)
	for i := range fakes {
		fakes[i] = &fakeStoreShard{shard: uint32(i + 1), where: fmt.Sprintf("10.0.0.%d:9099", i+1)}
	}
	return fakes
}

// fanoutWhats packs digest kinds into the fixed-size tsWhat.
func fanoutWhats(kinds ...data_model.DigestWhat) tsWhat {
	var w tsWhat
	for i, k := range kinds {
		w[i] = data_model.DigestSelector{What: k}
	}
	return w
}

// fanoutSeriesBatch builds one columnar batch from the parallel per-column
// slices — the shape the aggregator's renderer emits.
func fanoutSeriesBatch(rows int32, time []int64, tag [][]int64, stag [][]string, cols *tlstatshouse.StoreSeriesBatch) tlstatshouse.StoreSeriesBatch {
	b := tlstatshouse.StoreSeriesBatch{Rows: rows, Time: time, Tag: tag, Stag: stag}
	if cols != nil {
		b.Count = cols.Count
		b.Min = cols.Min
		b.Max = cols.Max
		b.Sum = cols.Sum
		b.Sumsquare = cols.Sumsquare
		b.Cardinality = cols.Cardinality
		b.Percentiles = cols.Percentiles
		b.UniqState = cols.UniqState
		b.MinHostValue = cols.MinHostValue
		b.MinHostTag = cols.MinHostTag
		b.MinHostStag = cols.MinHostStag
		b.MaxHostValue = cols.MaxHostValue
		b.MaxHostTag = cols.MaxHostTag
		b.MaxHostStag = cols.MaxHostStag
	}
	return b
}

func fanoutSeriesResp(shard uint32, batches ...tlstatshouse.StoreSeriesBatch) tlstatshouse.StoreSeriesResponse {
	return tlstatshouse.StoreSeriesResponse{ShardNum: int32(shard), Batches: batches}
}

func fanoutRunSeries(t *testing.T, src *duckQuerySource, q *seriesDataQuery) ([]tsSelectRow, error) {
	t.Helper()
	var rows []tsSelectRow
	err := src.querySeries(context.Background(), nil, q, data_model.LOD{FromSec: 2000, ToSec: 3000, StepSec: 60}, func(r tsSelectRow) error {
		rows = append(rows, r)
		return nil
	})
	return rows, err
}

func fanoutMetric(strategy string) *format.MetricMetaValue {
	return &format.MetricMetaValue{Name: "fanout_metric", MetricID: 1002, ShardStrategy: strategy}
}

// TestFanoutSeriesMergeAcrossShards folds three shards' partial aggregates
// into one row per (time, tags) key, in the deterministic merged order.
func TestFanoutSeriesMergeAcrossShards(t *testing.T) {
	fakes := fanoutFakes(3)
	what := fanoutWhats(data_model.DigestCount, data_model.DigestMin, data_model.DigestMax, data_model.DigestSum)
	fakes[0].seriesFn = func(context.Context, tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
		return fanoutSeriesResp(1,
			fanoutSeriesBatch(2, []int64{100, 200}, [][]int64{{1, 1}}, nil,
				&tlstatshouse.StoreSeriesBatch{
					Count: []float64{1, 1},
					Min:   []float64{1, 5},
					Max:   []float64{9, 5},
					Sum:   []float64{10, 50},
				}),
		), nil
	}
	fakes[1].seriesFn = func(context.Context, tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
		return fanoutSeriesResp(2,
			fanoutSeriesBatch(1, []int64{100}, [][]int64{{1}}, nil,
				&tlstatshouse.StoreSeriesBatch{Count: []float64{2}, Min: []float64{0}, Max: []float64{10}, Sum: []float64{20}}),
		), nil
	}
	fakes[2].seriesFn = func(context.Context, tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
		return fanoutSeriesResp(3,
			fanoutSeriesBatch(2, []int64{100, 300}, [][]int64{{2, 1}}, nil,
				&tlstatshouse.StoreSeriesBatch{Count: []float64{5, 7}}),
		), nil
	}
	rows, err := fanoutRunSeries(t, fanoutTestSource(fakes...), &seriesDataQuery{
		metric: fanoutMetric(format.ShardBuiltinDist), // not sharded: fans out
		what:   what,
		by:     []int{0},
	})
	require.NoError(t, err)
	require.Len(t, rows, 4)
	// ordered by time, then tags
	require.Equal(t, int64(100), rows[0].time)
	require.Equal(t, int64(1), rows[0].tag[0])
	require.Equal(t, float64(3), rows[0].count) // 1 + 2
	require.Equal(t, float64(0), rows[0].min)   // min across shards
	require.Equal(t, float64(10), rows[0].max)  // max across shards
	require.Equal(t, float64(30), rows[0].sum)  // sum across shards
	require.Equal(t, int64(100), rows[1].time)
	require.Equal(t, int64(2), rows[1].tag[0])
	require.Equal(t, float64(5), rows[1].count)
	require.Equal(t, int64(200), rows[2].time)
	require.Equal(t, float64(1), rows[2].count)
	require.Equal(t, int64(300), rows[3].time)
	require.Equal(t, float64(7), rows[3].count)
}

// TestFanoutSeriesStateMergesByValue checks the sketch and host merges by
// value: percentiles fold into one digest, uniques into one set with the
// shared ids counted once, and the host arg-min/arg-max pick the right shard's
// row by value.
func TestFanoutSeriesStateMergesByValue(t *testing.T) {
	tdA, tdB := tdigest.New(), tdigest.New()
	for _, v := range []float64{1, 2, 3} {
		tdA.Add(v, 1)
	}
	for _, v := range []float64{7, 8, 9} {
		tdB.Add(v, 1)
	}
	var uA, uB data_model.ChUnique
	uA.Insert(1)
	uA.Insert(2)
	uB.Insert(2)
	uB.Insert(3)

	hostBatch := func(minVal float64, minTag int32, minStr string) *tlstatshouse.StoreSeriesBatch {
		return &tlstatshouse.StoreSeriesBatch{
			Count:        []float64{1},
			Percentiles:  []string{string(rowbinary.AppendCentroids(nil, tdA, 1))},
			UniqState:    []string{string(uA.MarshallAppend(nil))},
			MinHostValue: []float64{minVal},
			MinHostTag:   []int32{minTag},
			MinHostStag:  []string{minStr},
			MaxHostValue: []float64{10},
			MaxHostTag:   []int32{7},
			MaxHostStag:  []string{"hostA"},
		}
	}
	batchA := hostBatch(10, 7, "hostA")
	batchB := &tlstatshouse.StoreSeriesBatch{
		Count:        []float64{1},
		Percentiles:  []string{string(rowbinary.AppendCentroids(nil, tdB, 1))},
		UniqState:    []string{string(uB.MarshallAppend(nil))},
		MinHostValue: []float64{5},
		MinHostTag:   []int32{9},
		MinHostStag:  []string{"hostB"},
		MaxHostValue: []float64{5},
		MaxHostTag:   []int32{9},
		MaxHostStag:  []string{"hostB"},
	}

	fakes := fanoutFakes(2)
	fakes[0].seriesFn = func(context.Context, tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
		return fanoutSeriesResp(1, fanoutSeriesBatch(1, []int64{100}, [][]int64{{1}}, nil, batchA)), nil
	}
	fakes[1].seriesFn = func(context.Context, tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
		return fanoutSeriesResp(2, fanoutSeriesBatch(1, []int64{100}, [][]int64{{1}}, nil, batchB)), nil
	}
	rows, err := fanoutRunSeries(t, fanoutTestSource(fakes...), &seriesDataQuery{
		metric:     fanoutMetric(format.ShardBuiltinDist),
		what:       fanoutWhats(data_model.DigestCount, data_model.DigestPercentile, data_model.DigestUnique),
		by:         []int{0},
		minMaxHost: [2]bool{true, true},
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	row := rows[0]

	// percentiles: one digest of 1,2,3,7,8,9 — the median sits between 3 and 7
	expected := tdigest.New()
	expected.Merge(tdA)
	expected.Merge(tdB)
	require.NotNil(t, row.percentile)
	require.InEpsilon(t, expected.Quantile(0.5), row.percentile.Quantile(0.5), 1e-9)
	require.InDelta(t, 5, row.percentile.Quantile(0.5), 1) // sanity: not a one-shard state (2 or 8)

	// uniques: {1,2} ∪ {2,3} counts 3, not 4
	require.Equal(t, uint64(3), row.unique.Size(false))

	// hosts by value: min host from shard 2 (5 < 10), max host from shard 1 (10 > 5)
	require.Equal(t, int32(9), row.minHost.Arg)
	require.Equal(t, float32(5), row.minHost.Val)
	require.Equal(t, "hostB", row.minHostStr.AsString)
	require.Equal(t, int32(9), row.minHostStr.AsInt32)
	require.Equal(t, int32(7), row.maxHost.Arg)
	require.Equal(t, float32(10), row.maxHost.Val)
	require.Equal(t, "hostA", row.maxHostStr.AsString)
}

// TestFanoutTagValuesGlobalTopN proves the top N is global: a value ranked
// second on every shard — below a per-shard cap of one — still leads the
// merged counts, because the shards never apply the user's N.
func TestFanoutTagValuesGlobalTopN(t *testing.T) {
	fakes := fanoutFakes(3)
	for i, f := range fakes {
		i := i
		f.tagValuesFn = func(context.Context, tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
			// every shard: its own leader first, then the global winner "common"
			return tlstatshouse.StoreTagValuesResponse{
				Tag:   []int64{int64(11 + i), 99},
				Stag:  []string{fmt.Sprintf("leader%d", i+1), "common"},
				Count: []float64{10, 9},
			}, nil
		}
	}
	src := fanoutTestSource(fakes...)
	var rows []selectRow
	err := src.queryTagValues(context.Background(), nil, &tagValuesDataQuery{
		metric: fanoutMetric(format.ShardBuiltinDist),
		tag:    format.MetricMetaTag{Index: 1},
	}, data_model.LOD{FromSec: 2000, ToSec: 3000, StepSec: 60}, func(r selectRow) error {
		rows = append(rows, r)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, rows, 4)
	// "common" summed across all shards outranks every per-shard leader
	require.Equal(t, "common", rows[0].val)
	require.Equal(t, int64(99), rows[0].valID)
	require.Equal(t, float64(27), rows[0].cnt)
	// the tied leaders order deterministically by id
	require.Equal(t, int64(11), rows[1].valID)
	require.Equal(t, float64(10), rows[1].cnt)
	require.Equal(t, int64(12), rows[2].valID)
	require.Equal(t, int64(13), rows[3].valID)

	// a caller taking the global top 2 keeps "common" — a per-shard top-1
	// (what the shards must never apply) would have dropped it
	require.Equal(t, "common", rows[:2][0].val)
}

// TestFanoutTagValuesMergeKeyedByValue checks the merge key is the value, not
// the row: the same (id, string) pair from different shards — and a mapped id
// alongside its string — folds into one summed row.
func TestFanoutTagValuesMergeKeyedByValue(t *testing.T) {
	fakes := fanoutFakes(2)
	fakes[0].tagValuesFn = func(context.Context, tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
		return tlstatshouse.StoreTagValuesResponse{Tag: []int64{5}, Stag: []string{"v"}, Count: []float64{2}}, nil
	}
	fakes[1].tagValuesFn = func(context.Context, tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
		// same value, different per-shard count: must fold into one row
		return tlstatshouse.StoreTagValuesResponse{Tag: []int64{5}, Stag: []string{"v"}, Count: []float64{3}}, nil
	}
	var rows []selectRow
	err := fanoutTestSource(fakes...).queryTagValues(context.Background(), nil, &tagValuesDataQuery{
		metric: fanoutMetric(format.ShardBuiltinDist),
		tag:    format.MetricMetaTag{Index: 1},
	}, data_model.LOD{}, func(r selectRow) error {
		rows = append(rows, r)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, float64(5), rows[0].cnt)
}

// TestFanoutDownShardFailsWholeQuery: one unreachable shard fails the whole
// query — no partial result is handed to the cache — with the error naming
// the shard and its address.
func TestFanoutDownShardFailsWholeQuery(t *testing.T) {
	fakes := fanoutFakes(3)
	down := errors.New("dial tcp: connection refused")
	fakes[1].seriesFn = func(context.Context, tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
		return tlstatshouse.StoreSeriesResponse{}, down
	}
	fakes[1].tagValuesFn = func(context.Context, tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
		return tlstatshouse.StoreTagValuesResponse{}, down
	}
	src := fanoutTestSource(fakes...)
	_, err := fanoutRunSeries(t, src, &seriesDataQuery{metric: fanoutMetric(format.ShardBuiltinDist), what: fanoutWhats(data_model.DigestCount)})
	require.Error(t, err)
	require.ErrorIs(t, err, down)
	require.Contains(t, err.Error(), "duck shard 2")
	require.Contains(t, err.Error(), "10.0.0.2:9099")

	err = src.queryTagValues(context.Background(), nil, &tagValuesDataQuery{
		metric: fanoutMetric(format.ShardBuiltinDist),
		tag:    format.MetricMetaTag{Index: 1},
	}, data_model.LOD{}, func(selectRow) error { return nil })
	require.ErrorIs(t, err, down)
	require.Contains(t, err.Error(), "duck shard 2")
}

// TestFanoutRowCaps trips both caps: a shard refusing under its own per-shard
// limit surfaces the structured row_limit code, and the global post-merge cap
// fails the query once the merged set — series rows or tag values — crosses
// it.
func TestFanoutRowCaps(t *testing.T) {
	t.Run("per-shard structured row limit", func(t *testing.T) {
		fakes := fanoutFakes(2)
		fakes[1].seriesFn = func(context.Context, tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
			return tlstatshouse.StoreSeriesResponse{}, duckstore.NewError(duckstore.ErrCodeRowLimit, "query produced more than %d rows", 1_000_000)
		}
		fakes[1].tagValuesFn = func(context.Context, tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
			return tlstatshouse.StoreTagValuesResponse{}, duckstore.NewError(duckstore.ErrCodeRowLimit, "query produced more than %d rows", 1_000_000)
		}
		src := fanoutTestSource(fakes...)
		_, err := fanoutRunSeries(t, src, &seriesDataQuery{metric: fanoutMetric(format.ShardBuiltinDist), what: fanoutWhats(data_model.DigestCount)})
		require.Error(t, err)
		require.True(t, duckstore.IsCode(err, duckstore.ErrCodeRowLimit), "expected structured row_limit code, got %v", err)
		require.Contains(t, err.Error(), "duck shard 2")
		err = src.queryTagValues(context.Background(), nil, &tagValuesDataQuery{
			metric: fanoutMetric(format.ShardBuiltinDist),
			tag:    format.MetricMetaTag{Index: 1},
		}, data_model.LOD{}, func(selectRow) error { return nil })
		require.True(t, duckstore.IsCode(err, duckstore.ErrCodeRowLimit), "expected structured row_limit code, got %v", err)
	})

	shrinkCap := func(t *testing.T, cap int) {
		t.Helper()
		old := fanoutRowCap
		fanoutRowCap = cap
		t.Cleanup(func() { fanoutRowCap = old })
	}

	t.Run("global series cap", func(t *testing.T) {
		shrinkCap(t, 5)
		fakes := fanoutFakes(2)
		rowsResp := func(shard uint32) tlstatshouse.StoreSeriesResponse {
			times := make([]int64, 6)
			tags := make([][]int64, 1)
			tags[0] = make([]int64, 6)
			for i := range times {
				times[i] = int64(100 + i)
				tags[0][i] = int64(i + int(shard)*100)
			}
			return fanoutSeriesResp(shard, fanoutSeriesBatch(6, times, tags, nil, nil))
		}
		fakes[0].seriesFn = func(context.Context, tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
			return rowsResp(1), nil
		}
		fakes[1].seriesFn = func(context.Context, tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
			return rowsResp(2), nil
		}
		_, err := fanoutRunSeries(t, fanoutTestSource(fakes...), &seriesDataQuery{
			metric: fanoutMetric(format.ShardBuiltinDist),
			by:     []int{0},
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "above the 5-row cap")
	})

	t.Run("global tag-values cap", func(t *testing.T) {
		shrinkCap(t, 3)
		fakes := fanoutFakes(1)
		fakes[0].tagValuesFn = func(context.Context, tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
			return tlstatshouse.StoreTagValuesResponse{
				Tag:   []int64{1, 2, 3, 4},
				Stag:  []string{"a", "b", "c", "d"},
				Count: []float64{1, 1, 1, 1},
			}, nil
		}
		err := fanoutTestSource(fakes...).queryTagValues(context.Background(), nil, &tagValuesDataQuery{
			metric: fanoutMetric(format.ShardBuiltinDist),
			tag:    format.MetricMetaTag{Index: 1},
		}, data_model.LOD{}, func(selectRow) error { return nil })
		require.Error(t, err)
		require.Contains(t, err.Error(), "above the 3-row cap")
	})
}

// TestFanoutSeriesPruning walks every pruning rule: fan out unless the
// metric's assignment provably covers the whole range, and a pruned shard
// missing from the configured set is a loud error.
func TestFanoutSeriesPruning(t *testing.T) {
	run := func(t *testing.T, src *duckQuerySource, m *format.MetricMetaValue) ([]int, error) {
		t.Helper()
		_, err := fanoutRunSeries(t, src, &seriesDataQuery{metric: m, what: fanoutWhats(data_model.DigestCount)})
		var visited []int
		for _, c := range src.clients {
			if f := c.(*fakeStoreShard); f.seriesCallCount() > 0 {
				visited = append(visited, int(f.shard))
			}
		}
		return visited, err
	}

	t.Run("unsharded fans out", func(t *testing.T) {
		visited, err := run(t, fanoutTestSource(fanoutFakes(3)...), fanoutMetric(format.ShardBuiltinDist))
		require.NoError(t, err)
		require.ElementsMatch(t, []int{1, 2, 3}, visited)
	})
	t.Run("fixed key pins one shard", func(t *testing.T) {
		m := fanoutMetric(format.ShardFixed)
		m.ShardFixedKey = 2 // 1-based: shard 2
		visited, err := run(t, fanoutTestSource(fanoutFakes(3)...), m)
		require.NoError(t, err)
		require.Equal(t, []int{2}, visited)
	})
	t.Run("by metric id pins its shard", func(t *testing.T) {
		// 1003 % 3 = 1 → 0-based shard 1 → shard 2
		m := fanoutMetric(format.ShardByMetricID)
		m.MetricID = 1003
		visited, err := run(t, fanoutTestSource(fanoutFakes(3)...), m)
		require.NoError(t, err)
		require.Equal(t, []int{2}, visited)
	})
	t.Run("fixed shard pruned when older than range", func(t *testing.T) {
		m := fanoutMetric(format.ShardFixed)
		m.ShardNum = 1 // 0-based: shard 2
		m.UpdateTime = 1000
		visited, err := run(t, fanoutTestSource(fanoutFakes(3)...), m)
		require.NoError(t, err)
		require.Equal(t, []int{2}, visited)
	})
	t.Run("fixed shard edited inside range fans out", func(t *testing.T) {
		m := fanoutMetric(format.ShardFixed)
		m.ShardNum = 1
		m.UpdateTime = 2500 // after FromSec 2000: the assignment may have moved mid-range
		visited, err := run(t, fanoutTestSource(fanoutFakes(3)...), m)
		require.NoError(t, err)
		require.ElementsMatch(t, []int{1, 2, 3}, visited)
	})
	t.Run("fixed shard with unknown change time fans out", func(t *testing.T) {
		m := fanoutMetric(format.ShardFixed)
		m.ShardNum = 1
		m.UpdateTime = 0
		visited, err := run(t, fanoutTestSource(fanoutFakes(3)...), m)
		require.NoError(t, err)
		require.ElementsMatch(t, []int{1, 2, 3}, visited)
	})
	t.Run("pruned shard missing from config is an error", func(t *testing.T) {
		m := fanoutMetric(format.ShardFixed)
		m.ShardFixedKey = 3 // only shards 1..2 configured
		_, err := run(t, fanoutTestSource(fanoutFakes(2)...), m)
		require.Error(t, err)
		require.Contains(t, err.Error(), "duck shard 3")
		require.Contains(t, err.Error(), "--duck-shard-query-addrs")
	})
}

// TestFanoutSeriesDecode lowers columnar batches against the by entries —
// the shard column into shardNum, the string-top column into slot 47 — and
// rejects malformed responses instead of panicking.
func TestFanoutSeriesDecode(t *testing.T) {
	q := &seriesDataQuery{
		metric: fanoutMetric(format.ShardBuiltinDist),
		what:   fanoutWhats(data_model.DigestCount, data_model.DigestSum),
		by:     []int{0, format.ShardTagIndex, format.StringTopTagIndex},
	}
	resp := fanoutSeriesResp(1, fanoutSeriesBatch(2,
		[]int64{100, 200},
		[][]int64{{5, 6}, {1, 1}, {500, 600}},
		[][]string{{"a", "b"}, {}, {"topA", "topB"}},
		&tlstatshouse.StoreSeriesBatch{Count: []float64{10, 20}, Sum: []float64{1.5, 2.5}},
	))
	rows, err := decodeSeriesResponse(q, resp)
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, int64(5), rows[0].tag[0])
	require.Equal(t, "a", rows[0].stag[0])
	require.Equal(t, uint32(1), rows[0].shardNum) // the shard column answers shardNum
	require.Equal(t, int64(500), rows[0].tag[format.StringTopTagIndexV3])
	require.Equal(t, "topA", rows[0].stag[format.StringTopTagIndexV3])
	require.Equal(t, float64(10), rows[0].count)
	require.Equal(t, float64(1.5), rows[0].sum)
	require.Equal(t, int64(600), rows[1].tag[format.StringTopTagIndexV3])

	// absent optional columns decode as zeros, like the CH decode
	noCols, err := decodeSeriesResponse(q, fanoutSeriesResp(1, fanoutSeriesBatch(1, []int64{100}, [][]int64{{7}, {2}, {1}}, [][]string{{"x"}, {}, {"y"}}, nil)))
	require.NoError(t, err)
	require.Equal(t, float64(0), noCols[0].count)
	require.Nil(t, noCols[0].percentile)

	// a batch whose tag columns do not match the grouped tags is malformed
	_, err = decodeSeriesResponse(q, fanoutSeriesResp(1, fanoutSeriesBatch(1, []int64{100}, [][]int64{{7}}, nil, nil)))
	require.ErrorContains(t, err, "tag columns")

	// corrupt sketch states fail with the shard named
	badTd := &seriesDataQuery{metric: fanoutMetric(format.ShardBuiltinDist), what: fanoutWhats(data_model.DigestPercentile), by: []int{0}}
	_, err = decodeSeriesResponse(badTd, fanoutSeriesResp(4, fanoutSeriesBatch(1, []int64{100}, [][]int64{{7}}, nil,
		&tlstatshouse.StoreSeriesBatch{Percentiles: []string{"not-a-digest"}})))
	require.ErrorContains(t, err, "duck shard 4")

	var u data_model.ChUnique
	u.Insert(5)
	blobWithTail := append(u.MarshallAppend(nil), 1, 2, 3)
	badUniq := &seriesDataQuery{metric: fanoutMetric(format.ShardBuiltinDist), what: fanoutWhats(data_model.DigestUnique), by: []int{0}}
	_, err = decodeSeriesResponse(badUniq, fanoutSeriesResp(1, fanoutSeriesBatch(1, []int64{100}, [][]int64{{7}}, nil,
		&tlstatshouse.StoreSeriesBatch{UniqState: []string{string(blobWithTail)}})))
	require.ErrorContains(t, err, "trailing bytes")

	// tag-values columns must be parallel
	_, err = decodeTagValuesResponse(tlstatshouse.StoreTagValuesResponse{Tag: []int64{1, 2}, Count: []float64{1}})
	require.ErrorContains(t, err, "columns")
}

// TestBuildStoreSeriesArgs lowers a semantic request onto the RPC: what
// kinds, grouped tags with their special entries, host and sort flags, the
// journal-derived layout and version, the resolution window with its zone,
// and every filter arm the CH builder writes.
func TestBuildStoreSeriesArgs(t *testing.T) {
	metric := testMetricWithTags(t, "req_metric",
		format.MetricMetaTag{Name: "t1"},
		format.MetricMetaTag{Name: "t2", RawKind: "int"},   // raw32
		format.MetricMetaTag{Name: "t3", RawKind: "int64"}, // raw64
	)
	metric.MetricID = 77
	metric.Version = 1234

	filterIn := data_model.TagFilters{}
	filterIn.Tags[1].Values = data_model.TagValues{
		data_model.NewTagValueM(7),        // mapped arm only
		data_model.NewTagValue("str", 12), // mapped + values arms
		data_model.NewTagValueS("plain"),  // values arm only
		data_model.NewTagValue("", 0),     // the empty arm
	}
	filterIn.Tags[2].Re2 = `^abc.*$`
	filterNotIn := data_model.TagFilters{}
	filterNotIn.Tags[1].Values = data_model.TagValues{data_model.NewTagValueM(3)}

	moscow, err := time.LoadLocation("Europe/Moscow")
	require.NoError(t, err)
	lod := data_model.LOD{FromSec: 1000, ToSec: 2000, StepSec: 60, Location: moscow}

	args := buildStoreSeriesArgs(&seriesDataQuery{
		metric:      metric,
		what:        fanoutWhats(data_model.DigestCount, data_model.DigestUnique),
		by:          []int{1, format.ShardTagIndex, format.StringTopTagIndex},
		filterIn:    filterIn,
		filterNotIn: filterNotIn,
		sort:        sortDescending,
		minMaxHost:  [2]bool{true, true},
		utcOffset:   10800,
	}, lod, 123)

	require.Equal(t, []int32{int32(data_model.DigestCount), int32(data_model.DigestUnique)}, args.What)
	require.Equal(t, []int32{1, format.ShardTagIndex, format.StringTopTagIndex}, args.By)
	require.True(t, args.IsSetMinHost())
	require.True(t, args.IsSetMaxHost())
	require.True(t, args.IsSetSortDesc())
	require.False(t, args.IsSetSortAsc())
	require.Equal(t, int32(123), args.Base.TimeoutMs)

	require.Equal(t, int32(77), args.Base.MetricId)
	require.Equal(t, int64(1234), args.Base.MetricVersion)
	// Tags[0] is the placeholder empty tag of testMetricWithTags: mapped,
	// then t1 mapped, t2 raw32, t3 raw64
	require.Equal(t, []int32{0, 0, 1, 2}, args.Base.TagLayout.Kinds)

	require.Equal(t, int64(1000), args.Base.Lod.FromSec)
	require.Equal(t, int64(2000), args.Base.Lod.ToSec)
	require.Equal(t, int64(60), args.Base.Lod.StepSec)
	require.Equal(t, int64(10800), args.Base.Lod.UtcOffset)
	require.Equal(t, "Europe/Moscow", args.Base.Lod.Location)

	require.Len(t, args.Base.FilterIn, 2)
	tag1 := args.Base.FilterIn[0]
	require.Equal(t, int32(1), tag1.TagIndex)
	require.Equal(t, []int64{7, 12}, tag1.Mapped)
	require.Equal(t, []string{"str", "plain"}, tag1.Values)
	require.True(t, tag1.IsSetEmpty())
	require.False(t, tag1.IsSetRe2())
	tag2 := args.Base.FilterIn[1]
	require.Equal(t, int32(2), tag2.TagIndex)
	require.Equal(t, `^abc.*$`, tag2.Re2)
	require.True(t, tag2.IsSetRe2())
	require.Len(t, args.Base.FilterNotIn, 1)
	require.Equal(t, []int64{3}, args.Base.FilterNotIn[0].Mapped)

	// a nil location names UTC
	argsUTC := buildStoreSeriesArgs(&seriesDataQuery{metric: metric}, data_model.LOD{FromSec: 1, ToSec: 2, StepSec: 60}, 0)
	require.Equal(t, "UTC", argsUTC.Base.Lod.Location)

	// sort and host flags default off
	argsPlain := buildStoreSeriesArgs(&seriesDataQuery{metric: metric}, data_model.LOD{}, 0)
	require.False(t, argsPlain.IsSetSortDesc())
	require.False(t, argsPlain.IsSetSortAsc())
	require.False(t, argsPlain.IsSetMinHost())
	require.False(t, argsPlain.IsSetMaxHost())
}

// TestBuildStoreQueryBaseMetricLists: without a single metric, the filter's
// metric lists address the query — with the one-entry collapse the CH
// builder applies.
func TestBuildStoreQueryBaseMetricLists(t *testing.T) {
	base := buildStoreQueryBase(nil, data_model.TagFilters{Metrics: []*format.MetricMetaValue{{MetricID: 1000}, {MetricID: 1001}}},
		data_model.TagFilters{Metrics: []*format.MetricMetaValue{{MetricID: 1002}}}, data_model.LOD{}, 0, 0)
	require.Equal(t, int32(0), base.MetricId)
	require.Equal(t, []int32{1000, 1001}, base.MetricIn)
	require.Equal(t, []int32{1002}, base.MetricNotIn)
	require.Nil(t, base.TagLayout.Kinds)

	// exactly one filter-in metric collapses to the single-id predicate
	base = buildStoreQueryBase(nil, data_model.TagFilters{Metrics: []*format.MetricMetaValue{{MetricID: 1005}}}, data_model.TagFilters{}, data_model.LOD{}, 0, 0)
	require.Equal(t, int32(1005), base.MetricId)
	require.Nil(t, base.MetricIn)
}

// TestBuildStoreTagValuesArgs: the tag-values verb carries the tag index and
// the ids-only mode — and deliberately no user top N for the shards to apply.
func TestBuildStoreTagValuesArgs(t *testing.T) {
	args := buildStoreTagValuesArgs(&tagValuesDataQuery{
		metric:     fanoutMetric(format.ShardFixed),
		tag:        format.MetricMetaTag{Index: 3},
		idsOnly:    true,
		numResults: 2,
	}, data_model.LOD{FromSec: 1, ToSec: 2, StepSec: 60}, 7)
	require.Equal(t, int32(3), args.TagIndex)
	require.True(t, args.IsSetIdsOnly())
	require.Equal(t, int64(1), args.Base.Lod.FromSec)
	require.Equal(t, int32(7), args.Base.TimeoutMs)
	require.Equal(t, int32(1002), args.Base.MetricId)

	plain := buildStoreTagValuesArgs(&tagValuesDataQuery{metric: fanoutMetric(format.ShardFixed), tag: format.MetricMetaTag{Index: 1}}, data_model.LOD{}, 0)
	require.False(t, plain.IsSetIdsOnly())
}

// TestFanoutRetryOnceOnMetadataMismatch: a metadata mismatch triggers one
// bounded journal wait, a metric re-read and a rebuilt request; the retry's
// layout and version are the fresh ones. A second mismatch fails the query.
func TestFanoutRetryOnceOnMetadataMismatch(t *testing.T) {
	mismatch := duckstore.NewError(duckstore.ErrCodeMetadataMismatch, "tag_layout disagrees at version %d", 42)

	t.Run("retry succeeds with the fresh metric", func(t *testing.T) {
		fakes := fanoutFakes(1)
		calls := 0
		fakes[0].seriesFn = func(_ context.Context, args tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
			calls++
			if calls == 1 {
				return tlstatshouse.StoreSeriesResponse{}, mismatch
			}
			require.Equal(t, int64(43), args.Base.MetricVersion) // the rebuilt request carries the fresh version
			return fanoutSeriesResp(1, fanoutSeriesBatch(1, []int64{100}, [][]int64{{1}}, nil,
				&tlstatshouse.StoreSeriesBatch{Count: []float64{3}})), nil
		}
		src := fanoutTestSource(fakes...)
		var waited []int64
		src.waitVersionOverride = func(_ context.Context, version int64) error {
			waited = append(waited, version)
			return nil
		}
		fresh := fanoutMetric(format.ShardFixed)
		fresh.Version = 43
		src.refreshMetricOverride = func(int32) *format.MetricMetaValue { return fresh }

		rows, err := fanoutRunSeries(t, src, &seriesDataQuery{
			metric: fanoutMetric(format.ShardFixed), // Version 0
			what:   fanoutWhats(data_model.DigestCount),
			by:     []int{0},
		})
		require.NoError(t, err)
		require.Equal(t, []int64{1}, waited) // waited for old version + 1
		require.Len(t, rows, 1)
		require.Equal(t, float64(3), rows[0].count)
		require.Equal(t, 2, calls)
	})

	t.Run("second mismatch fails", func(t *testing.T) {
		fakes := fanoutFakes(1)
		fakes[0].seriesFn = func(context.Context, tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
			return tlstatshouse.StoreSeriesResponse{}, mismatch
		}
		src := fanoutTestSource(fakes...)
		src.waitVersionOverride = func(context.Context, int64) error { return nil }
		fresh := fanoutMetric(format.ShardFixed)
		fresh.Version = 43
		src.refreshMetricOverride = func(int32) *format.MetricMetaValue { return fresh }

		_, err := fanoutRunSeries(t, src, &seriesDataQuery{metric: fanoutMetric(format.ShardFixed), what: fanoutWhats(data_model.DigestCount)})
		require.Error(t, err)
		require.True(t, duckstore.IsCode(err, duckstore.ErrCodeMetadataMismatch))
	})

	t.Run("metric gone from the journal fails without retrying", func(t *testing.T) {
		fakes := fanoutFakes(1)
		fakes[0].seriesFn = func(context.Context, tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
			return tlstatshouse.StoreSeriesResponse{}, mismatch
		}
		src := fanoutTestSource(fakes...)
		src.waitVersionOverride = func(context.Context, int64) error { return nil }
		src.refreshMetricOverride = func(int32) *format.MetricMetaValue { return nil }

		_, err := fanoutRunSeries(t, src, &seriesDataQuery{metric: fanoutMetric(format.ShardFixed), what: fanoutWhats(data_model.DigestCount)})
		require.ErrorContains(t, err, "left the journal")
	})
}

// TestStoreQueryTimeoutMs: the relative timeout rides the context deadline
// the HTTP handler already enforces, clamped to a positive int32; no deadline
// leaves the aggregator's own default in charge.
func TestStoreQueryTimeoutMs(t *testing.T) {
	require.Equal(t, int32(0), storeQueryTimeoutMs(context.Background()))

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	ms := storeQueryTimeoutMs(ctx)
	require.Greater(t, ms, int32(900))
	require.LessOrEqual(t, ms, int32(1500))

	expired, cancel2 := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel2()
	time.Sleep(time.Millisecond)
	require.Equal(t, int32(1), storeQueryTimeoutMs(expired))
}

// TestFanoutMergeDeterminism: two shards' contributions to the same key
// merge to the same result regardless of which shard's rows arrive first in
// the per-shard slice, because the merge folds rows in the order they were
// passed and the numeric folds are commutative.
func TestFanoutMergeDeterminism(t *testing.T) {
	makeRows := func(count float64) []tsSelectRow {
		var rows []tsSelectRow
		for i := 0; i < 3; i++ {
			r := tsSelectRow{time: 100}
			r.tag[0] = 1
			r.count = count
			r.sum = count
			rows = append(rows, r)
		}
		return rows
	}
	forward, err := mergeShardRows([][]tsSelectRow{makeRows(1), makeRows(2)}, fanoutRowCap)
	require.NoError(t, err)
	backward, err := mergeShardRows([][]tsSelectRow{makeRows(2), makeRows(1)}, fanoutRowCap)
	require.NoError(t, err)
	require.Len(t, forward, 1)
	require.Len(t, backward, 1)
	require.Equal(t, forward[0].count, backward[0].count)
	require.Equal(t, float64(9), forward[0].count) // 3 rows of 1 + 3 rows of 2
	// rows on distinct keys stay distinct: two shard-tag groups never fold
	a := tsSelectRow{time: 100}
	b := tsSelectRow{time: 100}
	b.shardNum = 2
	distinct, err := mergeShardRows([][]tsSelectRow{{a}, {b}}, fanoutRowCap)
	require.NoError(t, err)
	require.Len(t, distinct, 2)
}

// TestRPCStoreShardClientCryptoKey proves the fan-out client presents the
// configured RPC crypto key at the handshake: a store-query server that
// requires the key answers a client built with the same key, and refuses one
// built with a different key. The api and the aggregator run in different
// containers in production, so the nonce exchange requires encryption and a
// keyless client cannot talk to the shards at all — the exact failure the
// e2e duck stack would hit without the key threaded through.
func TestRPCStoreShardClientCryptoKey(t *testing.T) {
	const serverKey = "test-store-query-crypto-key-00000" // 34 bytes >= rpc.MinCryptoKeyLen (32)
	startServer := func(t *testing.T) string {
		t.Helper()
		h := tlstatshouse.Handler{
			RawStoreQuerySeries: func(_ context.Context, hctx *rpc.HandlerContext) error {
				var args tlstatshouse.StoreQuerySeries
				if _, err := args.ReadTL1(hctx.Request); err != nil {
					return err
				}
				var resp tlstatshouse.StoreSeriesResponse
				hctx.Response, _ = args.WriteResultTL1(hctx.Response, resp)
				return nil
			},
		}
		srv := rpc.NewServer(rpc.ServerWithCryptoKeys([]string{serverKey}),
			// production peers sit in different containers, so their nonce
			// exchange always runs encrypted; force the same on loopback so
			// the key is actually verified rather than skipped as same-machine
			rpc.ServerWithForceEncryption(true),
			rpc.ServerWithLogf(t.Logf),
			rpc.ServerWithMaxWorkers(8),
			rpc.ServerWithSyncHandler(h.Handle))
		ln, err := net.Listen("tcp4", "127.0.0.1:0")
		require.NoError(t, err)
		go func() { _ = srv.Serve(ln) }()
		t.Cleanup(srv.Shutdown)
		return ln.Addr().String()
	}
	addr := startServer(t)

	queryOne := func(key string, timeout time.Duration) error {
		clients := newRPCStoreShardClients(map[uint32]string{1: addr}, key)
		require.Len(t, clients, 1)
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		_, err := clients[0].querySeries(ctx, tlstatshouse.StoreQuerySeries{})
		return err
	}

	require.NoError(t, queryOne(serverKey, 10*time.Second), "the fan-out client must present the configured key and complete the handshake")
	// a wrong key fails each handshake attempt; the client retries with
	// backoff, so a short deadline still proves it can never succeed
	require.Error(t, queryOne("a-completely-different-key-000000", 2*time.Second), "a client with the wrong key must fail the handshake")
}

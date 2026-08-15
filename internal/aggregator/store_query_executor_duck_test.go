// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package aggregator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
)

// The real executor end to end: the journal validation of the previous file,
// then the DuckDB renderer over rows seeded through the real sink. A
// disagreeing layout is refused with metadata_mismatch while rows sit in the
// store — proof the renderer never answered them under a reinterpretation.

// executorShardNum is the shard number the fixture wires the executor with;
// the series response must echo it.
const executorShardNum = int32(7)

// tagValues builds a tag array with the given index/value pairs.
func tagValues(pairs ...int32) [format.MaxTags]int32 {
	var tags [format.MaxTags]int32
	for i := 0; i+1 < len(pairs); i += 2 {
		tags[pairs[i]] = pairs[i+1]
	}
	return tags
}

// newExecutorFixture opens the real store and journal fixture and seeds rows
// for both fixture metrics: the mapped one (values 11 and 12 on tag 1, value
// 11 in two partial rows) and the raw64 one (its value split across tags 1
// and 2). b1 is the minute-aligned bucket the rows land in, returned for
// building query windows.
func newExecutorFixture(t *testing.T) (storeQueryExecutor, int64) {
	t.Helper()
	config := DefaultConfigAggregator()
	config.DuckStoreDir = t.TempDir()
	handle, err := openDuckStore(config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })

	storage := validateTestStorage(t)
	executor := handle.(*duckStore).QueryExecutor(storage, executorShardNum)

	b1 := time.Now().Add(-time.Minute).Unix() / 60 * 60 // minute-aligned, inside the ingestion guard
	rows := []insertRow{
		{key: data_model.Key{Metric: validateMetricMapped, Tags: tagValues(1, 11)}, count: 3},
		{key: data_model.Key{Metric: validateMetricMapped, Tags: tagValues(1, 11)}, count: 4},
		{key: data_model.Key{Metric: validateMetricMapped, Tags: tagValues(1, 12)}, count: 9},
		// the raw64 metric's value -2: low half in tag 1, high half in tag 2
		{key: data_model.Key{Metric: validateMetricRaw64, Tags: tagValues(1, -2, 2, -1)}, count: 1},
	}
	sink := handle.NewSink()
	for i := range rows {
		rows[i].key.Timestamp = uint32(b1)
		sink.AppendRow(&rows[i])
	}
	_, _, _, err = sink.Send(context.Background())
	require.NoError(t, err)
	return executor, b1
}

// tagValuesArgs builds a tag-values query over the fixture's minute bucket.
func tagValuesArgs(metric int32, kinds []int32, tag int32, b1 int64) tlstatshouse.StoreQueryTagValues {
	return tlstatshouse.StoreQueryTagValues{
		Base:     withLod(b1, validateBase(metric, kinds, 11)),
		TagIndex: tag,
	}
}

func withLod(b1 int64, base tlstatshouse.StoreQueryBase) tlstatshouse.StoreQueryBase {
	base.Lod = tlstatshouse.StoreLod{FromSec: b1, ToSec: b1 + 60, StepSec: 60}
	return base
}

// The executor answers a validated tag-values query with the seeded counts,
// sums rows sharing a value, refuses a disagreeing layout before the renderer
// (rows for the metric exist and a reinterpreted reading would answer them),
// and maps the absent-metric and malformed-request cases onto their codes.
func TestDuckQueryExecutorValidatesThenRenders(t *testing.T) {
	e, b1 := newExecutorFixture(t)

	// the journal agrees: the renderer answers, folding both rows of value 11
	resp, err := e.QueryTagValues(context.Background(), tagValuesArgs(validateMetricMapped, []int32{0, 0}, 1, b1))
	require.NoError(t, err)
	require.Equal(t, []int64{12, 11}, resp.Tag)
	require.Equal(t, []string{"", ""}, resp.Stag)
	require.Equal(t, []float64{9, 7}, resp.Count)

	// the journal disagrees about the raw64 metric's tag 1: refused, though
	// the row sits in the store and a mapped reading of it would answer —
	// the value would come back as tag 1 alone, i.e. reinterpreted
	_, err = e.QueryTagValues(context.Background(), tagValuesArgs(validateMetricRaw64, []int32{0, 0, 0}, 1, b1))
	requireErrorCode(t, err, duckstore.ErrCodeMetadataMismatch, "disagreeing layout")

	// the same metric under its true layout answers with the whole 64 bits
	resp, err = e.QueryTagValues(context.Background(), tagValuesArgs(validateMetricRaw64, []int32{0, 2, 0}, 1, b1))
	require.NoError(t, err)
	require.Equal(t, []int64{-2}, resp.Tag, "the halves recombine into the whole value")

	// an absent metric is unknown_metric, not an empty answer
	_, err = e.QueryTagValues(context.Background(), tagValuesArgs(999, []int32{0, 0}, 1, b1))
	requireErrorCode(t, err, duckstore.ErrCodeUnknownMetric, "absent metric")

	// a malformed request (tag outside the claimed layout) survives validation
	// and fails in the renderer as bad_request
	_, err = e.QueryTagValues(context.Background(), tagValuesArgs(validateMetricMapped, []int32{0, 0}, 16, b1))
	requireErrorCode(t, err, duckstore.ErrCodeBadRequest, "tag outside the layout")

	// the series verb validates the same way and answers with this shard's number
	series := tlstatshouse.StoreQuerySeries{
		Base: withLod(b1, validateBase(validateMetricMapped, []int32{0, 0}, 11)),
		What: []int32{int32(data_model.DigestCount)},
		By:   []int32{1},
	}
	series.SetSortAsc(true)
	sresp, err := e.QuerySeries(context.Background(), series)
	require.NoError(t, err)
	require.Equal(t, executorShardNum, sresp.ShardNum)
	require.Len(t, sresp.Batches, 1)
	require.Equal(t, []int64{11, 12}, sresp.Batches[0].Tag[0])
	require.Equal(t, []float64{7, 9}, sresp.Batches[0].Count)
}

// The same executor behind the real RPC listener: correct counts arrive
// through the wire, and a disagreeing layout arrives as the structured
// metadata_mismatch code at the client.
func TestStoreQueryServerRealExecutorThroughRPC(t *testing.T) {
	e, b1 := newExecutorFixture(t)
	cl := startTestQueryServer(t, e, DefaultQueryConcurrency)

	var ret tlstatshouse.StoreTagValuesResponse
	require.NoError(t, cl.StoreQueryTagValues(context.Background(),
		tagValuesArgs(validateMetricMapped, []int32{0, 0}, 1, b1), nil, &ret))
	require.Equal(t, []int64{12, 11}, ret.Tag)
	require.Equal(t, []float64{9, 7}, ret.Count)

	// the disagreeing layout through the wire keeps its structured code
	var mret tlstatshouse.StoreTagValuesResponse
	err := cl.StoreQueryTagValues(context.Background(),
		tagValuesArgs(validateMetricRaw64, []int32{0, 0, 0}, 1, b1), nil, &mret)
	requireErrorCode(t, err, duckstore.ErrCodeMetadataMismatch, "disagreeing layout over RPC")

	// and the absent metric arrives as unknown_metric
	var aret tlstatshouse.StoreTagValuesResponse
	err = cl.StoreQueryTagValues(context.Background(),
		tagValuesArgs(999, []int32{0, 0}, 1, b1), nil, &aret)
	requireErrorCode(t, err, duckstore.ErrCodeUnknownMetric, "absent metric over RPC")
}

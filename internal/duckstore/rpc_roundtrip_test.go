// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"bytes"
	"testing"

	"github.com/VKCOM/tl/pkg/rpc"
	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
)

// Tag-layout kinds as defined by statshouse.storeTagLayout in schema.tl.
const (
	layoutMapped = 0
	layoutRaw32  = 1
	layoutRaw64  = 2
)

// fullBase builds a StoreQueryBase with every optional field set, so the
// round-trip exercises every conditional on the request side.
func fullBase() tlstatshouse.StoreQueryBase {
	base := tlstatshouse.StoreQueryBase{
		MetricId:      4242,
		MetricVersion: 987654321,
		TagLayout: tlstatshouse.StoreTagLayout{Kinds: []int32{
			layoutMapped, layoutRaw32, layoutRaw64, layoutMapped, layoutRaw64,
		}},
		Lod: tlstatshouse.StoreLod{
			FromSec:   1_700_000_000,
			ToSec:     1_700_360_000,
			StepSec:   60, // any of the 11 LOD steps
			UtcOffset: 3 * 3600,
			Location:  "Europe/Moscow",
		},
		RowLimit:  1_000_000,
		TimeoutMs: 30_000,
	}
	base.SetMetricIn([]int32{4242, 4243, 4244})
	base.SetMetricNotIn([]int32{9999})

	// one filter per optional arm, plus one with several arms at once
	mapped := tlstatshouse.StoreTagFilter{TagIndex: 2}
	// raw64 values must travel whole, so they need the full long range
	mapped.SetMapped([]int64{1 << 40, -1, 1<<62 + 12345})
	values := tlstatshouse.StoreTagFilter{TagIndex: 0}
	values.SetValues([]string{"user_a", "user_b", ""})
	empty := tlstatshouse.StoreTagFilter{TagIndex: 3}
	empty.SetEmpty(true)
	re2 := tlstatshouse.StoreTagFilter{TagIndex: 1}
	re2.SetRe2(".*prod.*")
	bare := tlstatshouse.StoreTagFilter{TagIndex: 4} // no arm set is wire-legal

	notIn := tlstatshouse.StoreTagFilter{TagIndex: 2}
	notIn.SetMapped([]int64{7, 8})
	notIn.SetValues([]string{"gone"})
	notIn.SetEmpty(true)
	notIn.SetRe2("^tmp_")

	base.FilterIn = []tlstatshouse.StoreTagFilter{mapped, values, empty, re2, bare}
	base.FilterNotIn = []tlstatshouse.StoreTagFilter{notIn}
	return base
}

func TestStoreQuerySeriesRoundTripMaximal(t *testing.T) {
	src := tlstatshouse.StoreQuerySeries{Base: fullBase()}
	src.SetMinHost(true)
	src.SetMaxHost(true)
	src.SetSortDesc(true)
	src.What = []int32{
		int32(data_model.DigestCount),
		int32(data_model.DigestAvg),
		int32(data_model.DigestMin),
		int32(data_model.DigestMax),
		int32(data_model.DigestSum),
		int32(data_model.DigestStdDev),
		int32(data_model.DigestPercentile),
		int32(data_model.DigestCardinality),
		int32(data_model.DigestUnique),
		int32(data_model.DigestLast),
	}
	src.By = []int32{0, 1, format.ShardTagIndex}

	w := src.WriteTL1Boxed(nil)
	// the wire must carry our assigned constructor, so a corrupted magic must fail the read
	corrupted := append([]byte(nil), w...)
	corrupted[0] ^= 0xff
	var bad tlstatshouse.StoreQuerySeries
	_, err := bad.ReadTL1Boxed(corrupted)
	require.Error(t, err, "reading with a wrong constructor magic must fail")

	var dst tlstatshouse.StoreQuerySeries
	rest, err := dst.ReadTL1Boxed(w)
	require.NoError(t, err)
	require.Empty(t, rest)
	require.Equal(t, src, dst)
	require.True(t, dst.IsSetMinHost() && dst.IsSetMaxHost() && dst.IsSetSortDesc())
	require.False(t, dst.IsSetSortAsc())
	// raw64 stays whole: the mapped filter on tag 2 keeps its full-range longs
	require.Equal(t, src.Base.FilterIn[0].Mapped, dst.Base.FilterIn[0].Mapped)
}

// The series verb with an empty `what` is legal ("grouped tags and timestamps
// only") and must survive the codec, as must a request with no optional field.
func TestStoreQuerySeriesRoundTripEmptyWhat(t *testing.T) {
	src := tlstatshouse.StoreQuerySeries{
		Base: tlstatshouse.StoreQueryBase{
			MetricId:      -61, // the cache-invalidation log query
			MetricVersion: 1,
			Lod: tlstatshouse.StoreLod{
				FromSec:  100,
				ToSec:    200,
				StepSec:  1,
				Location: "",
			},
			RowLimit:  1000,
			TimeoutMs: 1000,
		},
		What: nil,
		By:   []int32{1},
	}
	src.SetSortAsc(true)

	var dst tlstatshouse.StoreQuerySeries
	rest, err := dst.ReadTL1Boxed(src.WriteTL1Boxed(nil))
	require.NoError(t, err)
	require.Empty(t, rest)
	require.Equal(t, src, dst)
	require.Empty(t, dst.What)
	require.True(t, dst.IsSetSortAsc())
	require.False(t, dst.IsSetSortDesc())
}

func TestStoreQueryTagValuesRoundTrip(t *testing.T) {
	src := tlstatshouse.StoreQueryTagValues{Base: fullBase()}
	src.TagIndex = 2
	src.SetIdsOnly(true)

	var dst tlstatshouse.StoreQueryTagValues
	rest, err := dst.ReadTL1Boxed(src.WriteTL1Boxed(nil))
	require.NoError(t, err)
	require.Empty(t, rest)
	require.Equal(t, src, dst)
	require.True(t, dst.IsSetIdsOnly())

	// and without the ids-only flag
	var src2 tlstatshouse.StoreQueryTagValues = src
	src2.FieldsMask = 0
	var dst2 tlstatshouse.StoreQueryTagValues
	_, err = dst2.ReadTL1Boxed(src2.WriteTL1Boxed(nil))
	require.NoError(t, err)
	require.Equal(t, src2, dst2)
	require.False(t, dst2.IsSetIdsOnly())
}

// fullBatch builds a StoreSeriesBatch with every optional column set,
// including both host columns in full.
func fullBatch() tlstatshouse.StoreSeriesBatch {
	batch := tlstatshouse.StoreSeriesBatch{
		Rows: 2,
		Time: []int64{1_700_000_000, 1_700_000_060},
		// one vector per grouped tag, in `by` order; the raw64 and raw32 tags
		// carry no string halves, so their stag vectors are empty. An empty
		// inner vector and nil are the same on the wire (length 0) and decode
		// to nil, so consumers must treat them alike — as the column comment
		// "empty vector if raw" implies.
		Tag:  [][]int64{{1, 2}, {1 << 40, 1<<40 + 1}, {7, 7}},
		Stag: [][]string{{"a", "b"}, nil, nil},
	}
	batch.SetCount([]float64{10, 20})
	batch.SetMin([]float64{-1.5, -2.5})
	batch.SetMax([]float64{1.5, 2.5})
	batch.SetSum([]float64{100.25, 200.5})
	batch.SetSumsquare([]float64{1000.125, 2000.25})
	batch.SetCardinality([]float64{9, 19})
	// aggregate state bytes ride as opaque strings and may contain any byte
	batch.SetPercentiles([]string{string([]byte{0x00, 0xff, 0x0d, 0x04}), string([]byte{0xaa})})
	batch.SetUniqState([]string{"\x00\x01statbytes", ""})
	batch.SetMinHostValue([]float64{-1.5, -2.5})
	batch.SetMinHostTag([]int32{11, 0})
	batch.SetMinHostStag([]string{"host_min_1", ""})
	batch.SetMaxHostValue([]float64{1.5, 2.5})
	batch.SetMaxHostTag([]int32{0, 22})
	batch.SetMaxHostStag([]string{"", "host_max_2"})
	return batch
}

func TestStoreSeriesResponseRoundTripAllColumns(t *testing.T) {
	full := fullBatch()
	// a batch with no optional column at all: timestamps and grouped tags only
	minimal := tlstatshouse.StoreSeriesBatch{
		Rows: 1,
		Time: []int64{1_700_000_120},
		Tag:  [][]int64{{5}, {1 << 33}, {9}},
		Stag: [][]string{{"c"}, nil, nil},
	}
	src := tlstatshouse.StoreSeriesResponse{ShardNum: 7, Batches: []tlstatshouse.StoreSeriesBatch{full, minimal}}

	var dst tlstatshouse.StoreSeriesResponse
	rest, err := dst.ReadTL1Boxed(src.WriteTL1Boxed(nil))
	require.NoError(t, err)
	require.Empty(t, rest)
	require.Equal(t, src, dst)
	require.Equal(t, full.Percentiles, dst.Batches[0].Percentiles)
	require.Equal(t, full.UniqState, dst.Batches[0].UniqState)
	require.Nil(t, dst.Batches[1].Count) // column absent, not zero-filled
}

// The response must also travel through the function's own result codec,
// which is the path the generated RPC client takes.
func TestStoreSeriesResponseResultPathRoundTrip(t *testing.T) {
	args := tlstatshouse.StoreQuerySeries{Base: fullBase()}
	src := tlstatshouse.StoreSeriesResponse{ShardNum: 3, Batches: []tlstatshouse.StoreSeriesBatch{fullBatch()}}

	w, err := args.WriteResultTL1(nil, src)
	require.NoError(t, err)
	var dst tlstatshouse.StoreSeriesResponse
	rest, err := args.ReadResultTL1(w, &dst)
	require.NoError(t, err)
	require.Empty(t, rest)
	require.Equal(t, src, dst)
}

func TestStoreTagValuesResponseRoundTrip(t *testing.T) {
	// ids-only mode: stag empty, counts parallel to tag
	src := tlstatshouse.StoreTagValuesResponse{
		Tag:   []int64{5, 6, 1 << 40},
		Stag:  []string{"", "", ""},
		Count: []float64{100, 50, 1},
	}
	var dst tlstatshouse.StoreTagValuesResponse
	rest, err := dst.ReadTL1Boxed(src.WriteTL1Boxed(nil))
	require.NoError(t, err)
	require.Empty(t, rest)
	require.Equal(t, src, dst)

	// values mode: both halves present
	src2 := tlstatshouse.StoreTagValuesResponse{
		Tag:   []int64{5, 0},
		Stag:  []string{"", "unmapped_string"},
		Count: []float64{7, 8},
	}
	var dst2 tlstatshouse.StoreTagValuesResponse
	_, err = dst2.ReadTL1Boxed(src2.WriteTL1Boxed(nil))
	require.NoError(t, err)
	require.Equal(t, src2, dst2)
}

// The structured error codes are part of the RPC contract: they must travel as
// application-level rpc errors and classify without string parsing.
func TestStoreErrorCodes(t *testing.T) {
	codes := []int32{
		ErrCodeBadRequest,
		ErrCodeUnknownMetric,
		ErrCodeMetadataMismatch,
		ErrCodeRowLimit,
		ErrCodeOverloaded,
		ErrCodeDeadlineExceeded,
		ErrCodeCanceled,
		ErrCodeInternal,
	}
	seen := map[int32]string{}
	for _, code := range codes {
		require.True(t, rpc.NewError(code, "").IsApplicationLevelError(),
			"code %d must be in the application-level range", code)
		name, ok := CodeName(code)
		require.True(t, ok)
		other, ok := seen[code]
		require.False(t, ok, "code %d collides with %s", code, other)
		seen[code] = name

		err := NewError(code, "metric %d missing", 42)
		got, ok := ErrorCode(err)
		require.True(t, ok)
		require.Equal(t, code, got)
		require.True(t, IsCode(err, code))
		require.Contains(t, err.Error(), name+":", "description carries the code name")
	}

	// an application error that is not one of ours must not classify as one
	_, ok := ErrorCode(rpc.NewDefaultError("some other -4xxx failure"))
	require.False(t, ok)
	_, ok = ErrorCode(bytes.ErrTooLarge) // not an rpc error at all
	require.False(t, ok)

	// the panic guard really guards
	require.Panics(t, func() { NewError(-5000, "not ours") })
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
)

// The tag-values renderer's tests. Like the series tests, everything is
// asserted by decoded value — never on the built SQL — and the seeded rows go
// through the real writer, so the renderer reads exactly what ingestion lands.

// tagValuesReq builds a storeQueryTagValues over one metric with the given
// layout, addressing one tag over a LOD window (UTC, utc_offset 0).
func tagValuesReq(metric int32, kinds []int32, tag int32, from, to, step int64) tlstatshouse.StoreQueryTagValues {
	return tlstatshouse.StoreQueryTagValues{
		Base: tlstatshouse.StoreQueryBase{
			MetricId:  metric,
			TagLayout: tlstatshouse.StoreTagLayout{Kinds: kinds},
			Lod:       tlstatshouse.StoreLod{FromSec: from, ToSec: to, StepSec: step},
		},
		TagIndex: tag,
	}
}

func renderTagValues(t *testing.T, s *Store, args tlstatshouse.StoreQueryTagValues) tlstatshouse.StoreTagValuesResponse {
	t.Helper()
	resp, err := s.RenderTagValues(context.Background(), args)
	require.NoError(t, err)
	return resp
}

func renderTagValuesErr(t *testing.T, s *Store, args tlstatshouse.StoreQueryTagValues) error {
	t.Helper()
	_, err := s.RenderTagValues(context.Background(), args)
	return err
}

// seedTagValuesRows seeds the rows most tag-values tests read: mapped ids,
// one unmapped string on the queried tag, one count-zero group HAVING must
// drop, and one string on tag1 for filter tests.
func seedTagValuesRows(t *testing.T, w *Writer, b1 int64) {
	t.Helper()
	rows := []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 3},
		{Metric: testMetricID, Time: uint32(b1 + 30), Tags: tag0(11), Count: 4}, // same value, sums
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 9},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(13), Count: 0},  // zero-count group is dropped
		{Metric: testMetricID, Time: uint32(b1), Count: 2},                  // (0, "") — the empty pair
		{Metric: testMetricID, Time: uint32(b1), Count: 1},                  // (0, "beta") — unmapped string
		{Metric: testMetricID2, Time: uint32(b1), Tags: tag0(11), Count: 1}, // the other metric
	}
	rows[5].STags[0] = "beta"
	rows[6].STags[1] = "plain"
	require.NoError(t, w.WriteRound(context.Background(), rows))
}

// TestRenderTagValuesValuesMode covers values mode on a mapped tag: the value
// is the (id, unmapped string) pair, counts of rows sharing the pair sum,
// zero-count groups drop, and the answer arrives in the deterministic
// count-DESC order.
func TestRenderTagValuesValuesMode(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	seedTagValuesRows(t, w, b1)

	resp := renderTagValues(t, s, tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+60, 60))
	require.Equal(t, []int64{12, 11, 0, 0}, resp.Tag)
	require.Equal(t, []string{"", "", "", "beta"}, resp.Stag)
	require.Equal(t, []float64{9, 7, 2, 1}, resp.Count)
	require.Len(t, resp.Stag, len(resp.Tag), "values mode carries the string half")
}

// TestRenderTagValuesIdsOnly covers ids-only mode: the string half is not
// read, so every id-0 unmapped string collapses into the single zero group
// and the response carries no stag vector.
func TestRenderTagValuesIdsOnly(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	seedTagValuesRows(t, w, b1)

	q := tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+60, 60)
	q.SetIdsOnly(true)
	resp := renderTagValues(t, s, q)
	require.Equal(t, []int64{12, 11, 0}, resp.Tag)
	require.Empty(t, resp.Stag, "ids-only mode returns no string halves")
	require.Equal(t, []float64{9, 7, 3}, resp.Count, "both id-0 strings fold into the zero group")
}

// TestRenderTagValuesDeltaPlusArchive writes rows, rolls the generation,
// consumes it into archive windows and writes fresh rows into the new delta:
// one tag-values query over the range must sum the same value's counts across
// the window and the delta, seeing it exactly once per stored row.
func TestRenderTagValuesDeltaPlusArchive(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 1},
		{Metric: testMetricID, Time: uint32(b1 + 60), Tags: tag0(11), Count: 2},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 5},
	}))
	gen := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	require.NoError(t, s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{}))
	require.NotEmpty(t, s.Windows(), "consumption created archive windows")

	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1 + 120), Tags: tag0(11), Count: 4},
	}))

	resp := renderTagValues(t, s, tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+180, 60))
	require.Equal(t, []int64{11, 12}, resp.Tag)
	require.Equal(t, []float64{7, 5}, resp.Count, "value 11 sums 1+2 from the window and 4 from the delta")

	// a range covering only the window reads the window alone: value 11's
	// delta row (count 4) is absent, so its sum is 1, not 7
	resp = renderTagValues(t, s, tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+60, 60))
	require.Equal(t, []int64{12, 11}, resp.Tag)
	require.Equal(t, []float64{5, 1}, resp.Count)
}

// TestRenderTagValuesRawTags covers the raw layouts: a raw64 tag answers with
// the whole 64-bit value rebuilt from its two halves (negatives included) and
// a raw32 tag with its own column, both without any string half.
func TestRenderTagValuesRawTags(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	kinds := []int32{tagKindRaw64, tagKindRaw32}
	rows := []Row{
		{Metric: testMetricID, Time: uint32(b1), Count: 1}, // value 0
		{Metric: testMetricID, Time: uint32(b1), Count: 2}, // value -2
		{Metric: testMetricID, Time: uint32(b1), Count: 5}, // value 5
		{Metric: testMetricID, Time: uint32(b1), Count: 1}, // value 1<<32
		{Metric: testMetricID, Time: uint32(b1), Count: 3}, // tag1 value 9
	}
	rows[1].Tags[0], rows[1].Tags[1] = -2, -1
	rows[2].Tags[0] = 5
	rows[3].Tags[0], rows[3].Tags[1] = 0, 1
	rows[4].Tags[1] = 9
	require.NoError(t, w.WriteRound(context.Background(), rows))

	resp := renderTagValues(t, s, tagValuesReq(testMetricID, kinds, 0, b1, b1+60, 60))
	// tag1 doubles as the high half of tag0, so the row storing 9 there forms
	// its own raw64 value 9<<32 in tag0's domain
	require.Equal(t, []int64{5, 9 << 32, -2, 0, 1 << 32}, resp.Tag, "whole 64-bit values, negatives included")
	require.Empty(t, resp.Stag)
	require.Equal(t, []float64{5, 3, 2, 1, 1}, resp.Count)

	// tag1 doubles as the raw64 high half and its own raw32 tag: querying it
	// reads the raw column alone
	resp = renderTagValues(t, s, tagValuesReq(testMetricID, kinds, 1, b1, b1+60, 60))
	require.Equal(t, []int64{0, 9, -1, 1}, resp.Tag)
	require.Equal(t, []float64{6, 3, 2, 1}, resp.Count)
	require.Empty(t, resp.Stag)
}

// TestRenderTagValuesStringTop covers the string top's flag alias: tag -1
// resolves onto slot 47, and the slot answers with the (id, string) pairs the
// writer lands there.
func TestRenderTagValuesStringTop(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	kinds := make([]int32, format.MaxTags)
	require.NoError(t, w.WriteRound(context.Background(), []Row{
		{Metric: testMetricID, Time: uint32(b1), Count: 4, Top: HostTag{ID: 7}},
		{Metric: testMetricID, Time: uint32(b1), Count: 1, Top: HostTag{S: "topstr"}},
		{Metric: testMetricID, Time: uint32(b1), Count: 2}, // empty top
	}))

	resp := renderTagValues(t, s, tagValuesReq(testMetricID, kinds, format.StringTopTagIndex, b1, b1+60, 60))
	require.Equal(t, []int64{7, 0, 0}, resp.Tag)
	require.Equal(t, []string{"", "", "topstr"}, resp.Stag)
	require.Equal(t, []float64{4, 2, 1}, resp.Count)
}

// TestRenderTagValuesRowLimit pins the safety-cap contract: the request
// carries no top N at all — the shard returns every value up to its row limit,
// because the global top N belongs to the API after the shards' counts are
// summed — and a query that would produce more values than the limit fails
// whole with row_limit instead of returning a partial value set.
func TestRenderTagValuesRowLimit(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	var rows []Row
	for i := 0; i < 5; i++ {
		rows = append(rows, Row{Metric: testMetricID, Time: uint32(b1 + 60*int64(i)), Tags: tag0(int32(10 + i)), Count: float64(5 - i)})
	}
	require.NoError(t, w.WriteRound(context.Background(), rows))

	q := tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+300, 60)
	resp := renderTagValues(t, s, q)
	require.Equal(t, []int64{10, 11, 12, 13, 14}, resp.Tag, "every value returns, not a top N")
	require.Equal(t, []float64{5, 4, 3, 2, 1}, resp.Count)

	q.Base.RowLimit = 4
	err := renderTagValuesErr(t, s, q)
	require.True(t, IsCode(err, ErrCodeRowLimit), "got %v", err)
	require.Contains(t, err.Error(), "at least 5")

	q.Base.RowLimit = 5
	resp = renderTagValues(t, s, q)
	require.Len(t, resp.Tag, 5, "exactly at the limit succeeds")
}

// TestRenderTagValuesFilters covers the metric predicates and the tag-filter
// arms — on the queried tag and on other tags — including quoting-hazard
// values and patterns that would break an interpolated statement.
func TestRenderTagValuesFilters(t *testing.T) {
	s, w := newTestWriter(t)
	b1 := (writerNowUnix - 7200) / 60 * 60
	nasty := "o'brien\"\\x'--"
	rows := []Row{
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(11), Count: 1},
		{Metric: testMetricID, Time: uint32(b1), Tags: tag0(12), Count: 1},
		{Metric: testMetricID2, Time: uint32(b1), Tags: tag0(11), Count: 1},
	}
	rows[1].STags[1] = nasty
	rows[2].STags[1] = "plain"
	require.NoError(t, w.WriteRound(context.Background(), rows))

	q := tagValuesReq(0, twoMappedKinds, 0, b1, b1+60, 60)
	q.Base.SetMetricIn([]int32{testMetricID2})
	require.Equal(t, []int64{11}, renderTagValues(t, s, q).Tag, "the IN list selects the other metric")

	q = tagValuesReq(0, twoMappedKinds, 0, b1, b1+60, 60)
	q.Base.SetMetricNotIn([]int32{testMetricID2})
	require.Equal(t, []int64{11, 12}, renderTagValues(t, s, q).Tag, "the NOT IN list keeps the first metric")

	f := tlstatshouse.StoreTagFilter{TagIndex: 0}
	f.SetMapped([]int64{12})
	q = tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+60, 60)
	q.Base.FilterIn = []tlstatshouse.StoreTagFilter{f}
	require.Equal(t, []int64{12}, renderTagValues(t, s, q).Tag, "the mapped arm filters the queried tag itself")

	f = tlstatshouse.StoreTagFilter{TagIndex: 1}
	f.SetValues([]string{nasty})
	q = tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+60, 60)
	q.Base.FilterIn = []tlstatshouse.StoreTagFilter{f}
	require.Equal(t, []int64{12}, renderTagValues(t, s, q).Tag, "the values arm matches the quoting-hazard string")

	f = tlstatshouse.StoreTagFilter{TagIndex: 1}
	f.SetRe2(`^o'brien"\\x.*--$`)
	q = tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+60, 60)
	q.Base.FilterIn = []tlstatshouse.StoreTagFilter{f}
	require.Equal(t, []int64{12}, renderTagValues(t, s, q).Tag, "the re2 arm matches the pattern")

	f = tlstatshouse.StoreTagFilter{TagIndex: 1}
	f.SetEmpty(true)
	q = tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+60, 60)
	q.Base.FilterIn = []tlstatshouse.StoreTagFilter{f}
	require.Equal(t, []int64{11}, renderTagValues(t, s, q).Tag, "the empty arm keeps the row lacking tag1")

	// both arms set — the negative-regex shape: the pattern arm replaces the
	// values arm (the builder's `else if`), so the enumerated value the
	// pattern keeps survives a NOT IN instead of being excluded twice
	f = tlstatshouse.StoreTagFilter{TagIndex: 1}
	f.SetValues([]string{nasty})
	f.SetRe2(`^plain`)
	q = tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+60, 60)
	q.Base.FilterNotIn = []tlstatshouse.StoreTagFilter{f}
	require.Equal(t, []int64{11, 12}, renderTagValues(t, s, q).Tag,
		"the values arm yields to the pattern, which nothing matches here")
}

// TestRenderTagValuesValidation walks the malformed-request table: each entry
// must fail as bad_request naming what was wrong, before any storage is
// touched.
func TestRenderTagValuesValidation(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir())
	b1 := (writerNowUnix - 7200) / 60 * 60
	cases := []struct {
		name    string
		mutate  func(q *tlstatshouse.StoreQueryTagValues)
		problem string
	}{
		{"step not a LOD step", func(q *tlstatshouse.StoreQueryTagValues) { q.Base.Lod.StepSec = 7 },
			"step_sec 7 is not a LOD step"},
		{"tag outside layout", func(q *tlstatshouse.StoreQueryTagValues) { q.TagIndex = 16 },
			"tag values on tag 16 is outside the tag layout of 2 kinds"},
		{"shard tag", func(q *tlstatshouse.StoreQueryTagValues) { q.TagIndex = format.ShardTagIndex },
			"the shard tag is not a stored tag"},
		{"string top without its slot", func(q *tlstatshouse.StoreQueryTagValues) { q.TagIndex = format.StringTopTagIndex },
			"outside the tag layout"},
		{"unknown layout kind", func(q *tlstatshouse.StoreQueryTagValues) {
			q.Base.TagLayout.Kinds = []int32{5, 0}
		}, "tag layout kind 5 at index 0 is not mapped, raw32 or raw64"},
		{"layout longer than storage", func(q *tlstatshouse.StoreQueryTagValues) {
			q.Base.TagLayout.Kinds = make([]int32, format.MaxTags+1)
		}, "more than the 48 tags stored"},
		{"raw64 without a high half", func(q *tlstatshouse.StoreQueryTagValues) {
			q.Base.TagLayout.Kinds = []int32{tagKindMapped, tagKindRaw64}
			q.TagIndex = 1
		}, "raw64 tag 1 has no high half in the tag layout"},
		{"filter outside layout", func(q *tlstatshouse.StoreQueryTagValues) {
			f := tlstatshouse.StoreTagFilter{TagIndex: 9}
			f.SetMapped([]int64{1})
			q.Base.FilterIn = []tlstatshouse.StoreTagFilter{f}
		}, "filter on tag 9 is outside the tag layout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := tagValuesReq(testMetricID, twoMappedKinds, 0, b1, b1+60, 60)
			tc.mutate(&q)
			requireBadRequest(t, renderTagValuesErr(t, s, q), tc.problem)
		})
	}
}

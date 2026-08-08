package main

import (
	"strings"
	"testing"
)

// TestSignatureAlignment pins the key-prefix convention that lets the expected
// model and the live API response compare equal: the API emits legacy tag IDs
// "key"+index ("key0".."key5") while the generator's positional keys are bare
// indices ("0".."5"), so expectedSignature prefixes the index with "key". It also
// confirms tagSignature drops the _h host tag the go client injects.
func TestSignatureAlignment(t *testing.T) {
	cases := []struct {
		name    string
		apiTags map[string]apiMetaTag
		genTags []tag
	}{
		{"no tags", map[string]apiMetaTag{}, nil},
		{"tagged", map[string]apiMetaTag{"key0": {"alpha"}, "key1": {"beta"}}, []tag{{"0", "alpha"}, {"1", "beta"}}},
		{"many", map[string]apiMetaTag{"key0": {"a"}, "key1": {"b"}, "key2": {"c"}, "key3": {"d"}, "key4": {"e"}, "key5": {"f"}},
			[]tag{{"0", "a"}, {"1", "b"}, {"2", "c"}, {"3", "d"}, {"4", "e"}, {"5", "f"}}},
		// The _h host tag is added by the client, never generated; it must be stripped
		// so an API series carrying it still matches the (host-free) expected series.
		{"_h stripped", map[string]apiMetaTag{"key0": {"val"}, "_h": {"host-abc"}}, []tag{{"0", "val"}}},
	}
	for _, tc := range cases {
		api := tagSignature(tc.apiTags)
		gen := expectedSignature(tc.genTags)
		if api != gen {
			t.Errorf("%s: API signature %q != expected signature %q", tc.name, api, gen)
		}
	}
}

// TestCompareMetric covers the four failure modes compareMetric surfaces: a
// count mismatch, a missing expected series, an EXTRA series the model does not
// expect (the bidirectional half of the check), and the sampling tripwire that
// must fail even when the counts match exactly.
func TestCompareMetric(t *testing.T) {
	base := uint32(1_700_000_000)
	m := counterMetric{
		Name:   "e2e_t_c_multi",
		QBKeys: []string{"0", "1"},
		Series: []counterSeries{{
			Tags:   []tag{{"0", "x"}, {"1", "p"}},
			Counts: map[uint32]float64{base: 5, base + 1: 6},
		}},
	}
	const sig = "key0=x;key1=p" // expectedSignature for {0:x,1:p}
	wantMeta := apiSeriesMeta{Tags: map[string]apiMetaTag{"key0": {"x"}, "key1": {"p"}}}

	mkResp := func(metas []apiSeriesMeta, data [][]float64, samplingAgg float64) *apiSeriesResponse {
		return &apiSeriesResponse{Data: apiResponseData{
			SamplingFactorAgg: samplingAgg,
			Series: apiSeries{
				Time:       []int64{int64(base), int64(base + 1)},
				SeriesMeta: metas,
				SeriesData: data,
			},
		}}
	}

	t.Run("exact match", func(t *testing.T) {
		resp := mkResp([]apiSeriesMeta{wantMeta}, [][]float64{{5, 6}}, 0)
		mm, miss, ex, samp := compareMetric(m, resp)
		if len(mm) != 0 || len(miss) != 0 || len(ex) != 0 || samp != 0 {
			t.Errorf("exact match not clean: mm=%v miss=%v ex=%v samp=%g", mm, miss, ex, samp)
		}
	})

	t.Run("wrong count", func(t *testing.T) {
		resp := mkResp([]apiSeriesMeta{wantMeta}, [][]float64{{5, 99}}, 0)
		mm, miss, ex, samp := compareMetric(m, resp)
		if len(mm) != 1 || mm[0].bucket != base+1 || mm[0].actual != 99 {
			t.Errorf("mismatches=%+v, want one at bucket %d actual 99", mm, base+1)
		}
		if len(miss) != 0 || len(ex) != 0 || samp != 0 {
			t.Errorf("unexpected non-clean fields: miss=%v ex=%v samp=%g", miss, ex, samp)
		}
	})

	t.Run("missing series", func(t *testing.T) {
		resp := mkResp(nil, nil, 0) // no series_meta → the expected series is absent
		mm, miss, ex, samp := compareMetric(m, resp)
		if len(mm) != 0 || len(ex) != 0 || samp != 0 {
			t.Errorf("unexpected non-clean fields: mm=%v ex=%v samp=%g", mm, ex, samp)
		}
		if len(miss) != 1 || miss[0] != sig {
			t.Errorf("missing=%v, want [%s]", miss, sig)
		}
	})

	t.Run("extra series", func(t *testing.T) {
		// Expected series present and correct, plus an unexpected second series.
		extraMeta := apiSeriesMeta{Tags: map[string]apiMetaTag{"key0": {"surprise"}, "key1": {"z"}}}
		resp := mkResp([]apiSeriesMeta{wantMeta, extraMeta}, [][]float64{{5, 6}, {7, 8}}, 0)
		mm, miss, ex, samp := compareMetric(m, resp)
		if len(mm) != 0 || len(miss) != 0 || samp != 0 {
			t.Errorf("unexpected non-clean fields: mm=%v miss=%v samp=%g", mm, miss, samp)
		}
		if len(ex) != 1 || ex[0] != "key0=surprise;key1=z" {
			t.Errorf("extras=%v, want [key0=surprise;key1=z]", ex)
		}
	})

	t.Run("sampling tripwire", func(t *testing.T) {
		// Counts match exactly, but data was sampled → must still be flagged.
		resp := mkResp([]apiSeriesMeta{wantMeta}, [][]float64{{5, 6}}, 2)
		mm, miss, ex, samp := compareMetric(m, resp)
		if len(mm) != 0 || len(miss) != 0 || len(ex) != 0 {
			t.Errorf("unexpected non-empty fields: mm=%v miss=%v ex=%v", mm, miss, ex)
		}
		if samp == 0 {
			t.Error("sampling = 0, want nonzero (SamplingFactorAgg=2)")
		}
		if detail := formatFail(m, "http://x", mm, miss, ex, samp); !strings.Contains(detail, "sampling") {
			t.Errorf("formatFail missing sampling line:\n%s", detail)
		}
	})
}

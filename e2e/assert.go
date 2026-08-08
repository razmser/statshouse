package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// This file implements spec §5 assertions for the counter subset: poll
// /api/query (w=1, ac=1, qw=count) per metric until the expected series appear,
// then assert EXACT per-bucket, per-(metric, tag-set)-series equality, with the
// §4 normalization (strip the go _h host tag, drop empty-valued tags — already
// applied to the expected model — and ignore client meta-metrics, which never
// collide with the e2e_<runID>_ prefix).

// assertTimeout is the worst-case poll window for a metric to converge. The
// historic conveyor is ~24s end-to-end; 60s leaves headroom for auto-create and
// agg insertion (spec §5).
const assertTimeout = 60 * time.Second

// apiSeriesResponse mirrors the minimal slice of the API's query reply. The
// payload is wrapped under a top-level "data" key (internal/api handler.go emits
// an envelope whose data field is the SeriesResponse); series lives at
// data.series. SeriesData is [][]float64: the API marshals a missing point (NaN)
// as JSON null, which encoding/json turns into 0.0 — and every expected count is
// ≥1, so a 0 is an unambiguous failure.
type apiSeriesResponse struct {
	Data apiResponseData `json:"data"`
}

type apiResponseData struct {
	Series            apiSeries `json:"series"`
	SamplingFactorSrc float64   `json:"sampling_factor_src"`
	SamplingFactorAgg float64   `json:"sampling_factor_agg"`
}

type apiSeries struct {
	Time       []int64         `json:"time"`
	SeriesMeta []apiSeriesMeta `json:"series_meta"`
	SeriesData [][]float64     `json:"series_data"`
}

type apiSeriesMeta struct {
	Tags map[string]apiMetaTag `json:"tags"`
}

type apiMetaTag struct {
	Value string `json:"value"`
}

// assertCounters polls and asserts every metric in the stream. Returns pass/fail
// counts; each metric yields exactly one PASS or FAIL line (FAIL carries the
// metric, a bucket timestamp, expected vs actual, and the query URL).
func assertCounters(ctx context.Context, rec *recorder, apiAddr string, stream counterStream) (passed, failed int) {
	for _, m := range stream.Metrics {
		ok, detail := pollMetric(ctx, apiAddr, m, stream.Base)
		switch {
		case ok:
			passed++
			rec.logf("PASS metric=%s series=%d buckets=%d", m.Name, len(m.Series), numBuckets)
			fmt.Printf("PASS metric=%s series=%d buckets=%d\n", m.Name, len(m.Series), numBuckets)
		default:
			failed++
			rec.logf("FAIL metric=%s\n%s", m.Name, detail)
			fmt.Printf("FAIL metric=%s\n%s\n", m.Name, indent(detail))
		}
	}
	return passed, failed
}

// pollMetric queries a metric repeatedly until it matches the expected model or
// assertTimeout elapses. detail is the last observed failure (for the FAIL line).
func pollMetric(ctx context.Context, apiAddr string, m counterMetric, base uint32) (bool, string) {
	qurl := metricQueryURL(apiAddr, m, base)
	var lastDetail string
	if err := poll(ctx, assertTimeout, 2*time.Second, func() (bool, error) {
		resp, qerr := queryCounter(ctx, qurl)
		if qerr != nil {
			lastDetail = fmt.Sprintf("query error: %v\nurl: %s", qerr, qurl)
			return false, nil
		}
		mismatches, missing, extras, sampling := compareMetric(m, resp)
		// All four must be clean: exact counts, no missing series, no extra series,
		// and no sampling (spec §5: a nonzero sampling factor means data was
		// sampled and must fail even when the counts happen to match).
		if len(mismatches) == 0 && len(missing) == 0 && len(extras) == 0 && sampling == 0 {
			return true, nil
		}
		lastDetail = formatFail(m, qurl, mismatches, missing, extras, sampling)
		return false, nil
	}); err != nil {
		return false, lastDetail
	}
	return true, ""
}

// metricQueryURL builds GET /api/query for one metric at 1s LOD over its 70
// buckets. qb is repeated per group-by tag key; with no keys the API collapses to
// a single series (GROUP BY _time), which is correct for tag-less metrics.
func metricQueryURL(apiAddr string, m counterMetric, base uint32) string {
	q := url.Values{}
	q.Set("s", m.Name)
	q.Set("f", strconv.FormatUint(uint64(base), 10))
	q.Set("t", strconv.FormatUint(uint64(base+numBuckets), 10))
	// "1s" (not bare "1"): a bare width is parsed as screen-width=1 → auto-resolution
	// collapsing the range to ~1 point at the 1m table; the "s" suffix makes it an
	// explicit 1-second step so the 1s table (statshouse_v6_1s) is used.
	q.Set("w", "1s")
	q.Set("ac", "1") // defeat the ~1s API query cache
	q.Set("qw", "count")
	for _, k := range m.QBKeys {
		q.Add("qb", k)
	}
	return "http://" + apiAddr + "/api/query?" + q.Encode()
}

func queryCounter(ctx context.Context, qurl string) (*apiSeriesResponse, error) {
	body, code, err := httpGet(ctx, qurl)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("HTTP %d: %s", code, truncate(strings.TrimSpace(body), 300))
	}
	var resp apiSeriesResponse
	if err := json.Unmarshal([]byte(body), &resp); err != nil {
		return nil, fmt.Errorf("parse response: %w; body: %s", err, truncate(body, 300))
	}
	return &resp, nil
}

// seriesMismatch is one (series, bucket) where expected ≠ actual.
type seriesMismatch struct {
	seriesSig string
	bucket    uint32
	expected  float64
	actual    float64
}

// compareMetric indexes the API series by normalized tag signature and compares
// every expected (series, bucket) count, and rejects any EXTRA series the API
// returned beyond the expected model — the comparison is bidirectional, so a
// series we never wrote is as much a failure as one going missing.
// mismatches/missing/extras are empty on a match. sampling is the sum of the two
// sampling factors (must be 0 — spec §5 tripwire).
func compareMetric(m counterMetric, resp *apiSeriesResponse) (mismatches []seriesMismatch, missing, extras []string, sampling float64) {
	actual := make(map[string]map[uint32]float64, len(resp.Data.Series.SeriesMeta))
	for i, meta := range resp.Data.Series.SeriesMeta {
		data := resp.Data.Series.SeriesData[i]
		buckets := make(map[uint32]float64, len(resp.Data.Series.Time))
		for j, ts := range resp.Data.Series.Time {
			if j < len(data) {
				buckets[uint32(ts)] = data[j]
			}
		}
		actual[tagSignature(meta.Tags)] = buckets
	}
	want := make(map[string]bool, len(m.Series))
	for _, es := range m.Series {
		sig := expectedSignature(es.Tags)
		want[sig] = true
		got, ok := actual[sig]
		if !ok {
			missing = append(missing, sig)
			continue
		}
		for ts, exp := range es.Counts {
			// Missing buckets unmarshal to 0.0; expected is always ≥1, so a 0 is a
			// real failure rather than a missing-point ambiguity.
			if got[ts] != exp {
				mismatches = append(mismatches, seriesMismatch{sig, ts, exp, got[ts]})
			}
		}
	}
	for sig := range actual {
		if !want[sig] {
			extras = append(extras, sig)
		}
	}
	// Stable ordering for deterministic FAIL output.
	sort.Slice(mismatches, func(i, j int) bool {
		if mismatches[i].seriesSig != mismatches[j].seriesSig {
			return mismatches[i].seriesSig < mismatches[j].seriesSig
		}
		return mismatches[i].bucket < mismatches[j].bucket
	})
	sort.Strings(missing)
	sort.Strings(extras)
	sampling = resp.Data.SamplingFactorSrc + resp.Data.SamplingFactorAgg
	return mismatches, missing, extras, sampling
}

func formatFail(m counterMetric, qurl string, mismatches []seriesMismatch, missing, extras []string, sampling float64) string {
	var b strings.Builder
	if sampling != 0 {
		fmt.Fprintf(&b, "sampling_factor_src+agg=%g (expected 0 — data was sampled)\n", sampling)
	}
	for _, mm := range mismatches {
		fmt.Fprintf(&b, "series{%s} bucket=%d expected=%g actual=%g\n", mm.seriesSig, mm.bucket, mm.expected, mm.actual)
	}
	for _, sig := range missing {
		fmt.Fprintf(&b, "series{%s} expected but absent in response\n", sig)
	}
	for _, sig := range extras {
		fmt.Fprintf(&b, "series{%s} present in response but not expected\n", sig)
	}
	fmt.Fprintf(&b, "url: %s", qurl)
	return b.String()
}

// tagSignature is the normalized identity of an API series: sorted "k=v" pairs
// with the go client's _h host tag stripped (it is never in the harness's qb, so
// it does not appear here in practice — stripped defensively per spec §4).
func tagSignature(tags map[string]apiMetaTag) string {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		if k == "_h" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+tags[k].Value)
	}
	return strings.Join(parts, ";")
}

// expectedSignature mirrors tagSignature for a normalized expected series. The
// generator's tag keys are positional index strings ("0".."5"); the API emits the
// legacy tag ID "key"+index ("key0".."key5") as the series_meta map key
// (internal/format TagIDLegacy), so the index is prefixed here to match.
func expectedSignature(tags []tag) string {
	cp := append([]tag(nil), tags...)
	sort.Slice(cp, func(i, j int) bool { return cp[i].Key < cp[j].Key })
	parts := make([]string, 0, len(cp))
	for _, t := range cp {
		parts = append(parts, "key"+t.Key+"="+t.Val)
	}
	return strings.Join(parts, ";")
}

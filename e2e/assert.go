package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// This file implements spec §5 assertions for the FULL metric stream, polling
// /api/query (w=1, ac=1) per (metric, query-function) until the expected series
// appear, then asserting per-bucket, per-(metric, tag-set)-series equality —
// exact for counter/value/unique-small/stag, within a tolerance band for
// value_p percentiles and the big-unique estimate — with the §4 normalization
// (strip the go _h host tag, drop empty-valued tags — already applied to the
// expected model — and ignore client meta-metrics, which never collide with the
// e2e_<runID>_ prefix).

// assertTimeout is the worst-case poll window for one (metric, func) to
// converge. The historic conveyor is ~24s end-to-end; 60s leaves headroom for
// auto-create, agg insertion, and the big-unique bucket flush (spec §5).
const assertTimeout = 60 * time.Second

// percentileTol / percentileMinAbs are the value_p tolerance band (spec §5): an
// API percentile is accepted when |actual-truth| ≤ max(percentileTol·|truth|,
// percentileMinAbs). percentileTol is the spec's 1% relative band; percentileMinAbs
// is the absolute floor so a near-zero true quantile still has a usable band. The
// t-digest's quantile error is bounded by ~1/compression in quantile space (agent
// compression=40), comfortably inside 1%.
const (
	percentileTol    = 0.01
	percentileMinAbs = 1.0
)

// uniqueApproxTol is the big-unique ±relative band (>65536 distinct → ChUnique
// thinning estimator, 1σ≈0.45%, so ±2% is ~4σ).
const uniqueApproxTol = 0.02

// apiSeriesResponse mirrors the minimal slice of the API's query reply. The
// payload is wrapped under a top-level "data" key; series lives at data.series.
// SeriesData is [][]float64: the API marshals a missing point (NaN) as JSON null,
// which encoding/json turns into 0.0 — and every expected value is non-zero
// (counts ≥1, value aggregates over ≥1 value, cardinality ≥1, unique ≥1), so a 0
// is an unambiguous failure.
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

// queryFunc is one (function, quantile) the harness queries a metric with. qw is
// the API query-function string; q is the quantile arg for percentile funcs
// (unused otherwise). label decorates the PASS/FAIL line.
type queryFunc struct {
	qw    string
	q     float64
	label string
}

// funcsFor returns the query functions a metric kind is asserted with.
func funcsFor(kind string) []queryFunc {
	switch kind {
	case kindCounter, kindStag:
		// stag asserts cardinality; counter asserts count. Both are single-func,
		// exact, group-by-or-not depending on QBKeys.
		return []queryFunc{{qw: qwFor(kind), label: qwFor(kind)}}
	case kindValue:
		return []queryFunc{
			{qw: "sum", label: "sum"},
			{qw: "min", label: "min"},
			{qw: "max", label: "max"},
			{qw: "avg", label: "avg"},
		}
	case kindValueP:
		return []queryFunc{
			{qw: "p50", q: 0.50, label: "p50"},
			{qw: "p90", q: 0.90, label: "p90"},
			{qw: "p99", q: 0.99, label: "p99"},
		}
	case kindUnique:
		return []queryFunc{{qw: "unique", label: "unique"}}
	}
	return nil
}

// qwFor maps a kind to its single-function query string (counter/stag).
func qwFor(kind string) string {
	if kind == kindStag {
		return "cardinality"
	}
	return "count"
}

// assertStream polls and asserts every metric in the stream across all of its
// query functions. Returns pass/fail counts; each (metric, func) yields exactly
// one PASS or FAIL line labelled with the client tag (the per-client metric-name
// prefix already isolates clients; the tag makes the line readable). want is the
// per-metric sentWrites the conservation ledger balances against (precomputed by
// ledgerWriteCounts); it lets a FAILED value assertion also print that metric's
// ledger state, so a value mismatch and its likely cause (silent loss vs double-
// count) are visible in one place (spec §6: "the conservation ledger for that
// metric").
func assertStream(ctx context.Context, rec *recorder, apiAddr, clientTag string, stream metricStream) (passed, failed int) {
	want := ledgerWriteCounts(stream)
	for _, m := range stream.Metrics {
		for _, qf := range funcsFor(m.Kind) {
			ok, detail := pollMetricFunc(ctx, rec, apiAddr, clientTag, m, stream.Base, want[m.Name], qf)
			switch {
			case ok:
				passed++
				rec.logf("PASS client=%s metric=%s qw=%s series=%d", clientTag, m.Name, qf.label, len(m.Series))
				fmt.Printf("PASS client=%s metric=%s qw=%s\n", clientTag, m.Name, qf.label)
			default:
				failed++
				rec.logf("FAIL client=%s metric=%s qw=%s\n%s", clientTag, m.Name, qf.label, detail)
				fmt.Printf("FAIL client=%s metric=%s qw=%s\n%s\n", clientTag, m.Name, qf.label, indent(detail))
			}
		}
	}
	return passed, failed
}

// pollMetricFunc queries one (metric, func) repeatedly until it matches the
// expected model or assertTimeout elapses. detail is the last observed failure.
// On failure it ALSO records the raw /api/query response on the recorder (so the
// run artifacts carry the verbatim JSON of every failed query — spec §6), writes
// the final response to artifacts under -v, and appends the metric's conservation
// ledger state (sentWrites drives the balance verdict) so a mismatch and its
// probable cause read together.
func pollMetricFunc(ctx context.Context, rec *recorder, apiAddr, clientTag string, m metricModel, base uint32, sentWrites int, qf queryFunc) (bool, string) {
	qurl := metricQueryURL(apiAddr, m.Name, m.QBKeys, qf.qw, base)
	var (
		lastDetail string
		lastBody   string
		lastStatus int
	)
	if err := poll(ctx, assertTimeout, 2*time.Second, func() (bool, error) {
		resp, body, status, qerr := queryCounterRaw(ctx, qurl)
		lastBody, lastStatus = body, status
		if qerr != nil {
			lastDetail = fmt.Sprintf("query error: %v\nurl: %s", qerr, qurl)
			return false, nil
		}
		mismatches, missing, extras, sampling := compareByFunc(m, resp, qf)
		// All four must be clean: exact/tolerant values, no missing series, no
		// extra series, and no sampling (a nonzero sampling factor means data was
		// sampled and must fail even when the values happen to match).
		if len(mismatches) == 0 && len(missing) == 0 && len(extras) == 0 && sampling == 0 {
			return true, nil
		}
		lastDetail = formatFail(m.Name, qurl, qf, mismatches, missing, extras, sampling)
		return false, nil
	}); err != nil {
		// A cancelled context (the run deadline or a signal) is not a value
		// mismatch: bail BEFORE recording a spurious failed-query or fetching the
		// ledger (which would itself error under the cancelled ctx and emit a
		// misleading "ledger: unavailable" line). The run is already tearing down.
		if ctx.Err() != nil {
			return false, ""
		}
		// Timed out still mismatched → record the verbatim response and annotate
		// with the metric's ledger state, then surface the detail.
		if rec != nil {
			rec.recordFailedQuery(failedQuery{
				Label: "value", Client: clientTag, Metric: m.Name, Func: qf.label,
				URL: qurl, HTTPStatus: lastStatus, Body: lastBody,
			})
			if rec.verbose {
				rec.dumpQueryResponse(clientTag, m.Name, qf.label, lastBody)
			}
		}
		lastDetail += "\n" + metricLedgerLine(ctx, apiAddr, base, m.Name, sentWrites)
		return false, lastDetail
	}
	// Matched: under -v keep the verbatim response that satisfied the assertion.
	if rec != nil && rec.verbose {
		rec.dumpQueryResponse(clientTag, m.Name, qf.label, lastBody)
	}
	return true, ""
}

// metricLedgerLine returns a one-line snapshot of one metric's conservation
// ledger state for embedding in a value-assertion failure. It is a DIAGNOSTIC
// aid (the authoritative balance check is assertConservationLedger): the values
// are whatever has landed so far, so a "silent loss" verdict here is a strong
// hint, not a final ruling. sentWrites==0 marks a metric outside the ledger's
// exact scope (multi-value kinds, where item count ≠ write count). Pure callers
// (tests) use formatLedgerLine; this wrapper does the live fetch.
func metricLedgerLine(ctx context.Context, apiAddr string, base uint32, name string, sentWrites int) string {
	if sentWrites == 0 {
		return "ledger: not balance-checked (multi-value metric / no ledger-eligible writes)"
	}
	bd, _, err := fetchIngestionBreakdown(ctx, apiAddr, base+numBuckets, map[string]bool{name: true})
	if err != nil {
		return fmt.Sprintf("ledger: unavailable (%v)", err)
	}
	okCached, errSum, _, _ := ledgerBalance(bd[name])
	return formatLedgerLine(sentWrites, okCached, errSum)
}

// ledgerSnapshotCaveat is appended to a NON-balanced inline ledger line. That line
// is embedded in a VALUE-assertion failure by metricLedgerLine, which runs BEFORE
// the authoritative assertConservationLedger poll converges — so its "silent loss"
// / "over-counted" verdict is a point-in-time snapshot, not a final ruling (the
// ledger assertion is authoritative). A balanced line needs no caveat.
const ledgerSnapshotCaveat = " (snapshot at assertion time; the ledger assertion is authoritative)"

// formatLedgerLine renders the conservation equation verdict for one metric.
// Pure → unit-tested (TestFormatLedgerLine).
func formatLedgerLine(sentWrites int, okCached, errSum float64) string {
	got := okCached + errSum
	switch {
	case got == float64(sentWrites):
		return fmt.Sprintf("ledger: ok_cached=%g + Σerr=%g = %g == sentWrites=%d (balanced)", okCached, errSum, got, sentWrites)
	case got < float64(sentWrites):
		return fmt.Sprintf("ledger: ok_cached=%g + Σerr=%g = %g < sentWrites=%d → %g unaccounted (silent loss)%s",
			okCached, errSum, got, sentWrites, float64(sentWrites)-got, ledgerSnapshotCaveat)
	default:
		return fmt.Sprintf("ledger: ok_cached=%g + Σerr=%g = %g > sentWrites=%d → %g over-counted (double-counting)%s",
			okCached, errSum, got, sentWrites, got-float64(sentWrites), ledgerSnapshotCaveat)
	}
}

// metricQueryURL builds GET /api/query for one metric at 1s LOD over its buckets.
// qb is repeated per group-by tag key; an empty qb (stag cardinality) yields a
// single total series (GROUP BY _time only). "1s" defeats auto-resolution; ac=1
// defeats the ~1s query cache.
func metricQueryURL(apiAddr, name string, qb []string, qw string, base uint32) string {
	q := url.Values{}
	q.Set("s", name)
	q.Set("f", strconv.FormatUint(uint64(base), 10))
	q.Set("t", strconv.FormatUint(uint64(base+numBuckets), 10))
	// "1s" (not bare "1"): a bare width is parsed as screen-width=1 → auto-
	// resolution collapsing the range to ~1 point at the 1m table; the "s" suffix
	// makes it an explicit 1-second step so the 1s table (statshouse_v6_1s) is used.
	q.Set("w", "1s")
	q.Set("ac", "1")
	q.Set("qw", qw)
	for _, k := range qb {
		q.Add("qb", k)
	}
	return "http://" + apiAddr + "/api/query?" + q.Encode()
}

// queryCounterRaw is queryCounter that ALSO returns the raw response body and
// HTTP status. The raw body is what the diagnostics artifacts (failed-queries
// JSON, the -v per-query response dump) need: a re-formatted struct loses the
// exact bytes the API returned, which is precisely what a human diagnosing a
// mismatch wants verbatim. status/body are populated even on a parse error so
// the failure dump carries the offending payload. Pure callers use queryCounter.
func queryCounterRaw(ctx context.Context, qurl string) (resp *apiSeriesResponse, body string, status int, err error) {
	body, status, err = httpGet(ctx, qurl)
	if err != nil {
		return nil, body, status, err
	}
	if status != 200 {
		return nil, body, status, fmt.Errorf("HTTP %d: %s", status, truncate(strings.TrimSpace(body), 300))
	}
	var r apiSeriesResponse
	if jerr := json.Unmarshal([]byte(body), &r); jerr != nil {
		return nil, body, status, fmt.Errorf("parse response: %w; body: %s", jerr, truncate(body, 300))
	}
	return &r, body, status, nil
}

func queryCounter(ctx context.Context, qurl string) (*apiSeriesResponse, error) {
	resp, _, _, err := queryCounterRaw(ctx, qurl)
	return resp, err
}

// indexResponse turns the API's per-series arrays into map[signature]map[bucket]value,
// the shape every comparator consumes. A missing data point unmarshals to 0.0.
func indexResponse(resp *apiSeriesResponse) map[string]map[uint32]float64 {
	out := make(map[string]map[uint32]float64, len(resp.Data.Series.SeriesMeta))
	for i, meta := range resp.Data.Series.SeriesMeta {
		data := resp.Data.Series.SeriesData[i]
		buckets := make(map[uint32]float64, len(resp.Data.Series.Time))
		for j, ts := range resp.Data.Series.Time {
			if j < len(data) {
				buckets[uint32(ts)] = data[j]
			}
		}
		out[tagSignature(meta.Tags)] = buckets
	}
	return out
}

// seriesMismatch is one (series, bucket) where expected ≠ actual (outside tol).
type seriesMismatch struct {
	seriesSig string
	bucket    uint32
	expected  string // formatted for readability (float or "≈N±tol")
	actual    float64
}

// compareByFunc dispatches to the kind-appropriate comparator. mismatches/
// missing/extras are empty on a match; sampling is the sum of the two sampling
// factors (must be 0).
func compareByFunc(m metricModel, resp *apiSeriesResponse, qf queryFunc) (mismatches []seriesMismatch, missing, extras []string, sampling float64) {
	switch {
	case qf.qw == "count":
		mismatches, missing, extras = compareCounts(m, resp)
	case qf.qw == "cardinality":
		mismatches, missing, extras = compareCardinality(m, resp)
	case qf.qw == "sum" || qf.qw == "min" || qf.qw == "max" || qf.qw == "avg":
		mismatches, missing, extras = compareValueAgg(m, resp, qf.qw)
	case qf.qw == "p50" || qf.qw == "p90" || qf.qw == "p99":
		mismatches, missing, extras = comparePercentile(m, resp, qf.q)
	case qf.qw == "unique":
		mismatches, missing, extras = compareUnique(m, resp)
	}
	sortMismatches(mismatches)
	sort.Strings(missing)
	sort.Strings(extras)
	sampling = resp.Data.SamplingFactorSrc + resp.Data.SamplingFactorAgg
	return mismatches, missing, extras, sampling
}

// compareCounts is the counter exact per-series count comparison (bidirectional:
// a never-written series is as much a failure as a missing one).
func compareCounts(m metricModel, resp *apiSeriesResponse) (mismatches []seriesMismatch, missing, extras []string) {
	actual := indexResponse(resp)
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
			if got[ts] != exp {
				mismatches = append(mismatches, seriesMismatch{sig, ts, strconv.FormatFloat(exp, 'g', -1, 64), got[ts]})
			}
		}
	}
	extras = extraSeries(actual, want)
	return mismatches, missing, extras
}

// compareCardinality is the stag assertion: with NO group-by the API returns one
// series (signature "") whose per-bucket value is the distinct-series count
// (sum(1)). Expected = the number of series that wrote the bucket (all of them,
// for stag). Exact.
func compareCardinality(m metricModel, resp *apiSeriesResponse) (mismatches []seriesMismatch, missing, extras []string) {
	actual := indexResponse(resp)
	got, ok := actual[""]
	if !ok {
		// The total series itself is absent → flag every populated bucket missing.
		for bucket := range stagBuckets(m) {
			missing = append(missing, fmt.Sprintf("cardinality total absent at bucket %d", bucket))
		}
		return mismatches, missing, extras
	}
	for bucket, expCount := range stagBuckets(m) {
		if got[bucket] != float64(expCount) {
			mismatches = append(mismatches, seriesMismatch{"(cardinality)", bucket, strconv.Itoa(expCount), got[bucket]})
		}
	}
	// Any series besides the "" total is unexpected (cardinality returns one).
	for sig := range actual {
		if sig != "" {
			extras = append(extras, sig)
		}
	}
	return mismatches, missing, extras
}

// stagBuckets maps every bucket a stag series populates to the expected distinct-
// series count there. All stag series write all buckets, so every populated
// bucket expects len(m.Series); the set of buckets is the union of all series'
// Counts keys.
func stagBuckets(m metricModel) map[uint32]int {
	out := map[uint32]int{}
	for _, es := range m.Series {
		for ts := range es.Counts {
			out[ts]++
		}
	}
	return out
}

// compareValueAgg is the value exact per-series aggregate comparison. The
// expected sum/min/max/avg are computed from the model's merged values in WRITE
// ORDER (the same left-fold the agent's ValueSum uses), so the float64 result is
// bit-identical and compared with ==.
func compareValueAgg(m metricModel, resp *apiSeriesResponse, qw string) (mismatches []seriesMismatch, missing, extras []string) {
	actual := indexResponse(resp)
	want := make(map[string]bool, len(m.Series))
	for _, es := range m.Series {
		sig := expectedSignature(es.Tags)
		want[sig] = true
		got, ok := actual[sig]
		if !ok {
			missing = append(missing, sig)
			continue
		}
		for ts, vals := range es.Values {
			exp := valueAggregate(vals, qw)
			if got[ts] != exp {
				mismatches = append(mismatches, seriesMismatch{sig, ts, strconv.FormatFloat(exp, 'g', -1, 64), got[ts]})
			}
		}
	}
	extras = extraSeries(actual, want)
	return mismatches, missing, extras
}

// comparePercentile is the value_p tolerance comparison: each series' per-bucket
// API percentile must fall within max(percentileTol·|truth|, percentileMinAbs) of
// the true quantile (model Values are stored sorted).
func comparePercentile(m metricModel, resp *apiSeriesResponse, q float64) (mismatches []seriesMismatch, missing, extras []string) {
	actual := indexResponse(resp)
	want := make(map[string]bool, len(m.Series))
	for _, es := range m.Series {
		sig := expectedSignature(es.Tags)
		want[sig] = true
		got, ok := actual[sig]
		if !ok {
			missing = append(missing, sig)
			continue
		}
		for ts, vals := range es.Values {
			truth := quantile(vals, q) // vals stored sorted
			if !withinAbsTol(got[ts], truth, percentileTol, percentileMinAbs) {
				mismatches = append(mismatches, seriesMismatch{sig, ts, fmt.Sprintf("≈%g±tol", truth), got[ts]})
			}
		}
	}
	extras = extraSeries(actual, want)
	return mismatches, missing, extras
}

// compareUnique is the unique comparison: exact equality for the small case
// (distinct ≤ 65536 → ChUnique exact), ±uniqueApproxTol for the big case
// (>65536 → thinning estimator).
func compareUnique(m metricModel, resp *apiSeriesResponse) (mismatches []seriesMismatch, missing, extras []string) {
	actual := indexResponse(resp)
	want := make(map[string]bool, len(m.Series))
	for _, es := range m.Series {
		sig := expectedSignature(es.Tags)
		want[sig] = true
		got, ok := actual[sig]
		if !ok {
			missing = append(missing, sig)
			continue
		}
		for ts, exp := range es.Uniques {
			approx := exp > uniquesHashMaxSize // big-unique → thinning estimator
			truth := float64(exp)
			match := !approx && got[ts] == truth
			if approx {
				match = withinRelTol(got[ts], truth, uniqueApproxTol)
			}
			if !match {
				note := strconv.FormatFloat(truth, 'g', -1, 64)
				if approx {
					note = fmt.Sprintf("≈%g±%g%%", truth, uniqueApproxTol*100)
				}
				mismatches = append(mismatches, seriesMismatch{sig, ts, note, got[ts]})
			}
		}
	}
	extras = extraSeries(actual, want)
	return mismatches, missing, extras
}

// uniquesHashMaxSize is the exact→approximate threshold in ChUnique
// (internal/data_model/ch_unique.go: 1<<(17-1)). Replicated here so the asserter
// picks equality vs the ±band without importing data_model.
const uniquesHashMaxSize = 1 << 16

// valueAggregate computes the expected value-kind aggregate over vals in write
// order. sum is a left fold (matches the agent's ValueSum); avg = sum/len (the
// agent defaults count to len(values), so avg = sum/count). All exact for these
// deterministic inputs.
func valueAggregate(vals []float64, qw string) float64 {
	switch qw {
	case "min":
		m := math.Inf(1)
		for _, v := range vals {
			if v < m {
				m = v
			}
		}
		return m
	case "max":
		m := math.Inf(-1)
		for _, v := range vals {
			if v > m {
				m = v
			}
		}
		return m
	case "sum":
		s := 0.0
		for _, v := range vals {
			s += v
		}
		return s
	case "avg":
		if len(vals) == 0 {
			return 0
		}
		s := 0.0
		for _, v := range vals {
			s += v
		}
		return s / float64(len(vals))
	}
	return 0
}

// extraSeries returns the API series not present in the expected want set.
func extraSeries(actual map[string]map[uint32]float64, want map[string]bool) []string {
	var extras []string
	for sig := range actual {
		if !want[sig] {
			extras = append(extras, sig)
		}
	}
	return extras
}

func sortMismatches(mm []seriesMismatch) {
	sort.Slice(mm, func(i, j int) bool {
		if mm[i].seriesSig != mm[j].seriesSig {
			return mm[i].seriesSig < mm[j].seriesSig
		}
		return mm[i].bucket < mm[j].bucket
	})
}

func formatFail(name, qurl string, qf queryFunc, mismatches []seriesMismatch, missing, extras []string, sampling float64) string {
	var b strings.Builder
	if sampling != 0 {
		fmt.Fprintf(&b, "sampling_factor_src+agg=%g (expected 0 — data was sampled)\n", sampling)
	}
	for _, mm := range mismatches {
		fmt.Fprintf(&b, "series{%s} bucket=%d expected=%s actual=%g\n", mm.seriesSig, mm.bucket, mm.expected, mm.actual)
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
// with (a) the go client's _h host tag stripped — it is never in the harness's
// qb (stripped defensively per spec §4), and (b) empty-valued tags dropped. The
// drop mirrors the expected-model normalizeTags: go drops empty tags client-side,
// but rust/cpp SEND them verbatim (their libraries have no empty-drop). The agent
// maps an empty tag value to nothing (internal/agent/agent_mapping.go:
// len(v.Value)==0 case body is empty), so an empty tag is a no-op on the wire,
// but the API may still surface it in series_meta — dropping it here keeps the
// rust/cpp signature equal to the (empty-free) expected signature.
//
// (c) An absent group-by (qb) tag position — a series written with fewer tags
// than qb covers — is materialized by the API as the sentinel value " 0" (tag
// value ID 0, rendered with a leading space). The expected model has no entry
// for an absent position, so the sentinel is dropped too; the present tags alone
// distinguish every series. No harness metric uses "0" as a real tag value, so
// TrimSpace=="0" never over-drops. (Empty "" is the other absent rendering; it is
// already caught by the empty-value drop above.)
func tagSignature(tags map[string]apiMetaTag) string {
	keys := make([]string, 0, len(tags))
	for k, v := range tags {
		if k == "_h" {
			continue
		}
		if v.Value == "" {
			continue
		}
		if strings.TrimSpace(v.Value) == "0" {
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

// --- silent client-side loss tripwire (spec §4: TCP backpressure) -------------

// clientWriteErrMetric is the builtin every StatsHouse client emits when it
// SILENTLY drops bytes to the agent under TCP backpressure. The pinned go
// client's tcpConn.Write (client_conn.go) is a non-blocking send into a
// 512-packet channel (tcpConnBucketCount); on a would-block it drops the packet
// and reports the dropped byte count here (reportWouldBlockIfAny). The rust
// (append_write_err_metric) and cpp (report_would_block_metric_after_send)
// transports do the same on their own overflow paths. The dropped bytes never
// reach the agent, so the value assertions would otherwise see mysteriously-low
// counts with no cause — this tripwire turns that silent loss into one labelled,
// attributed failure instead. (internal/format/builtin_metrics.go:
// BuiltinMetricMetaClientWriteError; the clients tag lang at positional index 1
// and cause "would_block" at index 2.)
const clientWriteErrMetric = "__src_client_write_err"

// clientWriteErrLang maps a driver tag to the lang code its client writes on
// __src_client_write_err at tag index 1 (golang=1, rust=3, cpp=5 — confirmed in
// the pinned sources: go fillTag(&k,"1","1"), rust .tag("1","3"), cpp
// kb.tag("1","5")). Grouping the query by tag index 1 (qb=1) isolates one
// client's loss from the other two, which run as separate sequential phases
// against the shared stack. A client absent here does not emit the metric and is
// skipped (assertNoClientWriteErr returns ok=true at once); none of the three
// currently does, but the map keeps that skip path real rather than dead code.
var clientWriteErrLang = map[string]string{
	"go":   "1",
	"rust": "3",
	"cpp":  "5",
}

// writeErrTimeout bounds the absence poll. A dropped-bytes point is a REALTIME
// write (its ts is the driver's wall-clock burst, ≈ base+120 since base is
// floor(now)−120), and the historic conveyor lands it in ClickHouse ~24s after
// the burst. This runs after the driver exits (the burst is already several
// seconds in the past) and after waitAggConveyor proved the agent→agg→api
// conveyor live, so 30s comfortably covers the remaining drain. A non-zero point
// fails at the iteration it appears in — no full wait on red.
const writeErrTimeout = 30 * time.Second

// absenceQueryFunc is the injectable shape of ONE absence-poll query, so the
// fail-closed tripwire logic is unit-testable without a live API. It returns the
// largest per-bucket value seen on this query (the candidate "violation"
// magnitude) and an error when the query itself failed (api busy, auth, schema
// drift, …). A clean (err==nil) result — even one that finds only zeros — counts
// as a successful observation of absence.
type absenceQueryFunc func(ctx context.Context) (worst float64, err error)

// absenceOutcome is the result of a fail-closed absence poll.
//
//   - ok=true iff absence was confirmed by at least one CLEAN query over the window.
//   - worst is the largest value seen across clean queries (the violation magnitude
//     when a non-zero point surfaced; 0 when absent).
//   - confirmed reports whether at least one query returned without error. false over
//     the whole window means absence was NEVER observed — the tripwire fails closed.
//   - queryErr is the last query error (set when !confirmed, for the fail-closed
//     detail).
type absenceOutcome struct {
	ok        bool
	worst     float64
	confirmed bool
	queryErr  error
}

// pollAbsenceTripwire drives a FAIL-CLOSED absence assertion: a metric that must
// stay ABSENT/zero is queried every interval until either a violation surfaces
// (fail fast) or the timeout elapses with at least one CLEAN query confirming
// absence (pass). The fail-closed guarantee mirrors the conservation ledger: if
// EVERY query errors for the whole window — api down, wrong address, auth, schema
// drift — then absence was never actually confirmed, so the tripwire returns
// ok=false with confirmed=false rather than silently passing. A safety assertion
// must never pass on zero successful observations; the old code's "every error →
// keep polling → timeout → PASS" path did exactly that (a false PASS when, e.g.,
// the api never came up).
func pollAbsenceTripwire(ctx context.Context, timeout, interval time.Duration, query absenceQueryFunc) absenceOutcome {
	var (
		lastErr   error
		confirmed bool
		worst     float64
	)
	err := poll(ctx, timeout, interval, func() (bool, error) {
		v, qerr := query(ctx)
		if qerr != nil {
			lastErr = qerr
			return false, nil // keep polling; absence is unconfirmed while the query fails
		}
		confirmed = true
		if v > worst {
			worst = v
		}
		return worst > 0, nil // a non-zero point → stop (fail); stays false while absent
	})
	if err == nil {
		// poll returned nil → the condition fired → a non-zero point surfaced.
		return absenceOutcome{ok: false, worst: worst, confirmed: true}
	}
	if !confirmed {
		// Timed out (or ctx cancelled) WITHOUT one clean query: absence was never
		// confirmed. Fail closed — never pass a safety check on zero data.
		return absenceOutcome{ok: false, confirmed: false, queryErr: lastErr}
	}
	// Timed out (or ctx cancelled) AFTER at least one clean query saw only zero →
	// ABSENT → the tripwire holds.
	return absenceOutcome{ok: true, worst: worst, confirmed: true}
}

// assertNoClientWriteErr is the silent-loss tripwire: after a driver exits, poll
// __src_client_write_err for the client's language over this run's window and
// fail (ok=false, with a labelled detail) the moment a non-zero point appears.
// ok=true means a clean query confirmed no loss within writeErrTimeout. The window
// is [statusAnchor, statusAnchor+200]: __src_client_write_err is a REALTIME builtin
// recorded at the driver's wall-clock write, so it anchors at client-phase start
// (statusAnchor), NOT the historic base — a --skip-client-build replay keeps an OLD
// base while the dropped bytes land at replay-now (F1). 200s absorbs build+run+the
// historic conveyor with clock-skew headroom, and the run-unique metric names
// exclude other runs. Returns ok=true at once for a client whose language is
// unknown (it does not emit the metric, so there is nothing to assert). FAILS
// CLOSED: if every query errors over the whole window (api down / wrong address),
// ok=false — a down stack can never pass for "no loss" (see pollAbsenceTripwire).
func assertNoClientWriteErr(ctx context.Context, rec *recorder, apiAddr, clientTag string, statusAnchor uint32) (ok bool, detail string) {
	lang, knows := clientWriteErrLang[clientTag]
	if !knows {
		return true, "" // this client's library does not emit the metric; nothing to assert
	}
	q := url.Values{}
	q.Set("s", clientWriteErrMetric)
	q.Set("f", strconv.FormatUint(uint64(statusAnchor), 10))
	q.Set("t", strconv.FormatUint(uint64(statusAnchor+200), 10))
	q.Set("w", "1s")
	q.Set("ac", "1")
	q.Set("qw", "sum")
	q.Set("qb", "1") // group by the language tag → one series per client language
	qurl := "http://" + apiAddr + "/api/query?" + q.Encode()

	// A clean query (no error) confirms absence for this window iteration even when
	// the language's series is absent (clientWriteErrForLang returns found=false → 0).
	// pollAbsenceTripwire fails CLOSED: if every query errors for the whole window it
	// returns ok=false, so a down/misconfigured api can never masquerade as "no loss".
	var (
		violBody   string // raw reply of the first query that surfaced a non-zero point
		violStatus int
	)
	query := func(ctx context.Context) (float64, error) {
		resp, body, status, qerr := queryCounterRaw(ctx, qurl)
		if qerr != nil {
			return 0, qerr
		}
		maxLost, _ := clientWriteErrForLang(resp, lang)
		if maxLost > 0 && violBody == "" { // capture the violating payload once
			violBody, violStatus = body, status
		}
		return maxLost, nil
	}
	o := pollAbsenceTripwire(ctx, writeErrTimeout, 3*time.Second, query)
	if o.ok {
		return true, ""
	}
	if !o.confirmed {
		// recordFailedQuery is nil-safe (a nil recorder is a no-op), so the other
		// tripwire/assertion paths call it without a rec != nil guard too.
		rec.recordFailedQuery(failedQuery{
			Label: "write_err", Client: clientTag, Metric: clientWriteErrMetric,
			URL: qurl, HTTPStatus: 0, Body: "",
		})
		return false, fmt.Sprintf("client=%s lang=%s could not confirm absence of %s — every query failed over %s: %v\nurl: %s",
			clientTag, lang, clientWriteErrMetric, writeErrTimeout, o.queryErr, qurl)
	}
	// A non-zero point surfaced → a dropped-bytes loss was recorded.
	rec.recordFailedQuery(failedQuery{
		Label: "write_err", Client: clientTag, Metric: clientWriteErrMetric,
		URL: qurl, HTTPStatus: violStatus, Body: violBody,
	})
	return false, fmt.Sprintf("client=%s lang=%s lost_bytes≈%g (silent TCP-backpressure drop; __src_client_write_err non-zero)\nurl: %s",
		clientTag, lang, o.worst, qurl)
}

// clientWriteErrForLang scans a __src_client_write_err reply (grouped by the
// language tag, qb=1) for the given client's language and returns found=true with
// the largest per-bucket lost-byte value if that language has any non-zero
// bucket. With qb=1 the only grouped tag is the language (at index 1), so a value
// match unambiguously identifies the language; the sentinel " 0"/empty drop never
// equals "1"/"3"/"5".
func clientWriteErrForLang(resp *apiSeriesResponse, lang string) (maxLost float64, found bool) {
	for i, meta := range resp.Data.Series.SeriesMeta {
		if !seriesMetaHasLang(meta.Tags, lang) {
			continue
		}
		for _, v := range resp.Data.Series.SeriesData[i] {
			if v > 0 {
				found = true
				if v > maxLost {
					maxLost = v
				}
			}
		}
	}
	return maxLost, found
}

// seriesMetaHasLang reports whether any tag in a series_meta carries the given
// language code as its value. qb=1 leaves the language as the single grouped tag,
// so this matches the one distinguishing value regardless of the key name the API
// renders it under (key1 for a positional index).
func seriesMetaHasLang(tags map[string]apiMetaTag, lang string) bool {
	for _, t := range tags {
		if strings.TrimSpace(t.Value) == lang {
			return true
		}
	}
	return false
}

// --- ticket 12: rejection statuses, conservation ledger, sampling tripwire ------
//
// The rejected inputs have NO visible output, so they are asserted three ways
// against __src_ingestion_status (builtin -11, MetricKindCounter), the per-event
// accounting counter the agent writes once per received event:
//
//   tag0=env, tag1=metric(NAME the event was mapped to), tag2=status(numeric VALUE
//   ID — 10/ok_cached, 23/err_nan_inf_value, …; the human name lives only in the
//   builtin's ValueComments and is NOT in the query reply), tag3=tag_id, tag4=
//   component (1=agent, 2=agg).
//
// Each event is accounted EXACTLY ONCE in this metric:
//   - accepted → ok_cached (the agent compacts many into an Ok2 item, the agg
//     re-expands Ok2 back to ok_cached on insert; either way the count survives);
//   - rejected → the matching err_* status (one increment per event).
// The metric-not-found status (21) is the key exception. ApplyMetric's FIRST branch
// (agent.go:805, h.MetricMeta == nil) routes an UNMAPPED name — a metric not yet
// auto-created — to the sibling builtin __src_ingestion_status_no_shard (-148) with
// the metric's tag ID 0, NOT to -11 (it carries h.IngestionStatus, typically
// metric_not_found=21). So the cold-start SEEDS (unmapped on first contact) never
// touch a real metric's -11 ledger. The mapped-err branch (agent.go:827,
// h.IngestionStatus != 0) is a DIFFERENT path: it handles MAPPED metrics whose VALUE
// failed validation (a real rejection), routing the err_* to -11 — it is NOT the seed
// path (seeds are unmapped and caught at :805). The harness also excludes seeds from
// stream.Writes, so both sides of the ledger see only the real (mapped) writes and
// balance exactly. EXCEPTION: a PRE-CREATED metric (value_p, POST /api/metric) is
// already mapped when its seed arrives, so the seed skips :805 and lands as +1
// ok_cached in -11 — an over-count, which is why value_p is out of the ledger
// (see ledgerEligibleKind).

// ingestionStatusTail widens the realtime ledger/status query window. The caller
// passes statusAnchor (client-phase start, ≈ wall-clock now); the window is
// [statusAnchor, statusAnchor+ingestionStatusTail]. __src_ingestion_status is a
// REALTIME builtin: the agent accounts each event at RECEIVE time (≈ the driver's
// wall-clock write, statusAnchor..statusAnchor+(build+run duration)), NOT at the
// event's historic ts. The historic conveyor then adds ~24s before a point is
// queryable. On a normal run statusAnchor≈base+120 so the OLD base-derived window
// happened to cover the events — but a --skip-client-build REPLAY keeps the
// descriptor's OLD base while the agent records THIS run's events at replay-now, so
// the window must anchor at statusAnchor, not base (F1). The tail gives the conveyor
// + clock-skew headroom; metric names are run-unique, so the wide window cannot pick
// up another run's statuses.
const ingestionStatusTail = 400

// ingestionStatusNumSeries is the `n` (num-results / series cap) passed on the
// __src_ingestion_status query. The API DEFAULTS n to a small value (the live
// stack returned a hard 10 series with no n — so a 14-metric client silently
// dropped 4 metrics' statuses, every one of them then reading 0 and failing the
// ledger as "silent loss"). One (metric,status) series per metric×status is
// all we need (~14 metrics × a few statuses ≈ tens), well under the maxSeries
// (10_000) ceiling, so a generous fixed cap is both safe and cap-proof.
const ingestionStatusNumSeries = 1000

// ledgerTimeout bounds the ledger/status convergence poll. The conveyor lands the
// status counters ~24s after the writes; assertStream (which runs first, polling up
// to 60s per metric) has already waited well past that, so statuses are usually
// landed by the time these run — 90s is conservative headroom.
const ledgerTimeout = 90 * time.Second

// samplingTimeout bounds the whole-run __agg_sampling_factor absence poll. It runs
// after assertStream, whose per-query sampling check would have already failed had
// any view been sampled, so by here any sampling point has long since landed. A
// clean query showing zero is trustworthy; a non-zero point fails the moment it
// appears (poll returns nil on done=true) rather than waiting the full window.
const samplingTimeout = 30 * time.Second

// ingestionStatusURL builds GET /api/query for __src_ingestion_status grouped by
// metric (qb=1, tag1) and status (qb=2, tag2), qw=count (the counter's per-event
// increment summed per bucket). qw=count returns Σ counter value = total events
// for that (metric, status) over the window — 1 per accepted event into ok_cached,
// 1 per rejected event into its err_*.
//
// anchor is the REALTIME window start: __src_ingestion_status is recorded by the
// agent at RECEIVE time (≈ the driver's wall-clock write), NOT at the event's
// historic ts, so the window is [anchor, anchor+ingestionStatusTail]. The caller
// passes statusAnchor (client-phase start) so a --skip-client-build replay — which
// keeps the descriptor's OLD historic base while the agent records THIS run's
// events at replay-now — still queries the right window (F1).
func ingestionStatusURL(apiAddr string, anchor uint32) string {
	q := url.Values{}
	q.Set("s", ingestionStatusMetric)
	q.Set("f", strconv.FormatUint(uint64(anchor), 10))
	q.Set("t", strconv.FormatUint(uint64(anchor+ingestionStatusTail), 10))
	q.Set("w", "1s")
	q.Set("ac", "1")
	q.Set("qw", "count")
	q.Set("n", strconv.Itoa(ingestionStatusNumSeries)) // raise the default series cap (10) so no metric is dropped
	q.Add("qb", "1")                                   // metric name (tag1)
	q.Add("qb", "2")                                   // status value ID (tag2)
	return "http://" + apiAddr + "/api/query?" + q.Encode()
}

// fetchIngestionBreakdown queries __src_ingestion_status and folds it into
// map[metric][statusID]totalEvents over the run window, keeping only the metrics this
// client generated (known). classifyIngestionSeries identifies the metric (key1, the
// metric NAME, pinned to `known`) and the status (key2, a numeric VALUE ID) of each
// series; both are read by their rendered key (confirmed against the live API).
//
// anchor is the REALTIME window start (see ingestionStatusURL). The verbatim
// response body of the LAST successful query is also returned so a ledger/rejection
// failure can record the raw JSON in the artifacts (F4). metricLedgerLine (the
// diagnostic inline line) ignores the body and passes anchor=base+numBuckets to
// preserve its historic-relative window.
func fetchIngestionBreakdown(ctx context.Context, apiAddr string, anchor uint32, known map[string]bool) (map[string]map[int32]float64, string, error) {
	resp, body, _, err := queryCounterRaw(ctx, ingestionStatusURL(apiAddr, anchor))
	if err != nil {
		return nil, "", err
	}
	out := make(map[string]map[int32]float64)
	for i, meta := range resp.Data.Series.SeriesMeta {
		metric, statusID := classifyIngestionSeries(meta.Tags, known)
		if metric == "" || statusID == 0 {
			continue // another metric (not in known), or a series the API could not label
		}
		sum := seriesSum(resp.Data.Series.SeriesData[i])
		if sum == 0 {
			continue
		}
		if out[metric] == nil {
			out[metric] = make(map[int32]float64)
		}
		out[metric][statusID] += sum
	}
	return out, body, nil
}

// seriesSum sums every bucket value in one series' data array. The query window
// already restricts the buckets, so this is the total event count for that series.
func seriesSum(data []float64) float64 {
	var s float64
	for _, v := range data {
		s += v
	}
	return s
}

// classifyIngestionSeries identifies the metric and status of a __src_ingestion_status
// series from its series_meta tags. The API renders the status tag (key2) as the
// numeric status VALUE ID (e.g. " 10" for ok_cached, " 23" for err_nan_inf_value) —
// NOT the human-readable name (the name lives only in the builtin's ValueComments,
// which the query API does not return). So the status is parsed as an int32 ID and
// the ledger works in IDs throughout, matched against rejectionMetric.StatusID. The
// metric (key1) is the metric NAME, pinned to this client's generated set (`known`)
// so another run's metrics or the env tag are ignored. Both tags are read by their
// rendered key (key1/key2, confirmed against the live API); value-type fallbacks
// (known membership / integer parse) cover any future key-name drift.
func classifyIngestionSeries(tags map[string]apiMetaTag, known map[string]bool) (metric string, statusID int32) {
	if t, ok := tags["key1"]; ok {
		if v := strings.TrimSpace(t.Value); known[v] {
			metric = v
		}
	}
	if t, ok := tags["key2"]; ok {
		if id, err := strconv.Atoi(strings.TrimSpace(t.Value)); err == nil {
			statusID = int32(id)
		}
	}
	if metric == "" { // value-type fallback if key1 is ever renamed
		for _, t := range tags {
			if v := strings.TrimSpace(t.Value); known[v] {
				metric = v
				break
			}
		}
	}
	if statusID == 0 { // value-type fallback if key2 is ever renamed
		for _, t := range tags {
			if id, err := strconv.Atoi(strings.TrimSpace(t.Value)); err == nil && id != 0 {
				statusID = int32(id)
				break
			}
		}
	}
	return metric, statusID
}

// statusIDOKCached is the __src_ingestion_status value ID for an accepted event
// (TagValueIDSrcIngestionStatusOKCached, internal/format/builtin_tags.go).
const statusIDOKCached int32 = 10

// ingestionStatusNames maps a __src_ingestion_status value ID to its display name
// (mirroring the builtin's ValueComments in internal/format/builtin_metrics.go). The
// ledger logic works purely in IDs; this map exists only so failure detail reads as a
// name (err_zero_counter) instead of a bare number (62). isWarnStatus keys off the
// "warn_" prefix, so it stays correct as long as the warn entries are listed here.
var ingestionStatusNames = map[int32]string{
	10: "ok_cached",
	21: "err_metric_not_found",
	23: "err_nan_inf_value",
	24: "err_nan_inf_counter",
	25: "err_negative_counter",
	33: "warn_tag_not_found",
	34: "err_map_invalid_raw_tag_value",
	35: "err_map_tag_value_cached",
	36: "err_map_tag_value",
	39: "err_validate_tag_value_utf8",
	42: "err_metric_disabled",
	46: "warn_map_tag_set_twice",
	47: "warn_deprecated_tag_name",
	48: "err_validate_metric_utf8",
	49: "err_validate_tag_name_utf8",
	50: "err_value_unique_both_set",
	52: "warn_map_invalid_raw_tag_value",
	53: "warn_tag_draft_found",
	54: "err_metric_sharding_failed",
	55: "warn_timestamp_clamped_past",
	56: "warn_timestamp_clamped_future_agg",
	57: "err_metric_builtin",
	59: "warn_timestamp_clamped_future",
	60: "err_too_big_counter",
	61: "err_too_big_value",
	62: "err_zero_counter",
	63: "err_map_tag_value_corrupted",
}

// ingestionStatusName returns the display name for a status ID, or "status_<id>" for
// an ID not in the map (a forward-compat sentinel for an enum added upstream).
func ingestionStatusName(id int32) string {
	if n, ok := ingestionStatusNames[id]; ok {
		return n
	}
	return fmt.Sprintf("status_%d", id)
}

// isWarnStatus reports whether a status ID is a WARNING (warn_*). A warning
// accompanies an ACCEPTED event (the event is still ok_cached — e.g. a clamped
// timestamp is accepted with a warn; agent.go writes warns AFTER the ok_cached
// increment), so the ledger EXCLUDES warnings: counting one would double-count its
// event.
//
// FORWARD-COMPAT CAVEAT: classification keys off the "warn_" prefix the
// ingestionStatusNames map assigns. A NEW upstream warn_* status ID NOT yet in that
// map renders as "status_<id>" (ingestionStatusName's sentinel), which LACKS the
// "warn_" prefix → isWarnStatus returns false → it is misclassified as an err and
// folded into the error sum → the ledger fails loudly (an over-count) on the next
// run. If that happens, add the new ID→name to ingestionStatusNames — that is the
// only fix needed.
func isWarnStatus(id int32) bool {
	return strings.HasPrefix(ingestionStatusName(id), "warn_")
}

// ledgerBalance splits one metric's __src_ingestion_status counts (keyed by status ID)
// into the accepted total (ok_cached, ID 10), the rejected total (Σ of every
// non-ok, non-warn status — i.e. Σ err_*), and the per-status error and WARNING
// breakdowns (the latter kept for diagnostic context in a failure). Warnings are
// excluded from the balance on both sides (see isWarnStatus): a warn accompanies an
// accepted event (still ok_cached), so counting it would double-count.
func ledgerBalance(byID map[int32]float64) (okCached, errSum float64, errs, warns map[int32]float64) {
	errs = make(map[int32]float64)
	warns = make(map[int32]float64)
	for id, count := range byID {
		switch {
		case id == statusIDOKCached:
			okCached += count
		case isWarnStatus(id):
			warns[id] = count // excluded from the balance — neither an acceptance nor a loss
		default:
			errSum += count
			errs[id] = count
		}
	}
	return okCached, errSum, errs, warns
}

// ledgerEligibleKind reports whether a metric kind is in the conservation ledger's
// EXACT scope. ok_cached counts accepted wire ITEMS: ApplyMetric (agent.go:800) runs
// once per TL item and records ok_cached +1 (agent.go:856, count=1), so ok_cached is
// the item count, not the write-call count. A driver write call maps 1:1 to an item
// only for single-payload kinds — counter/stag (one count), value/value_nan/value_inf
// (a small value set) — and there the identity sentWrites==ok_cached+err holds exactly.
//
// kindUnique and kindValueP are excluded, for TWO DIFFERENT reasons:
//
//   - kindUnique (u_approx): one write carries 100k int64 ≈ 800KB, which exceeds the
//     per-packet cap, so EVERY client splits it into multiple wire items (go: TCP
//     maxPacketSize=65535 → ~8k values/packet; rust/cpp: split loops). The go client's
//     UniquesHistoric reservoir (driver sets MaxBucketSize=1<<18) bounds the sampled
//     VALUES but does NOT prevent the wire split, so ok_cached (item count) lands well
//     above sentWrites (write-call count) and the identity breaks. (u_exact is small
//     enough to be 1:1, but both unique metrics share this kind, so the kind is out.)
//
//   - kindValueP is NOT a split case — its payload is 1:1 on the wire (2000 floats =
//     16KB < go's 65535 TCP cap). It is excluded because the harness PRE-CREATES it
//     (POST /api/metric, metric_create.go) so its cold-start SEED arrives MAPPED → the
//     agent accounts it as +1 ok_cached in -11 (agent.go:856). Seeds live in
//     streamSeeds, NOT stream.Writes, so sentWrites misses that +1 → a permanent
//     over-count. Every auto-creating metric's seed is UNMAPPED on first contact and
//     lands in -148 instead (see the seed-exclusion note below), so only the
//     pre-created value_p has this over-count.
//
// Both excluded kinds are still covered by their own value/percentile/unique
// assertions + the per-query sampling check; v_mix (kindValue, 4 values/write) stays
// in — 4 values fit one item, confirmed exact.
func ledgerEligibleKind(kind string) bool {
	return kind != kindUnique && kind != kindValueP
}

// ledgerWriteCounts returns sentWrites per ELIGIBLE metric — the number of real writes
// the harness generated for each (normal + rejection) — from stream.Writes, skipping
// the multi-value kinds ledgerEligibleKind excludes (their item count ≠ write count).
// This is the true input cardinality the conservation ledger balances against. Seeds
// are NOT in stream.Writes (they live in streamSeeds), so the cold-start metric-not-
// found accounting (which lands in __src_ingestion_status_no_shard, not -11) is
// excluded on both sides and the ledger balances exactly.
func ledgerWriteCounts(stream metricStream) map[string]int {
	counts := make(map[string]int)
	for _, w := range stream.Writes {
		if !ledgerEligibleKind(w.Kind) {
			continue
		}
		counts[w.Metric]++
	}
	return counts
}

// knownMetricNames is the set of metric names a client generated (normal + rejection),
// the `known` set classifyIngestionSeries uses to pin a series to its metric.
func knownMetricNames(stream metricStream) map[string]bool {
	out := make(map[string]bool, len(stream.Metrics)+len(stream.Rejections))
	for _, m := range stream.Metrics {
		out[m.Name] = true
	}
	for _, r := range stream.Rejections {
		out[r.Name] = true
	}
	return out
}

// assertRejections is ticket-12 criterion 2: each rejected input must surface its
// EXACT __src_ingestion_status status with count == sentWrites. It polls the shared
// breakdown until every generated rejection converges or ledgerTimeout elapses, then
// reports one PASS/FAIL per rejection metric. A rejection with Sent==false is a
// documented client-side drop (go/rust refuse a non-positive count before the wire):
// the input never entered the pipeline, so there is no server status to assert and
// the case is a documented SKIP — not a silent disappearance. Returns pass/fail
// counts (one per rejection metric).
func assertRejections(ctx context.Context, rec *recorder, apiAddr, clientTag string, stream metricStream, statusAnchor uint32) (passed, failed int) {
	if len(stream.Rejections) == 0 {
		return 0, 0
	}
	known := knownMetricNames(stream)
	// statusAnchor anchors the REALTIME __src_ingestion_status window (F1); qurl is
	// reused for every failure's failed-query record + the detail builder's url line.
	qurl := ingestionStatusURL(apiAddr, statusAnchor)
	var (
		last     map[string]map[int32]float64
		lastBody string
	)
	_ = poll(ctx, ledgerTimeout, 3*time.Second, func() (bool, error) {
		bd, body, err := fetchIngestionBreakdown(ctx, apiAddr, statusAnchor, known)
		if err != nil {
			return false, nil // transient query error → keep polling (absence is only trustworthy once clean)
		}
		last, lastBody = bd, body
		return rejectionsConverged(stream.Rejections, bd), nil
	})
	for _, r := range stream.Rejections {
		if !r.Sent {
			passed++
			rec.logf("PASS client=%s rejection=%s status=%s(%d) SKIP: %s", clientTag, r.Name, r.StatusName, r.StatusID, r.SkipReason)
			fmt.Printf("PASS client=%s rejection=%s status=%s SKIP\n", clientTag, r.Name, r.StatusName)
			continue
		}
		got := statusCount(last[r.Name], r.StatusID)
		if got == float64(r.Writes) {
			passed++
			rec.logf("PASS client=%s rejection=%s status=%s(%d) count=%d", clientTag, r.Name, r.StatusName, r.StatusID, r.Writes)
			fmt.Printf("PASS client=%s rejection=%s status=%s\n", clientTag, r.Name, r.StatusName)
			continue
		}
		failed++
		det := rejectionFailDetail(r, last[r.Name], qurl)
		rec.recordFailedQuery(failedQuery{
			Label: "rejection", Client: clientTag, Metric: r.Name,
			URL: qurl, Body: lastBody,
		})
		rec.logf("FAIL client=%s rejection=%s status=%s(%d)\n%s", clientTag, r.Name, r.StatusName, r.StatusID, indent(det))
		fmt.Printf("FAIL client=%s rejection=%s status=%s(%d)\n%s\n", clientTag, r.Name, r.StatusName, r.StatusID, indent(det))
	}
	return passed, failed
}

// rejectionsConverged reports whether every GENERATED rejection (Sent==true) has
// reached its exact status count. Skipped (Sent==false) rejections are unconstrained.
func rejectionsConverged(rejections []rejectionMetric, bd map[string]map[int32]float64) bool {
	for _, r := range rejections {
		if !r.Sent {
			continue
		}
		if statusCount(bd[r.Name], r.StatusID) != float64(r.Writes) {
			return false
		}
	}
	return true
}

// statusCount looks up one status ID's total in a metric's breakdown (nil-safe).
func statusCount(breakdown map[int32]float64, statusID int32) float64 {
	if breakdown == nil {
		return 0
	}
	return breakdown[statusID]
}

// assertConservationLedger is ticket-12 criterion 3 + 5: the conservation invariant
// per test metric M, EXACT:
//
//	sentWrites(M) == okCached(M) + Σ err_*(M)
//
// for EVERY metric the client generated (normal + rejection). sentWrites is the
// harness's true input cardinality; okCached is Σ __src_ingestion_status{M,ok_cached};
// err_* is the sum of all rejection statuses. Warnings are excluded (they accompany
// accepted events). Under the no-sampling config (assertNoAggSampling) the equation
// is exact; any drift is a real conservation violation — silent loss (okCached+err
// < sentWrites) or double-counting (>). On failure it prints the FULL breakdown
// (sentWrites, okCached, every err_* status with its count) so the imbalance is
// diagnosed at a glance. Returns pass/fail counts (one per metric).
func assertConservationLedger(ctx context.Context, rec *recorder, apiAddr, clientTag string, stream metricStream, statusAnchor uint32) (passed, failed int) {
	want := ledgerWriteCounts(stream)
	known := knownMetricNames(stream)
	excluded := ledgerExcludedMetricNames(stream) // multi-value metrics outside the exact 1:1 scope (see ledgerEligibleKind)
	// statusAnchor anchors the REALTIME __src_ingestion_status window (F1); qurl is
	// reused for every failure's failed-query record + the detail builder's url line.
	qurl := ingestionStatusURL(apiAddr, statusAnchor)
	var (
		last     map[string]map[int32]float64
		lastBody string
	)
	_ = poll(ctx, ledgerTimeout, 3*time.Second, func() (bool, error) {
		bd, body, err := fetchIngestionBreakdown(ctx, apiAddr, statusAnchor, known)
		if err != nil {
			return false, nil
		}
		last, lastBody = bd, body
		return ledgerConverged(want, bd), nil
	})
	// Sorted iteration for stable, readable output.
	names := make([]string, 0, len(want))
	for n := range want {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		sentWrites := want[name]
		okCached, errSum, errs, warns := ledgerBalance(last[name])
		if okCached+errSum == float64(sentWrites) {
			passed++
			continue
		}
		failed++
		det := ledgerFailDetail(name, sentWrites, okCached, errSum, errs, warns, qurl)
		rec.recordFailedQuery(failedQuery{
			Label: "ledger", Client: clientTag, Metric: name,
			URL: qurl, Body: lastBody,
		})
		rec.logf("FAIL client=%s ledger metric=%s\n%s", clientTag, name, indent(det))
		fmt.Printf("FAIL client=%s ledger metric=%s\n%s\n", clientTag, name, indent(det))
	}
	exclNote := ""
	if len(excluded) != 0 {
		sort.Strings(excluded)
		exclNote = fmt.Sprintf(" [%d multi-value excluded (item count ≠ write count): %s]", len(excluded), strings.Join(excluded, ","))
	}
	if failed == 0 {
		rec.logf("PASS client=%s conservation ledger: %d metric(s) balance (sentWrites == ok_cached + Σerr_*)%s", clientTag, len(want), exclNote)
		fmt.Printf("PASS client=%s conservation ledger: %d metric(s)%s\n", clientTag, len(want), exclNote)
	} else {
		rec.logf("FAIL client=%s conservation ledger: %d/%d metric(s) unbalanced%s", clientTag, failed, len(want), exclNote)
		fmt.Printf("FAIL client=%s conservation ledger: %d/%d unbalanced%s\n", clientTag, failed, len(want), exclNote)
	}
	return passed, failed
}

// ledgerExcludedMetricNames returns the distinct metric names whose kind is outside
// the ledger's exact scope (ledgerEligibleKind), so the summary can state explicitly
// which metrics are not balance-checked and why (avoids a silent "14 → 11" gap).
func ledgerExcludedMetricNames(stream metricStream) []string {
	seen := make(map[string]bool)
	var out []string
	for _, m := range stream.Metrics {
		if !ledgerEligibleKind(m.Kind) && !seen[m.Name] {
			seen[m.Name] = true
			out = append(out, m.Name)
		}
	}
	for _, r := range stream.Rejections {
		if !ledgerEligibleKind(r.Kind) && !seen[r.Name] {
			seen[r.Name] = true
			out = append(out, r.Name)
		}
	}
	return out
}

// ledgerConverged reports whether every metric balances yet (used to stop polling
// early once the whole ledger has landed).
func ledgerConverged(want map[string]int, bd map[string]map[int32]float64) bool {
	for name, sentWrites := range want {
		okCached, errSum, _, _ := ledgerBalance(bd[name])
		if okCached+errSum != float64(sentWrites) {
			return false
		}
	}
	return true
}

// aggSamplingFactorMetric is the builtin the agg increments when an insert is
// SAMPLED (sampling factor >1). The harness's --min-insert-budget=100000000 /
// --receive-budget-warming=0 / --disable-receive-sample-budget config keeps every
// insert unsampled (sf=1), so this metric must stay ABSENT/zero across the whole run.
const aggSamplingFactorMetric = "__agg_sampling_factor"

// assertNoAggSampling is ticket-12 criterion 4, the whole-run sampling tripwire:
// __agg_sampling_factor must stay absent/zero. A non-zero point means an insert was
// sampled, which would break the ledger's exactness (err statuses ride the metric's
// own sample budget) and the value assertions' representativeness. compareByFunc's
// per-query sampling check already guards each queried VIEW; this is the historical,
// whole-run confirmation over __agg_sampling_factor itself. ok=true means no sampling
// point appeared within samplingTimeout (absent), confirmed by at least one CLEAN
// query. Polls until a non-zero point shows (fails fast) or the window elapses clean.
// The window is [statusAnchor, statusAnchor+ingestionStatusTail] — a REALTIME builtin
// anchored at client-phase start, NOT the historic base (F1). FAILS CLOSED: if every
// query errors over the whole window (api down / wrong address), ok=false — an
// unobservable stack can never pass for "no sampling" (see pollAbsenceTripwire).
func assertNoAggSampling(ctx context.Context, rec *recorder, apiAddr, clientTag string, statusAnchor uint32) (ok bool, detail string) {
	q := url.Values{}
	q.Set("s", aggSamplingFactorMetric)
	q.Set("f", strconv.FormatUint(uint64(statusAnchor), 10))
	q.Set("t", strconv.FormatUint(uint64(statusAnchor+ingestionStatusTail), 10))
	q.Set("w", "1s")
	q.Set("ac", "1")
	q.Set("qw", "count")
	qurl := "http://" + apiAddr + "/api/query?" + q.Encode()

	// pollAbsenceTripwire fails CLOSED: if every query errors for the whole window
	// (api down / wrong address / schema drift) it returns ok=false, so a sampling
	// event is never hidden behind a stack that could not be queried.
	var (
		violBody   string // raw reply of the first query that surfaced a non-zero point
		violStatus int
	)
	query := func(ctx context.Context) (float64, error) {
		resp, body, status, qerr := queryCounterRaw(ctx, qurl)
		if qerr != nil {
			return 0, qerr
		}
		mx := maxSeriesValue(resp)
		if mx > 0 && violBody == "" { // capture the violating payload once
			violBody, violStatus = body, status
		}
		return mx, nil
	}
	o := pollAbsenceTripwire(ctx, samplingTimeout, 3*time.Second, query)
	if o.ok {
		// No non-zero point appeared within samplingTimeout → no insert was sampled.
		return true, ""
	}
	if !o.confirmed {
		rec.recordFailedQuery(failedQuery{
			Label: "sampling", Client: clientTag, Metric: aggSamplingFactorMetric,
			URL: qurl, HTTPStatus: 0, Body: "",
		})
		return false, fmt.Sprintf("client=%s could not confirm absence of %s — every query failed over %s: %v\nurl: %s",
			clientTag, aggSamplingFactorMetric, samplingTimeout, o.queryErr, qurl)
	}
	// A non-zero point surfaced → an insert was sampled during the run.
	rec.recordFailedQuery(failedQuery{
		Label: "sampling", Client: clientTag, Metric: aggSamplingFactorMetric,
		URL: qurl, HTTPStatus: violStatus, Body: violBody,
	})
	return false, fmt.Sprintf("client=%s %s non-zero (max=%g) — an insert was sampled during the run\nurl: %s",
		clientTag, aggSamplingFactorMetric, o.worst, qurl)
}

// maxSeriesValue is the largest value across every series and bucket in a reply
// (0 for an empty/absent metric). Used to detect any non-zero __agg_sampling_factor.
func maxSeriesValue(resp *apiSeriesResponse) float64 {
	var mx float64
	for _, data := range resp.Data.Series.SeriesData {
		for _, v := range data {
			if v > mx {
				mx = v
			}
		}
	}
	return mx
}

// sortedStatusIDs returns the status IDs of a per-status breakdown in ascending
// order, so the failure-detail renderers emit their status rows in a stable order
// (independent of map iteration order).
func sortedStatusIDs(m map[int32]float64) []int32 {
	ids := make([]int32, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// ledgerFailDetail renders the full conservation breakdown for one imbalanced
// metric: the equation that failed, then every status→count the pipeline recorded
// for it (status name + ID) — including warn_* rows (marked as warnings, not losses)
// for diagnostic context — so a loss vs a double-count vs an unexpected status is
// visible at once. qurl (the __src_ingestion_status query) is appended so the
// failing query is pinpointed alongside the breakdown (F4).
func ledgerFailDetail(name string, sentWrites int, okCached, errSum float64, errs, warns map[int32]float64, qurl string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "conservation imbalance: ok_cached(%g) + Σerr(%g) = %g ≠ sentWrites=%d\n",
		okCached, errSum, okCached+errSum, sentWrites)
	if okCached+errSum < float64(sentWrites) {
		fmt.Fprintf(&b, "  → %g event(s) UNACCOUNTED (silent loss)\n", float64(sentWrites)-(okCached+errSum))
	} else {
		fmt.Fprintf(&b, "  → %g event(s) OVER-COUNTED (double-counting)\n", (okCached+errSum)-float64(sentWrites))
	}
	fmt.Fprintf(&b, "  full __src_ingestion_status breakdown for %s (status=count):\n", name)
	fmt.Fprintf(&b, "    %s(10)=%g\n", ingestionStatusName(statusIDOKCached), okCached)
	ids := sortedStatusIDs(errs)
	if len(ids) == 0 {
		fmt.Fprintf(&b, "    (no err_* statuses recorded)\n")
	}
	for _, id := range ids {
		fmt.Fprintf(&b, "    %s(%d)=%g\n", ingestionStatusName(id), id, errs[id])
	}
	// Warnings accompany ACCEPTED events (excluded from the balance); shown here only
	// for diagnostic context (e.g. a clamped timestamp) — they are NOT losses.
	for _, id := range sortedStatusIDs(warns) {
		fmt.Fprintf(&b, "    %s(%d)=%g (warning — accepted, not a loss)\n", ingestionStatusName(id), id, warns[id])
	}
	fmt.Fprintf(&b, "url: %s", qurl)
	return b.String()
}

// rejectionFailDetail renders the status breakdown for one rejection that did not
// produce its exact status count: what was expected vs got, then every status the
// pipeline recorded for that metric (status name + ID), so a wrong status or a
// partial rejection is visible. qurl (the __src_ingestion_status query) is appended
// so the failing query is pinpointed alongside the breakdown (F4).
func rejectionFailDetail(r rejectionMetric, breakdown map[int32]float64, qurl string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "expected %s(%d) count=%d, got count=%g\n",
		r.StatusName, r.StatusID, r.Writes, statusCount(breakdown, r.StatusID))
	if len(breakdown) == 0 {
		fmt.Fprintf(&b, "  (no __src_ingestion_status rows for %s — metric never reached the agent, or window missed it)\n", r.Name)
		fmt.Fprintf(&b, "url: %s", qurl)
		return b.String()
	}
	fmt.Fprintf(&b, "  full breakdown for %s (status=count):\n", r.Name)
	for _, id := range sortedStatusIDs(breakdown) {
		fmt.Fprintf(&b, "    %s(%d)=%g\n", ingestionStatusName(id), id, breakdown[id])
	}
	fmt.Fprintf(&b, "url: %s", qurl)
	return b.String()
}

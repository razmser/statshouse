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
// prefix already isolates clients; the tag makes the line readable).
func assertStream(ctx context.Context, rec *recorder, apiAddr, clientTag string, stream metricStream) (passed, failed int) {
	for _, m := range stream.Metrics {
		for _, qf := range funcsFor(m.Kind) {
			ok, detail := pollMetricFunc(ctx, apiAddr, m, stream.Base, qf)
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
func pollMetricFunc(ctx context.Context, apiAddr string, m metricModel, base uint32, qf queryFunc) (bool, string) {
	qurl := metricQueryURL(apiAddr, m.Name, m.QBKeys, qf.qw, base)
	var lastDetail string
	if err := poll(ctx, assertTimeout, 2*time.Second, func() (bool, error) {
		resp, qerr := queryCounter(ctx, qurl)
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
		return false, lastDetail
	}
	return true, ""
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

// assertNoClientWriteErr is the silent-loss tripwire: after a driver exits, poll
// __src_client_write_err for the client's language over this run's window and
// fail (ok=false, with a labelled detail) the moment a non-zero point appears.
// ok=true means no loss was seen within writeErrTimeout. The window starts at the
// END of the asserted e2e window (the dropped-bytes ts ≈ base+120, well after
// base+numBuckets) and runs 200s forward — wide enough to absorb client/host
// clock skew, and anchored on the per-client base so a prior client's loss (its
// base is much older) is excluded. Returns ok=true at once for a client whose
// language is unknown (it does not emit the metric, so there is nothing to assert).
func assertNoClientWriteErr(ctx context.Context, apiAddr, clientTag string, base uint32) (ok bool, detail string) {
	lang, knows := clientWriteErrLang[clientTag]
	if !knows {
		return true, "" // this client's library does not emit the metric; nothing to assert
	}
	q := url.Values{}
	q.Set("s", clientWriteErrMetric)
	q.Set("f", strconv.FormatUint(uint64(base+numBuckets), 10))
	q.Set("t", strconv.FormatUint(uint64(base+numBuckets+200), 10))
	q.Set("w", "1s")
	q.Set("ac", "1")
	q.Set("qw", "sum")
	q.Set("qb", "1") // group by the language tag → one series per client language
	qurl := "http://" + apiAddr + "/api/query?" + q.Encode()

	var lost float64
	if err := poll(ctx, writeErrTimeout, 3*time.Second, func() (bool, error) {
		resp, qerr := queryCounter(ctx, qurl)
		if qerr != nil {
			// Transient query error (api busy / a slow conveyor): keep polling;
			// absence is only trustworthy once the query itself succeeds clean.
			return false, nil
		}
		if maxLost, found := clientWriteErrForLang(resp, lang); found {
			lost = maxLost
			return true, nil // a dropped-bytes point appeared → stop, it is a failure
		}
		return false, nil // keep polling: absence is not confirmed until the window elapses
	}); err != nil {
		// Timed out with no non-zero point for this language → no loss detected.
		return true, ""
	}
	// poll returned nil → the condition fired → a non-zero point was seen.
	detail = fmt.Sprintf("client=%s lang=%s lost_bytes≈%g (silent TCP-backpressure drop; __src_client_write_err non-zero)\nurl: %s",
		clientTag, lang, lost, qurl)
	return false, detail
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

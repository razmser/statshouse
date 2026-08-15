// Command e2e's differential conformance mode (--conformance): one network,
// ClickHouse, ONE shared metadata, and TWO daemon stacks — agg/api/agent on
// ClickHouse and agg/api/agent on duck-store — fed the IDENTICAL deterministic
// stream by the harness itself (in-process statshouse-go clients), then one
// semantic request set issued to BOTH apis with the decoded answers compared
// under the frozen suite tolerances. ClickHouse is the reference: a duck answer
// diverging beyond tolerance is a loud FAIL recorded with both raw responses,
// so the mode runs in CI (`bash e2e/lima.sh --conformance`) and fails non-zero.
//
// Why in-process seeding: the go client stamps every packet with _h=<its own
// hostname> (client_bucket.go fillTag). Seeding both agents from THIS process
// makes the _h tag — and therefore every max_host string — literally identical
// across the two backends, so host columns compare by exact value instead of by
// luck of which container wrote. It also replays the go driver template's exact
// seed/poll/settle/paced-write/Close sequence (drivers/go/main.go.tmpl), so the
// data landing in both stores is what the frozen model already describes.
//
// Why one shared metadata: both aggregators auto-create the same names into one
// metadata (deduped by name), metric/tag mappings are identical for both
// stores, and both apis' journals converge on the same view — the conformance
// question is purely "do the two STORES answer identically", which a second
// metadata would only add noise to.
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

	statshouse "github.com/VKCOM/statshouse-go"
)

// conformanceClientTag isolates the conformance stream's metric names from the
// per-client streams (stream.go generateStream folds it into the prefix), so a
// leftover --keep stack's data can never blend into a conformance run.
const conformanceClientTag = "conf"

// conformanceSeedLead is the seconds-before-Base timestamp of the cold-start
// seeds, matching the go driver template's SeedTS (client.go) so auto-create
// behavior is identical to the frozen client phase.
const conformanceSeedLead = 60

// conformanceTimeout bounds one request's differential poll: the reference must
// be non-empty AND both decoded answers must agree. The historic conveyor lands
// data in ClickHouse ~24s after the writes, so this is comfortably above the
// frozen assertTimeout while staying small enough that a genuinely divergent
// request fails fast.
const conformanceTimeout = 120 * time.Second

// conformancePollInterval is the differential poll tick.
const conformancePollInterval = 3 * time.Second

// confDivergenceSamples is how many consecutive disagreeing polls (with a
// non-empty reference) confirm a divergence. Agreement polling exists for data
// still in flight; once both stores hold their data a disagreement is final,
// so failing fast keeps a red run inside the overall --timeout instead of
// burning conformanceTimeout per bad request (the first live run hit exactly
// that: two real divergences × 120 s + cascading teardown refusals).
const confDivergenceSamples = 3

// statshouseGoEmptyAddrErr is the message statshouse-go v0.5.17's idle
// secondary TCP connection reports on Close for a single-address client (see
// seedConformanceStream for the full story).
const statshouseGoEmptyAddrErr = "empty statshouse address"

// Stack tags (daemonStackOpts.stackTag) so the two conformance stacks coexist
// on one network with distinct container names, and their captured logs land in
// distinct artifacts files (serviceLogName).
const (
	confStackCH   = "ch"
	confStackDuck = "duck"
)

// --- request set ------------------------------------------------------

// confKind is which API endpoint a conformance request hits.
type confKind int

const (
	confSeries    confKind = iota // GET /api/query (incl. PromQL-shaped and host-column forms)
	confTable                     // GET /api/table
	confPoint                     // GET /api/point
	confTagValues                 // GET /api/metric-tag-values
)

// confRequest is one semantic request issued to BOTH apis verbatim (path+query;
// only the authority differs). qw is the tolerance key (count/sum/…/p90/
// unique/max_count_host), not necessarily a query param of every endpoint kind.
type confRequest struct {
	kind  confKind
	label string // human-readable, e.g. "series/c_tagged/count"
	qw    string
	path  string // URL path+query, identical for both backends
}

// buildConformanceRequests derives the full differential request set from one
// generated stream:
//
//   - series per metric × funcsFor(kind) — the SAME param shape the frozen
//     asserter uses (parity asserted in conformance_test.go), covering count,
//     cardinality, sum/min/max/avg, p50/p90/p99 and unique;
//   - series-exact companions (count, sum) for every value_p metric: exact
//     columns over the same data as the banded percentiles, so a percentile
//     divergence is diagnosable in-run as data-level vs estimator-level;
//   - host-column variants: qw=count with mh=1 (max_hosts alongside counts) and
//     qw=max_count_host (DigestMax+maxhost);
//   - table view: v_mix qw=sum and c_matrix qw=count n=10000 (below the API's
//     maxTableRowsPage clamp, so both sides return every row);
//   - point view: c_tagged count, vp_mix p90, u_exact unique over the last
//     bucket;
//   - one PromQL-shaped multi-metric regex query ({__name__=~"a|b",…}), the
//     dialect the UI uses for multi-metric graphs;
//   - tag-values: c_matrix tag 0 (6 distinct values incl. unicode) and u_exact
//     tag 0.
//
// Metric lookup is by generator suffix (c_tagged, v_mix, …): suffixes are unique
// within a stream, so this is stable regardless of the run-id prefix.
func buildConformanceRequests(stream metricStream) []confRequest {
	var reqs []confRequest
	base := stream.Base

	// Series per metric per query function, with param parity to the frozen
	// asserter (metricQueryURL): the differential must query the same shape the
	// model-verified suite queries.
	for _, m := range stream.Metrics {
		for _, qf := range funcsFor(m.Kind) {
			path := strings.TrimPrefix(metricQueryURL("", m.Name, m.QBKeys, qf.qw, base), "http://")
			reqs = append(reqs, confRequest{
				kind:  confSeries,
				label: "series/" + confShortName(m.Name) + "/" + qf.qw,
				qw:    qf.qw,
				path:  path,
			})
		}
	}

	// Exact-function companions for every value_p metric: count (number of
	// values) and sum are EXACT columns over the same per-bucket data whose
	// percentiles are only banded. When a percentile request diverges, these
	// requests make the run itself diagnose the class: count/sum differing
	// means the two stores ingested different values (data-path divergence —
	// fix the write path); count/sum identical means same data folded
	// differently (estimator divergence — fix the quantile folding). This is
	// not hypothetical: the first live run diverged on exactly one vp_mix
	// percentile bucket (run 20260815-152438), and without these companions
	// the artifacts could not separate the two hypotheses.
	for _, m := range stream.Metrics {
		if m.Kind != kindValueP {
			continue
		}
		for _, qw := range []string{"count", "sum"} {
			reqs = append(reqs, confRequest{
				kind:  confSeries,
				label: "series-exact/" + confShortName(m.Name) + "/" + qw,
				qw:    qw,
				path: confQueryPath(m.Name, m.QBKeys, base, func(q url.Values) {
					q.Set("qw", qw)
				}),
			})
		}
	}

	// Host-column variants.
	if m, ok := confMetric(stream, "c_tagged"); ok {
		reqs = append(reqs, confRequest{
			kind:  confSeries,
			label: "series-mh/c_tagged/count",
			qw:    "count",
			path: confQueryPath(m.Name, m.QBKeys, base, func(q url.Values) {
				q.Set("qw", "count")
				q.Set("mh", "1")
			}),
		})
	}
	if m, ok := confMetric(stream, "v_mix"); ok {
		reqs = append(reqs, confRequest{
			kind:  confSeries,
			label: "series-host/v_mix/max_count_host",
			qw:    "max_count_host",
			path: confQueryPath(m.Name, m.QBKeys, base, func(q url.Values) {
				q.Set("qw", "max_count_host")
			}),
		})
	}

	// Table view.
	if m, ok := confMetric(stream, "v_mix"); ok {
		reqs = append(reqs, confRequest{
			kind:  confTable,
			label: "table/v_mix/sum",
			qw:    "sum",
			path:  confTablePath(m.Name, m.QBKeys, base, "sum", 0),
		})
	}
	if m, ok := confMetric(stream, "c_matrix"); ok {
		reqs = append(reqs, confRequest{
			kind:  confTable,
			label: "table/c_matrix/count",
			qw:    "count",
			path:  confTablePath(m.Name, m.QBKeys, base, "count", 10000),
		})
	}

	// Point view over the last bucket.
	pointReq := func(suffix, qw string) {
		if m, ok := confMetric(stream, suffix); ok {
			reqs = append(reqs, confRequest{
				kind:  confPoint,
				label: "point/" + suffix + "/" + qw,
				qw:    qw,
				path: confPointPath(m.Name, m.QBKeys, base, func(q url.Values) {
					q.Set("qw", qw)
				}),
			})
		}
	}
	pointReq("c_tagged", "count")
	pointReq("vp_mix", "p90")
	pointReq("u_exact", "unique")

	// PromQL-shaped multi-metric regex (the UI's multi-metric dialect); series
	// are labelled with "__name__" inside tags, which tagSignature keeps so the
	// two metrics' series stay distinguishable. Label names must stay UNQUOTED:
	// Prometheus accepts `"__name__"=~…` but statshouse's parser only allows
	// bare identifiers in label matching (verified live — the quoted form is a
	// 400 parse error).
	if multi, ok := confMetric(stream, "c_multi"); ok {
		if matrix, ok := confMetric(stream, "c_matrix"); ok {
			promql := fmt.Sprintf(`{__name__=~"%s|%s",__what__="count",__by__="0"}`, multi.Name, matrix.Name)
			reqs = append(reqs, confRequest{
				kind:  confSeries,
				label: "promql/c_multi|c_matrix/count-by-0",
				qw:    "count",
				path: confRangePath(base, func(q url.Values) {
					q.Set("q", promql)
				}),
			})
		}
	}

	// Tag values.
	for _, suffix := range []string{"c_matrix", "u_exact"} {
		if m, ok := confMetric(stream, suffix); ok {
			q := url.Values{}
			q.Set("s", m.Name)
			q.Set("f", strconv.FormatUint(uint64(base), 10))
			q.Set("t", strconv.FormatUint(uint64(base+numBuckets), 10))
			q.Set("k", "0")
			q.Set("n", "1000")
			reqs = append(reqs, confRequest{
				kind:  confTagValues,
				label: "tag-values/" + suffix + "/k0",
				qw:    "count",
				path:  "/api/metric-tag-values?" + q.Encode(),
			})
		}
	}

	return reqs
}

// confMetric finds a stream metric by its generator suffix (c_tagged, v_mix…).
func confMetric(stream metricStream, suffix string) (metricModel, bool) {
	for _, m := range stream.Metrics {
		if strings.HasSuffix(m.Name, suffix) {
			return m, true
		}
	}
	return metricModel{}, false
}

// confShortName strips the run-id/client prefix for readable labels. It cuts
// at the conformance client tag so the full generator suffix survives
// (…_conf_c_tagged -> c_tagged, …_conf_vp_mix -> vp_mix) — a plain last-
// underscore split would collide v_mix and vp_mix on "mix".
func confShortName(name string) string {
	if i := strings.LastIndex(name, "_"+conformanceClientTag+"_"); i >= 0 {
		return name[i+len(conformanceClientTag)+2:]
	}
	if i := strings.LastIndexByte(name, '_'); i >= 0 {
		return name[i+1:]
	}
	return name
}

// confQueryPath builds GET /api/query with the frozen asserter's param shape
// (s/f/t/w=1s/ac=1) plus whatever set adds (qw/mh/…), and qb repeated per
// group-by key. Parity with metricQueryURL is pinned by unit test.
func confQueryPath(name string, qb []string, base uint32, set func(q url.Values)) string {
	return confRangePath(base, func(q url.Values) {
		q.Set("s", name)
		set(q)
		for _, k := range qb {
			q.Add("qb", k)
		}
	})
}

// confRangePath applies set over the shared range params every /api/query-style
// endpoint takes: f/t over the stream's buckets and w=1s (explicit 1-second
// step so the 1s tier is used), plus ac=1 to defeat the ~1s query cache.
func confRangePath(base uint32, set func(q url.Values)) string {
	q := url.Values{}
	q.Set("f", strconv.FormatUint(uint64(base), 10))
	q.Set("t", strconv.FormatUint(uint64(base+numBuckets), 10))
	q.Set("w", "1s")
	q.Set("ac", "1")
	set(q)
	return "/api/query?" + q.Encode()
}

// confTablePath builds GET /api/table (same range params; n caps rows; 0 lets
// the API default apply). n=10000 for the matrix metric sits exactly at
// maxTableRowsPage, the API's clamp.
func confTablePath(name string, qb []string, base uint32, qw string, n int) string {
	p := confQueryPath(name, qb, base, func(q url.Values) {
		q.Set("qw", qw)
		if n > 0 {
			q.Set("n", strconv.Itoa(n))
		}
	})
	return strings.Replace(p, "/api/query", "/api/table", 1)
}

// confPointPath builds GET /api/point over exactly the LAST bucket of the
// stream (f=t-1), so each point is one unambiguous bucket.
func confPointPath(name string, qb []string, base uint32, set func(q url.Values)) string {
	q := url.Values{}
	q.Set("s", name)
	q.Set("f", strconv.FormatUint(uint64(base+numBuckets-1), 10))
	q.Set("t", strconv.FormatUint(uint64(base+numBuckets), 10))
	q.Set("w", "1s")
	q.Set("ac", "1")
	set(q)
	for _, k := range qb {
		q.Add("qb", k)
	}
	return "/api/point?" + q.Encode()
}

// --- decoders ----------------------------------------------------------

// confSeriesResp decodes GET /api/query (and the PromQL form): the payload sits
// under "data"; sampling factors must be 0 (no sampling was configured);
// series_meta carries per-series tags and — when hosts were requested — the
// max_hosts strings. A missing point unmarshals to 0.0 (JSON null), which is
// valid to compare: every expected value is non-zero.
type confSeriesResp struct {
	Data struct {
		Series            confSeriesData `json:"series"`
		SamplingFactorSrc float64        `json:"sampling_factor_src"`
		SamplingFactorAgg float64        `json:"sampling_factor_agg"`
	} `json:"data"`
}

type confSeriesData struct {
	Time       []int64          `json:"time"`
	SeriesMeta []confSeriesMeta `json:"series_meta"`
	SeriesData [][]float64      `json:"series_data"`
}

type confSeriesMeta struct {
	Tags     map[string]apiMetaTag `json:"tags"`
	MaxHosts []string              `json:"max_hosts"`
}

// confTableResp decodes GET /api/table: one row per (bucket, tag-set), its Data
// aligned with What; More marks truncation. Rows are compared order-
// insensitively keyed by (time, tag signature).
type confTableResp struct {
	Data struct {
		Rows []struct {
			Time int64                 `json:"time"`
			Data []float64             `json:"data"`
			Tags map[string]apiMetaTag `json:"tags"`
		} `json:"rows"`
		What []string `json:"what"`
		More bool     `json:"more"`
	} `json:"data"`
}

// confPointResp decodes GET /api/point: point_meta[i] describes the point
// (tags, max_host, from/to seconds) and point_data[i] is its value.
type confPointResp struct {
	Data struct {
		PointMeta []struct {
			Tags    map[string]apiMetaTag `json:"tags"`
			MaxHost string                `json:"max_host"`
			FromSec int64                 `json:"from_sec"`
			ToSec   int64                 `json:"to_sec"`
		} `json:"point_meta"`
		PointData []float64 `json:"point_data"`
	} `json:"data"`
}

// confTagValuesResp decodes GET /api/metric-tag-values: the tag's distinct
// values with their counts, and whether the list was truncated.
type confTagValuesResp struct {
	Data struct {
		TagValues []struct {
			Value string  `json:"value"`
			Count float64 `json:"count"`
		} `json:"tag_values"`
		TagValuesMore bool `json:"tag_values_more"`
	} `json:"data"`
}

// confDecoded is a type-erased decoded response: exactly the field for the
// request's kind is populated.
type confDecoded struct {
	series *confSeriesResp
	table  *confTableResp
	point  *confPointResp
	tags   *confTagValuesResp
}

// --- comparators --------------------------------------------------------

// confSumOrderTol is the relative band for sum/avg in the CROSS-STORE
// comparison only: both stores aggregate the identical value multiset, but
// float64 addition is not associative, and ClickHouse's native sum and the
// Go-side fold over delta+archive rows accumulate in different orders — the
// answers legitimately differ in the last few ulps (observed live:
// 644601.744 vs 644601.7439999995, ~1e-16). Any semantic divergence (dropped
// or duplicated values, wrong factor) is orders of magnitude above 1e-9.
// The frozen asserter's exact-sum check against the generated model is
// unaffected — this is the differential comparator only.
const confSumOrderTol = 1e-9

// confValueMatches applies the suite's frozen tolerance table to one decoded
// value with CH as truth (ref): exact equality for count/counter-derivatives
// (count, countraw, cardinality, min, max, max_count_host's value column —
// and table/point values of those kinds), the float-ordering band for sum/avg
// (confSumOrderTol), the percentile band max(1%·|ref|, 1.0) for p50/p90/p99,
// and exact-below/±2%-above the uniquesHashMaxSize threshold for unique
// (mirroring compareUnique's exact-vs-thinning split, derived from the
// reference magnitude).
func confValueMatches(qw string, ref, got float64) bool {
	switch qw {
	case "p50", "p90", "p99":
		return withinAbsTol(got, ref, percentileTol, percentileMinAbs)
	case "sum", "avg":
		return withinRelTol(got, ref, confSumOrderTol)
	case "unique":
		if ref > uniquesHashMaxSize {
			return withinRelTol(got, ref, uniqueApproxTol)
		}
		return got == ref
	default:
		return got == ref
	}
}

// compareConfSeries compares two /api/query replies: sampling factors must be 0
// in EACH response independently, time axes exactly equal, series sets exactly
// equal (matched by tagSignature — which keeps "__name__" so multi-metric
// series distinguish), per-bucket values under confValueMatches, and — when the
// reference carries max_hosts — the host strings exactly equal (both backends
// were seeded by THIS process, so the host identity is literally shared).
func compareConfSeries(ref, got *confSeriesResp, qw string) []string {
	var diffs []string
	for _, r := range []struct {
		name string
		resp *confSeriesResp
	}{{"reference", ref}, {"duck", got}} {
		if s := r.resp.Data.SamplingFactorSrc + r.resp.Data.SamplingFactorAgg; s != 0 {
			diffs = append(diffs, fmt.Sprintf("%s: sampling_factor_src+agg=%g (expected 0 — data was sampled)", r.name, s))
		}
	}
	if !equalInt64s(ref.Data.Series.Time, got.Data.Series.Time) {
		diffs = append(diffs, fmt.Sprintf("time axes differ: reference %d points, duck %d points", len(ref.Data.Series.Time), len(got.Data.Series.Time)))
	}
	refIdx := indexConfSeries(ref)
	gotIdx := indexConfSeries(got)
	for _, sig := range sortedKeys(refIdx) {
		rs := refIdx[sig]
		gs, ok := gotIdx[sig]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("series{%s} present in reference but absent in duck", sig))
			continue
		}
		for j, ts := range ref.Data.Series.Time {
			rv, gv := atFloat(rs.data, j), atFloat(gs.data, j)
			if !confValueMatches(qw, rv, gv) {
				diffs = append(diffs, fmt.Sprintf("series{%s} bucket=%d reference=%s duck=%g", sig, ts, formatConfRef(qw, rv), gv))
			}
		}
		if strings.Join(rs.hosts, "|") != strings.Join(gs.hosts, "|") {
			diffs = append(diffs, fmt.Sprintf("series{%s} max_hosts reference=%q duck=%q", sig, rs.hosts, gs.hosts))
		}
	}
	for _, sig := range sortedKeys(gotIdx) {
		if _, ok := refIdx[sig]; !ok {
			diffs = append(diffs, fmt.Sprintf("series{%s} present in duck but absent in reference", sig))
		}
	}
	return diffs
}

// confSeriesRow is one indexed series' comparable payload.
type confSeriesRow struct {
	data  []float64
	hosts []string
}

func indexConfSeries(r *confSeriesResp) map[string]confSeriesRow {
	out := make(map[string]confSeriesRow, len(r.Data.Series.SeriesMeta))
	for i, meta := range r.Data.Series.SeriesMeta {
		var row confSeriesRow
		if i < len(r.Data.Series.SeriesData) {
			row.data = r.Data.Series.SeriesData[i]
		}
		row.hosts = meta.MaxHosts
		out[tagSignature(meta.Tags)] = row
	}
	return out
}

// compareConfTable compares two /api/table replies order-insensitively: rows
// keyed by (bucket, tagSignature), What columns and the truncation flag equal,
// per-cell values exact (only exact-tolerance kinds are issued as table
// requests).
func compareConfTable(ref, got *confTableResp) []string {
	var diffs []string
	if strings.Join(ref.Data.What, ",") != strings.Join(got.Data.What, ",") {
		diffs = append(diffs, fmt.Sprintf("what columns differ: reference %v duck %v", ref.Data.What, got.Data.What))
	}
	if ref.Data.More != got.Data.More {
		diffs = append(diffs, fmt.Sprintf("truncation flag differs: reference more=%v duck more=%v", ref.Data.More, got.Data.More))
	}
	type rowKey struct {
		ts  int64
		sig string
	}
	refRows := make(map[rowKey][]float64, len(ref.Data.Rows))
	for _, r := range ref.Data.Rows {
		refRows[rowKey{r.Time, tagSignature(r.Tags)}] = r.Data
	}
	gotRows := make(map[rowKey][]float64, len(got.Data.Rows))
	for _, r := range got.Data.Rows {
		gotRows[rowKey{r.Time, tagSignature(r.Tags)}] = r.Data
	}
	for k, rd := range refRows {
		gd, ok := gotRows[k]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("table row{%s,%d} present in reference but absent in duck", k.sig, k.ts))
			continue
		}
		if len(rd) != len(gd) {
			diffs = append(diffs, fmt.Sprintf("table row{%s,%d} cell count reference=%d duck=%d", k.sig, k.ts, len(rd), len(gd)))
			continue
		}
		for i, rv := range rd {
			if rv != gd[i] {
				diffs = append(diffs, fmt.Sprintf("table row{%s,%d} cell=%d reference=%g duck=%g", k.sig, k.ts, i, rv, gd[i]))
			}
		}
	}
	for k := range gotRows {
		if _, ok := refRows[k]; !ok {
			diffs = append(diffs, fmt.Sprintf("table row{%s,%d} present in duck but absent in reference", k.sig, k.ts))
		}
	}
	return diffs
}

// compareConfPoint compares two /api/point replies: points keyed by
// tagSignature, from/to seconds and max_host exactly equal, values under
// confValueMatches.
func compareConfPoint(ref, got *confPointResp, qw string) []string {
	var diffs []string
	if len(ref.Data.PointMeta) != len(got.Data.PointMeta) {
		return append(diffs, fmt.Sprintf("point count differs: reference=%d duck=%d", len(ref.Data.PointMeta), len(got.Data.PointMeta)))
	}
	type pointKey struct {
		sig, host string
		from, to  int64
	}
	refPts := make(map[pointKey]float64, len(ref.Data.PointMeta))
	for i, m := range ref.Data.PointMeta {
		var v float64
		if i < len(ref.Data.PointData) {
			v = ref.Data.PointData[i]
		}
		refPts[pointKey{tagSignature(m.Tags), m.MaxHost, m.FromSec, m.ToSec}] = v
	}
	for i, m := range got.Data.PointMeta {
		var v float64
		if i < len(got.Data.PointData) {
			v = got.Data.PointData[i]
		}
		k := pointKey{tagSignature(m.Tags), m.MaxHost, m.FromSec, m.ToSec}
		rv, ok := refPts[k]
		if !ok {
			diffs = append(diffs, fmt.Sprintf("point{%s host=%q from=%d to=%d} present in duck but absent in reference", k.sig, k.host, k.from, k.to))
			continue
		}
		if !confValueMatches(qw, rv, v) {
			diffs = append(diffs, fmt.Sprintf("point{%s} reference=%s duck=%g", k.sig, formatConfRef(qw, rv), v))
		}
		delete(refPts, k)
	}
	for k := range refPts {
		diffs = append(diffs, fmt.Sprintf("point{%s host=%q from=%d to=%d} present in reference but absent in duck", k.sig, k.host, k.from, k.to))
	}
	return diffs
}

// compareConfTagValues compares two /api/metric-tag-values replies exactly:
// the (value, count) lists (order-insensitive — counts are compared by value)
// and the truncation flag.
func compareConfTagValues(ref, got *confTagValuesResp) []string {
	var diffs []string
	if ref.Data.TagValuesMore != got.Data.TagValuesMore {
		diffs = append(diffs, fmt.Sprintf("tag_values_more differs: reference=%v duck=%v", ref.Data.TagValuesMore, got.Data.TagValuesMore))
	}
	key := func(v string, c float64) string { return v + "\x00" + strconv.FormatFloat(c, 'g', -1, 64) }
	refSet := make(map[string]string, len(ref.Data.TagValues))
	for _, tv := range ref.Data.TagValues {
		refSet[key(tv.Value, tv.Count)] = tv.Value
	}
	gotSet := make(map[string]string, len(got.Data.TagValues))
	for _, tv := range got.Data.TagValues {
		gotSet[key(tv.Value, tv.Count)] = tv.Value
	}
	for k, v := range refSet {
		if _, ok := gotSet[k]; !ok {
			diffs = append(diffs, fmt.Sprintf("tag value{%q} with reference count absent in duck", v))
		}
	}
	for k, v := range gotSet {
		if _, ok := refSet[k]; !ok {
			diffs = append(diffs, fmt.Sprintf("tag value{%q} present in duck but absent in reference", v))
		}
	}
	return diffs
}

// compareConfRequest dispatches on the request kind.
func compareConfRequest(req confRequest, ref, got confDecoded) []string {
	switch req.kind {
	case confSeries:
		return compareConfSeries(ref.series, got.series, req.qw)
	case confTable:
		return compareConfTable(ref.table, got.table)
	case confPoint:
		return compareConfPoint(ref.point, got.point, req.qw)
	case confTagValues:
		return compareConfTagValues(ref.tags, got.tags)
	}
	return []string{fmt.Sprintf("unknown request kind %d", req.kind)}
}

// confNonEmpty reports whether the REFERENCE answer carries data — the guard
// against vacuous passes (both sides answering empty agree trivially).
func confNonEmpty(kind confKind, dec confDecoded) bool {
	switch kind {
	case confSeries:
		s := dec.series.Data.Series
		return len(s.Time) > 0 && len(s.SeriesMeta) > 0 && len(s.SeriesData) > 0
	case confTable:
		return len(dec.table.Data.Rows) > 0
	case confPoint:
		return len(dec.point.Data.PointMeta) > 0 && len(dec.point.Data.PointData) > 0
	case confTagValues:
		return len(dec.tags.Data.TagValues) > 0
	}
	return false
}

// formatConfRef renders a reference value for a diff line, banding the
// approximate kinds so the expectation reads like the suite's own FAIL lines.
func formatConfRef(qw string, ref float64) string {
	switch qw {
	case "p50", "p90", "p99":
		return fmt.Sprintf("%g±max(%g%%,1)", ref, percentileTol*100)
	case "unique":
		if ref > uniquesHashMaxSize {
			return fmt.Sprintf("≈%g±%g%%", ref, uniqueApproxTol*100)
		}
	}
	return strconv.FormatFloat(ref, 'g', -1, 64)
}

func equalInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func atFloat(v []float64, i int) float64 {
	if i < len(v) {
		return v[i]
	}
	return 0
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// --- live: fetch + differential driver ---------------------------------

// fetchConf issues one request to one api and decodes the kind's shape.
func fetchConf(ctx context.Context, apiAddr string, req confRequest) (dec confDecoded, body string, status int, err error) {
	body, status, err = httpGet(ctx, "http://"+apiAddr+req.path)
	if err != nil {
		return dec, body, status, err
	}
	if status != 200 {
		return dec, body, status, fmt.Errorf("HTTP %d: %s", status, truncate(strings.TrimSpace(body), 300))
	}
	unmarshal := func(target any) error {
		if jerr := json.Unmarshal([]byte(body), target); jerr != nil {
			return fmt.Errorf("parse response: %w; body: %s", jerr, truncate(body, 300))
		}
		return nil
	}
	switch req.kind {
	case confSeries:
		dec.series = &confSeriesResp{}
		err = unmarshal(dec.series)
	case confTable:
		dec.table = &confTableResp{}
		err = unmarshal(dec.table)
	case confPoint:
		dec.point = &confPointResp{}
		err = unmarshal(dec.point)
	case confTagValues:
		dec.tags = &confTagValuesResp{}
		err = unmarshal(dec.tags)
	}
	return dec, body, status, err
}

// runConformanceDifferential drives the whole request set against both apis.
// Per request: poll until the CH reference is non-empty (the historic conveyor
// lands data ~24s after the writes; bounded by conformanceTimeout) and both
// decoded answers agree under the tolerances. Agreement passes immediately. A
// divergence does NOT wait out the full timeout — both stores already hold
// their data, so a disagreement will not self-heal; it is confirmed after
// confDivergenceSamples consecutive polls (absorbs transient per-api cache
// staleness) and then FAILs loudly with both raw responses recorded to the
// artifacts (failed-queries.json). Returns pass/fail counts.
func runConformanceDifferential(ctx context.Context, rec *recorder, chAPI, duckAPI string, reqs []confRequest) (passed, failed int) {
	for _, req := range reqs {
		var (
			diffs       []string
			refBody     string
			gotBody     string
			lastStatus  int
			refSeen     bool // the reference carried data at least once
			stableDiffs int  // consecutive polls with non-empty reference AND disagreement
		)
		deadline := time.Now().Add(conformanceTimeout)
		done := false
		for !done {
			if cerr := ctx.Err(); cerr != nil {
				// The run is tearing down (deadline/signal) — not a divergence.
				return passed, failed
			}
			ref, rb, _, rerr := fetchConf(ctx, chAPI, req)
			refBody = rb
			if rerr == nil && confNonEmpty(req.kind, ref) {
				refSeen = true
				got, gb, status, gerr := fetchConf(ctx, duckAPI, req)
				gotBody, lastStatus = gb, status
				switch {
				case gerr != nil:
					diffs = []string{fmt.Sprintf("duck query error: %v", gerr)}
					stableDiffs = 0
				default:
					diffs = compareConfRequest(req, ref, got)
					if len(diffs) == 0 {
						done = true // agreement
						break
					}
					stableDiffs++
					if stableDiffs >= confDivergenceSamples {
						done = true // confirmed divergence
					}
				}
			} else if rerr != nil {
				diffs = []string{fmt.Sprintf("reference query error: %v", rerr)}
				stableDiffs = 0
			} else {
				diffs = []string{"reference response carries no data yet (conveyor has not landed it)"}
				stableDiffs = 0
			}
			if !done {
				if !time.Now().Add(conformancePollInterval).Before(deadline) {
					done = true // landing window exhausted
				} else {
					select {
					case <-ctx.Done():
						return passed, failed
					case <-time.After(conformancePollInterval):
					}
				}
			}
		}
		if len(diffs) == 0 {
			passed++
			rec.logf("PASS conformance %s", req.label)
			fmt.Printf("PASS conformance %s\n", req.label)
			continue
		}
		failed++
		rec.recordFailedQuery(failedQuery{
			Label:      "conformance",
			Client:     conformanceClientTag,
			Func:       req.label,
			URL:        "http://" + duckAPI + req.path,
			HTTPStatus: lastStatus,
			Body:       "REFERENCE (clickhouse):\n" + refBody + "\n\nUNDER TEST (duck):\n" + gotBody,
		})
		detail := strings.Join(diffs, "\n")
		if !refSeen {
			detail += "\n(the ClickHouse reference never became non-empty — see its own PASS lines above)"
		}
		rec.logf("FAIL conformance %s\n%s", req.label, detail)
		fmt.Printf("FAIL conformance %s\n%s\n", req.label, indent(detail))
	}
	return passed, failed
}

// --- live: in-process seeding -------------------------------------------

// newConformanceClient builds the in-process statshouse-go client pointed at
// one agent. MaxBucketSize 1<<18 mirrors the go driver template: the default
// 1024 reservoir would silently sample the big-unique bucket client-side.
func newConformanceClient(agentAddr string) *statshouse.Client {
	return statshouse.NewClientEx(statshouse.ConfigureArgs{
		StatsHouseAddr: agentAddr,
		Network:        "tcp",
		MaxBucketSize:  1 << 18,
	})
}

// confNamedTags renders generated tags as the client's NamedTags, empty values
// verbatim (exercising the client's own empty-drop on the wire, as the driver
// template does).
func confNamedTags(tags []tag) statshouse.NamedTags {
	out := make(statshouse.NamedTags, 0, len(tags))
	for _, t := range tags {
		out = append(out, [2]string{t.Key, t.Val})
	}
	return out
}

// confSeedMetric sends one metric's cold-start seed with the kind-matching
// write (so auto-create derives the right kind), exactly as the driver
// template dispatches streamSeeds.
func confSeedMetric(cl *statshouse.Client, s seedDef, ts uint32) {
	switch s.Kind {
	case kindValue:
		cl.NamedValueHistoric(s.Name, nil, 1, ts)
	case kindUnique:
		cl.NamedUniqueHistoric(s.Name, nil, 1, ts)
	default:
		cl.NamedCountHistoric(s.Name, nil, 1, ts)
	}
}

// confWrite replays ONE generated write onto one client with the driver
// template's exact dispatch (including the 2ms/50ms burst pacing between
// large-payload writes). Values for value_p/unique are regenerated from the
// deterministic genSpec, so both clients see byte-identical payloads.
func confWrite(cl *statshouse.Client, w metricWrite) error {
	tags := confNamedTags(w.Tags)
	switch w.Kind {
	case kindCounter, kindStag:
		cl.NamedCountHistoric(w.Metric, tags, w.Count, w.TS)
	case kindValue:
		cl.NamedValuesHistoric(w.Metric, tags, w.Values, w.TS)
	case kindValueNaN:
		cl.NamedValuesHistoric(w.Metric, tags, []float64{math.NaN()}, w.TS)
	case kindValueInf:
		cl.NamedValuesHistoric(w.Metric, tags, []float64{math.Inf(1)}, w.TS)
	case kindValueP:
		var v []float64
		switch w.Gen.Kind {
		case genKindValueUniform:
			v = genValueUniform(w.Gen.N)
		case genKindValueSkewed:
			v = genValueSkewed(w.Gen.N)
		default:
			return fmt.Errorf("value_p write with unknown generator %q", w.Gen.Kind)
		}
		cl.NamedValuesHistoric(w.Metric, tags, v, w.TS)
		time.Sleep(2 * time.Millisecond)
	case kindUnique:
		var u []int64
		switch w.Gen.Kind {
		case genKindUniqueDistinct:
			u = genUniqueDistinct(w.Gen.N)
		case genKindUniqueDedup:
			u = make([]int64, w.Gen.N*2)
			for i := range u {
				u[i] = int64(i%w.Gen.N) + 1
			}
		default:
			return fmt.Errorf("unique write with unknown generator %q", w.Gen.Kind)
		}
		cl.NamedUniquesHistoric(w.Metric, tags, u, w.TS)
		if w.Gen.Kind == genKindUniqueDistinct {
			time.Sleep(50 * time.Millisecond)
		}
	default:
		return fmt.Errorf("unhandled write kind %q", w.Kind)
	}
	return nil
}

// metricsListNames is the minimal /api/metrics-list reply shape the pre-warm
// poll needs (same slice as the driver template's metricsListReply).
type metricsListNames struct {
	Data struct {
		Metrics []struct {
			Name string `json:"name"`
		} `json:"metrics"`
	} `json:"data"`
}

// waitConformanceMetrics blocks until /api/metrics-list (full=1) reports every
// name — the driver template's pre-warm poll — so the agent's mapping cache is
// subscribed before the real (counted) writes begin.
func waitConformanceMetrics(ctx context.Context, apiAddr string, names []string) error {
	return poll(ctx, 60*time.Second, 500*time.Millisecond, func() (bool, error) {
		body, status, err := httpGet(ctx, "http://"+apiAddr+"/api/metrics-list?full=1")
		if err != nil || status != 200 {
			return false, nil
		}
		var r metricsListNames
		if jerr := json.Unmarshal([]byte(body), &r); jerr != nil {
			return false, nil
		}
		have := make(map[string]bool, len(r.Data.Metrics))
		for _, m := range r.Data.Metrics {
			have[m.Name] = true
		}
		for _, n := range names {
			if !have[n] {
				return false, nil
			}
		}
		return true, nil
	})
}

// seedConformanceStream feeds the identical stream to both agents in-process:
// cold-start seeds → metrics-list poll on BOTH apis → 2s mapping settle → the
// paced writes (each write to both clients, deterministic generators shared) →
// Close flush on both → 1s settle. The sequence mirrors drivers/go/main.go.tmpl
// step for step; the only difference is that one process holds both clients,
// which is what makes _h (and thus max_host) identical across backends.
func seedConformanceStream(ctx context.Context, rec *recorder, stream metricStream, apiAddrs, agentAddrs [2]string) error {
	if len(agentAddrs) != 2 || len(apiAddrs) != 2 {
		return fmt.Errorf("conformance seeding needs exactly 2 agents and 2 apis")
	}
	ch := newConformanceClient(agentAddrs[0])
	dk := newConformanceClient(agentAddrs[1])

	seeds, names := streamSeeds(stream)
	seedTS := stream.Base - conformanceSeedLead
	for _, s := range seeds {
		confSeedMetric(ch, s, seedTS)
		confSeedMetric(dk, s, seedTS)
	}
	rec.logf("conformance: seeded %d metric(s) to both agents (seed ts=%d)", len(seeds), seedTS)

	for i, api := range apiAddrs {
		if err := waitConformanceMetrics(ctx, api, names); err != nil {
			return fmt.Errorf("seeded metrics never appeared via api %s: %w", api, err)
		}
		rec.logf("conformance: all %d metric(s) visible via api %d (%s)", len(names), i+1, api)
	}
	time.Sleep(2 * time.Second) // mapping-cache settle (driver parity)

	for i, w := range stream.Writes {
		if err := confWrite(ch, w); err != nil {
			return fmt.Errorf("write %d (%s/%s) to ch agent: %w", i, w.Metric, w.Kind, err)
		}
		if err := confWrite(dk, w); err != nil {
			return fmt.Errorf("write %d (%s/%s) to duck agent: %w", i, w.Metric, w.Kind, err)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	rec.logf("conformance: replayed %d write(s) to both agents", len(stream.Writes))

	for name, cl := range map[string]*statshouse.Client{"ch": ch, "duck": dk} {
		if err := cl.Close(); err != nil {
			// statshouse-go v0.5.17's TCP transport splits the (single-element)
			// address list into a primary and a secondary connection, leaving the
			// secondary's pool empty; its idle send loop then exits Close with
			// errEmptyAddr even though the connected primary flushed cleanly —
			// every healthy single-address client reproduces this (pinned by
			// TestStatshouseGoSingleAddrCloseQuirk). tcpPoolConn.Close returns
			// the PRIMARY's error first when it is non-nil, so a genuinely failed
			// flush still surfaces here; only the structural secondary noise is
			// skipped. (The go driver template is unaffected: it builds against
			// the pinned fork in e2e/clients.txt, whose single-connection Close
			// reports only real errors.)
			if err.Error() != statshouseGoEmptyAddrErr {
				return fmt.Errorf("%s client close: %w", name, err)
			}
			rec.logf("conformance: %s client closed with the known v0.5.17 single-address quirk (empty secondary pool); primary flushed cleanly", name)
		}
	}
	time.Sleep(time.Second) // drain settle (driver parity)
	return nil
}

// --- live: phase driver --------------------------------------------------

// conformancePhaseOpts wires runConformancePhase from realMain.
type conformancePhaseOpts struct {
	runID     string
	chAPI     string // published/host-reachable CH-stack api
	duckAPI   string // duck-stack api container <ip>:port
	chAgent   string // CH-stack agent <ip>:13337
	duckAgent string
}

// runConformancePhase is the --conformance replacement for the client phase:
// generate the stream, pre-create the value_p metrics via the CH api (one
// shared metadata serves both stacks), seed both agents in-process, verify the
// CH reference still matches the frozen model (a broken reference must abort
// the differential rather than bless a coincidence), then run the differential
// request set. Returns pass/fail counts.
func runConformancePhase(ctx context.Context, rec *recorder, o conformancePhaseOpts) (passed, failed int) {
	stream := generateStream(o.runID, conformanceClientTag, time.Now())

	if err := createValuePMetrics(ctx, rec, o.chAPI, stream); err != nil {
		rec.logf("FAIL conformance: pre-create value_p metrics: %v", err)
		fmt.Printf("FAIL conformance pre-create value_p metrics: %v\n", err)
		return 0, 1
	}

	if err := seedConformanceStream(ctx, rec, stream, [2]string{o.chAPI, o.duckAPI}, [2]string{o.chAgent, o.duckAgent}); err != nil {
		rec.logf("FAIL conformance: seed stream: %v", err)
		fmt.Printf("FAIL conformance seed stream: %v\n", err)
		return 0, 1
	}

	// Reference gate: ClickHouse must match the frozen model before its answers
	// are trusted as the reference.
	refPass, refFail := assertStream(ctx, rec, o.chAPI, conformanceClientTag, stream)
	if refFail > 0 {
		rec.logf("FAIL conformance: the ClickHouse reference does not match the expected model (%d assertion(s) failed) — aborting the differential; the reference itself is broken", refFail)
		fmt.Printf("FAIL conformance reference gate: %d CH assertion(s) failed; differential aborted\n", refFail)
		return refPass, refFail
	}

	reqs := buildConformanceRequests(stream)
	rec.logf("conformance: comparing %d semantic request(s) between clickhouse (reference) and duck", len(reqs))
	diffPass, diffFail := runConformanceDifferential(ctx, rec, o.chAPI, o.duckAPI, reqs)
	return refPass + diffPass, diffFail
}

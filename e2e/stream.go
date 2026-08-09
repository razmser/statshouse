package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// numBuckets is the number of consecutive 1-second buckets every series fills
// (spec §5: "70 consecutive 1s buckets"). Each series writes all 70, so the
// expected model has a non-zero count for every bucket and a missing/null point
// back from /api/query is unambiguously a failure — EXCEPT for the big-unique
// stress metric, which populates a subset (see bigUniqueBuckets) for the wire/
// memory budget; its asserter checks only the buckets it actually wrote.
const numBuckets = 70

// bigUniqueBuckets is how many 1s buckets the ~100k-distinct unique metric
// fills. 100k×70×3 clients ≈ 168 MB on the wire and through agent→agg→CH; a
// subset keeps the run fast while still forcing the ChUnique thinning estimator
// (>65536 distinct) every populated bucket. A documented, scoped deviation from
// the "all 70 buckets" default — see DEVIATIONS in the ticket-11 report.
const bigUniqueBuckets = 10

// bigUniqueDistinct is the distinct-value count for the approximate-unique
// case: >65536 forces ChUnique's power-of-2 thinning (the only path whose
// ±2% band is meaningful); the 1σ relative error there is ≈0.45%, so ±2% is
// ~4σ (very safe).
const bigUniqueDistinct = 100000

// smallUniqueDistinct is the exact-unique case distinct count (with each value
// emitted twice to exercise within-bucket dedup). Well inside the 65536 exact
// region → the asserter compares equality, not a band.
const smallUniqueDistinct = 300

// valuePBucketPoints is the per-bucket sample count for value_p (spec §5: "a
// few thousand points per bucket"). Enough for the t-digest to place centroids
// well, small enough to compile/run fast across all three clients.
const valuePBucketPoints = 2000

// fullKeyCap is the rendered full-key (metric name + tags) ceiling (spec §5).
// The generator keeps every key well under it; the assertion path logs the max
// so a future generator change that blows the budget is visible. Stricter than
// the clients' own caps (cpp 1024 B, rust 4096 B) so none silently drops a
// series. NOTE: tag STRING values are also capped at MaxStringLen=128 by the
// receiver (format.go); the stag generator stays under it.
const fullKeyCap = 768

// Metric kinds the generator emits. kindCounter/kindValue/kindUnique auto-create
// from a kind-matching seed (autocreate.go: counter default, unique if IsSetUnique,
// value if IsSetValue). kindValueP NEVER auto-creates, so the harness pre-creates
// it via POST /api/metric before the client runs (metric_create.go). kindStag is a
// counter metric whose assertion is cardinality (qw=cardinality, no group-by).
//
// kindValueNaN/kindValueInf (ticket 12) are REJECTED value payloads — a NaN and a
// +Inf — every write of which the pipeline rejects. They auto-create as VALUE
// metrics from a kind-matching (valid, value=1) seed, then the real writes are
// rejected and asserted via __src_ingestion_status (status 23 / 61). They are
// rendered by dedicated template branches because NaN/+Inf are not valid float
// literals in any of the three driver languages (a plain {{printf "%v"}} would
// emit "NaN"/"+Inf", which none of go/rust/cpp compile).
const (
	kindCounter  = "counter"
	kindValue    = "value"
	kindValueP   = "value_p"
	kindUnique   = "unique"
	kindStag     = "stag"
	kindValueNaN = "value_nan" // rejected value payload: NaN  → status 23 err_nan_inf_value
	kindValueInf = "value_inf" // rejected value payload: +Inf → status 61 err_too_big_value
)

// --- ticket 12: rejection cases + conservation ledger ------------------------
//
// __src_ingestion_status (builtin metric ID -11, internal/format/builtin_metrics:
// BuiltinMetricMetaIngestionStatus) is the per-event accounting record the agent
// writes for every metric observation: status=ok_cached (10) when accepted, or one
// err_* status when rejected. Its tags are positional: tag1=metric (the referenced
// metric NAME), tag2=status (rendered as the numeric VALUE ID via format.CodeTagValue
// — e.g. " 10"=ok_cached, " 23"=err_nan_inf_value; the human name lives only in the
// builtin's ValueComments and is NOT in the query reply), tag3=tag_id, tag4=component.
// Grouping a query by tag1+tag2 (qb=1&qb=2) yields one series per (metric, status).
//
// Source-of-truth status values (internal/format/builtin_tags.go +
// internal/format/format.go ValidateCounter/ValidateValue):
//
//	zero counter   → ValidateMetricData: counter==0 && no value/unique → 62 err_zero_counter
//	negative count → ValidateCounter: f < 0                               → 25 err_negative_counter
//	NaN value      → ValidateValue: IsNaN                                  → 23 err_nan_inf_value
//	+Inf value     → ValidateValue: +Inf > MaxFloat32                      → 61 err_too_big_value
//
// NOTE on the spec's "23/24" claim: spec §5 / ticket 12 say a +Inf VALUE yields
// status 24. The source disagrees — ValidateValue maps +Inf to 61 (too_big_value),
// and 24 (err_nan_inf_COUNTER) only fires for a NaN COUNTER via ValidateCounter,
// which no spec-matrix input exercises. Per the ticket's own instruction ("verify
// against … source of truth") the harness asserts the source-true status 61 and
// documents the deviation; see DEVIATIONS in the ticket-12 report.
const (
	ingestionStatusMetric = "__src_ingestion_status"

	// statusName* are the human DISPLAY names for the __src_ingestion_status tag2
	// value IDs (mirroring the builtin's ValueComments in internal/format). The query
	// API does NOT render these names — it renders the numeric ID (CodeTagValue, e.g.
	// " 23") — so the assertions work in numeric IDs throughout (classifyIngestionSeries
	// parses the ID, matched against rejectionMetric.StatusID); these name constants
	// exist only to label PASS/FAIL lines readably. Classification: ok_cached =
	// accepted, err_* = rejected (a loss), warn_* = warning (accepted, NOT a loss).
	statusNameOKCached    = "ok_cached"
	statusNameZeroCounter = "err_zero_counter"     // 62
	statusNameNegCounter  = "err_negative_counter" // 25
	statusNameNanInfValue = "err_nan_inf_value"    // 23
	statusNameTooBigValue = "err_too_big_value"    // 61 (NOT 24 — see note above)

	// statusID* mirror the numeric IDs the query API renders (CodeTagValue); the
	// assertions key off these IDs (what the API actually returns), not the names.
	statusIDZeroCounter = 62
	statusIDNegCounter  = 25
	statusIDNanInfValue = 23
	statusIDTooBigValue = 61

	// clientTag* are the per-client metric-name prefixes / --client selectors,
	// matching the tags in clientDrivers (main.go). addRejections branches on them
	// to model client-side rejection behavior (go/rust drop non-positive counts;
	// cpp sends them).
	goClientTag   = "go"
	rustClientTag = "rust"
	cppClientTag  = "cpp"
)

// tag is one positional StatsHouse tag: Key is the tag index ("0".."47"), Val its
// value. The go client's NamedTags ([2]string pairs) and the receiver both treat
// the key verbatim, so positional keys round-trip without a metric mapping.
type tag struct {
	Key string
	Val string
}

// metricWrite is one observation injected into a client driver template. Kind
// selects how the driver renders it (write_count / write_values / write_uniques,
// or a deterministic loop for the large value_p/unique payloads). Tags is the
// RAW tag set — empty values are kept here on purpose so the template emits
// them and the client's own empty-value handling is exercised; the expected
// model applies the same normalization. Count is always > 0 for counter/stag
// (zero/negative rejection is ticket 12).
type metricWrite struct {
	Kind    string
	Metric  string
	Tags    []tag
	TS      uint32
	Count   float64   // counter / stag
	Values  []float64 // value (literal payload); value_p uses Gen
	Uniques []int64   // unique small (literal); big unique uses Gen
	Gen     *genSpec  // value_p / unique loop descriptor; nil → literal payload
}

// genSpec describes a deterministic generator loop the driver emits in-place
// (keeping the rendered source small for multi-thousand-point payloads) and the
// harness replicates byte-for-byte to build the expected model. The "pinned
// seed" principle is preserved: a deterministic formula is pinned, not a
// per-language RNG. Kind is one of genValueUniform/genValueSkewed/
// genUniqueDistinct/genUniqueDedup; N is the item count.
type genSpec struct {
	Kind string
	N    int
}

// genKind* are the genSpec.Kind discriminator strings (the STRING VALUES the
// driver templates match on with {{if eq .Gen.Kind "valueUniform"}}, so do not
// change them). They are named genKind* (not genValueUniform etc.) to avoid
// colliding with the generator FUNCTIONS of those names in quantile.go, which
// the harness calls directly (expectedValues/expectedUnique). The harness and
// every driver template render the EXACT same formula for each — see quantile.go
// for the Go reference and drivers/{go,rust,cpp}/main.*.tmpl for the loop bodies.
const (
	genKindValueUniform   = "valueUniform"   // 0..N-1 (sorted; spec "0–999 step 1")
	genKindValueSkewed    = "valueSkewed"    // shared-LCG r²/1000, mass near 0
	genKindUniqueDistinct = "uniqueDistinct" // 1..N, distinct=N
	genKindUniqueDedup    = "uniqueDedup"    // 1..N each emitted twice, distinct=N
)

// seriesModel is one (metric, normalized-tag-set) series in the expected model.
// Tags is normalized exactly as the wire sees it: empty-valued tags dropped, the
// client's _h host tag excluded (it is added by the client, not generated). Only
// the map matching the metric's kind is populated; the asserter reads the right
// one. Each map is keyed by absolute bucket timestamp and fully populated for
// every bucket the series wrote (numBuckets, or bigUniqueBuckets for the stress
// case).
type seriesModel struct {
	Tags    []tag
	Counts  map[uint32]float64   // counter / stag: expected count
	Values  map[uint32][]float64 // value / value_p: merged values (sorted for value_p)
	Uniques map[uint32]int       // unique: expected distinct count
}

// metricModel is the expected model for one metric: Kind drives the asserter
// (count / sum-min-max-avg / percentile / unique / cardinality); QBKeys are the
// positional tag keys the harness groups by when querying it back (qb=…; empty
// for stag, which queries cardinality with no group-by); Series are the expected
// per-tag-set series.
type metricModel struct {
	Name   string
	Kind   string
	QBKeys []string
	Series []seriesModel
}

// metricStream is the single generated stream: Base anchors the buckets, Writes
// is injected into every client driver (the "pinned seed"), Metrics is the
// shared expected model the VISIBLE assertions compare against, and Rejections
// (ticket 12) are the rejection-case metrics — every write rejected by the
// pipeline, asserted via __src_ingestion_status + the conservation ledger, never
// via visible output (so they stay out of Metrics and thus out of assertStream).
type metricStream struct {
	Base       uint32
	Writes     []metricWrite
	Metrics    []metricModel
	Rejections []rejectionMetric
}

// rejectionMetric is one metric whose every write the pipeline REJECTS (Sent==true),
// or — for go/rust's counter cases — every write the CLIENT drops before the wire
// (Sent==false). It has no visible output either way. A Sent==true rejection is
// asserted two ways: (1) the exact __src_ingestion_status status (StatusName) appears
// with count == Writes, and (2) the conservation ledger balances for it
// (sentWrites == 0 ok_cached + Writes rejected). Sent==false marks a case the client
// drops CLIENT-SIDE — the go client only sends a counter when countToSend > 0
// (client_bucket.go) and rust rejects count <= 0 inside write_count (lib.rs), so a
// zero/negative count never reaches the agent and no server status is recorded: the
// case has no writes (Writes==0) and is asserted as an explicit SKIP (assertRejections
// prints SkipReason), documenting the drop rather than letting it pass silently.
type rejectionMetric struct {
	Name       string // e2e_<runID>_<client>_c_zero, …
	Kind       string // kindCounter / kindValueNaN / kindValueInf (driver render + seed kind)
	StatusName string // expected __src_ingestion_status name (err_zero_counter, …)
	StatusID   int32  // numeric status (62/25/23/61) — diagnostics only
	Writes     int    // sentWrites: #writes the harness generated for this metric
	Sent       bool   // false ⇒ client drops client-side; ledger/status skip
	SkipReason string // documented reason when Sent==false
}

// generateStream builds the full spec §5 stream once. The same Writes slice is
// rendered into the driver template, and Metrics is derived from the same
// construction, so there is exactly one source of truth (no per-language RNG).
//
// runID prefixes every metric name for isolation; clientTag ("go"/"rust"/"cpp")
// is folded into the prefix too, so the three clients — all driven against the
// same single agent/stack in one run — write disjoint metric names and their
// per-bucket values cannot collide (a shared name would merge every client's
// data and break the exact-match assertions). now is passed in (not read inside)
// so Base is deterministic for a given invocation.
//
// The runID is SANITIZED for the metric name (hyphens → underscores): the
// default runID is a datetime "20060102-150405" and resource names
// (e2e-<runID>-clickhouse …) keep the hyphens, but a StatsHouse metric name
// must match validMetricName — ASCII letters, digits, and '_' only (format.go).
// The auto-create path tolerates a hyphen (so the counter metrics of tickets
// 09/10 happened to work), but POST /api/metric (the value_p pre-create path)
// runs RestoreCachedInfo → ValidMetricName and rejects it. Sanitizing the prefix
// makes every metric name valid for BOTH paths.
func generateStream(runID, clientTag string, now time.Time) metricStream {
	prefix := "e2e_" + strings.ReplaceAll(runID, "-", "_") + "_" + clientTag + "_"
	base := uint32(now.Unix()) - 120 // floor(now) − 120s (now is already second-granular)
	b := &streamBuilder{prefix: prefix, base: base}
	b.addCounters()
	b.addValue()
	b.addValueP()
	b.addUnique()
	b.addStag()
	b.addRejections(clientTag)
	if b.maxKey > fullKeyCap {
		panic(fmt.Sprintf("e2e: generated full key %d B exceeds cap %d B", b.maxKey, fullKeyCap))
	}
	return metricStream{Base: base, Writes: b.writes, Metrics: b.metrics, Rejections: b.rejections}
}

// streamBuilder accumulates writes + the expected model and tracks the max
// rendered key length for the fullKeyCap guard.
type streamBuilder struct {
	prefix     string
	base       uint32
	writes     []metricWrite
	metrics    []metricModel
	rejections []rejectionMetric
	maxKey     int
}

// counterSeriesSpec is one counter series: raw tags + a per-bucket count fn.
type counterSeriesSpec struct {
	tags  []tag
	count func(bucket int) float64
}

// addCounterMetric is the shared builder for the counter-kind metrics (counter
// and stag both write counts; stag differs only in how it is asserted). Each
// series fills all numBuckets; empty tag values are kept raw so the client's
// empty-drop is exercised, and dropped again in the expected model.
func (b *streamBuilder) addCounterMetric(suffix, kind string, qb []string, series []counterSeriesSpec) {
	name := b.prefix + suffix
	m := metricModel{Name: name, Kind: kind, QBKeys: qb}
	seenKeys := map[string]bool{}
	for _, ss := range series {
		counts := make(map[uint32]float64, numBuckets)
		for i := 0; i < numBuckets; i++ {
			ts := b.base + uint32(i)
			c := ss.count(i)
			counts[ts] = c
			b.writes = append(b.writes, metricWrite{Kind: kind, Metric: name, Tags: ss.tags, Count: c, TS: ts})
		}
		nt := normalizeTags(ss.tags)
		if n := fullKeyLen(name, ss.tags); n > b.maxKey {
			b.maxKey = n
		}
		for _, t := range nt {
			seenKeys[t.Key] = true
		}
		m.Series = append(m.Series, seriesModel{Tags: nt, Counts: counts})
	}
	if len(qb) == 0 {
		m.QBKeys = sortedKeys(seenKeys) // group-by every tag the series carry
	}
	b.metrics = append(b.metrics, m)
}

// addCounters builds the counter subset (spec §5): the original six counter
// metrics (kept as-is) plus a formal tag-matrix metric that systematically
// covers 0–6 tags, value pools, unicode, and an empty tag value.
func (b *streamBuilder) addCounters() {
	b.addCounterMetric("c_ones", kindCounter, nil, []counterSeriesSpec{
		{tags: nil, count: func(int) float64 { return 1 }}, // no tags, count 1
	})
	b.addCounterMetric("c_tagged", kindCounter, nil, []counterSeriesSpec{
		{tags: []tag{{"0", "alpha"}, {"1", "beta"}}, count: func(int) float64 { return 1 }},
	})
	b.addCounterMetric("c_multi", kindCounter, nil, []counterSeriesSpec{
		// Four series over tag keys {0,1} with small value pools → exercises the
		// API group-by splitting one metric into several series (qb=0&qb=1).
		{tags: []tag{{"0", "x"}, {"1", "p"}}, count: off(2)},
		{tags: []tag{{"0", "x"}, {"1", "q"}}, count: off(10)},
		{tags: []tag{{"0", "y"}, {"1", "p"}}, count: off(20)},
		{tags: []tag{{"0", "y"}, {"1", "q"}}, count: off(30)},
	})
	b.addCounterMetric("c_empty", kindCounter, nil, []counterSeriesSpec{
		// Tag 1 has an empty value: the client drops it, so the series arrives as
		// {0:"val"}; the expected model normalizes the same way.
		{tags: []tag{{"0", "val"}, {"1", ""}}, count: func(int) float64 { return 3 }},
	})
	b.addCounterMetric("c_unicode", kindCounter, nil, []counterSeriesSpec{
		// Unicode tag values round-trip as UTF-8; per-bucket alternation 1/>1.
		{tags: []tag{{"0", "東京"}, {"1", "café"}}, count: alt(1, 5)},
	})
	b.addCounterMetric("c_many", kindCounter, nil, []counterSeriesSpec{
		// Six tags (the spec's 0–6 range maxed out) so auto-create provisions a
		// mapping with enough tag slots and qb covers indices 0..5.
		{tags: []tag{{"0", "a"}, {"1", "b"}, {"2", "c"}, {"3", "d"}, {"4", "e"}, {"5", "f"}}, count: alt(1, 7)},
	})
	b.addCounterMetric("c_matrix", kindCounter, nil, []counterSeriesSpec{
		// Formal tag matrix (spec §5): tag-set cardinality 1..6, a value pool of
		// two at index 0, a unicode pair, and an empty tag value. Each series
		// carries a distinct per-bucket count so a group-by error can't stay
		// hidden; group-by covers indices 0..5 (fewer-tag series surface with
		// their higher tags empty-dropped, so signatures stay distinct).
		//
		// Cardinality 0 (a tagless series) is covered by c_ones, NOT here: a
		// tagless series mixed with tagged series in ONE metric, asserted under
		// group-by, is silently dropped by the api's series resolution (the
		// all-empty-stag group is omitted when the metric's user tags are
		// unmapped — the auto-create steady state — even though the rows are
		// stored correctly in ClickHouse). That is an upstream api quirk, not
		// harness behavior, so the matrix exercises cardinalities 1..6 only.
		{tags: []tag{{"0", "m0"}}, count: off(110)},
		{tags: []tag{{"0", "m0"}, {"1", "m1"}}, count: off(120)},
		{tags: []tag{{"0", "m0"}, {"1", "m1"}, {"2", "m2"}}, count: off(130)},
		{tags: []tag{{"0", "m0"}, {"1", "m1"}, {"2", "m2"}, {"3", "m3"}}, count: off(140)},
		{tags: []tag{{"0", "m0"}, {"1", "m1"}, {"2", "m2"}, {"3", "m3"}, {"4", "m4"}}, count: off(145)},
		{tags: []tag{{"0", "m0"}, {"1", "m1"}, {"2", "m2"}, {"3", "m3"}, {"4", "m4"}, {"5", "m5"}}, count: off(150)},
		{tags: []tag{{"0", "p_a"}}, count: off(160)},
		{tags: []tag{{"0", "p_b"}}, count: off(170)},
		{tags: []tag{{"0", "café"}, {"1", "東京"}}, count: off(180)},
		{tags: []tag{{"0", "x"}, {"1", ""}}, count: off(190)}, // empty tag → arrives as {0:"x"}
	})
}

// seedKind returns the wire-write kind a driver must use to SEED this metric
// during cold-start pre-warm so auto-create derives the right metric kind
// (value/unique auto-create only from a kind-matching seed). value_p is
// pre-created, so its seed kind is harmless; value matches its kind. The rejected
// value kinds (value_nan/value_inf) seed as a plain VALUE so auto-create derives
// a value metric — the seed itself is a valid value=1 write (the real, rejected
// writes come after pre-warm).
func seedKind(kind string) string {
	switch kind {
	case kindValue, kindValueP, kindValueNaN, kindValueInf:
		return kindValue
	case kindUnique:
		return kindUnique
	default: // counter, stag
		return kindCounter
	}
}

// seedDef is one metric's cold-start seed: the name and the wire-write KIND the
// driver must use so auto-create derives the right metric kind. It is injected
// into the driver templates alongside the metric-name list (the pre-warm poll
// needs only names; the seed DISPATCH needs the kind).
type seedDef struct {
	Name string
	Kind string
}

// streamSeeds returns the per-metric seeds and the parallel name list the driver
// templates consume. value/unique metrics seed with a kind-matching write so
// auto-create derives value/unique (not the counter default); value_p is
// pre-created by the harness so its seed (a value write) maps cleanly. A Sent==true
// rejection seeds with a VALID kind-matching write (count=1 / value=1) so auto-create
// provisions the metric; the seed itself is dropped on arrival (unmapped → status
// metric_not_found in __src_ingestion_status_no_shard, NOT in this metric's
// ledger), and the real rejected writes follow after pre-warm. A Sent==false
// rejection (go/rust counter drop) is NOT seeded: the client never sends it, so a
// seed would only orphan an empty metric with no writes to follow.
func streamSeeds(stream metricStream) (seeds []seedDef, names []string) {
	seeds = make([]seedDef, 0, len(stream.Metrics)+len(stream.Rejections))
	names = make([]string, 0, len(stream.Metrics)+len(stream.Rejections))
	for _, m := range stream.Metrics {
		seeds = append(seeds, seedDef{Name: m.Name, Kind: seedKind(m.Kind)})
		names = append(names, m.Name)
	}
	for _, r := range stream.Rejections {
		if !r.Sent {
			continue // client drops client-side before the wire; no real writes follow → no seed
		}
		seeds = append(seeds, seedDef{Name: r.Name, Kind: seedKind(r.Kind)})
		names = append(names, r.Name)
	}
	return seeds, names
}

// off returns a per-bucket count function: baseN+bucket (baseN ≥ 2 → always > 1,
// distinct per bucket). Used for the multi-series metrics so group-by
// correctness is verifiable bucket by bucket.
func off(baseN int) func(int) float64 {
	return func(bucket int) float64 { return float64(baseN + bucket) }
}

// alt returns a per-bucket count alternating between evenC (even buckets) and
// oddC (odd buckets), so a single series carries both count == 1 and count > 1
// across the buckets.
func alt(evenC, oddC float64) func(int) float64 {
	return func(bucket int) float64 {
		if bucket%2 == 0 {
			return evenC
		}
		return oddC
	}
}

// normalizeTags drops empty-valued tags, mirroring the go client's fillTag drop
// (client_packet.go) and the receiver-side normalization (receiver.go). The _h
// host tag is never generated, so it is not stripped here; assertions strip it
// from the API response separately. Order is preserved for stable
// rendering/debugging.
func normalizeTags(raw []tag) []tag {
	var out []tag
	for _, t := range raw {
		if t.Val == "" {
			continue
		}
		out = append(out, t)
	}
	return out
}

// fullKeyLen estimates the rendered full key length (metric name + tag keys +
// values) for the spec §5 cap check. It is an upper bound (ignores TL framing),
// which is what matters for the drop thresholds.
func fullKeyLen(metric string, tags []tag) int {
	n := len(metric)
	for _, t := range tags {
		n += len(t.Key) + len(t.Val)
	}
	return n
}

func sortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

package main

import "sort"

// This file adds the non-counter generators to streamBuilder (spec §5): value,
// value_p (percentile), unique (exact + approximate), and stag (cardinality).
// Each builds the same writes the driver renders AND the expected model the
// asserter compares against from one construction, so harness and drivers share
// one source of truth. Large multi-thousand-point payloads are emitted as
// deterministic generator loops (genSpec), not literals, to keep the rendered
// client source small enough to compile.

// addValue builds the value metric: two series, each writing a fixed set of
// floats per bucket chosen to exercise negatives, a small negative, a long
// decimal, and a large positive — so sum/min/max/avg are all non-trivial and
// avg is negative for one series. Below the go client's 1024-value reservoir
// cap, so sum/min/max/avg are EXACT (no client sampling); the agent defaults
// count to len(values), and avg = sum/len. The harness sums the values in write
// order (the same left-fold the agent's ValueSum uses) so the float64 result is
// bit-identical.
func (b *streamBuilder) addValue() {
	const suffix = "v_mix"
	name := b.prefix + suffix
	m := metricModel{Name: name, Kind: kindValue, QBKeys: []string{"0"}}
	series := []struct {
		tags   []tag
		values []float64
	}{
		{tags: []tag{{"0", "a"}}, values: []float64{-3.5, -0.01, 2.718281828459045, 100.0}},
		{tags: []tag{{"0", "b"}}, values: []float64{-99.9, 0.001, 0.125, 7.5}},
	}
	for _, ss := range series {
		vals := append([]float64(nil), ss.values...)
		values := make(map[uint32][]float64, numBuckets)
		for i := 0; i < numBuckets; i++ {
			ts := b.base + uint32(i)
			values[ts] = vals // same set every bucket; exact per-bucket check over all 70
			b.writes = append(b.writes, metricWrite{Kind: kindValue, Metric: name, Tags: ss.tags, Values: vals, TS: ts})
		}
		nt := normalizeTags(ss.tags)
		if n := fullKeyLen(name, ss.tags); n > b.maxKey {
			b.maxKey = n
		}
		m.Series = append(m.Series, seriesModel{Tags: nt, Values: values})
	}
	b.metrics = append(b.metrics, m)
}

// addValueP builds the value_p metric: TWO series (a uniform and a skewed
// distribution), each emitting valuePBucketPoints values per bucket via a
// generator loop. value_p never auto-creates, so the harness pre-creates the
// metric (metric_create.go) before the client runs. The expected per-bucket
// quantiles come from the SAME generator (quantile.go), sorted; the asserter
// compares the API's t-digest p50/p90/p99 to them within a tolerance.
func (b *streamBuilder) addValueP() {
	const suffix = "vp_mix"
	name := b.prefix + suffix
	m := metricModel{Name: name, Kind: kindValueP, QBKeys: []string{"0"}}
	series := []struct {
		tags []tag
		gen  genSpec
	}{
		{tags: []tag{{"0", "unif"}}, gen: genSpec{Kind: genKindValueUniform, N: valuePBucketPoints}},
		{tags: []tag{{"0", "skew"}}, gen: genSpec{Kind: genKindValueSkewed, N: valuePBucketPoints}},
	}
	for _, ss := range series {
		expected := expectedValues(ss.gen) // sorted reference the asserter quantifies
		values := make(map[uint32][]float64, numBuckets)
		gen := ss.gen // copy: the loop var address would be shared otherwise
		for i := 0; i < numBuckets; i++ {
			ts := b.base + uint32(i)
			values[ts] = expected
			b.writes = append(b.writes, metricWrite{Kind: kindValueP, Metric: name, Tags: ss.tags, Gen: &gen, TS: ts})
		}
		nt := normalizeTags(ss.tags)
		if n := fullKeyLen(name, ss.tags); n > b.maxKey {
			b.maxKey = n
		}
		m.Series = append(m.Series, seriesModel{Tags: nt, Values: values})
	}
	b.metrics = append(b.metrics, m)
}

// addUnique builds the two unique metrics: an EXACT case (small distinct count,
// each value emitted twice to exercise within-bucket dedup) and an APPROXIMATE
// case (>65536 distinct, forcing the ChUnique thinning estimator). Both
// auto-create from a unique seed. The exact case fills all 70 buckets and is
// compared by equality; the approximate case fills only bigUniqueBuckets (wire/
// memory budget) and is compared within ±2%.
func (b *streamBuilder) addUnique() {
	// Exact: ≤65536 distinct → ChUnique.Size is exact.
	b.addUniqueMetric("u_exact", []tag{{"0", "small"}}, genSpec{Kind: genKindUniqueDedup, N: smallUniqueDistinct}, numBuckets)
	// Approximate: >65536 distinct → thinning estimator, ±2% band, fewer buckets.
	b.addUniqueMetric("u_approx", []tag{{"0", "big"}}, genSpec{Kind: genKindUniqueDistinct, N: bigUniqueDistinct}, bigUniqueBuckets)
}

// addUniqueMetric builds one unique metric: one series writing the generator's
// distinct set into each of nBuckets buckets. The asserter infers exact-vs-±2%
// from the distinct count (≤65536 → equality, else the thinning band).
func (b *streamBuilder) addUniqueMetric(suffix string, tags []tag, gen genSpec, nBuckets int) {
	name := b.prefix + suffix
	distinct := expectedUnique(gen)
	m := metricModel{Name: name, Kind: kindUnique, QBKeys: []string{"0"}}
	uniques := make(map[uint32]int, nBuckets)
	for i := 0; i < nBuckets; i++ {
		ts := b.base + uint32(i)
		uniques[ts] = distinct
		g := gen
		b.writes = append(b.writes, metricWrite{Kind: kindUnique, Metric: name, Tags: tags, Gen: &g, TS: ts})
	}
	nt := normalizeTags(tags)
	if n := fullKeyLen(name, tags); n > b.maxKey {
		b.maxKey = n
	}
	m.Series = append(m.Series, seriesModel{Tags: nt, Uniques: uniques})
	b.metrics = append(b.metrics, m)
}

// addStag builds the stag metric: a COUNTER metric (auto-creates) whose series
// differ only in one string tag value (including empty, unicode, and a long
// value), asserted via qw=cardinality with NO group-by — cardinality counts
// distinct (metric, tag-set) rows (sum(1)), independent of the metric kind.
// Each distinct tag value is one series; the empty value maps to the "absent"
// tag (Tags[i]=0) and is still a distinct row since no other series omits that
// tag, so it counts. Expected cardinality per bucket = number of series.
func (b *streamBuilder) addStag() {
	const suffix = "s_dist"
	name := b.prefix + suffix
	// Distinct values: plain, unicode (valid UTF-8 → distinct series), empty
	// (→ absent-tag row), and a long string under the 128 B receiver cap. Long
	// but unique within the first 128 bytes so it never merges with another.
	longVal := "L" + repeatChar('x', 120)
	values := []string{"alpha", "beta", "café", "東京", "", longVal}
	m := metricModel{Name: name, Kind: kindStag}
	// Stag asserts cardinality (no group-by); QBKeys stays empty so metricQueryURL
	// omits qb and the API returns a single total series per bucket.
	for _, v := range values {
		tags := []tag{{"0", v}}
		counts := make(map[uint32]float64, numBuckets)
		for i := 0; i < numBuckets; i++ {
			ts := b.base + uint32(i)
			counts[ts] = 1
			b.writes = append(b.writes, metricWrite{Kind: kindStag, Metric: name, Tags: tags, Count: 1, TS: ts})
		}
		nt := normalizeTags(tags) // empty value dropped in the model's identity
		if n := fullKeyLen(name, tags); n > b.maxKey {
			b.maxKey = n
		}
		m.Series = append(m.Series, seriesModel{Tags: nt, Counts: counts})
	}
	b.metrics = append(b.metrics, m)
}

// expectedValues returns the SORTED expected value population for a value-kind
// generator — the reference the value_p asserter quantifies against the
// t-digest. valueUniform is already sorted; valueSkewed is sorted here (the LCG
// order is not).
func expectedValues(g genSpec) []float64 {
	switch g.Kind {
	case genKindValueUniform:
		return genValueUniform(g.N)
	case genKindValueSkewed:
		out := genValueSkewed(g.N)
		sort.Float64s(out)
		return out
	}
	return nil
}

// expectedUnique returns the distinct-value count a unique generator produces,
// used as the expected per-bucket unique. Both generators yield exactly N
// distinct (uniqueDistinct: 1..N; uniqueDedup: 1..N each twice).
func expectedUnique(g genSpec) int {
	switch g.Kind {
	case genKindUniqueDistinct, genKindUniqueDedup:
		return g.N
	}
	return 0
}

// repeatChar returns a string of n copies of c (used for the long stag value).
func repeatChar(c byte, n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = c
	}
	return string(buf)
}

// --- ticket 12: rejection cases (spec §5) ------------------------------------
//
// addRejections builds the four spec §5 rejection inputs as DEDICATED metrics so
// their rejections never bleed into a normal metric's expected model. Each write is
// rejected by the pipeline and asserted through __src_ingestion_status (the exact
// status) plus the conservation ledger — never via visible output.
//
// CLIENT-SIDE REJECTION is the crux of "conservation of input: nothing accepted
// silently disappears". The go client only sends a counter when countToSend > 0
// (statshouse-go/client_bucket.go: send's `if b.countToSend > 0` guard) and the
// rust client rejects count <= 0 inside write_count (statshouse-rs/lib.rs: `if
// count <= 0. { return false }`). So a ZERO or NEGATIVE count is dropped by BOTH
// before it ever reaches the agent — no server-side status is recorded, and there
// is nothing for the ledger to account for (the input never entered the pipeline;
// the drop is an explicit client decision, not a silent loss). The cpp client
// (statshouse-cpp: write_count_impl has no such guard) sends count <= 0, so the
// server rejects it (62 zero_counter / 25 negative_counter). We therefore generate
// the counter-rejection metrics as REAL writes for cpp (the server records 62/25);
// for go/rust it records the same two cases as Sent==false SKIP entries — no writes,
// since the input never reached the agent — so assertRejections prints an explicit
// SKIP line per case instead of the cases being invisible ("rejections 2/2" with no
// trace of what a cpp run would exercise). The value cases (NaN / +Inf) are sent by
// all three clients — none validates value payloads — so they are generated as real
// writes for every client.
func (b *streamBuilder) addRejections(clientTag string) {
	switch clientTag {
	case cppClientTag:
		// cpp sends count<=0 (no client-side guard) → the server rejects it
		// (ValidateMetricData→62 / ValidateCounter→25). Real writes, Sent==true.
		b.addRejectionCounter("c_zero", 0, statusNameZeroCounter, statusIDZeroCounter)
		b.addRejectionCounter("c_neg", -5, statusNameNegCounter, statusIDNegCounter)
	default:
		// go/rust DROP count<=0 before the wire (see the note above): no packet
		// reaches the agent, so there is no server status and nothing for the ledger
		// to account. Record the case Sent==false so it surfaces as a documented SKIP
		// rather than silently disappearing. No writes are appended.
		b.addRejectionCounterSkipped("c_zero", statusNameZeroCounter, statusIDZeroCounter)
		b.addRejectionCounterSkipped("c_neg", statusNameNegCounter, statusIDNegCounter)
	}
	// Value rejections are sent by all three clients (none validates value payloads).
	b.addRejectionValue("v_nan", kindValueNaN, statusNameNanInfValue, statusIDNanInfValue)
	b.addRejectionValue("v_inf", kindValueInf, statusNameTooBigValue, statusIDTooBigValue)
}

// addRejectionCounter builds one counter metric whose every write carries an
// invalid count (zero or negative) that the server rejects (ValidateMetricData →
// 62 / ValidateCounter → 25). The writes use the plain counter kind, so the
// existing driver counter branch renders them. cpp sends count <= 0 (no client-side
// guard), so addRejections calls this only for cpp: every entry is Sent==true and
// the ledger accounts its numBuckets writes as rejected.
func (b *streamBuilder) addRejectionCounter(suffix string, count float64, statusName string, statusID int32) {
	name := b.prefix + suffix
	for i := 0; i < numBuckets; i++ {
		ts := b.base + uint32(i)
		b.writes = append(b.writes, metricWrite{Kind: kindCounter, Metric: name, Count: count, TS: ts})
	}
	if n := len(name); n > b.maxKey {
		b.maxKey = n
	}
	b.rejections = append(b.rejections, rejectionMetric{
		Name:       name,
		Kind:       kindCounter,
		StatusName: statusName,
		StatusID:   statusID,
		Writes:     numBuckets,
		Sent:       true,
	})
}

// addRejectionCounterSkipped records a counter-rejection case the client DROPS
// client-side (go/rust refuse count<=0 before the wire — see addRejections). Unlike
// addRejectionCounter it appends NO writes: the input never reaches the agent, so
// there is no server status and nothing for the conservation ledger to balance. It
// marks the rejection Sent==false with a SkipReason so assertRejections renders an
// explicit SKIP line — a documented client-side drop, not a silent disappearance.
func (b *streamBuilder) addRejectionCounterSkipped(suffix, statusName string, statusID int32) {
	name := b.prefix + suffix
	b.rejections = append(b.rejections, rejectionMetric{
		Name:       name,
		Kind:       kindCounter,
		StatusName: statusName,
		StatusID:   statusID,
		Writes:     0,
		Sent:       false,
		SkipReason: "client drops count<=0 before the wire",
	})
}

// addRejectionValue builds one value metric whose every write carries a single
// invalid float (NaN or +Inf) that the server rejects (ValidateValue → 23 / 61).
// NaN/+Inf are not valid float literals in any driver language, so the write uses
// a dedicated kind (kindValueNaN/kindValueInf) rendered by a dedicated template
// branch (math.NaN()/f64::NAN/NAN, …). The metric auto-creates as a VALUE metric
// from its valid value=1 seed.
func (b *streamBuilder) addRejectionValue(suffix, kind, statusName string, statusID int32) {
	name := b.prefix + suffix
	for i := 0; i < numBuckets; i++ {
		ts := b.base + uint32(i)
		b.writes = append(b.writes, metricWrite{Kind: kind, Metric: name, TS: ts})
	}
	if n := len(name); n > b.maxKey {
		b.maxKey = n
	}
	b.rejections = append(b.rejections, rejectionMetric{
		Name:       name,
		Kind:       kind,
		StatusName: statusName,
		StatusID:   statusID,
		Writes:     numBuckets,
		Sent:       true,
	})
}

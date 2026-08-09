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

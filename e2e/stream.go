package main

import (
	"fmt"
	"sort"
	"time"
)

// numBuckets is the number of consecutive 1-second buckets every series fills
// (spec §5: "70 consecutive 1s buckets"). Each series writes all 70, so the
// expected model has a non-zero count for every bucket and a missing/null point
// back from /api/query is unambiguously a failure.
const numBuckets = 70

// fullKeyCap is the rendered full-key (metric name + tags) ceiling (spec §5). The
// generator keeps every key well under it; the assertion path logs the max so a
// future generator change that blows the budget is visible. Stricter than the
// clients' own caps (cpp 1024 B, rust 4096 B) so none silently drops a series.
const fullKeyCap = 768

// tag is one positional StatsHouse tag: Key is the tag index ("0".."47"), Val its
// value. The go client's NamedTags ([2]string pairs) and the receiver both treat
// the key verbatim, so positional keys round-trip without a metric mapping.
type tag struct {
	Key string
	Val string
}

// counterWrite is one counter observation injected into a client driver template.
// Tags is the RAW tag set — empty values are kept here on purpose so the template
// emits them and the client's own empty-value-drop (fillTag, client_packet.go) is
// exercised; the expected model applies the same drop when normalizing. Count is
// always > 0 (zero/negative rejection is ticket 12).
type counterWrite struct {
	Metric string
	Tags   []tag
	Count  float64
	TS     uint32
}

// counterSeries is one (metric, normalized-tag-set) series in the expected model.
// Tags is normalized exactly as the wire sees it: empty-valued tags dropped, the
// client's _h host tag excluded (it is added by the client, not generated). Counts
// is keyed by absolute bucket timestamp and is fully populated (numBuckets entries).
type counterSeries struct {
	Tags   []tag
	Counts map[uint32]float64
}

// counterMetric is the expected model for one metric: QBKeys are the positional tag
// keys the harness groups by when querying it back (qb=…; the API collapses every
// series into one row when qb is empty, so the keys must be exactly the tags the
// series carry), and Series are the expected per-tag-set series.
type counterMetric struct {
	Name   string
	QBKeys []string
	Series []counterSeries
}

// counterStream is the single generated stream: Base anchors the 70 buckets, Writes
// is injected into every client driver (the "pinned seed"), and Metrics is the
// shared expected model the assertions compare against.
type counterStream struct {
	Base    uint32
	Writes  []counterWrite
	Metrics []counterMetric
}

// generateCounterStream builds the counter subset of spec §5 once. The same Writes
// slice is rendered into the driver template, and Metrics is derived from the same
// construction, so there is exactly one source of truth (no per-language RNG).
//
// runID prefixes every metric name (e2e_<runID>_…) for isolation; now is passed in
// (not read inside) so Base is deterministic for a given invocation and the harness
// can log it before the clients run.
func generateCounterStream(runID string, now time.Time) counterStream {
	prefix := "e2e_" + runID + "_"
	base := uint32(now.Unix()) - 120 // floor(now) − 120s (now is already second-granular)

	// One builder per metric. Each series carries raw tags (empties kept for the
	// client to drop) and a per-bucket count function. base+n keeps every count > 1
	// and distinct per (series, bucket) so a group-by mistake can't stay hidden; the
	// "ones" metrics and the even-bucket branches exercise count == 1.
	type seriesSpec struct {
		tags  []tag
		count func(bucket int) float64
	}
	metricSpecs := []struct {
		suffix string
		series []seriesSpec
	}{
		{
			suffix: "c_ones",
			series: []seriesSpec{
				{tags: nil, count: func(int) float64 { return 1 }}, // no tags, count 1
			},
		},
		{
			suffix: "c_tagged",
			series: []seriesSpec{
				{tags: []tag{{"0", "alpha"}, {"1", "beta"}}, count: func(int) float64 { return 1 }},
			},
		},
		{
			// Four series over tag keys {0,1} with small value pools → exercises the
			// API group-by splitting one metric into several series (qb=0&qb=1).
			suffix: "c_multi",
			series: []seriesSpec{
				{tags: []tag{{"0", "x"}, {"1", "p"}}, count: off(2)},
				{tags: []tag{{"0", "x"}, {"1", "q"}}, count: off(10)},
				{tags: []tag{{"0", "y"}, {"1", "p"}}, count: off(20)},
				{tags: []tag{{"0", "y"}, {"1", "q"}}, count: off(30)},
			},
		},
		{
			// Tag 1 has an empty value: the client drops it (fillTag), so the series
			// arrives as {0:"val"}. The expected model normalizes the same way and
			// the query groups by qb=0 only.
			suffix: "c_empty",
			series: []seriesSpec{
				{tags: []tag{{"0", "val"}, {"1", ""}}, count: func(int) float64 { return 3 }},
			},
		},
		{
			// Unicode tag values round-trip as UTF-8; per-bucket alternation 1/>1.
			suffix: "c_unicode",
			series: []seriesSpec{
				{tags: []tag{{"0", "東京"}, {"1", "café"}}, count: alt(1, 5)},
			},
		},
		{
			// Six tags (the spec's 0–6 range maxed out) so auto-create provisions a
			// metric mapping with enough tag slots and qb covers indices 0..5.
			suffix: "c_many",
			series: []seriesSpec{
				{tags: []tag{{"0", "a"}, {"1", "b"}, {"2", "c"}, {"3", "d"}, {"4", "e"}, {"5", "f"}}, count: alt(1, 7)},
			},
		},
	}

	var (
		writes  []counterWrite
		metrics []counterMetric
		maxKey  int
	)
	for _, ms := range metricSpecs {
		name := prefix + ms.suffix
		m := counterMetric{Name: name}
		seenKeys := map[string]bool{}
		for _, ss := range ms.series {
			counts := make(map[uint32]float64, numBuckets)
			for i := 0; i < numBuckets; i++ {
				ts := base + uint32(i)
				c := ss.count(i)
				counts[ts] = c
				writes = append(writes, counterWrite{Metric: name, Tags: ss.tags, Count: c, TS: ts})
			}
			nt := normalizeTags(ss.tags)
			if n := fullKeyLen(name, ss.tags); n > maxKey {
				maxKey = n
			}
			for _, t := range nt {
				seenKeys[t.Key] = true
			}
			m.Series = append(m.Series, counterSeries{Tags: nt, Counts: counts})
		}
		m.QBKeys = sortedKeys(seenKeys)
		metrics = append(metrics, m)
	}
	if maxKey > fullKeyCap {
		// Defensive: the generator is hand-tuned to stay well under the cap. A panic
		// here means a future edit pushed a series over the clients' drop threshold.
		panic(fmt.Sprintf("e2e: generated full key %d B exceeds cap %d B", maxKey, fullKeyCap))
	}
	return counterStream{Base: base, Writes: writes, Metrics: metrics}
}

// off returns a per-bucket count function: baseN+bucket (baseN ≥ 2 → always > 1,
// distinct per bucket). Used for the multi-series metric so group-by correctness is
// verifiable bucket by bucket.
func off(baseN int) func(int) float64 {
	return func(bucket int) float64 { return float64(baseN + bucket) }
}

// alt returns a per-bucket count alternating between evenC (even buckets) and oddC
// (odd buckets), so a single series carries both count == 1 and count > 1 across
// the 70 buckets.
func alt(evenC, oddC float64) func(int) float64 {
	return func(bucket int) float64 {
		if bucket%2 == 0 {
			return evenC
		}
		return oddC
	}
}

// normalizeTags drops empty-valued tags, mirroring the go client's fillTag drop
// (client_packet.go) and the receiver-side normalization (receiver.go). The _h host
// tag is never generated, so it is not stripped here; assertions strip it from the
// API response separately. Order is preserved for stable rendering/debugging.
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

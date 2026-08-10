package main

import (
	"math"
	"sort"
)

// This file holds the PURE logic shared between the harness's expected model
// and the deterministic generator loops emitted into every client driver
// (a "pinned seed": one deterministic formula, not per-language RNG):
//
//   - quantile: the TRUE quantile the value_p percentile assertions compare the
//     t-digest's output against, using the SAME linear-interpolation definition
//     (NumPy "linear" / R type 7) the t-digest itself approximates.
//   - genValueUniform / genValueSkewed / genUniqueDistinct: the exact value and
//     unique sequences the driver loops reproduce. They are bit-identical across
//     Go (harness), Rust, and C++ because they use only unsigned-64 wrapping
//     integer arithmetic and a single float multiply/divide.
//   - withinAbsTol / withinRelTol: the tolerance-band checks the percentile and
//     unique assertions use.
//
// Everything here is deterministic and side-effect-free → unit-tested
// (quantile_test.go).

// lcgMul / lcgAdd are the Knuth MMIX LCG constants (also PCG's multiplier). The
// skewed-distribution generator advances a uint64 state with `x = x*lcgMul +
// lcgAdd` (mod 2^64). Go wraps uint64 implicitly; the templates render the
// exact same constants as wrapping_mul/wrapping_add (Rust) and ULL overflow
// (C++, which is defined to wrap for unsigned). Keep these in sync with the
// generator blocks in drivers/{go,rust,cpp}/main.*.tmpl.
const (
	lcgMul  uint64 = 6364136223846793005
	lcgAdd  uint64 = 1442695040888963407
	lcgSeed uint64 = 0x9e3779b97f4a7c15 // a fixed, nonzero starting state (golden ratio splinter)
)

// skewedRange is the modulus for the skewed value's raw residue; the emitted
// value is r*r/skewedScale, so values land in [0, (skewedRange-1)²/skewedScale].
const (
	skewedRange = 1000
	skewedScale = 1000.0
)

// quantile returns the q-quantile (0≤q≤1) of an ALREADY SORTED slice using
// linear interpolation between the two bracketing order statistics — NumPy's
// default "linear" method, R type 7, and the definition the t-digest
// approximates (hrissan/tdigest Quantile does weighted-linear interpolation
// between adjacent centroid means). An empty input yields NaN so a caller bug
// surfaces rather than silently comparing against 0.
//
// The harness sorts the merged per-bucket values once at generation time and
// passes the sorted slice here; the assertions compare the API's t-digest
// result to this value within a tolerance (withinAbsTol).
func quantile(sorted []float64, q float64) float64 {
	n := len(sorted)
	if n == 0 {
		return math.NaN()
	}
	if n == 1 {
		return sorted[0]
	}
	switch {
	case q <= 0:
		return sorted[0]
	case q >= 1:
		return sorted[n-1]
	}
	pos := q * float64(n-1)
	lo := int(math.Floor(pos))
	if lo >= n-1 {
		return sorted[n-1]
	}
	frac := pos - float64(lo)
	return sorted[lo] + frac*(sorted[lo+1]-sorted[lo])
}

// quantileOf sorts a copy of values and returns its q-quantile. Use when the
// input is not already sorted (the skewed LCG sequence is not).
func quantileOf(values []float64, q float64) float64 {
	cp := append([]float64(nil), values...)
	sort.Float64s(cp)
	return quantile(cp, q)
}

// genValueUniform returns {0, 1, ..., n-1} — the "0–999 step 1" uniform
// distribution generalised to n points. Already sorted.
func genValueUniform(n int) []float64 {
	out := make([]float64, n)
	for i := 0; i < n; i++ {
		out[i] = float64(i)
	}
	return out
}

// genValueSkewed returns n values from a deterministic, skewed distribution
// built from the shared LCG. Each step: advance the LCG, take the top 32 bits
// mod skewedRange (0..999), emit r*r/skewedScale. The density is ∝ 1/√v, so
// mass concentrates near 0 (true p50≈¼·range, p99≈⅘·range²-ish) — clearly
// distinct from the uniform case, exercising the t-digest on a non-uniform
// population. The output is NOT sorted; quantileOf sorts a copy.
//
// Bit-identity across languages hinges on: uint64 wrapping mul/add, a `>>32`
// to uint32, `% skewedRange`, and `float64(r)*float64(r)/skewedScale` — all
// defined identically in Go/Rust/C++.
func genValueSkewed(n int) []float64 {
	out := make([]float64, n)
	x := lcgSeed
	for i := 0; i < n; i++ {
		x = x*lcgMul + lcgAdd
		r := uint32(x>>32) % skewedRange
		out[i] = float64(r) * float64(r) / skewedScale
	}
	return out
}

// genUniqueDistinct returns {1, 2, ..., n} — n distinct int64 values. Used by
// the big-unique case (>65536 distinct forces the ChUnique thinning estimator,
// exercising the approximate path) and, with repeats folded in by the caller,
// the small exact case.
func genUniqueDistinct(n int) []int64 {
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		out[i] = int64(i + 1)
	}
	return out
}

// withinAbsTol is the percentile tolerance band: an actual value is
// accepted when |actual-truth| ≤ max(absFrac·|truth|, minAbs). absFrac is the
// relative part (default 1%) and minAbs the absolute floor (default 1.0, so a
// near-zero true quantile still has a usable band). The t-digest's quantile
// error is bounded by ~1/compression in quantile space, well inside the 1% band
// (see percentileTol).
func withinAbsTol(actual, truth, absFrac, minAbs float64) bool {
	tol := math.Max(absFrac*math.Abs(truth), minAbs)
	return math.Abs(actual-truth) <= tol
}

// withinRelTol is the unique ±relative band: accepted when
// |actual-truth| ≤ rel·|truth|. Used for the big-unique approximate case at
// rel=0.02 (±2%), which is ~4σ for the ChUnique thinning estimator at 100k
// distinct (1σ≈0.45%). The exact small-unique case compares equality directly.
func withinRelTol(actual, truth, rel float64) bool {
	return math.Abs(actual-truth) <= rel*math.Abs(truth)
}

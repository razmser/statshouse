package main

import (
	"math"
	"reflect"
	"sort"
	"testing"
)

// TestQuantileUniform pins the type-7 linear-interpolation values the value_p
// assertions compare the t-digest against. For the uniform integers 0..999
// (the spec's "0–999 step 1" distribution) the true quantiles are exact and
// known: pos=q·(n-1), interpolated. Any change to quantile() that shifts these
// would silently change the assertion's "truth" and must surface here.
func TestQuantileUniform(t *testing.T) {
	xs := genValueUniform(1000) // 0..999, already sorted
	cases := []struct {
		q    float64
		want float64
	}{
		{0.50, 499.5},
		{0.90, 899.1},
		{0.99, 989.01},
		{0.00, 0},
		{1.00, 999},
	}
	for _, c := range cases {
		if got := quantile(xs, c.q); got != c.want {
			t.Errorf("quantile(0..999, %g) = %g, want %g", c.q, got, c.want)
		}
	}
}

// TestQuantileOfUnsorted confirms quantileOf sorts before interpolating: the
// skewed LCG sequence is not ordered, yet its median must equal the median of
// the same values sorted by hand.
func TestQuantileOfUnsorted(t *testing.T) {
	xs := []float64{3, 1, 4, 1, 5, 9, 2, 6}
	sorted := append([]float64(nil), xs...)
	sort.Float64s(sorted)
	if got := quantileOf(xs, 0.5); got != quantile(sorted, 0.5) {
		t.Errorf("quantileOf(unsorted,0.5)=%g != quantile(sorted,0.5)=%g", got, quantile(sorted, 0.5))
	}
}

// TestQuantileEdge covers degenerate inputs so the asserter never divides by
// zero or indexes out of range: empty → NaN (caller bug, not a silent 0),
// single → that value, q clamped to [0,1].
func TestQuantileEdge(t *testing.T) {
	if q := quantile(nil, 0.5); !math.IsNaN(q) {
		t.Errorf("quantile(empty) = %g, want NaN", q)
	}
	if q := quantile([]float64{42}, 0.5); q != 42 {
		t.Errorf("quantile({42}) = %g, want 42", q)
	}
	xs := genValueUniform(10)
	if quantile(xs, -1) != 0 || quantile(xs, 2) != 9 {
		t.Errorf("quantile clamp failed: q=-1→%g q=2→%g", quantile(xs, -1), quantile(xs, 2))
	}
}

// TestGenValueSkewedDeterministic pins the skewed generator's first few values.
// These EXACT bytes are what every client driver loop must reproduce: if the
// harness and a driver ever diverge (a constants/overflow typo), the value_p
// assertions compare against the wrong "truth" and a real t-digest error would
// be masked. The values here are the reference computed from the shared LCG.
func TestGenValueSkewedDeterministic(t *testing.T) {
	got := genValueSkewed(4)
	// Reproduce by hand from lcgSeed to lock the formula.
	x := lcgSeed
	var want []float64
	for i := 0; i < 4; i++ {
		x = x*lcgMul + lcgAdd
		r := uint32(x>>32) % skewedRange
		want = append(want, float64(r)*float64(r)/skewedScale)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("genValueSkewed(4)=%v, want %v", got, want)
	}
	// The skew is real: the median of a large sample is well below the midpoint
	// of the range (mass concentrates near 0).
	big := genValueSkewed(4000)
	med := quantileOf(big, 0.5)
	if med > 350 { // uniform midpoint would be ~ (999²/1000)/2 ≈ 499
		t.Errorf("skewed median %g too high — distribution not skewed toward 0", med)
	}
}

// TestGenValueUniformAndUnique pins the trivial generators' length and content.
func TestGenValueUniformAndUnique(t *testing.T) {
	u := genValueUniform(5)
	if !reflect.DeepEqual(u, []float64{0, 1, 2, 3, 4}) {
		t.Errorf("genValueUniform(5)=%v", u)
	}
	d := genUniqueDistinct(3)
	if !reflect.DeepEqual(d, []int64{1, 2, 3}) {
		t.Errorf("genUniqueDistinct(3)=%v", d)
	}
}

// TestWithinTol covers both tolerance bands the assertions depend on: the
// percentile absolute+relative band and the unique relative band, including the
// near-zero-truth floor that keeps a tiny true quantile usable.
func TestWithinTol(t *testing.T) {
	// Percentile: max(1%·|truth|, 1.0). truth=499.5 → tol 4.995.
	if !withinAbsTol(502, 499.5, 0.01, 1.0) {
		t.Error("502 should be within 1% of 499.5")
	}
	if withinAbsTol(510, 499.5, 0.01, 1.0) {
		t.Error("510 should be OUTSIDE 1% of 499.5")
	}
	// Near-zero truth: floor 1.0 binds, so ±1 is accepted.
	if !withinAbsTol(0.9, 0, 0.01, 1.0) {
		t.Error("0.9 should be within the 1.0 floor of truth 0")
	}
	// Unique ±2%: truth=100000 → tol 2000.
	if !withinRelTol(101500, 100000, 0.02) {
		t.Error("101500 should be within 2% of 100000")
	}
	if withinRelTol(105000, 100000, 0.02) {
		t.Error("105000 should be OUTSIDE 2% of 100000")
	}
}

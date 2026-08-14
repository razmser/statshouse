// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// Fixed seeds: the assertions are tolerance-banded statistical checks, and a
// deterministic input keeps them reproducible run to run.

// fillUnique feeds u n distinct values drawn from rng. Collisions of a PRNG
// stream over uint64 are negligible (<<1 expected even at 1.6M draws).
func fillUnique(u *ChUnique, n int, rng *rand.Rand) {
	for i := 0; i < n; i++ {
		u.Insert(rng.Uint64())
	}
}

// TestChUniqueMergeAboveMaxSize merges two halves of well over uniquesHashMaxSize
// (65536) distinct values and checks the merged estimate. Above that threshold
// shrinkIfNeed raises skipDegree mid-merge, so a source-screened Merge inserts
// items the target's filter should have dropped while Size() still extrapolates
// by 1 << skipDegree — measured +17% at 80K, +1289% at 400K before the fix.
func TestChUniqueMergeAboveMaxSize(t *testing.T) {
	for _, tt := range []struct {
		name string
		n    int
		seed int64
	}{
		{"80K", 80_000, 101},
		{"400K", 400_000, 102},
		{"800K", 800_000, 103},
		{"1.6M", 1_600_000, 104},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rng := rand.New(rand.NewSource(tt.seed))
			var lhs, rhs ChUnique
			fillUnique(&lhs, tt.n/2, rng)
			fillUnique(&rhs, tt.n-tt.n/2, rng)
			// Input sanity: each half alone must already estimate its own count.
			require.InDelta(t, float64(tt.n/2), float64(lhs.Size(false)), 0.02*float64(tt.n/2))
			require.InDelta(t, float64(tt.n-tt.n/2), float64(rhs.Size(false)), 0.02*float64(tt.n-tt.n/2))

			lhs.Merge(rhs)
			size := lhs.Size(false)
			require.InDelta(t,
				float64(tt.n), float64(size), 0.02*float64(tt.n),
				"merged Size() must estimate distinct count within ±2%%")
		})
	}
}

// TestChUniqueMergeOrderInvariance merges the same four parts in two different
// orders (first part as target vs last part as target) and requires both
// estimates to stay within tolerance of the true count and of each other.
func TestChUniqueMergeOrderInvariance(t *testing.T) {
	const (
		n     = 1_600_000
		parts = 4
	)
	rng := rand.New(rand.NewSource(105))
	var ps [parts]ChUnique
	for i := 0; i < n; i++ {
		ps[i%parts].Insert(rng.Uint64())
	}

	forward := ps[0]
	for i := 1; i < parts; i++ {
		forward.Merge(ps[i])
	}
	backward := ps[parts-1]
	for i := parts - 2; i >= 0; i-- {
		backward.Merge(ps[i])
	}

	sizeF := forward.Size(false)
	sizeB := backward.Size(false)
	require.InDelta(t, float64(n), float64(sizeF), 0.02*float64(n), "forward order must estimate within ±2%%")
	require.InDelta(t, float64(n), float64(sizeB), 0.02*float64(n), "backward order must estimate within ±2%%")
	require.InDelta(t, float64(sizeF), float64(sizeB), 0.02*float64(n),
		"the two merge orders must agree within tolerance")
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"bytes"
	"testing"

	"github.com/hrissan/tdigest"
	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

// pctState encodes one digest's values as a quantilesTDigest state blob, the
// way the insert path encodes the percentiles column.
func pctState(t *testing.T, values ...float64) []byte {
	t.Helper()
	td := tdigest.NewWithCompression(rowbinary.TDigestCompression)
	for _, v := range values {
		td.Add(v, 1)
	}
	return rowbinary.AppendCentroids(nil, td, 1)
}

// uniqState encodes values as a uniq state blob, the way the insert path
// encodes the uniq_state column.
func uniqState(t *testing.T, values ...uint64) []byte {
	t.Helper()
	var u data_model.ChUnique
	for _, v := range values {
		u.Insert(v)
	}
	return u.MarshallAppend(nil)
}

// decodePct is the fold result read back the way a query reply decodes it.
func decodePct(t *testing.T, b []byte) *tdigest.TDigest {
	t.Helper()
	td, err := decodeTDigestState(b)
	require.NoError(t, err)
	return td
}

// decodeUniq reads a uniq state blob back.
func decodeUniq(t *testing.T, b []byte) data_model.ChUnique {
	t.Helper()
	var u data_model.ChUnique
	require.NoError(t, u.ReadFrom(bytes.NewReader(b)))
	return u
}

// TestFoldDegenerateBlobs pins the identity cases: no blob and a lone blob
// fold to themselves, and empty blobs count as no state.
func TestFoldDegenerateBlobs(t *testing.T) {
	emptyPct := rowbinary.AppendEmptyCentroids(nil)
	emptyUniq := rowbinary.AppendEmptyUnique(nil)

	got, err := foldPercentiles(nil)
	require.NoError(t, err)
	require.Equal(t, emptyPct, got)
	got, err = foldUniques(nil)
	require.NoError(t, err)
	require.Equal(t, emptyUniq, got)

	one := pctState(t, 1, 2, 3)
	got, err = foldPercentiles([][]byte{one})
	require.NoError(t, err)
	require.Equal(t, one, got, "folding one state is that state")
	oneU := uniqState(t, 7, 8, 9)
	got, err = foldUniques([][]byte{oneU})
	require.NoError(t, err)
	require.Equal(t, oneU, got)

	// only-empty blobs fold to the empty state
	got, err = foldPercentiles([][]byte{emptyPct, emptyPct})
	require.NoError(t, err)
	require.Equal(t, emptyPct, got)
	got, err = foldUniques([][]byte{emptyUniq, emptyUniq})
	require.NoError(t, err)
	require.Equal(t, emptyUniq, got)
	got, err = foldPercentiles([][]byte{emptyPct, nil, one})
	require.NoError(t, err)
	require.Equal(t, one, got, "an empty blob among real ones contributes nothing")
}

// TestFoldPercentilesMatchesDirectMerge compares the blob fold with merging
// the same digests directly in Go, by decoded quantile — the comparison
// between merge orders is by value, never by bytes.
func TestFoldPercentilesMatchesDirectMerge(t *testing.T) {
	groups := [][][]float64{
		{{1, 2, 3}, {4, 5, 6}, {7, 8, 9, 10}},
		{{100, 200, 300}, {150, 250}, {}, {175}},
		{{0.5, 0.25}, {1, 2, 4, 8, 16, 32}, {-1, -2, -3}},
	}
	for _, values := range groups {
		states := make([][]byte, 0, len(values))
		direct := tdigest.NewWithCompression(rowbinary.TDigestCompression)
		for _, vs := range values {
			states = append(states, pctState(t, vs...))
			d := tdigest.NewWithCompression(rowbinary.TDigestCompression)
			for _, v := range vs {
				d.Add(v, 1)
			}
			direct.Merge(d)
		}
		folded, err := foldPercentiles(states)
		require.NoError(t, err)
		got := decodePct(t, folded)
		for _, q := range []float64{0.01, 0.25, 0.5, 0.75, 0.99} {
			require.InDelta(t, direct.Quantile(q), got.Quantile(q), 1e-6, "quantile %v", q)
		}
		require.InDelta(t, direct.Count(), got.Count(), 1e-9)
	}
}

// TestFoldUniquesMatchesDirectMerge compares the blob fold with a direct
// ChUnique merge of the same decoded states, by Size — exact below the
// thinning threshold, banded above it.
func TestFoldUniquesMatchesDirectMerge(t *testing.T) {
	// below 65,536 distinct: no thinning, so the fold is exact
	{
		var states [][]byte
		var direct data_model.ChUnique
		base := uint64(1000)
		for g := 0; g < 4; g++ {
			vals := make([]uint64, 5000)
			for i := range vals {
				vals[i] = base + uint64(g)*5000 + uint64(i)
			}
			states = append(states, uniqState(t, vals...))
			direct.Merge(decodeUniq(t, states[len(states)-1]))
		}
		folded, err := foldUniques(states)
		require.NoError(t, err)
		got := decodeUniq(t, folded)
		require.Equal(t, direct.Size(true), got.Size(true), "the fold is the set union of its inputs")
	}

	// above 65,536 distinct: thinning makes the estimate order-sensitive; the
	// fold must land within the tolerance the e2e suite applies to uniques
	{
		var states [][]byte
		var direct data_model.ChUnique
		base := uint64(1 << 20)
		for g := 0; g < 4; g++ {
			vals := make([]uint64, 60000)
			for i := range vals {
				vals[i] = base + uint64(g)*60000 + uint64(i)
			}
			states = append(states, uniqState(t, vals...))
			direct.Merge(decodeUniq(t, states[len(states)-1]))
		}
		folded, err := foldUniques(states)
		require.NoError(t, err)
		decoded := decodeUniq(t, folded)
		want, got := direct.Size(false), decoded.Size(false)
		require.InDelta(t, want, got, 0.02*float64(want), "folded %d vs direct %d", got, want)
	}
}

// TestFoldRejectsCorruptStates proves a malformed blob fails the fold rather
// than silently merging a prefix.
func TestFoldRejectsCorruptStates(t *testing.T) {
	good := pctState(t, 1, 2, 3)
	goodU := uniqState(t, 1, 2, 3)

	_, err := foldPercentiles([][]byte{good, []byte{5}}) // centroid count 5, no centroids
	require.Error(t, err)
	require.Contains(t, err.Error(), "tdigest state")
	_, err = foldPercentiles([][]byte{good, append(append([]byte(nil), good...), 0)}) // trailing byte
	require.Error(t, err)
	require.Contains(t, err.Error(), "trailing bytes")

	_, err = foldUniques([][]byte{goodU, []byte{1, 5, 0}}) // item count 5, no items
	require.Error(t, err)
}

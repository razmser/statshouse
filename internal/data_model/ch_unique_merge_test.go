// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package data_model

import (
	"bytes"
	"encoding/binary"
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

// requireRejectedKeepsState pins the rejection-without-mutation contract every
// malformed-blob test rests on: each blob's decode fails, and the accumulator
// — its marshalled state and its estimate — comes out exactly as it went in.
// The marshalled comparison is the codec's wire format, the bytes ClickHouse
// and duck-store store and exchange, not an internal representation.
func requireRejectedKeepsState(t *testing.T, target *ChUnique, what string, blobs [][]byte) {
	t.Helper()
	before := target.MarshallAppend(nil)
	beforeSize := target.Size(false)
	for i, blob := range blobs {
		require.Error(t, target.MergeRead(bytes.NewBuffer(blob)),
			"case %d: the %s must be rejected", i, what)
		require.Equal(t, before, target.MarshallAppend(nil),
			"case %d: the rejected %s must leave the accumulator byte-identical", i, what)
		require.Equal(t, beforeSize, target.Size(false),
			"case %d: the rejected %s must leave the estimate unchanged", i, what)
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

// TestChUniqueMergeReadSkipDegree merges the marshalled form of a set whose
// skipDegree was raised by shrinking into a target whose degree is still 0 —
// the direction every fold of stored uniq states takes when a large partial
// meets a small one. MergeRead must raise the target's degree exactly as
// Merge does; before the fix it rehashed at the old degree, so every item of
// the large blob entered the table while Size() still extrapolated by
// 1 << 0 — a 4x undercount at these sizes, and order-dependent (merging the
// other way around was correct).
func TestChUniqueMergeReadSkipDegree(t *testing.T) {
	const (
		bigN   = 200_000 // above uniquesHashMaxSize, so shrinkIfNeed raises the degree
		smallN = 1_000
		seed   = 106
	)
	rng := rand.New(rand.NewSource(seed))
	var big, small ChUnique
	fillUnique(&big, bigN, rng)
	fillUnique(&small, smallN, rng)
	require.GreaterOrEqual(t, big.skipDegree, uint32(1), "the big set must carry a raised skipDegree")
	require.Equal(t, uint32(0), small.skipDegree)

	bigBlob := big.MarshallAppend(nil)
	smallBlob := small.MarshallAppend(nil)
	n := bigN + smallN

	// the raising direction: small target, big blob
	var intoSmall ChUnique
	require.NoError(t, intoSmall.MergeRead(bytes.NewBuffer(smallBlob)))
	require.NoError(t, intoSmall.MergeRead(bytes.NewBuffer(bigBlob)))
	sizeSmallFirst := intoSmall.Size(false)

	// the already-correct direction: big target, small blob
	var intoBig ChUnique
	require.NoError(t, intoBig.MergeRead(bytes.NewBuffer(bigBlob)))
	require.NoError(t, intoBig.MergeRead(bytes.NewBuffer(smallBlob)))
	sizeBigFirst := intoBig.Size(false)

	require.InDelta(t, float64(n), float64(sizeSmallFirst), 0.02*float64(n),
		"merging a raised-degree blob into a degree-0 set must estimate within ±2%%")
	require.InDelta(t, float64(n), float64(sizeBigFirst), 0.02*float64(n),
		"the other merge order must estimate within ±2%%")
	require.InDelta(t, float64(sizeSmallFirst), float64(sizeBigFirst), 0.02*float64(n),
		"the two merge orders must agree within tolerance")
}

// TestChUniqueMergeReadMatchesMerge folds the same two states with MergeRead
// and with Merge and requires the same resulting estimate: the two merge
// entry points are two views of one operation, and a fold whose result
// depends on which one ran is wrong somewhere.
func TestChUniqueMergeReadMatchesMerge(t *testing.T) {
	const (
		bigN   = 200_000
		smallN = 1_000
	)
	rng := rand.New(rand.NewSource(107))
	var big, small ChUnique
	fillUnique(&big, bigN, rng)
	fillUnique(&small, smallN, rng)

	var viaRead ChUnique
	require.NoError(t, viaRead.MergeRead(bytes.NewBuffer(small.MarshallAppend(nil))))
	require.NoError(t, viaRead.MergeRead(bytes.NewBuffer(big.MarshallAppend(nil))))

	var viaMerge ChUnique
	viaMerge.Merge(small)
	viaMerge.Merge(big)

	require.Equal(t, viaMerge.skipDegree, viaRead.skipDegree,
		"both merge paths must settle on the same skipDegree")
	require.InDelta(t, float64(viaMerge.Size(false)), float64(viaRead.Size(false)), 0.02*float64(bigN),
		"MergeRead and Merge must estimate the same merged set within tolerance")
}

// TestChUniqueMergeReadMalformedHeaderKeepsState feeds MergeRead a blob whose
// header is malformed — truncated right after a skip-degree byte higher than
// the target's, or a bogus item count — and requires the error to leave the
// accumulator exactly as it was. The ingestion caller (MergeWithTL2) ignores
// this error, so mutating on the way to it would thin the live set to a
// coarser, noisier estimator with nothing logged; before the fix the degree
// was raised and the table rehashed before the item count was even read.
func TestChUniqueMergeReadMalformedHeaderKeepsState(t *testing.T) {
	const n = 1_000
	rng := rand.New(rand.NewSource(108))
	var target ChUnique
	fillUnique(&target, n, rng)
	require.Equal(t, uint32(0), target.skipDegree)

	// A valid blob for 200K items has skipDegree >= 2; reuse just its header
	// shapes against the degree-0 target.
	var big ChUnique
	fillUnique(&big, 200_000, rng)
	require.GreaterOrEqual(t, big.skipDegree, uint32(2))

	truncated := append([]byte{}, big.MarshallAppend(nil)[0]) // stop after the skip-degree byte
	tooManyItems := append(truncated, binary.AppendUvarint(nil, uniquesHashMaxSize+1)...)
	requireRejectedKeepsState(t, &target, "malformed header", [][]byte{truncated, tooManyItems})
}

// TestChUniqueMergeReadTruncatedPayloadKeepsState feeds MergeRead a blob whose
// header is valid but whose item payload is cut short, and requires the same
// untouched-accumulator contract as a malformed header. A mid-payload cut used
// to raise skipDegree and rehash before the item loop hit the missing bytes;
// a 1-3 byte tail is worse — bytes.Buffer.Read short-reads it (n < 4, no
// error), so the last item was silently padded from stale bytes and the merge
// reported success.
func TestChUniqueMergeReadTruncatedPayloadKeepsState(t *testing.T) {
	const n = 1_000
	rng := rand.New(rand.NewSource(109))
	var target ChUnique
	fillUnique(&target, n, rng)
	require.Equal(t, uint32(0), target.skipDegree)

	// A valid blob for 200K items carries skipDegree >= 2, so the merge path
	// under test is the one that raises the target's degree and rehashes.
	var big ChUnique
	fillUnique(&big, 200_000, rng)
	require.GreaterOrEqual(t, big.skipDegree, uint32(2))

	blob := big.MarshallAppend(nil)
	_, countLen := binary.Uvarint(blob[1:]) // header = skip-degree byte + uvarint count
	headerLen := 1 + countLen

	malformed := [][]byte{
		blob[:headerLen+4], // header plus one item of many
		blob[:len(blob)-1], // 1-byte tail: short-read, no error before the fix
		blob[:len(blob)-2], // 2-byte tail
		blob[:len(blob)-3], // 3-byte tail
	}
	requireRejectedKeepsState(t, &target, "truncated payload", malformed)
}

// TestChUniqueMergeReadImpossibleSkipDegreeKeepsState feeds MergeRead blobs
// whose skip degree no accumulator can reach — shrinkIfNeed cannot push the
// degree past uniquesHashMaxSkipDegree, because raising it to d requires over
// uniquesHashMaxSize distinct hashes good at degree d-1 — and requires the
// same untouched-accumulator contract as the other malformed blobs. The
// two-byte blob {0xff, 0x00} is the worst case: a zero-item payload passes
// every length check, the merge raises the target's degree to 255, and
// good() rejects every nonzero hash at that degree — before the fix the
// accumulator was silently erased and the merge still returned success.
func TestChUniqueMergeReadImpossibleSkipDegreeKeepsState(t *testing.T) {
	const n = 1_000
	rng := rand.New(rand.NewSource(110))
	var target ChUnique
	fillUnique(&target, n, rng)
	require.Equal(t, uint32(0), target.skipDegree)

	impossible := [][]byte{
		{0xff, 0x00}, // degree 255, zero items: erased with a success return
		{17, 0x00},   // one past the reachable maximum
		{32, 0x00},   // first degree at which no nonzero hash is good
	}
	// the same impossible degree with a complete item payload: every length
	// check passes, so the degree check is the only thing standing before the
	// wipe-by-rehash
	withItems := append([]byte{0xff}, binary.AppendUvarint(nil, 2)...)
	withItems = binary.LittleEndian.AppendUint32(withItems, 0x12345678)
	withItems = binary.LittleEndian.AppendUint32(withItems, 0x9abcdef0)
	impossible = append(impossible, withItems)

	requireRejectedKeepsState(t, &target, "impossible skip degree", impossible)

	// the first decode of a fresh accumulator goes through UmMarshall and must
	// reject the same impossible degrees without initializing the accumulator
	var fresh ChUnique
	require.Error(t, fresh.MergeRead(bytes.NewBuffer([]byte{0xff, 0x00})))
	require.Nil(t, fresh.buf, "the rejected first decode must not initialize the accumulator")

	// the reachable maximum itself stays accepted on both paths: a state at
	// degree uniquesHashMaxSkipDegree holding hashes good at that degree is a
	// legal, if thin, state
	atMax := append([]byte{byte(uniquesHashMaxSkipDegree)}, binary.AppendUvarint(nil, 2)...)
	atMax = binary.LittleEndian.AppendUint32(atMax, 0x00010000) // low 16 bits zero
	atMax = binary.LittleEndian.AppendUint32(atMax, 0x80000000)
	var decoded ChUnique
	require.NoError(t, decoded.MergeRead(bytes.NewBuffer(atMax)))
	require.Equal(t, uint64(2*(1<<uniquesHashMaxSkipDegree)), decoded.Size(true))
	require.NoError(t, target.MergeRead(bytes.NewBuffer(atMax)))
	require.Equal(t, uint32(uniquesHashMaxSkipDegree), target.skipDegree)
}

// TestChUniqueMergeReadUnencodableItemsKeepsState feeds MergeRead blobs whose
// headers pass every check this far — a reachable skip degree, a plausible
// count, a full payload — but whose items no encoder could have written: an
// empty payload at the maximum degree, a hash unaligned at the declared
// degree, a duplicated hash, or fewer distinct items than the count declares.
// Each would raise the target's skip degree and rehash away nearly all of its
// items before the item loop silently dropped the very hashes that justified
// the raise, erasing the accumulator with a success return; the payload must
// be rejected with the accumulator untouched.
func TestChUniqueMergeReadUnencodableItemsKeepsState(t *testing.T) {
	const n = 1_000
	rng := rand.New(rand.NewSource(111))
	var target ChUnique
	fillUnique(&target, n, rng)
	require.Equal(t, uint32(0), target.skipDegree)

	unencodable := [][]byte{
		// the maximum degree with an empty payload: the degree is reachable
		// and zero a plausible count, but a set only crosses to degree 16 by
		// thinning from over 65,536 items and at least one always survives
		// (see readUniquesHeader), so no encoder writes {max, 0}
		{uniquesHashMaxSkipDegree, 0x00},
		// degree 16 with one unaligned hash: every header check passes
		append(append([]byte{16}, binary.AppendUvarint(nil, 1)...),
			0x78, 0x56, 0x34, 0x12), // 0x12345678
		// the same shape at a mid degree
		append(append([]byte{2}, binary.AppendUvarint(nil, 2)...),
			0x04, 0x00, 0x00, 0x00, // 0x4: aligned at 2
			0x05, 0x00, 0x00, 0x00), // 0x5: not aligned at 2
		// two aligned copies of one hash at degree 16: alignment intact, the
		// count declares what no table can hold
		append(append([]byte{16}, binary.AppendUvarint(nil, 2)...),
			0x00, 0x00, 0x01, 0x00, // 0x00010000
			0x00, 0x00, 0x01, 0x00),
		// the zero item written twice and counted... zero counted once leaves
		// the declared count one past what the payload holds
		append(append([]byte{0}, binary.AppendUvarint(nil, 3)...),
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x01, 0x00, 0x00, 0x00),
		// a payload longer than the declared count is framing, the caller's to
		// check — but count and distinct must still agree for the items read
		append(append([]byte{0}, binary.AppendUvarint(nil, 2)...),
			0x01, 0x00, 0x00, 0x00,
			0x01, 0x00, 0x00, 0x00),
	}
	requireRejectedKeepsState(t, &target, "unencodable items", unencodable)

	// the fresh decode through UmMarshall rejects the same blobs without
	// initializing the accumulator
	for i, blob := range unencodable {
		var fresh ChUnique
		require.Error(t, fresh.MergeRead(bytes.NewBuffer(blob)),
			"fresh case %d: the unencodable items must be rejected", i)
		require.Nil(t, fresh.buf, "fresh case %d: the rejected first decode must not initialize the accumulator", i)
	}

	// the encodable neighbours stay accepted: a zero item written twice with
	// the count counting it once decodes to a consistent set (the zero item
	// collapses), and the spill past the stack array of the dedup — nine
	// distinct hashes — round-trips through both decode paths
	twoZeros := append([]byte{0}, binary.AppendUvarint(nil, 1)...)
	twoZeros = binary.LittleEndian.AppendUint32(twoZeros, 0)
	twoZeros = binary.LittleEndian.AppendUint32(twoZeros, 0)
	var twoZerosDecoded ChUnique
	require.NoError(t, twoZerosDecoded.MergeRead(bytes.NewBuffer(twoZeros)))
	require.True(t, twoZerosDecoded.hasZeroItem)
	require.Equal(t, int32(1), twoZerosDecoded.itemsCount)

	var spill ChUnique
	for i := 0; i < 9; i++ {
		spill.Insert(uint64(i + 1))
	}
	spillBlob := spill.MarshallAppend(nil)
	var spillDecoded ChUnique
	require.NoError(t, spillDecoded.MergeRead(bytes.NewBuffer(spillBlob)))
	require.Equal(t, spill.itemsCount, spillDecoded.itemsCount)
	require.NoError(t, target.MergeRead(bytes.NewBuffer(spillBlob)))
}

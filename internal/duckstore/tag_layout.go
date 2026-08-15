// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"slices"

	"github.com/VKCOM/statshouse/internal/format"
)

// The tag-layout kinds of statshouse.storeTagLayout, and the one derivation of
// them both sides of the store query RPC must agree on. The API derives the
// request's layout from its journal with TagLayoutKinds; the aggregator
// derives its own from its journal the same way and refuses the query when the
// two disagree, rather than reinterpret stored rows.
const (
	// tagKindMapped marks tagN as holding a mapping id, with stagN holding
	// the unmapped string when the id is zero.
	tagKindMapped = int32(0)
	// tagKindRaw32 marks tagN as holding the value directly; stagN is unused.
	tagKindRaw32 = int32(1)
	// tagKindRaw64 marks the value as split: the low half in tagN, the high
	// half in tagN+1.
	tagKindRaw64 = int32(2)
)

// TagLayoutKinds derives the canonical storeTagLayout kinds of a metric from
// its journal entity: one entry per defined tag, raw64 → TagKindRaw64, raw →
// TagKindRaw32, otherwise TagKindMapped. A metric with a string top pads with
// mapped entries through the string top's own slot, because that column exists
// for every row and a query may address it.
func TagLayoutKinds(metric *format.MetricMetaValue) []int32 {
	if metric == nil {
		return nil
	}
	kinds := make([]int32, len(metric.Tags))
	for i := range metric.Tags {
		switch {
		case metric.Tags[i].Raw64():
			kinds[i] = tagKindRaw64
		case metric.Tags[i].Raw():
			kinds[i] = tagKindRaw32
		default:
			kinds[i] = tagKindMapped
		}
	}
	if metric.StringTopName != "" || metric.StringTopDescription != "" {
		if n := int(format.StringTopTagIndexV3) + 1; len(kinds) < n && n <= format.MaxTags {
			kinds = append(kinds, make([]int32, n-len(kinds))...)
		}
	}
	return kinds
}

// TagLayoutsEqual reports whether two tag layouts interpret every column the
// same way — the exact comparison the aggregator's journal validation makes.
func TagLayoutsEqual(a, b []int32) bool {
	return slices.Equal(a, b)
}

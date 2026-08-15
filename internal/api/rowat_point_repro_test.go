// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the CPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package api

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/require"
)

// TestRowAtPointMappedTagRepro pins the crash the e2e conformance suite found
// in the live stack (run 20260815-154542): /api/point grouping by a MAPPED tag
// registers the tag column through writeSelectInt's Int32 arm (the plain
// column), leaving dataInt64 nil; rowAtPoint read dataInt64 unguarded and
// panicked ("index out of range [0] with length 0"), killing the api process
// and every request after it. rowAt has always guarded both arms; rowAtPoint
// must too.
func TestRowAtPointMappedTagRepro(t *testing.T) {
	var c seriesQuery
	c.tag = append(c.tag, &tagCol{dataInt32: proto.ColInt32{7}, tagX: 2})
	row := c.rowAtPoint(0)
	require.Equal(t, int64(7), row.tag[2])
}

// TestRowAtPointStringTagRepro pins the second point-mode bug the conformance
// suite exposed (run 20260815-155805): v6 stores a string tag's value in the
// stagN column with tagN=0, and rowAt copies BOTH columns — but rowAtPoint
// copied only tag, so every /api/point query grouped by a string tag rendered
// it through the mapped fallback as CodeTagValue(0) (" 0") instead of the
// actual string. The stag columns must flow into the point row the same way.
func TestRowAtPointStringTagRepro(t *testing.T) {
	var c seriesQuery
	var stag stagCol
	stag.tagX = 3
	stag.data.Append("alpha")
	c.stag = append(c.stag, &stag)
	row := c.rowAtPoint(0)
	require.Equal(t, "alpha", row.stag[3])
}

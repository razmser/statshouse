// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/hrissan/tdigest"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

// The Go fold of the two aggregate-state columns — the half of compaction SQL
// cannot do. DuckDB can neither merge ClickHouse's aggregate states nor re-import
// them, so wherever rows are folded (compaction here, query replies later) the
// blob lists the SQL GROUP BY produces are merged in Go with the same codecs
// the write path and the API already use. The fold happens before the group is
// written, never lazily.
//
// Both folds are pure functions over state bytes and run in the untagged
// build; only their callers touch DuckDB.

// foldPercentiles merges the quantilesTDigest state blobs of one collapsed
// group into a single state blob, encoding the merged digest at sampling
// factor 1 (the partial rows' states already carry their own factors).
func foldPercentiles(blobs [][]byte) ([]byte, error) {
	switch len(blobs) {
	case 0:
		return rowbinary.AppendEmptyCentroids(nil), nil
	case 1:
		// folding one state is that state
		return append([]byte(nil), blobs[0]...), nil
	}
	var merged *tdigest.TDigest
	for _, b := range blobs {
		td, err := DecodeTDigestState(b)
		if err != nil {
			return nil, err
		}
		if td == nil {
			continue // no state
		}
		if merged == nil {
			merged = td
		} else {
			merged.Merge(td)
		}
	}
	if merged == nil {
		return rowbinary.AppendEmptyCentroids(nil), nil
	}
	return rowbinary.AppendCentroids(nil, merged, 1), nil
}

// foldUniques merges the uniq state blobs of one collapsed group into a single
// state blob.
func foldUniques(blobs [][]byte) ([]byte, error) {
	switch len(blobs) {
	case 0:
		return rowbinary.AppendEmptyUnique(nil), nil
	case 1:
		return append([]byte(nil), blobs[0]...), nil
	}
	var u data_model.ChUnique
	for _, b := range blobs {
		if len(b) == 0 {
			continue // no state
		}
		buf := bytes.NewBuffer(b)
		if err := u.MergeRead(buf); err != nil {
			return nil, err
		}
		if buf.Len() != 0 {
			return nil, fmt.Errorf("duck-store: unique state has %d trailing bytes", buf.Len())
		}
	}
	return u.MarshallAppend(nil), nil
}

// DecodeTDigestState decodes one quantilesTDigest state blob into a digest —
// the same wire format ColTDigest decodes: a uvarint centroid count, then per
// centroid a float32 LE mean and a float32 LE weight. nil (without error)
// means the blob holds no state. Exported because the API's duck query source
// decodes the very same blobs out of store query responses.
func DecodeTDigestState(b []byte) (*tdigest.TDigest, error) {
	if len(b) == 0 {
		return nil, nil
	}
	r := bytes.NewReader(b)
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, fmt.Errorf("duck-store: tdigest state: %w", err)
	}
	td := tdigest.NewWithCompression(rowbinary.TDigestCompression)
	var bs [8]byte
	for j := uint64(0); j < n; j++ {
		if _, err := io.ReadFull(r, bs[:]); err != nil {
			return nil, fmt.Errorf("duck-store: tdigest state: %w", err)
		}
		td.AddCentroid(tdigest.Centroid{
			Mean:   float64(math.Float32frombits(binary.LittleEndian.Uint32(bs[:4]))),
			Weight: float64(math.Float32frombits(binary.LittleEndian.Uint32(bs[4:]))),
		})
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("duck-store: tdigest state has %d trailing bytes", r.Len())
	}
	td.Normalize()
	return td, nil
}

// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package aggregator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/VKCOM/statshouse/internal/duckstore"
)

// openDuckStore opens the shard's duck-store and starts its single writer,
// producing the handle the insert threads take their sinks from.
func openDuckStore(config ConfigAggregator) (duckStoreHandle, error) {
	s, err := duckstore.OpenStore(duckstore.StoreConfig{
		Dir:  config.DuckStoreDir,
		Logf: log.Printf,
	})
	if err != nil {
		return nil, err
	}
	w, err := duckstore.NewWriter(s, duckstore.WriterConfig{})
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	return &duckStore{store: s, writer: w}, nil
}

// duckStore implements duckStoreHandle over the store and its writer.
type duckStore struct {
	store  *duckstore.Store
	writer *duckstore.Writer
}

func (d *duckStore) NewSink() InsertSink { return newDuckSink(d.writer) }

// Close stops the writer before releasing the store, so the appenders' dedicated
// connection is never closed underneath a live writer.
func (d *duckStore) Close() error {
	return errors.Join(d.writer.Close(), d.store.Close())
}

// duckSink is the duck-store side of the InsertSink seam: it collects the
// resolved rows of one insert round and hands the whole round to the shared
// writer goroutine. Send returns only after the round is flushed and fsynced
// into the delta file, so a contributor acknowledgement keeps meaning what it
// means under ClickHouse; failures map onto the same status/exception/elapsed
// quadruple the conveyor already reacts to, leaving insert budgeting and
// sampling untouched.
type duckSink struct {
	writer *duckstore.Writer
	rows   []duckstore.Row
	size   int
}

func newDuckSink(w *duckstore.Writer) *duckSink {
	return &duckSink{writer: w}
}

// AppendRow converts one resolved row and copies its sketch bytes (the
// conveyor reuses the scratch they were encoded into), reporting the row's
// RowBinary size so the insertSize accounting matches the ClickHouse sink's.
func (s *duckSink) AppendRow(row *insertRow) int {
	var dr duckstore.Row
	dr.Metric = row.key.Metric
	dr.Time = row.key.Timestamp
	dr.Tags = row.key.Tags
	dr.STags = row.key.STags
	dr.Top = duckstore.HostTag{ID: row.top.I, S: row.top.S} // the string top slot is empty in the key
	dr.Count = row.count
	dr.Min = row.min
	dr.Max = row.max
	dr.Sum = row.sum
	dr.SumSquare = row.sumSquare
	dr.Percentiles = append([]byte(nil), row.percentiles...)
	dr.Unique = append([]byte(nil), row.unique...)
	dr.MinHost = duckHostTag(row.minHost)
	dr.MaxHost = duckHostTag(row.maxHost)
	dr.MaxCountHost = duckHostTag(row.maxCountHost)
	s.rows = append(s.rows, dr)
	n := rowBinarySize(row)
	s.size += n
	return n
}

func duckHostTag(h hostPair) duckstore.HostTag {
	return duckstore.HostTag{ID: h.tag.I, S: h.tag.S}
}

// Send writes the pending round. Success maps onto ClickHouse's 200 the way
// the conveyor's metrics tag it; a failure maps onto a zero status and the
// error, which drives the same no-ack/resample path a failed ClickHouse
// insert drives.
func (s *duckSink) Send(ctx context.Context) (int, int, time.Duration, error) {
	start := time.Now()
	err := s.writer.WriteRound(ctx, s.rows)
	dur := time.Since(start)
	dur = dur / time.Millisecond * time.Millisecond // same granularity as sendToClickhouse
	if err != nil {
		return 0, 0, dur, fmt.Errorf("duck-store insert round failed: %w", err)
	}
	return http.StatusOK, 0, dur, nil
}

func (s *duckSink) RoundSize() int { return s.size }

func (s *duckSink) Reset() {
	s.rows = s.rows[:0]
	s.size = 0
}

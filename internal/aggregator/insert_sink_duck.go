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

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/metajournal"
)

// openDuckStore opens the shard's duck-store and starts its single writer and
// its retainer, producing the handle the insert threads take their sinks from
// and the query listener takes its executor from. The DuckDB resource bounds
// from the config ride into every store file the store opens.
func openDuckStore(config ConfigAggregator) (duckStoreHandle, error) {
	s, err := duckstore.OpenStore(duckstore.StoreConfig{
		Dir:  config.DuckStoreDir,
		Logf: log.Printf,
		Resources: duckstore.ResourcesConfig{
			MemoryLimitBytes: config.DuckMemoryLimit,
		},
	})
	if err != nil {
		return nil, err
	}
	w, err := duckstore.NewWriter(s, duckstore.WriterConfig{})
	if err != nil {
		_ = s.Close()
		return nil, err
	}
	retainer := duckstore.NewRetainer(s, duckstore.RetentionConfig{
		Retention1s:        config.DuckRetention1s,
		Retention1m:        config.DuckRetention1m,
		Retention1h:        config.DuckRetention1h,
		FreeSpaceWatermark: uint64(config.DuckFreeSpaceWatermark),
		Logf:               log.Printf,
	})
	retCtx, stopRetainer := context.WithCancel(context.Background())
	retainerDone := make(chan struct{})
	go func() {
		defer close(retainerDone)
		_ = retainer.Run(retCtx)
	}()
	return &duckStore{store: s, writer: w, stopRetainer: stopRetainer, retainerDone: retainerDone}, nil
}

// duckStore implements duckStoreHandle over the store, its writer and its
// retainer.
type duckStore struct {
	store        *duckstore.Store
	writer       *duckstore.Writer
	stopRetainer context.CancelFunc
	retainerDone chan struct{}
}

func (d *duckStore) NewSink() InsertSink { return newDuckSink(d.writer) }

// QueryExecutor serves the two structured store-query verbs: journal
// validation first (unknown_metric, metadata_mismatch), then the renderer.
// storage is the aggregator's metrics journal and shardNum the shard's own
// 1-based number — the same convention ClickHouse's _shard_num column uses —
// which the series response answers its shard-tag column from.
func (d *duckStore) QueryExecutor(storage *metajournal.MetricsStorage, shardNum int32) storeQueryExecutor {
	return &duckQueryExecutor{store: d.store, storage: storage, shardNum: shardNum}
}

// duckQueryExecutor renders store queries against the shard's store behind
// the aggregator's journal validation, so no query reaches storage under a
// tag layout the journal disagrees with.
type duckQueryExecutor struct {
	store    *duckstore.Store
	storage  *metajournal.MetricsStorage
	shardNum int32
}

func (e *duckQueryExecutor) QuerySeries(ctx context.Context, args tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
	if err := validateStoreQueryMetadata(ctx, e.storage, args.Base); err != nil {
		return tlstatshouse.StoreSeriesResponse{}, err
	}
	return e.store.RenderSeries(ctx, e.shardNum, args)
}

func (e *duckQueryExecutor) QueryTagValues(ctx context.Context, args tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
	if err := validateStoreQueryMetadata(ctx, e.storage, args.Base); err != nil {
		return tlstatshouse.StoreTagValuesResponse{}, err
	}
	return e.store.RenderTagValues(ctx, args)
}

// Close stops the retainer and the writer before releasing the store, so the
// appenders' dedicated connection is never closed underneath a live writer.
func (d *duckStore) Close() error {
	d.stopRetainer()
	<-d.retainerDone
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

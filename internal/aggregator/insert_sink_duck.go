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
	"sync"
	"time"

	"github.com/VKCOM/statshouse/internal/agent"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/metajournal"
)

// openDuckStore opens the shard's duck-store and starts its single writer and
// its background maintenance — compaction, sealing, retention, the size
// sampler and the liveness sampler — producing the handle the insert threads take their sinks from and
// the query listener takes its executor from. The DuckDB resource bounds from
// the config ride into every store file the store opens, and sh2 (the
// aggregator's builtin-metrics agent, may be nil in tests) receives the
// store's observability events as __duck_store_* builtin metrics.
func openDuckStore(config ConfigAggregator, sh2 *agent.Agent) (duckStoreHandle, error) {
	var rec duckstore.MetricsRecorder
	if sh2 != nil {
		rec = &duckMetrics{sh: sh2}
	}
	s, err := duckstore.OpenStore(duckstore.StoreConfig{
		Dir:     config.DuckStoreDir,
		Logf:    log.Printf,
		Metrics: rec,
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
	compactor := duckstore.NewCompactor(s, duckstore.CompactorConfig{Metrics: rec, Logf: log.Printf})
	sealer := duckstore.NewSealer(s, duckstore.SealerConfig{Metrics: rec, Logf: log.Printf})
	retainer := duckstore.NewRetainer(s, duckstore.RetentionConfig{
		Retention1s:        config.DuckRetention1s,
		Retention1m:        config.DuckRetention1m,
		Retention1h:        config.DuckRetention1h,
		FreeSpaceWatermark: uint64(config.DuckFreeSpaceWatermark),
		Metrics:            rec,
		Logf:               log.Printf,
	})
	sampler := func(ctx context.Context) error {
		return duckstore.RunSizeSampler(ctx, s, rec, 0)
	}
	liveness := func(ctx context.Context) error {
		return duckstore.RunLivenessSampler(ctx, s,
			[]duckstore.MaintenanceLiveness{compactor.Liveness(), sealer.Liveness(), retainer.Liveness()}, rec, 0)
	}

	// The five maintenance loops share one lifecycle: cancel on Close, wait
	// for every goroutine, then the writer and the store shut down in order.
	mntCtx, stopMaintenance := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(5)
	for _, loop := range []func(context.Context) error{compactor.Run, sealer.Run, retainer.Run, sampler, liveness} {
		loop := loop
		go func() {
			defer wg.Done()
			_ = loop(mntCtx)
		}()
	}
	maintenanceDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(maintenanceDone)
	}()
	return &duckStore{
		store:           s,
		writer:          w,
		stopMaintenance: stopMaintenance,
		maintenanceDone: maintenanceDone,
	}, nil
}

// duckMetricsSink is the slice of agent.Agent's surface the store's events
// flow through, named here — where it is consumed — so the forwarding itself
// is testable against a capturing fake: a live agent needs the full shard
// machinery. *agent.Agent satisfies it as-is.
type duckMetricsSink interface {
	AddValueCounter(t uint32, metricInfo *format.MetricMetaValue, tags []int32, value float64, counter float64)
	AddCounter(t uint32, metricInfo *format.MetricMetaValue, tags []int32, count float64)
}

// duckMetrics forwards the store's observability events to the __duck_store_*
// builtin metrics through the aggregator's own agent, the same way
// reportInsertMetric forwards the insert-path metrics: one tags slice per
// event in final tag positions (environment at 0, filled by the agent), the
// metric metas carrying the value comments that name the numbers.
type duckMetrics struct {
	sh duckMetricsSink
}

var _ duckstore.MetricsRecorder = (*duckMetrics)(nil)

func (m *duckMetrics) MaintenancePass(kind duckstore.MaintenanceKind, err error, dur time.Duration) {
	m.sh.AddValueCounter(m.now(), format.BuiltinMetricMetaDuckMaintenanceTime,
		[]int32{0, duckMaintenanceTag(kind), duckStatusTag(err)}, dur.Seconds(), 1)
}

func (m *duckMetrics) MaintenanceWindow(kind duckstore.WindowEventKind, tier string) {
	m.sh.AddCounter(m.now(), format.BuiltinMetricMetaDuckWindows,
		[]int32{0, duckWindowEventTag(kind), duckTierTag(tier)}, 1)
}

func (m *duckMetrics) QuarantinedFiles(axis duckstore.QuarantineAxis, count int) {
	m.sh.AddCounter(m.now(), format.BuiltinMetricMetaDuckQuarantinedFiles,
		[]int32{0, duckQuarantineAxisTag(axis)}, float64(count))
}

func (m *duckMetrics) StoreQuery(verb duckstore.QueryVerb, err error, dur time.Duration) {
	m.sh.AddValueCounter(m.now(), format.BuiltinMetricMetaDuckQueryTime,
		[]int32{0, duckQueryVerbTag(verb), duckStatusTag(err)}, dur.Seconds(), 1)
}

func (m *duckMetrics) StoreSize(location duckstore.SizeLocation, used, free int64) {
	t := m.now()
	m.sh.AddValueCounter(t, format.BuiltinMetricMetaDuckStoreSize,
		[]int32{0, duckSizeLocationTag(location), format.TagValueIDDuckSizeUsed}, float64(used), 1)
	m.sh.AddValueCounter(t, format.BuiltinMetricMetaDuckStoreSize,
		[]int32{0, duckSizeLocationTag(location), format.TagValueIDDuckSizeFree}, float64(free), 1)
}

func (m *duckMetrics) StoreBacklog(generations int, oldestAge time.Duration) {
	t := m.now()
	m.sh.AddValueCounter(t, format.BuiltinMetricMetaDuckBacklog,
		[]int32{0, format.TagValueIDDuckBacklogGenerations}, float64(generations), 1)
	m.sh.AddValueCounter(t, format.BuiltinMetricMetaDuckBacklog,
		[]int32{0, format.TagValueIDDuckBacklogOldestAgeSeconds}, oldestAge.Seconds(), 1)
}

func (m *duckMetrics) MaintenanceAge(kind duckstore.MaintenanceKind, age time.Duration) {
	m.sh.AddValueCounter(m.now(), format.BuiltinMetricMetaDuckMaintenanceAge,
		[]int32{0, duckMaintenanceTag(kind)}, age.Seconds(), 1)
}

func (m *duckMetrics) now() uint32 { return uint32(time.Now().Unix()) }

// The duck*Tag helpers map the store's typed event values onto the tag-value
// constants the metas' value comments name. Unknown values map to 0, which no
// comment names and which therefore stands out on a dashboard.

func duckStatusTag(err error) int32 {
	if err != nil {
		return format.TagValueIDStatusError
	}
	return format.TagValueIDStatusOK
}

func duckMaintenanceTag(kind duckstore.MaintenanceKind) int32 {
	switch kind {
	case duckstore.MaintenanceCompaction:
		return format.TagValueIDDuckMaintenanceCompaction
	case duckstore.MaintenanceSealing:
		return format.TagValueIDDuckMaintenanceSealing
	case duckstore.MaintenanceRetention:
		return format.TagValueIDDuckMaintenanceRetention
	}
	return 0
}

func duckWindowEventTag(kind duckstore.WindowEventKind) int32 {
	switch kind {
	case duckstore.WindowSealed:
		return format.TagValueIDDuckWindowSealed
	case duckstore.WindowUnlinked:
		return format.TagValueIDDuckWindowUnlinked
	case duckstore.WindowEarlyEvicted:
		return format.TagValueIDDuckWindowEarlyEvicted
	case duckstore.WindowLeaseDeferred:
		return format.TagValueIDDuckWindowLeaseDeferred
	case duckstore.WindowLateDropped:
		return format.TagValueIDDuckWindowLateDropped
	}
	return 0
}

func duckTierTag(tier string) int32 {
	switch tier {
	case duckstore.Tier1s:
		return format.TagValueIDDuckTier1s
	case duckstore.Tier1m:
		return format.TagValueIDDuckTier1m
	case duckstore.Tier1h:
		return format.TagValueIDDuckTier1h
	}
	return 0
}

func duckQuarantineAxisTag(axis duckstore.QuarantineAxis) int32 {
	switch axis {
	case duckstore.QuarantineSchema:
		return format.TagValueIDDuckQuarantineSchema
	case duckstore.QuarantineStorage:
		return format.TagValueIDDuckQuarantineStorage
	case duckstore.QuarantineStatshouse:
		return format.TagValueIDDuckQuarantineStatshouse
	case duckstore.QuarantineUnreadable:
		return format.TagValueIDDuckQuarantineUnreadable
	}
	return 0
}

func duckQueryVerbTag(verb duckstore.QueryVerb) int32 {
	switch verb {
	case duckstore.QuerySeries:
		return format.TagValueIDDuckQuerySeries
	case duckstore.QueryTagValues:
		return format.TagValueIDDuckQueryTagValues
	}
	return 0
}

func duckSizeLocationTag(location duckstore.SizeLocation) int32 {
	switch location {
	case duckstore.SizeDelta:
		return format.TagValueIDDuckSizeDelta
	case duckstore.SizeArchive:
		return format.TagValueIDDuckSizeArchive
	}
	return 0
}

// duckStore implements duckStoreHandle over the store, its writer and its
// maintenance goroutines.
type duckStore struct {
	store           *duckstore.Store
	writer          *duckstore.Writer
	stopMaintenance context.CancelFunc
	maintenanceDone chan struct{}
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

// Close stops the maintenance loops and the writer before releasing the
// store, so the appenders' dedicated connection is never closed underneath a
// live writer.
func (d *duckStore) Close() error {
	d.stopMaintenance()
	<-d.maintenanceDone
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

// AppendRow converts one resolved row and copies its aggregate-state bytes (the
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
	dr.MinHost = duckHostPair(row.minHost)
	dr.MaxHost = duckHostPair(row.maxHost)
	dr.MaxCountHost = duckHostPair(row.maxCountHost)
	s.rows = append(s.rows, dr)
	n := rowBinarySize(row)
	s.size += n
	return n
}

// duckHostPair converts one resolved host column, keeping the skewed
// comparison value: it is the argMin/argMax state's payload — exactly the
// value the ClickHouse sink writes into the state — and the store orders and
// serves hosts by it.
func duckHostPair(h hostPair) duckstore.HostPair {
	return duckstore.HostPair{Tag: duckstore.HostTag{ID: h.tag.I, S: h.tag.S}, Value: float64(h.value)}
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

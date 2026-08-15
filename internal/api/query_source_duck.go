// Copyright 2025 V Kontate LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
)

// The duck QuerySource: it lowers a semantic request onto the structured
// store query RPC, decides which shards the request must visit, fans it out
// to all of them in parallel and merges the partial aggregates — the job a
// ClickHouse Distributed table does inside the database. The merge itself
// lives in fanout.go; this file is the request side: building, pruning,
// retrying and emitting.

// fanoutJournalWait bounds how long a metadata-mismatched query waits for the
// API's own journal to advance before rebuilding the request and retrying
// once. It is a variable only so tests can shrink it; production leaves it
// alone.
var fanoutJournalWait = 2 * time.Second

// metricsStorageRef is the metrics-journal surface the source needs for its
// one-shot mismatch retry; *metajournal.MetricsStorage satisfies it.
type metricsStorageRef interface {
	WaitVersion(ctx context.Context, version int64) error
	GetMetaMetric(metricID int32) *format.MetricMetaValue
}

// duckQuerySource serves semantic queries from the aggregator shards' duck
// stores over the structured query RPC.
type duckQuerySource struct {
	// clients holds one entry per configured shard, sorted by shard number —
	// the whole shard set a query fans out over.
	clients []storeShardClient
	// numShards is the highest configured shard number: the modulus of the
	// by-metric-id assignment, which numbers shards 1..N contiguously.
	numShards int
	// journal re-reads metrics when a shard refuses a query as a metadata
	// mismatch. Nil (tests without one) makes the retry fail fast.
	journal metricsStorageRef

	// waitVersionOverride and refreshMetricOverride replace the journal
	// interactions in tests; production leaves both nil.
	waitVersionOverride   func(ctx context.Context, version int64) error
	refreshMetricOverride func(metricID int32) *format.MetricMetaValue
}

// newDuckQuerySource builds the source over the configured per-shard query
// addresses. An empty address set returns nil: the duck backend is then
// selected but not servable, which newQuerySource reports per query.
func newDuckQuerySource(addrs map[uint32]string, journal metricsStorageRef) *duckQuerySource {
	if len(addrs) == 0 {
		return nil
	}
	rpcClients := newRPCStoreShardClients(addrs)
	clients := make([]storeShardClient, len(rpcClients))
	for i, c := range rpcClients {
		clients[i] = c
	}
	numShards := 0
	for shard := range addrs {
		if int(shard) > numShards {
			numShards = int(shard)
		}
	}
	return &duckQuerySource{clients: clients, numShards: numShards, journal: journal}
}

// shardForRange returns the 0-based shard the metric's rows live on for the
// whole range starting at fromSec, or -1 when the query must fan out. Fan-out
// is the always-correct answer — data lands wherever agent-to-aggregator
// routing put it — so pruning happens only when the assignment provably
// covers the range: the by-metric-id and fixed-key assignments never move,
// and a fixed shard assignment holds from the metric's last change onward,
// so a metric untouched since before the range began sits entirely on its
// current shard. A metric possibly re-assigned mid-range fans out for
// correctness.
func (s *duckQuerySource) shardForRange(m *format.MetricMetaValue, fromSec int64) int {
	if !m.Sharded() {
		return -1
	}
	if m.ShardFixedKey == 0 && m.ShardStrategy == format.ShardFixed {
		if m.UpdateTime == 0 || int64(m.UpdateTime) > fromSec {
			// never pruned on an unknown last-change time, nor when the
			// metric was touched inside the queried range: an edit may have
			// moved the assignment mid-range. A fixed-key assignment never
			// moves, so it needs no such guard.
			return -1
		}
	}
	return m.Shard(s.numShards)
}

// clientsForShard returns the single client serving shard (0-based), or an
// error naming the shard and the flag when it is not configured.
func (s *duckQuerySource) clientsForShard(shard int, metricID int32) ([]storeShardClient, error) {
	for _, c := range s.clients {
		if int(c.shardNum()) == shard+1 {
			return []storeShardClient{c}, nil
		}
	}
	return nil, fmt.Errorf("duck shard %d, where metric %d lives, has no query address configured (--duck-shard-query-addrs)", shard+1, metricID)
}

func (s *duckQuerySource) querySeries(ctx context.Context, h *requestHandler, q *seriesDataQuery, lod data_model.LOD, onRow func(tsSelectRow) error) error {
	clients := s.clients
	if q.metric != nil {
		if shard := s.shardForRange(q.metric, lod.FromSec); shard >= 0 {
			var err error
			clients, err = s.clientsForShard(shard, q.metric.MetricID)
			if err != nil {
				return err
			}
		}
	}
	args := buildStoreSeriesArgs(q, lod, storeQueryTimeoutMs(ctx))
	start := time.Now()
	resps, err := s.callShards(ctx, clients, args, q, lod)
	if err != nil {
		return err
	}
	if h != nil {
		h.reportQueryDuration("statshouse.storeQuerySeries", time.Since(start))
	}
	perShard := make([][]tsSelectRow, len(resps))
	for i := range resps {
		rows, err := decodeSeriesResponse(q, resps[i])
		if err != nil {
			return err
		}
		perShard[i] = rows
	}
	merged, err := mergeShardRows(perShard, fanoutRowCap)
	if err != nil {
		return err
	}
	for i := range merged {
		if err := onRow(merged[i]); err != nil {
			return err
		}
	}
	return nil
}

func (s *duckQuerySource) queryTagValues(ctx context.Context, h *requestHandler, q *tagValuesDataQuery, lod data_model.LOD, onRow func(selectRow) error) error {
	clients := s.clients
	if q.metric != nil {
		if shard := s.shardForRange(q.metric, lod.FromSec); shard >= 0 {
			var err error
			clients, err = s.clientsForShard(shard, q.metric.MetricID)
			if err != nil {
				return err
			}
		}
	}
	args := buildStoreTagValuesArgs(q, lod, storeQueryTimeoutMs(ctx))
	start := time.Now()
	resps, err := fanoutCall(ctx, clients, func(ctx context.Context, c storeShardClient) (tlstatshouse.StoreTagValuesResponse, error) {
		return c.queryTagValues(ctx, args)
	})
	if err != nil {
		return err
	}
	if h != nil {
		h.reportQueryDuration("statshouse.storeQueryTagValues", time.Since(start))
	}
	perShard := make([][]selectRow, len(resps))
	for i := range resps {
		rows, err := decodeTagValuesResponse(resps[i])
		if err != nil {
			return err
		}
		perShard[i] = rows
	}
	merged, err := mergeTagValueRows(perShard, fanoutRowCap)
	if err != nil {
		return err
	}
	for i := range merged {
		if err := onRow(merged[i]); err != nil {
			return err
		}
	}
	return nil
}

// callShards issues the series request to every shard and, when a shard
// refuses it as a metadata mismatch, retries the whole fan-out once with a
// rebuilt request: the mismatch means the two journals disagree about the
// metric's tag layout, so the API first waits briefly for its own journal to
// advance past the version the request carried, then re-reads the metric and
// re-derives the layout. A second mismatch fails the query — rows are never
// read through a layout either side is unsure of.
func (s *duckQuerySource) callShards(ctx context.Context, clients []storeShardClient, args tlstatshouse.StoreQuerySeries, q *seriesDataQuery, lod data_model.LOD) ([]tlstatshouse.StoreSeriesResponse, error) {
	resps, err := fanoutCall(ctx, clients, func(ctx context.Context, c storeShardClient) (tlstatshouse.StoreSeriesResponse, error) {
		return c.querySeries(ctx, args)
	})
	if !duckstore.IsCode(err, duckstore.ErrCodeMetadataMismatch) || q.metric == nil || q.metric.MetricID == 0 {
		return resps, err
	}
	if s.waitVersion(ctx, q.metric.Version+1) != nil {
		return nil, fmt.Errorf("duck shard refused the query at journal version %d and the metrics journal never reached %d: %w",
			q.metric.Version, q.metric.Version+1, err)
	}
	fresh := s.refreshMetric(q.metric.MetricID)
	if fresh == nil {
		return nil, fmt.Errorf("duck shard refused the query at journal version %d and metric %d left the journal: %w",
			q.metric.Version, q.metric.MetricID, err)
	}
	retry := *q
	retry.metric = fresh
	return fanoutCall(ctx, clients, func(ctx context.Context, c storeShardClient) (tlstatshouse.StoreSeriesResponse, error) {
		return c.querySeries(ctx, buildStoreSeriesArgs(&retry, lod, storeQueryTimeoutMs(ctx)))
	})
}

// waitVersion waits — at most fanoutJournalWait — for the API's metrics
// journal to reach version.
func (s *duckQuerySource) waitVersion(ctx context.Context, version int64) error {
	if s.waitVersionOverride != nil {
		return s.waitVersionOverride(ctx, version)
	}
	if s.journal == nil {
		return fmt.Errorf("no metrics journal to wait on")
	}
	waitCtx, cancel := context.WithTimeout(ctx, fanoutJournalWait)
	defer cancel()
	return s.journal.WaitVersion(waitCtx, version)
}

// refreshMetric re-reads a metric from the API's journal.
func (s *duckQuerySource) refreshMetric(metricID int32) *format.MetricMetaValue {
	if s.refreshMetricOverride != nil {
		return s.refreshMetricOverride(metricID)
	}
	if s.journal == nil {
		return nil
	}
	return s.journal.GetMetaMetric(metricID)
}

// buildStoreSeriesArgs lowers a semantic series request onto the RPC's series
// verb: the what kinds, the grouped tags (including the shard and string-top
// entries the renderer resolves itself), the host flags and the table-view
// ordering.
func buildStoreSeriesArgs(q *seriesDataQuery, lod data_model.LOD, timeoutMs int32) tlstatshouse.StoreQuerySeries {
	args := tlstatshouse.StoreQuerySeries{
		Base: buildStoreQueryBase(q.metric, q.filterIn, q.filterNotIn, lod, q.utcOffset, timeoutMs),
		By:   byTagIndices(q.by),
	}
	for i := 0; q.what.specifiedAt(i); i++ {
		args.What = append(args.What, int32(q.what[i].What))
	}
	if q.minMaxHost[0] {
		args.SetMinHost(true)
	}
	if q.minMaxHost[1] {
		args.SetMaxHost(true)
	}
	switch q.sort {
	case sortDescending:
		args.SetSortDesc(true)
	case sortAscending:
		args.SetSortAsc(true)
	}
	return args
}

// buildStoreTagValuesArgs lowers a semantic tag-values request onto the RPC's
// tag-values verb. The user's top N is deliberately not carried: the shards
// must not apply it (a value ranked below N everywhere can still be globally
// top-N), the API sums counts across shards and the handler takes the top N.
func buildStoreTagValuesArgs(q *tagValuesDataQuery, lod data_model.LOD, timeoutMs int32) tlstatshouse.StoreQueryTagValues {
	args := tlstatshouse.StoreQueryTagValues{
		Base:     buildStoreQueryBase(q.metric, q.filterIn, q.filterNotIn, lod, q.utcOffset, timeoutMs),
		TagIndex: int32(q.tag.Index),
	}
	if q.idsOnly {
		args.SetIdsOnly(true)
	}
	return args
}

// buildStoreQueryBase fills the shared request base: the addressed metric
// (single id, or the in/not-in lists a multi-metric query carries), the tag
// layout as the API's journal derives it plus the journal version it was
// read at, the resolution window, and the tag filters with the same arm
// semantics the ClickHouse builder writes.
func buildStoreQueryBase(metric *format.MetricMetaValue, filterIn, filterNotIn data_model.TagFilters, lod data_model.LOD, utcOffset int64, timeoutMs int32) tlstatshouse.StoreQueryBase {
	var metricVersion int64
	if metric != nil {
		metricVersion = metric.Version
	}
	base := tlstatshouse.StoreQueryBase{
		MetricVersion: metricVersion,
		TagLayout:     tlstatshouse.StoreTagLayout{Kinds: duckstore.TagLayoutKinds(metric)},
		Lod: tlstatshouse.StoreLod{
			FromSec:   lod.FromSec,
			ToSec:     lod.ToSec,
			StepSec:   lod.StepSec,
			UtcOffset: utcOffset,
			Location:  lodLocationName(lod),
		},
		FilterIn:    storeTagFilters(filterIn),
		FilterNotIn: storeTagFilters(filterNotIn),
		TimeoutMs:   timeoutMs,
	}
	if metric != nil {
		base.MetricId = metric.MetricID
		return base
	}
	// no single metric: the filter's metric lists address the query, exactly
	// as the ClickHouse builder's writeMetricFilter treats them — a one-entry
	// filter-in list collapses to that metric's id
	var metricIn []int32
	for _, m := range filterIn.Metrics {
		metricIn = append(metricIn, m.MetricID)
	}
	if len(metricIn) == 1 {
		base.MetricId = metricIn[0]
	} else if len(metricIn) > 0 {
		base.SetMetricIn(metricIn)
	}
	var metricNotIn []int32
	for _, m := range filterNotIn.Metrics {
		metricNotIn = append(metricNotIn, m.MetricID)
	}
	if len(metricNotIn) > 0 {
		base.SetMetricNotIn(metricNotIn)
	}
	return base
}

// lodLocationName renders the LOD's zone for the one step that needs it;
// every other step truncates against utc_offset.
func lodLocationName(lod data_model.LOD) string {
	if lod.Location == nil {
		return "UTC"
	}
	return lod.Location.String()
}

// storeTagFilters lowers the API's per-tag filters onto the RPC's filter
// arms. The arms mirror the ClickHouse builder exactly: mapped ids, string
// values, the empty arm and an RE2 pattern, OR-ed within an IN filter.
func storeTagFilters(f data_model.TagFilters) []tlstatshouse.StoreTagFilter {
	var out []tlstatshouse.StoreTagFilter
	for tagX := range f.Tags {
		filter := &f.Tags[tagX]
		if len(filter.Values) == 0 && filter.Re2 == "" {
			continue
		}
		sf := tlstatshouse.StoreTagFilter{TagIndex: int32(tagX)}
		var mapped []int64
		var values []string
		hasEmpty := false
		for _, v := range filter.Values {
			if v.Empty() {
				hasEmpty = true
				continue
			}
			if v.IsMapped() {
				mapped = append(mapped, v.Mapped)
			}
			if v.HasValue() {
				values = append(values, v.Value)
			}
		}
		if len(mapped) != 0 {
			sf.SetMapped(mapped)
		}
		if len(values) != 0 {
			sf.SetValues(values)
		}
		if hasEmpty {
			sf.SetEmpty(true)
		}
		if filter.Re2 != "" {
			sf.SetRe2(filter.Re2)
		}
		out = append(out, sf)
	}
	return out
}

// byTagIndices widens the grouped-tag indices.
func byTagIndices(by []int) []int32 {
	if len(by) == 0 {
		return nil
	}
	out := make([]int32, len(by))
	for i, x := range by {
		out[i] = int32(x)
	}
	return out
}

// storeQueryTimeoutMs derives the request's relative timeout from the
// context deadline the HTTP handler already enforces, so the aggregator's
// deadline and the API's give up together. No deadline means no explicit
// timeout: the aggregator's own default response timeout applies.
func storeQueryTimeoutMs(ctx context.Context) int32 {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	ms := time.Until(deadline).Milliseconds()
	if ms < 1 {
		ms = 1
	}
	if ms > math.MaxInt32 {
		ms = math.MaxInt32
	}
	return int32(ms)
}

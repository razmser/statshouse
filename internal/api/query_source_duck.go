// Copyright 2025 V Kontakte LLC
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
	// numShards is the modulus of the by-metric-id assignment — the copy of
	// the aggregator cluster's --shard-by-metric-shards, the count agents
	// shard metric data by at write time. It comes from the API's own flag,
	// not from the number of configured addresses: a partial address list is
	// a startup-validation error, not a routing modulus.
	numShards int
	// journal re-reads metrics when a shard refuses a query as a metadata
	// mismatch. Nil (tests without one) makes the retry fail fast.
	journal metricsStorageRef
}

// duckQuerySourceConfig is everything the duck source is built from: the
// per-shard query addresses, the by-metric-id routing modulus (the API's
// --shard-by-metric-shards), the RPC crypto key the shards' store-query
// listeners require, and the metrics journal for its mismatch retry.
type duckQuerySourceConfig struct {
	addrs     map[uint32]string
	numShards int
	cryptoKey string
	journal   metricsStorageRef
}

// newDuckQuerySource builds the source over the configured per-shard query
// addresses. numShards zero or below falls back to the highest configured
// shard number, the way startup validation resolves an unset count — a path
// only tests building a bare source take. An empty address set returns nil:
// the duck backend is then selected but not servable, which newQuerySource
// reports per query.
func newDuckQuerySource(cfg duckQuerySourceConfig) *duckQuerySource {
	if len(cfg.addrs) == 0 {
		return nil
	}
	rpcClients := newRPCStoreShardClients(cfg.addrs, cfg.cryptoKey)
	clients := make([]storeShardClient, len(rpcClients))
	for i, c := range rpcClients {
		clients[i] = c
	}
	numShards := cfg.numShards
	if numShards <= 0 {
		numShards = 0
		for shard := range cfg.addrs {
			if int(shard) > numShards {
				numShards = int(shard)
			}
		}
	}
	return &duckQuerySource{clients: clients, numShards: numShards, journal: cfg.journal}
}

// shardForRange returns the 0-based shard the metric's rows live on for the
// whole range starting at fromSec, or -1 when the query must fan out. Fan-out
// is the always-correct answer — data lands wherever agent-to-aggregator
// routing put it — so pruning happens only when the assignment provably
// covers the range. Compiled-in builtins qualify outright: they cannot be
// edited, so their assignment never moves, and write-side routing resolves
// them from the same immutable table — the passed copy is replaced by it.
// Every user-metric assignment — by-metric-id included, because the strategy
// fields themselves are admin-editable and a metric flipped from a fixed key
// or shard onto by-metric-id keeps its pre-flip rows on the old shard while
// id % numShards points elsewhere — holds only from the metric's last change
// onward: a metric with an unknown last-change time, or one touched inside
// the queried range, fans out for correctness.
func (s *duckQuerySource) shardForRange(m *format.MetricMetaValue, fromSec int64) int {
	// A promql aggregate carries a zero placeholder as its metric and
	// addresses the real metric (or several) through the filter — there is no
	// one assignment to prune by.
	if m.MetricID == 0 {
		return -1
	}
	if builtin, ok := format.BuiltinMetrics[m.MetricID]; ok {
		m = builtin
	} else if m.UpdateTime == 0 || int64(m.UpdateTime) > fromSec {
		return -1
	}
	if !m.Sharded() {
		return -1
	}
	// The effective assignment, resolved the way Shard() resolves it: the
	// fixed key wins over the strategy, so a metric carrying both prunes by
	// the editable key, not by its id.
	return m.Shard(s.numShards)
}

// clientsForMetric returns the shard set a query about m starting at fromSec
// must visit: the single shard shardForRange pins it to, or the whole set.
func (s *duckQuerySource) clientsForMetric(m *format.MetricMetaValue, fromSec int64) ([]storeShardClient, error) {
	if shard := s.shardForRange(m, fromSec); shard >= 0 {
		return s.clientsForShard(shard, m.MetricID)
	}
	return s.clients, nil
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

// blockedPolicyError is the duck path's share of the blocked-metric-prefix
// and blocked-user policy the ClickHouse path applies in doSelect: the same
// check over the same pair the ClickHouse query metadata carried — the
// addressed metric's name and the requesting user. A nil handler (tests)
// skips it, the way it skips the duration reporting; a nil metric (a
// multi-metric query) reads as the empty name, the way the ClickHouse
// builder's placeholder does.
func (h *requestHandler) blockedPolicyError(metric *format.MetricMetaValue, user string) error {
	if h == nil {
		return nil
	}
	name := ""
	if metric != nil {
		name = metric.Name
	}
	return h.blockedQueryError(name, user)
}

func (s *duckQuerySource) querySeries(ctx context.Context, h *requestHandler, q *seriesDataQuery, lod data_model.LOD, onRow func(tsSelectRow) error) error {
	if err := h.blockedPolicyError(q.metric, q.user); err != nil {
		return err
	}
	// The addressed metric decides the shard set — for a promql aggregate
	// that is the filter's lone member, not the placeholder the query carries.
	clients := s.clients
	if addressed := singleAddressedMetric(q.metric, q.filterIn); addressed != nil {
		var err error
		clients, err = s.clientsForMetric(addressed, lod.FromSec)
		if err != nil {
			return err
		}
	}
	args, err := buildStoreSeriesArgs(q, lod, storeQueryTimeoutMs(ctx))
	if err != nil {
		return err
	}
	start := time.Now()
	resps, err := retryOnMismatch(s, ctx, clients, lod.FromSec, singleAddressedMetric(q.metric, q.filterIn),
		func(ctx context.Context, c storeShardClient) (tlstatshouse.StoreSeriesResponse, error) {
			return c.querySeries(ctx, args)
		},
		func(fresh *format.MetricMetaValue) (func(context.Context, storeShardClient) (tlstatshouse.StoreSeriesResponse, error), error) {
			retry := *q
			retry.metric = fresh
			retryArgs, err := buildStoreSeriesArgs(&retry, lod, storeQueryTimeoutMs(ctx))
			if err != nil {
				return nil, err
			}
			return func(ctx context.Context, c storeShardClient) (tlstatshouse.StoreSeriesResponse, error) {
				return c.querySeries(ctx, retryArgs)
			}, nil
		})
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
	merged, err := mergeShardRows(perShard, fanoutRowCap, mergedRowLess(q.by, q.sort == sortDescending))
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
	if err := h.blockedPolicyError(q.metric, q.user); err != nil {
		return err
	}
	clients := s.clients
	if q.metric != nil {
		var err error
		clients, err = s.clientsForMetric(q.metric, lod.FromSec)
		if err != nil {
			return err
		}
	}
	args, err := buildStoreTagValuesArgs(q, lod, storeQueryTimeoutMs(ctx))
	if err != nil {
		return err
	}
	start := time.Now()
	resps, err := retryOnMismatch(s, ctx, clients, lod.FromSec, singleAddressedMetric(q.metric, q.filterIn),
		func(ctx context.Context, c storeShardClient) (tlstatshouse.StoreTagValuesResponse, error) {
			return c.queryTagValues(ctx, args)
		},
		func(fresh *format.MetricMetaValue) (func(context.Context, storeShardClient) (tlstatshouse.StoreTagValuesResponse, error), error) {
			retry := *q
			retry.metric = fresh
			retryArgs, err := buildStoreTagValuesArgs(&retry, lod, storeQueryTimeoutMs(ctx))
			if err != nil {
				return nil, err
			}
			return func(ctx context.Context, c storeShardClient) (tlstatshouse.StoreTagValuesResponse, error) {
				return c.queryTagValues(ctx, retryArgs)
			}, nil
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
	// The ClickHouse source truncates every LOD's query to the user's top
	// numResults+1 (its SQL LIMIT), and the handler sums those per-LOD
	// partials — so the same cut is applied here, once per LOD, after the
	// cross-shard merge has produced the LOD's global ranking. Without it a
	// value below one LOD's cut would still contribute its full count here
	// and the two backends' answers would drift apart over multi-LOD ranges.
	if q.numResults >= 0 && len(merged) > q.numResults+1 {
		merged = merged[:q.numResults+1]
	}
	for i := range merged {
		if err := onRow(merged[i]); err != nil {
			return err
		}
	}
	return nil
}

// retryOnMismatch issues call to every shard and, when a shard refuses it as a
// metadata mismatch, retries the whole fan-out once with a rebuilt request:
// the mismatch means the two journals disagree about the metric's tag layout,
// so the API first waits briefly for its own journal to advance past the
// version the request carried, then re-reads the metric and re-derives the
// layout. A second mismatch fails the query — rows are never read through a
// layout either side is unsure of. metric is the single metric the request
// addresses (nil when it addresses several or none, in which case there is no
// one metric to refresh and the mismatch is final); rebuild re-derives the
// call from a fresh journal copy of that metric, and the shard set is
// re-selected from that same fresh copy — an edit that moved the assignment
// mid-query must move the retry's destinations too. Both verbs go through it:
// the shards validate a tag-values request's base exactly like a series one.
func retryOnMismatch[R any](s *duckQuerySource, ctx context.Context, clients []storeShardClient, fromSec int64, metric *format.MetricMetaValue,
	call func(ctx context.Context, c storeShardClient) (R, error),
	rebuild func(fresh *format.MetricMetaValue) (func(ctx context.Context, c storeShardClient) (R, error), error)) ([]R, error) {
	resps, shardErrs, err := fanoutCall(ctx, clients, call)
	// The mismatch routinely comes from a higher-numbered shard — the one
	// whose journal already advanced — while lower-numbered shards are still
	// executing and merely fail because the mismatch cancelled them, so the
	// gate keys on any shard's own refusal, never on the headline error.
	mismatch := shardCode(shardErrs, duckstore.ErrCodeMetadataMismatch)
	if mismatch == nil || metric == nil || metric.MetricID == 0 {
		return resps, err
	}
	if s.waitVersion(ctx, metric.Version+1) != nil {
		return nil, fmt.Errorf("duck shard refused the query at journal version %d and the metrics journal never reached %d: %w",
			metric.Version, metric.Version+1, mismatch)
	}
	fresh := s.refreshMetric(metric.MetricID)
	if fresh == nil {
		return nil, fmt.Errorf("duck shard refused the query at journal version %d and metric %d left the journal: %w",
			metric.Version, metric.MetricID, mismatch)
	}
	retry, rerr := rebuild(fresh)
	if rerr != nil {
		return nil, rerr
	}
	retryClients, cerr := s.clientsForMetric(fresh, fromSec)
	if cerr != nil {
		return nil, cerr
	}
	retryResps, _, retryErr := fanoutCall(ctx, retryClients, retry)
	return retryResps, retryErr
}

// shardCode finds the first per-shard failure carrying the given structured
// store error code. The headline fan-out error cannot be trusted for this:
// the failure that cancelled the fan-out often lives at a higher shard index
// than the siblings it cancelled, and those artifacts come first in shard
// order.
func shardCode(errs []error, code int32) error {
	for _, err := range errs {
		if duckstore.IsCode(err, code) {
			return err
		}
	}
	return nil
}

// waitVersion waits — at most fanoutJournalWait — for the API's metrics
// journal to reach version.
func (s *duckQuerySource) waitVersion(ctx context.Context, version int64) error {
	if s.journal == nil {
		return fmt.Errorf("no metrics journal to wait on")
	}
	waitCtx, cancel := context.WithTimeout(ctx, fanoutJournalWait)
	defer cancel()
	return s.journal.WaitVersion(waitCtx, version)
}

// refreshMetric re-reads a metric from the API's journal.
func (s *duckQuerySource) refreshMetric(metricID int32) *format.MetricMetaValue {
	if s.journal == nil {
		return nil
	}
	return s.journal.GetMetaMetric(metricID)
}

// buildStoreSeriesArgs lowers a semantic series request onto the RPC's series
// verb: the what kinds, the grouped tags (including the shard and string-top
// entries the renderer resolves itself), the host flags and the table-view
// ordering.
func buildStoreSeriesArgs(q *seriesDataQuery, lod data_model.LOD, timeoutMs int32) (tlstatshouse.StoreQuerySeries, error) {
	base, err := buildStoreQueryBase(q.metric, q.filterIn, q.filterNotIn, lod, q.utcOffset, timeoutMs)
	if err != nil {
		return tlstatshouse.StoreQuerySeries{}, err
	}
	args := tlstatshouse.StoreQuerySeries{
		Base: base,
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
	return args, nil
}

// buildStoreTagValuesArgs lowers a semantic tag-values request onto the RPC's
// tag-values verb. The user's top N is deliberately not carried: the shards
// must not apply it (a value ranked below N everywhere can still be globally
// top-N), the API sums counts across shards and applies N itself — per LOD,
// after the cross-shard merge, the way the ClickHouse source's SQL LIMIT
// does.
func buildStoreTagValuesArgs(q *tagValuesDataQuery, lod data_model.LOD, timeoutMs int32) (tlstatshouse.StoreQueryTagValues, error) {
	base, err := buildStoreQueryBase(q.metric, q.filterIn, q.filterNotIn, lod, q.utcOffset, timeoutMs)
	if err != nil {
		return tlstatshouse.StoreQueryTagValues{}, err
	}
	args := tlstatshouse.StoreQueryTagValues{
		Base:     base,
		TagIndex: int32(q.tag.Index),
	}
	if q.idsOnly {
		args.SetIdsOnly(true)
	}
	return args, nil
}

// singleAddressedMetric resolves the one metric a query is about, when there
// is one: the request's own metric when it carries a real id, else the lone
// member of the filter-in list. A PromQL aggregate replaces the queried
// metric with an empty placeholder (the engine's nilMetric) and addresses the
// real metric through that list, so the placeholder itself never counts —
// this mirrors the ClickHouse builder, whose metricID() reads the same
// placeholder as id 0 and lets writeMetricFilter fall through to the list.
func singleAddressedMetric(metric *format.MetricMetaValue, filterIn data_model.TagFilters) *format.MetricMetaValue {
	if metric != nil && metric.MetricID != 0 {
		return metric
	}
	if len(filterIn.Metrics) == 1 && filterIn.Metrics[0] != nil && filterIn.Metrics[0].MetricID != 0 {
		return filterIn.Metrics[0]
	}
	return nil
}

// buildStoreQueryBase fills the shared request base: the addressed metric
// (single id, or the in/not-in lists a multi-metric query carries), the tag
// layout as the API's journal derives it plus the journal version it was
// read at, the resolution window, and the tag filters with the same arm
// semantics the ClickHouse builder writes. A query that addresses several
// metrics with differing tag layouts is refused here rather than read through
// a layout that reinterprets some of them.
func buildStoreQueryBase(metric *format.MetricMetaValue, filterIn, filterNotIn data_model.TagFilters, lod data_model.LOD, utcOffset int64, timeoutMs int32) (tlstatshouse.StoreQueryBase, error) {
	base := tlstatshouse.StoreQueryBase{
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
	if single := singleAddressedMetric(metric, filterIn); single != nil {
		base.MetricId = single.MetricID
		base.MetricVersion = single.Version
		base.TagLayout = tlstatshouse.StoreTagLayout{Kinds: duckstore.TagLayoutKinds(single)}
		return base, nil
	}
	// no single metric: the filter's metric lists address the query, exactly
	// as the ClickHouse builder's writeMetricFilter treats them
	var metricIn []int32
	for _, m := range filterIn.Metrics {
		metricIn = append(metricIn, m.MetricID)
	}
	if len(metricIn) > 1 {
		base.SetMetricIn(metricIn)
		// The shards validate every listed member against the one layout this
		// request carries, so the members must agree on it; the first member
		// is the natural representative.
		kinds := duckstore.TagLayoutKinds(filterIn.Metrics[0])
		for _, m := range filterIn.Metrics[1:] {
			if !duckstore.TagLayoutsEqual(kinds, duckstore.TagLayoutKinds(m)) {
				return tlstatshouse.StoreQueryBase{}, fmt.Errorf(
					"metrics %d and %d have differing tag layouts: a multi-metric query cannot read both through one layout",
					filterIn.Metrics[0].MetricID, m.MetricID)
			}
		}
		base.TagLayout = tlstatshouse.StoreTagLayout{Kinds: kinds}
		// MetricVersion stays 0: several members carry several versions, so
		// there is no one version to wait for — the shards compare the layout
		// against their own journal without waiting.
	}
	var metricNotIn []int32
	for _, m := range filterNotIn.Metrics {
		metricNotIn = append(metricNotIn, m.MetricID)
	}
	if len(metricNotIn) > 0 {
		base.SetMetricNotIn(metricNotIn)
	}
	return base, nil
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

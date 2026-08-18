// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
)

// The DuckDB series renderer: one structured storeQuerySeries against this
// shard's store, answered as the columnar storeSeriesResponse.
//
// The read is a UNION ALL over the active delta generation, every
// rolled-but-unconsumed generation below it (one descriptor per archive
// window consumption has not yet taken from it, bounded by that window's own
// range) and every served archive window of the tier overlapping the range —
// one source descriptor per contribution, resolved into a single atomic
// snapshot (query_snapshot.go) with the files attached read-only on demand,
// the windows leased against retention and every generation pinned against
// consumption's unlink — followed by the outer GROUP BY that is the
// correctness mechanism: the answer is the same whether or not compaction has
// collapsed the rows it reads. Aggregate states (percentiles, uniques) come
// out of the GROUP BY as lists of blobs and are folded in Go before the
// reply, because DuckDB can neither merge nor re-import ClickHouse's states.
//
// Every request value — including RE2 patterns and unmapped string values —
// binds as a prepared-statement parameter and is never interpolated into the
// SQL text: DuckDB rejects ClickHouse's backslash escaping outright, so there
// is no transliteration of the existing escaping to fall back on.

// monthLodStep is the one step with a genuine timezone dependency: calendar
// months. Every other step truncates by integer arithmetic against
// utc_offset. The value is data_model's _1M: 31 days of seconds.
const monthLodStep = int64(2678400)

// lodStepTiers maps each of the 11 LOD steps onto the tier table the
// aggregator serves it from, mirroring data_model.LODTables.
var lodStepTiers = map[int64]string{
	1:       Tier1s,
	5:       Tier1s,
	15:      Tier1s,
	60:      Tier1m,
	300:     Tier1m,
	900:     Tier1m,
	3600:    Tier1h,
	14400:   Tier1h,
	86400:   Tier1h,
	604800:  Tier1h,
	2678400: Tier1h,
}

// queryAliasSeq numbers per-query ATTACH aliases, so two concurrent queries
// attaching windows to the shared delta instance never collide on a name.
var queryAliasSeq atomic.Int64

// seriesCols is the set of aggregate columns one query asks for, derived from
// its `what` list and host flags exactly the way the ClickHouse builder
// derives its SELECT list.
type seriesCols struct {
	count, min, max, sum, sumsquare, cardinality bool
	percentiles, uniq                            bool
	minHost, maxHost                             bool
}

// byCol is one `by` entry as the renderer resolved it against the tag layout.
// The shard entry (format.ShardTagIndex) has no storage: the response's tag
// column is filled from the shard number literal.
type byCol struct {
	index     int32  // format.ShardTagIndex or 0..format.MaxTags-1
	expr      string // SQL expression producing the tag value ("" for the shard entry)
	alias     string // its SELECT/GROUP BY alias
	stag      bool   // mapped kind: the string half is selected and grouped too
	stagExpr  string
	stagAlias string
}

// storeQueryPlan is the validated, tier-resolved form of one StoreQueryBase:
// the parts of a request that do not depend on which store files the query
// ends up reading. Both renderers build on it.
type storeQueryPlan struct {
	base     tlstatshouse.StoreQueryBase
	tier     string
	table    string
	from     int64
	to       int64
	rowLimit int
}

// planStoreQuery validates the request base every store query shares and
// resolves its tier. Every malformed base fails as bad_request, naming what
// was wrong.
func planStoreQuery(base tlstatshouse.StoreQueryBase) (*storeQueryPlan, error) {
	lod := base.Lod
	p := &storeQueryPlan{base: base, from: lod.FromSec, to: lod.ToSec}

	tier, known := lodStepTiers[lod.StepSec]
	if lod.StepSec <= 0 || !known {
		return nil, NewError(ErrCodeBadRequest, "step_sec %d is not a LOD step", lod.StepSec)
	}
	p.tier, p.table = tier, TierTable(tier)

	// an absent or oversized row limit takes the shard cap
	p.rowLimit = int(base.RowLimit)
	if p.rowLimit <= 0 || p.rowLimit > MaxSeriesRowLimit {
		p.rowLimit = MaxSeriesRowLimit
	}

	kinds := base.TagLayout.Kinds
	if len(kinds) > format.MaxTags {
		return nil, NewError(ErrCodeBadRequest, "tag layout holds %d kinds, more than the %d tags stored", len(kinds), format.MaxTags)
	}
	for i, k := range kinds {
		if k != tagKindMapped && k != tagKindRaw32 && k != tagKindRaw64 {
			return nil, NewError(ErrCodeBadRequest, "tag layout kind %d at index %d is not mapped, raw32 or raw64", k, i)
		}
	}
	return p, nil
}

// layoutIndex resolves one tag reference against the request's layout. The
// string top's flag alias names slot StringTopTagIndexV3, exactly as the
// ClickHouse builder's colIntV3 does. A reference inside the stored tag
// columns but beyond the layout's kinds — a group-over-everything query
// against a metric with fewer tags, or a metric-excluding query with no
// layout at all — reads the plain tag and string columns, the way the
// ClickHouse builder's writeSelectTagsV3 renders every grouped reference
// unconditionally: every row carries all the columns, zero and the empty
// string beyond its
// metric's own tags, so the reference groups by a constant and changes
// nothing. Anything outside [0, MaxTags) is a bad_request naming the
// offending reference.
func (p *storeQueryPlan) layoutIndex(x int32, what string) (int32, error) {
	if x == format.StringTopTagIndex {
		x = format.StringTopTagIndexV3
	}
	if x < 0 || int(x) >= format.MaxTags {
		return 0, NewError(ErrCodeBadRequest, "%s tag %d is outside the %d stored tag columns", what, x, format.MaxTags)
	}
	return x, nil
}

// kindAt returns the layout kind of tag slot x, with the slots beyond the
// layout's kinds reading as mapped tags: the tag-and-string column pair both
// backends read for a reference the layout does not describe (see
// layoutIndex).
func (p *storeQueryPlan) kindAt(x int32) int32 {
	if int(x) < len(p.base.TagLayout.Kinds) {
		return p.base.TagLayout.Kinds[x]
	}
	return tagKindMapped
}

// seriesPlan is the validated form of one storeQuerySeries on top of its base.
type seriesPlan struct {
	*storeQueryPlan
	args     tlstatshouse.StoreQuerySeries
	monthLod bool
	order    string // "", "ASC" or "DESC"
	whatMax  bool   // `what` includes max: max_host rides max, else max_count
	cols     seriesCols
	by       []byCol
}

// planSeriesQuery validates the request and resolves its columns.
func planSeriesQuery(args tlstatshouse.StoreQuerySeries) (*seriesPlan, error) {
	base, err := planStoreQuery(args.Base)
	if err != nil {
		return nil, err
	}
	p := &seriesPlan{storeQueryPlan: base, args: args}
	if p.monthLod = args.Base.Lod.StepSec == monthLodStep; p.monthLod {
		if _, err := time.LoadLocation(args.Base.Lod.Location); err != nil {
			return nil, NewError(ErrCodeBadRequest, "location %q is not an IANA time zone: %v", args.Base.Lod.Location, err)
		}
	}

	desc, asc := args.IsSetSortDesc(), args.IsSetSortAsc()
	switch {
	case desc && asc:
		return nil, NewError(ErrCodeBadRequest, "sort_desc and sort_asc are both set")
	case desc:
		p.order = "DESC"
	case asc:
		p.order = "ASC"
	}

	for _, w := range args.What {
		switch data_model.DigestWhat(w) {
		case data_model.DigestAvg:
			p.cols.count, p.cols.sum = true, true
		case data_model.DigestCount:
			p.cols.count = true
		case data_model.DigestMax:
			p.cols.max, p.whatMax = true, true
		case data_model.DigestMin:
			p.cols.min = true
		case data_model.DigestSum:
			p.cols.sum = true
		case data_model.DigestStdDev:
			p.cols.count, p.cols.sum, p.cols.sumsquare = true, true, true
		case data_model.DigestPercentile:
			p.cols.percentiles = true
		case data_model.DigestCardinality:
			p.cols.cardinality = true
		case data_model.DigestUnique:
			p.cols.uniq = true
		default:
			return nil, NewError(ErrCodeBadRequest, "what kind %d is not a digest selector", w)
		}
	}
	p.cols.minHost = args.IsSetMinHost()
	p.cols.maxHost = args.IsSetMaxHost()

	kinds := p.base.TagLayout.Kinds
	for _, ref := range args.By {
		if ref == format.ShardTagIndex {
			p.by = append(p.by, byCol{index: ref})
			continue
		}
		x, err := p.layoutIndex(ref, "by")
		if err != nil {
			return nil, err
		}
		bc := byCol{
			index:     x,
			expr:      fmt.Sprintf("tag%d::BIGINT", x),
			alias:     fmt.Sprintf("_tag%d", x),
			stagExpr:  fmt.Sprintf("stag%d", x),
			stagAlias: fmt.Sprintf("_stag%d", x),
		}
		switch p.kindAt(x) {
		case tagKindMapped:
			bc.stag = true
		case tagKindRaw64:
			if int(x)+1 >= format.MaxTags || int(x)+1 >= len(kinds) {
				return nil, NewError(ErrCodeBadRequest, "raw64 tag %d has no high half in the tag layout", x)
			}
			bc.expr = raw64ValueExpr(x)
		}
		p.by = append(p.by, bc)
	}
	return p, nil
}

// raw64ValueExpr rebuilds a raw64 tag's whole 64-bit value from its low and
// high halves, zero-extending each 32-bit column the way the ClickHouse
// builder's bitOr/bitShiftLeft expression does. The halves are combined in
// HUGEINT — wide enough that no intermediate overflows — and the result is
// wrapped back into signed 64-bit arithmetically, because this DuckDB build
// has no bit-shift function and its << operator overflow-checks, so no shift
// form can rebuild a value with its top bit set.
func raw64ValueExpr(tagX int32) string {
	lo := fmt.Sprintf("tag%d", tagX)
	hi := fmt.Sprintf("tag%d", tagX+1)
	v := "(" + hi + "::HUGEINT & 4294967295) * 4294967296 + (" + lo + "::HUGEINT & 4294967295)"
	return "((" + v + " - CASE WHEN " + v + " >= 9223372036854775808 THEN 18446744073709551616 ELSE 0 END)::BIGINT)"
}

// seriesQuerySQL is one built query: its text with $N placeholders and the
// arguments in order.
type seriesQuerySQL struct {
	sql  string
	args []any
}

// queryParams binds statement parameters in order, handing back $N
// placeholders. rangeParam additionally dedupes the time-range parameters by
// value, so sources sharing a range share one placeholder pair — today's
// uniform sources, every descriptor carrying the query's own [from, to),
// produce exactly the statement the retired qualifier-per-source path built.
type queryParams struct {
	args   []any
	ranges map[int64]string
}

func (q *queryParams) param(v any) string {
	q.args = append(q.args, v)
	return fmt.Sprintf("$%d", len(q.args))
}

// sourceArm builds one source's arm of the UNION ALL the read folds: the
// plain star over the source's own time column when the stored time is the
// tier's own bucket — every 1s-tier and archive-window arm, byte-identical to
// the retired qualifier-per-source path — and, for a delta arm serving a
// coarser tier, every column with the tier's truncation projected AS time and
// the same expression doing the range filtering, so an unaligned range never
// keeps a partial leading bucket nor drops a trailing one.
func (q *queryParams) sourceArm(src querySource) string {
	if src.timeExpr == "" {
		return "SELECT * FROM " + src.tableRef() +
			" WHERE time >= " + q.rangeParam(src.from) + " AND time < " + q.rangeParam(src.to)
	}
	return "SELECT " + derivedTierSelect(src.timeExpr) + " FROM " + src.tableRef() +
		" WHERE " + src.timeExpr + " >= " + q.rangeParam(src.from) +
		" AND " + src.timeExpr + " < " + q.rangeParam(src.to)
}

func (q *queryParams) rangeParam(v int64) string {
	if ph, ok := q.ranges[v]; ok {
		return ph
	}
	ph := q.param(v)
	if q.ranges == nil {
		q.ranges = make(map[int64]string)
	}
	q.ranges[v] = ph
	return ph
}

// buildSeriesSQL renders the plan into parameterized SQL over the given
// sources: one UNION ALL arm per source, each addressing the source's own
// table under its own qualifier and bounded by the range the source is
// allowed to contribute — the descriptor shape both renderers build on.
func buildSeriesSQL(p *seriesPlan, sources []querySource) (*seriesQuerySQL, error) {
	var qp queryParams
	param := qp.param
	base := p.base
	lod := base.Lod

	var sel []string
	if p.monthLod {
		// to the local wall clock in the requested zone, truncated to the
		// month, then re-attached to the zone before epoch — so _time is the
		// true instant of the local month start, exactly the unix seconds
		// ClickHouse's toStartOfInterval(time, INTERVAL 1 MONTH, '<loc>')
		// reports. Skipping the re-attachment would read the naive local
		// boundary as UTC, shifting every month bucket by the zone offset.
		loc := param(lod.Location)
		sel = append(sel, "(epoch(timezone("+loc+
			", date_trunc('month', timezone("+loc+
			", timezone('UTC', make_timestamp(time * 1000000)))))))::BIGINT AS _time")
	} else {
		utc := param(lod.UtcOffset)
		step := param(lod.StepSec)
		sel = append(sel, "((((time + "+utc+") // "+step+") * "+step+") - "+utc+") AS _time")
	}
	for i := range p.by {
		bc := &p.by[i]
		if bc.index == format.ShardTagIndex {
			continue // answered from the shard number literal, not from storage
		}
		sel = append(sel, bc.expr+" AS "+bc.alias)
		if bc.stag {
			sel = append(sel, bc.stagExpr+" AS "+bc.stagAlias)
		}
	}
	if p.cols.count {
		sel = append(sel, "sum(count) AS _count")
	}
	if p.cols.min {
		sel = append(sel, "min(min) AS _min")
	}
	if p.cols.max {
		sel = append(sel, "max(max) AS _max")
	}
	if p.cols.sum {
		sel = append(sel, "sum(sum) AS _sum")
	}
	if p.cols.sumsquare {
		sel = append(sel, "sum(sumsquare) AS _sumsquare")
	}
	if p.cols.cardinality {
		sel = append(sel, "CAST(count(*) AS DOUBLE) AS _cardinality")
	}
	if p.cols.percentiles {
		sel = append(sel, "list(percentiles) AS _pct_list")
	}
	if p.cols.uniq {
		sel = append(sel, "list(uniq_state) AS _uniq_list")
	}
	if p.cols.minHost {
		// one arg_min over a packed struct, so the value, the mapped id and
		// the raw string of the winning host always come from the same row.
		// The order key is the skewed comparison value ClickHouse keeps
		// inside the argMin state (drawn once per row by the conveyor — host
		// selection is value-weighted, not a plain extremum), and that skew
		// is also the value served back: the payload argMinMergeState
		// carries and every later merge orders by. Rows without a host sit
		// the aggregate out, the way ClickHouse's empty argMin states lose
		// to any real one.
		agg := "arg_min(struct_pack(v := min_host_value, i := min_host, s := min_shost), min_host_value)" +
			" FILTER (WHERE (min_host <> 0 OR min_shost <> ''))"
		sel = append(sel,
			"coalesce(("+agg+").v, 0) AS _min_host_value",
			"coalesce(("+agg+").i, 0) AS _min_host_tag",
			"coalesce(("+agg+").s, '') AS _min_host_stag")
	}
	if p.cols.maxHost {
		// the host of the max value when `what` includes max, else the host
		// of the max count — the ClickHouse builder's choice — ordered and
		// served by that column's own skewed state value, as min_host above
		valueCol, hostCol, hostSCol := "max_host_value", "max_host", "max_shost"
		if !p.whatMax {
			valueCol, hostCol, hostSCol = "max_count_host_value", "max_count_host", "max_count_shost"
		}
		agg := "arg_max(struct_pack(v := " + valueCol + ", i := " + hostCol + ", s := " + hostSCol + "), " + valueCol + ")" +
			" FILTER (WHERE (" + hostCol + " <> 0 OR " + hostSCol + " <> ''))"
		sel = append(sel,
			"coalesce(("+agg+").v, 0) AS _max_host_value",
			"coalesce(("+agg+").i, 0) AS _max_host_tag",
			"coalesce(("+agg+").s, '') AS _max_host_stag")
	}

	var arms []string
	for _, src := range sources {
		arms = append(arms, qp.sourceArm(src))
	}

	where, err := p.where(param)
	if err != nil {
		return nil, err
	}

	group := []string{"_time"}
	for i := range p.by {
		if p.by[i].index == format.ShardTagIndex {
			continue
		}
		group = append(group, p.by[i].alias)
		if p.by[i].stag {
			group = append(group, p.by[i].stagAlias)
		}
	}

	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(sel, ", "))
	sb.WriteString(" FROM (")
	sb.WriteString(strings.Join(arms, " UNION ALL "))
	sb.WriteString(") WHERE ")
	sb.WriteString(where)
	sb.WriteString(" GROUP BY ")
	sb.WriteString(strings.Join(group, ", "))
	if p.order != "" {
		// the builder's writeOrderBy shape: the plain column list with no
		// per-column directions and one trailing DESC when sorting
		// descending — _time and every column but the last stay ascending.
		// Which rows survive the table view's page truncation depends on
		// this order, so it must match ClickHouse's clause exactly.
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(group, ", "))
		if p.order == "DESC" {
			sb.WriteString(" DESC")
		}
	}
	sb.WriteString(" LIMIT " + param(int64(p.rowLimit+1)))
	return &seriesQuerySQL{sql: sb.String(), args: qp.args}, nil
}

// where renders the metric predicate and every tag filter, mirroring the
// ClickHouse builder: within one filter the mapped/values/empty/re2 arms are
// OR-ed for an IN filter and AND-ed (each arm negated) for a NOT IN filter.
func (p *storeQueryPlan) where(param func(any) string) (string, error) {
	base := p.base
	var preds []string

	hasIn := base.IsSetMetricIn() && len(base.MetricIn) > 0
	hasNotIn := base.IsSetMetricNotIn() && len(base.MetricNotIn) > 0
	if base.MetricId != 0 || (!hasIn && !hasNotIn) {
		preds = append(preds, "metric = "+param(int64(base.MetricId)))
	} else {
		if hasIn {
			preds = append(preds, "list_contains("+param(int64s(base.MetricIn))+", metric::BIGINT)")
		}
		if hasNotIn {
			preds = append(preds, "NOT list_contains("+param(int64s(base.MetricNotIn))+", metric::BIGINT)")
		}
	}

	for _, group := range []struct {
		filters []tlstatshouse.StoreTagFilter
		in      bool
	}{{base.FilterIn, true}, {base.FilterNotIn, false}} {
		for _, f := range group.filters {
			pred, err := p.tagFilterPred(f, group.in, param)
			if err != nil {
				return "", err
			}
			if pred != "" {
				preds = append(preds, pred)
			}
		}
	}
	return strings.Join(preds, " AND "), nil
}

// tagFilterPred renders one storeTagFilter into a parenthesized predicate, ""
// for a fully-empty filter (the builder's Empty() skip), or the always-false
// "(0!=0)" for an IN filter whose arms all rendered away — a raw tag's re2 and
// string-values arms never render, so such a filter has no row satisfying it.
func (p *storeQueryPlan) tagFilterPred(f tlstatshouse.StoreTagFilter, in bool, param func(any) string) (string, error) {
	x, err := p.layoutIndex(f.TagIndex, "filter on")
	if err != nil {
		return "", err
	}
	kinds := p.base.TagLayout.Kinds
	tagCol := fmt.Sprintf("tag%d", x)
	stagCol := fmt.Sprintf("stag%d", x)
	valueExpr := tagCol + "::BIGINT"
	if p.kindAt(x) == tagKindRaw64 {
		if int(x)+1 >= format.MaxTags || int(x)+1 >= len(kinds) {
			return "", NewError(ErrCodeBadRequest, "raw64 filter on tag %d has no high half in the tag layout", x)
		}
		valueExpr = raw64ValueExpr(x)
	}

	var arms []string
	if f.IsSetMapped() && len(f.Mapped) > 0 {
		if p.kindAt(x) == tagKindRaw64 {
			arms = append(arms, valueExpr+" = ANY("+param(f.Mapped)+")")
		} else {
			arms = append(arms, "list_contains("+param(f.Mapped)+", "+tagCol+"::BIGINT)")
		}
	}
	if p.kindAt(x) == tagKindMapped {
		// the string half exists only for a mapped tag; a raw tag's stag
		// column is unused and these arms are skipped, exactly as the
		// ClickHouse builder skips them for raw tags
		if f.IsSetRe2() && f.Re2 != "" {
			// the pattern arm replaces the values arm, not adds to it — the
			// builder's `else if`. Every negative-regex matcher arrives with
			// both set (the engine enumerates the non-matching values too),
			// and the values arm would then exclude rows the pattern keeps
			arms = append(arms, "regexp_matches("+stagCol+", "+param(f.Re2)+")")
		} else if f.IsSetValues() && len(f.Values) > 0 {
			arms = append(arms, "list_contains("+param(f.Values)+", "+stagCol+")")
		}
		if f.IsSetEmpty() {
			arms = append(arms, "("+tagCol+" = 0 AND "+stagCol+" = '')")
		}
	} else if f.IsSetEmpty() {
		arms = append(arms, valueExpr+" = 0")
	}
	if len(arms) == 0 {
		// the builder's empty-filter fallback: an IN filter with no rendered
		// arm has no row satisfying it (its literal 0!=0), while an empty NOT
		// IN filter is a nop (0=0), so dropping it is equivalent. A filter
		// with nothing set at all is the builder's Empty() skip above.
		if in && (f.IsSetMapped() || f.IsSetValues() || f.IsSetRe2() || f.IsSetEmpty()) {
			return "(0!=0)", nil
		}
		return "", nil
	}

	sep := " OR "
	if !in {
		sep = " AND "
		for i := range arms {
			arms[i] = "NOT (" + arms[i] + ")"
		}
	}
	return "(" + strings.Join(arms, sep) + ")", nil
}

// int64s widens an int32 slice for a list parameter.
func int64s(v []int32) []int64 {
	out := make([]int64, len(v))
	for i, x := range v {
		out[i] = int64(x)
	}
	return out
}

// RenderSeries executes one structured series query against the store and
// returns its columnar batches. The caller supplies this shard's number: it
// is not stored anywhere, and the response's shard-tag column is answered
// from that literal. Failures that are the request's fault come back as
// structured store errors (bad_request, row_limit); infrastructure failures
// come back plain for the server to map onto the remaining codes.
func (s *Store) RenderSeries(ctx context.Context, shardNum int32, args tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
	start := time.Now()
	resp, err := s.renderSeries(ctx, shardNum, args)
	recordQuery(s.cfg.Metrics, QuerySeries, start, err)
	return resp, err
}

func (s *Store) renderSeries(ctx context.Context, shardNum int32, args tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
	p, err := planSeriesQuery(args)
	if err != nil {
		return tlstatshouse.StoreSeriesResponse{}, err
	}
	var resp tlstatshouse.StoreSeriesResponse
	err = s.withQuerySources(ctx, p.tier, p.from, p.to, func(ctx context.Context, conn *sql.Conn, sources []querySource) error {
		q, err := buildSeriesSQL(p, sources)
		if err != nil {
			return err
		}
		rows, err := conn.QueryContext(ctx, q.sql, q.args...)
		if err != nil {
			return fmt.Errorf("duck-store: series query: %w", err)
		}
		defer rows.Close()
		resp, err = scanSeriesRows(rows, p, shardNum)
		return err
	})
	if err != nil {
		return tlstatshouse.StoreSeriesResponse{}, err
	}
	return resp, nil
}

// withQuerySources gathers everything one store query reads — one atomic
// snapshot of the store's query-relevant state (see querySnapshot) — and runs
// read against it on the snapshot's own connection, with one source
// descriptor per contribution: the active delta generation, addressed as the
// connection's own database; every rolled-but-unconsumed generation below
// it, attached read-only once and contributing one descriptor per archive
// window consumption has not yet taken from it, bounded by that window's own
// range; and every served archive window of the tier overlapping the range,
// each attached read-only under a unique alias. The snapshot pins every
// generation and leases every window, so neither consumption nor retention
// can remove a file underneath the read; the files are attached on demand
// and detached again after it — keeping them attached buys latency that is
// not needed and costs resident memory that is.
//
// Which candidate windows each rolled generation serves is decided inside,
// by serveQuerySources under the read locks of every window involved — after
// the snapshot, so a consumption committing in that gap is absorbed rather
// than answered wrong. The one interleaving that cannot be absorbed is the
// snapshot's own active generation being consumed into a window the query
// reads (a roll happened in between); the boundary reports it and this loop
// retries on a fresh snapshot, where the generation is rolled like any
// other.
func (s *Store) withQuerySources(ctx context.Context, tier string, from, to int64, read func(ctx context.Context, conn *sql.Conn, sources []querySource) error) error {
	// A consumption of the snapshot's own generation is rare and each retry
	// reads the new state, so a handful covers any realistic interleaving;
	// more means something else is wrong and the query fails loudly rather
	// than spin.
	const attempts = 5
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		snap, err := s.acquireQuerySnapshot(ctx, tier, from, to)
		if err != nil {
			return err
		}
		err = s.serveQuerySources(ctx, snap, tier, from, to, read)
		snap.release()
		if !errors.Is(err, errQuerySnapshotInvalidated) {
			return err
		}
		lastErr = err
	}
	return fmt.Errorf("duck-store: store query could not settle on a consistent view: %w", lastErr)
}

// serveQuerySources runs one read against a snapshot. The rolled generations
// are attached to the snapshot's connection read-only first — the same
// attachments the read itself addresses — and their window candidates are
// read through them; a generation is never opened as a second DuckDB handle
// for that (see resolveRolledWindows for why the instance cache forbids it).
// Then the read locks of every window involved — the snapshot's windows plus
// the candidates, deduplicated — are held for the serving boundary
// (resolveServingBoundary) and the read itself, so no consumption of any of
// those windows can commit while the query's source set stands: every row is
// served exactly once, from its generation or from the window that durably
// recorded it. LIFO with the defers below: detach the aliases, then hand the
// windows' read locks back; withQuerySources releases the snapshot — the
// leases, the pins and the connection go last, after nothing else addresses
// the files.
func (s *Store) serveQuerySources(ctx context.Context, snap *querySnapshot, tier string, from, to int64, read func(ctx context.Context, conn *sql.Conn, sources []querySource) error) error {
	// The alias is unique to this query, so two concurrent queries attaching
	// files to the shared delta instance never collide on a name. The rolled
	// generations' aliases come first — the plan read below addresses them —
	// and every alias is assigned with the DETACH defer registered before the
	// first ATTACH runs, so a failed or cancelled attach still detaches the
	// ones that made it — a leftover attachment would hold the file open on
	// this pooled connection and make every later attach of the same file to
	// it fail. Detaching an alias that never attached is a harmless ignored
	// error; a window's alias stays empty until the boundary has settled
	// which windows the read leases, so empty means not-yet-attached.
	seq := queryAliasSeq.Add(1)
	for i := range snap.rolled {
		snap.rolled[i].alias = fmt.Sprintf("q%d_g%d", seq, snap.rolled[i].gen)
	}
	defer func() {
		for i := range snap.windows {
			if snap.windows[i].src.alias != "" {
				_, _ = snap.conn.ExecContext(context.Background(), "DETACH "+snap.windows[i].src.alias)
			}
		}
		for i := range snap.rolled {
			_, _ = snap.conn.ExecContext(context.Background(), "DETACH "+snap.rolled[i].alias)
		}
	}()
	for i := range snap.rolled {
		if _, err := snap.conn.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS %s (READ_ONLY)",
			sqlString(snap.rolled[i].path), snap.rolled[i].alias)); err != nil {
			return fmt.Errorf("duck-store: attach %s for store query: %w", snap.rolled[i].path, err)
		}
	}
	if err := snap.resolveRolledWindows(ctx, tier, from, to); err != nil {
		return err
	}

	// The read lock of each window this query touches — one per file
	// (window_locks.go), not the store-global archive lock this replaced:
	// DuckDB allows a file one handle per process, so the read-only attach
	// must never overlap a maintenance open of the same file, but a
	// maintenance pass on any *other* window no longer fences this query.
	// lockWindowsRead nests them in the canonical sorted order; queries
	// still run concurrently with each other. The keys are deduplicated
	// first: a candidate window can already be one of the snapshot's served
	// windows, and one goroutine taking the same file's read lock twice —
	// legal in itself — can park its second taker behind a writer queued
	// between the two and deadlock the pair.
	keys := make([]windowKey, 0, len(snap.windows))
	seen := make(map[windowKey]struct{}, len(snap.windows))
	for i := range snap.windows {
		if _, ok := seen[snap.windows[i].src.key]; !ok {
			seen[snap.windows[i].src.key] = struct{}{}
			keys = append(keys, snap.windows[i].src.key)
		}
	}
	for i := range snap.rolled {
		for _, start := range snap.rolled[i].windows {
			k := windowKey{tier: tier, start: start}
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				keys = append(keys, k)
			}
		}
	}
	if len(keys) > 0 {
		releaseWindows := s.lockWindowsRead(keys)
		defer releaseWindows()
	}

	// Which windows each rolled generation serves — and whether the snapshot
	// still describes a consistent view at all — is decided here, under the
	// locks above: a consumption of any window involved would need that
	// window's write lock first, so the decision stands for the whole read.
	// A generation none of whose windows survive stays attached — the
	// detachment is one ignored error away — but contributes no sources.
	if !s.resolveServingBoundary(snap, tier, from, to) {
		return errQuerySnapshotInvalidated
	}

	for i := range snap.windows {
		snap.windows[i].src.alias = fmt.Sprintf("q%d_a%d", seq, i)
	}
	for i := range snap.windows {
		if _, err := snap.conn.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS %s (READ_ONLY)",
			sqlString(snap.windows[i].path), snap.windows[i].src.alias)); err != nil {
			return fmt.Errorf("duck-store: attach %s for store query: %w", snap.windows[i].path, err)
		}
	}

	// The active delta serves the tier's buckets out of its 1s rows through
	// the truncation; the 1s tier itself reads the plain time column.
	active := querySource{
		kind: fileKindDelta,
		gen:  snap.gen,
		from: from,
		to:   to,
	}
	if tier == Tier1s {
		active.table = tierTables[Tier1s]
	} else {
		active.table, active.timeExpr = tierTables[Tier1s], tierTimeExpr(tier)
	}
	sources := make([]querySource, 0, len(snap.windows)+1)
	sources = append(sources, active)
	sources = append(sources, snap.rolledSources(tier, from, to)...)
	for i := range snap.windows {
		sources = append(sources, snap.windows[i].src)
	}
	return read(ctx, snap.conn, sources)
}

// seriesScanRow is the scan target for one result row, in the SELECT list's
// order; the blob lists are folded into strings before the row joins a batch.
type seriesScanRow struct {
	time                                         int64
	tags                                         []int64
	stags                                        []string
	count, min, max, sum, sumsquare, cardinality float64
	pctList, uniqList                            any
	minHostValue                                 float64
	minHostTag                                   int32
	minHostStag                                  string
	maxHostValue                                 float64
	maxHostTag                                   int32
	maxHostStag                                  string
}

// scanSeriesRows consumes the query's rows into the response, folding the two
// aggregate-state columns per row and cutting batches at the target size. LIMIT
// row_limit+1 ran query-side: an extra row means truncation, and the whole
// call fails with row_limit rather than return — and let the API cache — a
// partial series result.
func scanSeriesRows(rows *sql.Rows, p *seriesPlan, shardNum int32) (tlstatshouse.StoreSeriesResponse, error) {
	r := seriesScanRow{tags: make([]int64, len(p.by)), stags: make([]string, len(p.by))}
	var dest []any
	dest = append(dest, &r.time)
	for i := range p.by {
		if p.by[i].index == format.ShardTagIndex {
			r.tags[i] = int64(shardNum) // constant: answered from the literal
			continue
		}
		dest = append(dest, &r.tags[i])
		if p.by[i].stag {
			dest = append(dest, &r.stags[i])
		}
	}
	if p.cols.count {
		dest = append(dest, &r.count)
	}
	if p.cols.min {
		dest = append(dest, &r.min)
	}
	if p.cols.max {
		dest = append(dest, &r.max)
	}
	if p.cols.sum {
		dest = append(dest, &r.sum)
	}
	if p.cols.sumsquare {
		dest = append(dest, &r.sumsquare)
	}
	if p.cols.cardinality {
		dest = append(dest, &r.cardinality)
	}
	if p.cols.percentiles {
		dest = append(dest, &r.pctList)
	}
	if p.cols.uniq {
		dest = append(dest, &r.uniqList)
	}
	if p.cols.minHost {
		dest = append(dest, &r.minHostValue, &r.minHostTag, &r.minHostStag)
	}
	if p.cols.maxHost {
		dest = append(dest, &r.maxHostValue, &r.maxHostTag, &r.maxHostStag)
	}

	b := newSeriesBatcher(p)
	n := 0
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return tlstatshouse.StoreSeriesResponse{}, fmt.Errorf("duck-store: series query row: %w", err)
		}
		n++
		if n > p.rowLimit {
			return tlstatshouse.StoreSeriesResponse{}, NewError(ErrCodeRowLimit,
				"series query produced at least %d rows, above the %d-row limit", n, p.rowLimit)
		}
		var pct, uniq string
		if p.cols.percentiles {
			blobs, err := blobList(r.pctList)
			if err != nil {
				return tlstatshouse.StoreSeriesResponse{}, fmt.Errorf("duck-store: percentiles of the row at %d: %w", r.time, err)
			}
			folded, err := foldPercentiles(blobs)
			if err != nil {
				return tlstatshouse.StoreSeriesResponse{}, fmt.Errorf("duck-store: percentiles of the row at %d: %w", r.time, err)
			}
			pct = string(folded)
		}
		if p.cols.uniq {
			blobs, err := blobList(r.uniqList)
			if err != nil {
				return tlstatshouse.StoreSeriesResponse{}, fmt.Errorf("duck-store: uniq_state of the row at %d: %w", r.time, err)
			}
			folded, err := foldUniques(blobs)
			if err != nil {
				return tlstatshouse.StoreSeriesResponse{}, fmt.Errorf("duck-store: uniq_state of the row at %d: %w", r.time, err)
			}
			uniq = string(folded)
		}
		b.add(&r, pct, uniq)
	}
	if err := rows.Err(); err != nil {
		return tlstatshouse.StoreSeriesResponse{}, fmt.Errorf("duck-store: series query: %w", err)
	}
	return b.response(shardNum), nil
}

// seriesBatcher accumulates the scan's rows into the response's single batch,
// one column vector per grouped tag (the shard entry's vector filled from the
// literal) and one column per requested aggregate.
type seriesBatcher struct {
	cols    seriesCols
	by      []byCol
	batches []tlstatshouse.StoreSeriesBatch
	cur     tlstatshouse.StoreSeriesBatch
}

func newSeriesBatcher(p *seriesPlan) *seriesBatcher {
	b := &seriesBatcher{cols: p.cols, by: p.by}
	b.reset()
	return b
}

func (b *seriesBatcher) reset() {
	b.cur = tlstatshouse.StoreSeriesBatch{
		Tag:  make([][]int64, len(b.by)),
		Stag: make([][]string, len(b.by)),
	}
}

// add appends one row to the batch.
func (b *seriesBatcher) add(r *seriesScanRow, pct, uniq string) {
	b.cur.Time = append(b.cur.Time, r.time)
	for i := range b.by {
		b.cur.Tag[i] = append(b.cur.Tag[i], r.tags[i])
		if b.by[i].stag {
			b.cur.Stag[i] = append(b.cur.Stag[i], r.stags[i])
		}
	}
	if b.cols.count {
		b.cur.Count = append(b.cur.Count, r.count)
	}
	if b.cols.min {
		b.cur.Min = append(b.cur.Min, r.min)
	}
	if b.cols.max {
		b.cur.Max = append(b.cur.Max, r.max)
	}
	if b.cols.sum {
		b.cur.Sum = append(b.cur.Sum, r.sum)
	}
	if b.cols.sumsquare {
		b.cur.Sumsquare = append(b.cur.Sumsquare, r.sumsquare)
	}
	if b.cols.cardinality {
		b.cur.Cardinality = append(b.cur.Cardinality, r.cardinality)
	}
	if b.cols.percentiles {
		b.cur.Percentiles = append(b.cur.Percentiles, pct)
	}
	if b.cols.uniq {
		b.cur.UniqState = append(b.cur.UniqState, uniq)
	}
	if b.cols.minHost {
		b.cur.MinHostValue = append(b.cur.MinHostValue, r.minHostValue)
		b.cur.MinHostTag = append(b.cur.MinHostTag, r.minHostTag)
		b.cur.MinHostStag = append(b.cur.MinHostStag, r.minHostStag)
	}
	if b.cols.maxHost {
		b.cur.MaxHostValue = append(b.cur.MaxHostValue, r.maxHostValue)
		b.cur.MaxHostTag = append(b.cur.MaxHostTag, r.maxHostTag)
		b.cur.MaxHostStag = append(b.cur.MaxHostStag, r.maxHostStag)
	}
}

// flush seals the current batch, unless it is empty.
func (b *seriesBatcher) flush() {
	if len(b.cur.Time) == 0 {
		return
	}
	b.cur.Rows = int32(len(b.cur.Time))
	if b.cols.count {
		b.cur.SetCount(b.cur.Count)
	}
	if b.cols.min {
		b.cur.SetMin(b.cur.Min)
	}
	if b.cols.max {
		b.cur.SetMax(b.cur.Max)
	}
	if b.cols.sum {
		b.cur.SetSum(b.cur.Sum)
	}
	if b.cols.sumsquare {
		b.cur.SetSumsquare(b.cur.Sumsquare)
	}
	if b.cols.cardinality {
		b.cur.SetCardinality(b.cur.Cardinality)
	}
	if b.cols.percentiles {
		b.cur.SetPercentiles(b.cur.Percentiles)
	}
	if b.cols.uniq {
		b.cur.SetUniqState(b.cur.UniqState)
	}
	if b.cols.minHost {
		b.cur.FieldsMask |= (1 << 8)
	}
	if b.cols.maxHost {
		b.cur.FieldsMask |= (1 << 9)
	}
	b.batches = append(b.batches, b.cur)
	b.reset()
}

func (b *seriesBatcher) response(shardNum int32) tlstatshouse.StoreSeriesResponse {
	b.flush()
	return tlstatshouse.StoreSeriesResponse{ShardNum: shardNum, Batches: b.batches}
}

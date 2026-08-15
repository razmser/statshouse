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
// The read is a UNION ALL over the active delta generation and every served
// archive window of the tier overlapping the range — windows attached
// read-only on demand, leased against retention and detached again — followed
// by the outer GROUP BY that is the correctness mechanism: the answer is the
// same whether or not compaction has collapsed the rows it reads. Aggregate
// states (percentiles, uniques) come out of the GROUP BY as lists of blobs and
// are folded in Go before the reply, because DuckDB can neither merge nor
// re-import ClickHouse's states.
//
// Every request value — including RE2 patterns and unmapped string values —
// binds as a prepared-statement parameter and is never interpolated into the
// SQL text: DuckDB rejects ClickHouse's backslash escaping outright, so there
// is no transliteration of the existing escaping to fall back on.

// seriesBatchTargetBytes is the target size of one storeSeriesBatch. It
// matches the API transport's chunk size, because a batch is the unit a
// future streaming RPC would emit one at a time. It is a variable only so
// tests can shrink it; production leaves it alone.
var seriesBatchTargetBytes = 10_000_000

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
// ClickHouse builder's colIntV3 does; anything else outside the layout is a
// bad_request naming the offending reference.
func (p *storeQueryPlan) layoutIndex(x int32, what string) (int32, error) {
	if x == format.StringTopTagIndex {
		x = format.StringTopTagIndexV3
	}
	if x < 0 || int(x) >= format.MaxTags || int(x) >= len(p.base.TagLayout.Kinds) {
		return 0, NewError(ErrCodeBadRequest, "%s tag %d is outside the tag layout of %d kinds", what, x, len(p.base.TagLayout.Kinds))
	}
	return x, nil
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
		switch kinds[x] {
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

// buildSeriesSQL renders the plan into parameterized SQL over the given
// sources: an empty qualifier addresses the delta (the connection's own
// database), anything else is an attached archive window's alias.
func buildSeriesSQL(p *seriesPlan, sources []string) (*seriesQuerySQL, error) {
	var args []any
	param := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}
	base := p.base
	lod := base.Lod

	var sel []string
	if p.monthLod {
		// to the local wall clock in the requested zone, truncated to the
		// month, and read back as the UTC unix seconds of that boundary
		sel = append(sel, "(epoch(date_trunc('month', timezone("+param(lod.Location)+
			", timezone('UTC', make_timestamp(time * 1000000))))))::BIGINT AS _time")
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
		// the raw string of the winning host always come from the same row;
		// rows without a host sit the aggregate out, the way ClickHouse's
		// empty argMin states lose to any real one
		agg := "arg_min(struct_pack(v := min, i := min_host, s := min_shost), min)" +
			" FILTER (WHERE (min_host <> 0 OR min_shost <> ''))"
		sel = append(sel,
			"coalesce(("+agg+").v, 0) AS _min_host_value",
			"coalesce(("+agg+").i, 0) AS _min_host_tag",
			"coalesce(("+agg+").s, '') AS _min_host_stag")
	}
	if p.cols.maxHost {
		// the host of the max value when `what` includes max, else the host
		// of the max count — the ClickHouse builder's choice
		valueCol, hostCol, hostSCol := "max", "max_host", "max_shost"
		if !p.whatMax {
			valueCol, hostCol, hostSCol = "max_count", "max_count_host", "max_count_shost"
		}
		agg := "arg_max(struct_pack(v := " + valueCol + ", i := " + hostCol + ", s := " + hostSCol + "), " + valueCol + ")" +
			" FILTER (WHERE (" + hostCol + " <> 0 OR " + hostSCol + " <> ''))"
		sel = append(sel,
			"coalesce(("+agg+").v, 0) AS _max_host_value",
			"coalesce(("+agg+").i, 0) AS _max_host_tag",
			"coalesce(("+agg+").s, '') AS _max_host_stag")
	}

	from, to := param(p.from), param(p.to)
	var arms []string
	for _, src := range sources {
		table := p.table
		if src != "" {
			table = src + "." + table
		}
		arms = append(arms, "SELECT * FROM "+table+" WHERE time >= "+from+" AND time < "+to)
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
		ordered := make([]string, len(group))
		for i, g := range group {
			ordered[i] = g + " " + p.order
		}
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(ordered, ", "))
	}
	sb.WriteString(" LIMIT " + param(int64(p.rowLimit+1)))
	return &seriesQuerySQL{sql: sb.String(), args: args}, nil
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

// tagFilterPred renders one storeTagFilter into a parenthesized predicate, or
// "" for a filter with no arms at all (the builder's Empty() skip).
func (p *storeQueryPlan) tagFilterPred(f tlstatshouse.StoreTagFilter, in bool, param func(any) string) (string, error) {
	x, err := p.layoutIndex(f.TagIndex, "filter on")
	if err != nil {
		return "", err
	}
	kinds := p.base.TagLayout.Kinds
	tagCol := fmt.Sprintf("tag%d", x)
	stagCol := fmt.Sprintf("stag%d", x)
	valueExpr := tagCol + "::BIGINT"
	if kinds[x] == tagKindRaw64 {
		if int(x)+1 >= format.MaxTags || int(x)+1 >= len(kinds) {
			return "", NewError(ErrCodeBadRequest, "raw64 filter on tag %d has no high half in the tag layout", x)
		}
		valueExpr = raw64ValueExpr(x)
	}

	var arms []string
	if f.IsSetMapped() && len(f.Mapped) > 0 {
		if kinds[x] == tagKindRaw64 {
			arms = append(arms, valueExpr+" = ANY("+param(f.Mapped)+")")
		} else {
			arms = append(arms, "list_contains("+param(f.Mapped)+", "+tagCol+"::BIGINT)")
		}
	}
	if kinds[x] == tagKindMapped {
		// the string half exists only for a mapped tag; a raw tag's stag
		// column is unused and these arms are skipped, exactly as the
		// ClickHouse builder skips them for raw tags
		if f.IsSetValues() && len(f.Values) > 0 {
			arms = append(arms, "list_contains("+param(f.Values)+", "+stagCol+")")
		}
		if f.IsSetRe2() && f.Re2 != "" {
			arms = append(arms, "regexp_matches("+stagCol+", "+param(f.Re2)+")")
		}
		if f.IsSetEmpty() {
			arms = append(arms, "("+tagCol+" = 0 AND "+stagCol+" = '')")
		}
	} else if f.IsSetEmpty() {
		arms = append(arms, valueExpr+" = 0")
	}
	if len(arms) == 0 {
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
	p, err := planSeriesQuery(args)
	if err != nil {
		return tlstatshouse.StoreSeriesResponse{}, err
	}
	var resp tlstatshouse.StoreSeriesResponse
	err = s.withQuerySources(ctx, p.tier, p.from, p.to, func(ctx context.Context, conn *sql.Conn, sources []string) error {
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

// withQuerySources gathers everything one store query reads — the active
// delta generation plus every served archive window of the tier overlapping
// the range — and runs read against them: the delta's connection with one
// source qualifier per file ("" for the delta itself, a unique alias per
// attached window). Windows are leased so retention cannot unlink them under
// the read — a window whose lease comes back nil was dropped in between and
// is simply absent, the same as under ClickHouse after a TTL pass — attached
// read-only on demand and detached again after the read: keeping them
// attached buys latency that is not needed and costs resident memory that
// is.
func (s *Store) withQuerySources(ctx context.Context, tier string, from, to int64, read func(ctx context.Context, conn *sql.Conn, sources []string) error) error {
	type queryWindow struct {
		alias string
		path  string
		lease *Lease
	}
	var wins []queryWindow
	for _, wf := range s.Windows() {
		if wf.Tier != tier || wf.WindowStart >= to || wf.WindowStart+tierWindowSecs[tier] <= from {
			continue
		}
		l := s.AcquireWindowLease(wf.Tier, wf.WindowStart)
		if l == nil {
			continue
		}
		wins = append(wins, queryWindow{path: wf.Path, lease: l})
	}
	if len(wins) > 0 {
		// Shared with window maintenance for as long as a window is
		// attached: DuckDB allows a file one handle per process, so the
		// read-only attach must never overlap a maintenance open of the same
		// file. Queries still run concurrently with each other.
		s.archiveMu.RLock()
		defer s.archiveMu.RUnlock()
	}
	for i := range wins {
		defer wins[i].lease.Release()
	}

	conn, err := s.deltaConn(ctx)
	if err != nil {
		return fmt.Errorf("duck-store: store query connection: %w", err)
	}
	defer conn.Close()

	// The alias is unique to this query, so two concurrent queries attaching
	// windows to the shared delta instance never collide on a name.
	seq := queryAliasSeq.Add(1)
	for i := range wins {
		alias := fmt.Sprintf("q%d_a%d", seq, i)
		if _, err := conn.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS %s (READ_ONLY)", sqlString(wins[i].path), alias)); err != nil {
			return fmt.Errorf("duck-store: attach %s for store query: %w", wins[i].path, err)
		}
		wins[i].alias = alias
	}
	defer func() {
		for i := range wins {
			if wins[i].alias != "" {
				_, _ = conn.ExecContext(context.Background(), "DETACH "+wins[i].alias)
			}
		}
	}()

	sources := make([]string, 0, len(wins)+1)
	sources = append(sources, "") // the delta is the connection's own database
	for i := range wins {
		sources = append(sources, wins[i].alias)
	}
	return read(ctx, conn, sources)
}

// deltaConn checks a connection out of the active delta generation's pool. A
// generation roll swaps the pool under the store lock and closes the old one,
// which waits for in-flight queries; a query racing the swap retries once on
// the new pool rather than failing.
func (s *Store) deltaConn(ctx context.Context) (*sql.Conn, error) {
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		db := s.Delta()
		if db == nil {
			return nil, fmt.Errorf("duck-store: store is closed")
		}
		var conn *sql.Conn
		conn, err = db.Conn(ctx)
		if err == nil {
			return conn, nil
		}
	}
	return nil, err
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
// sketch columns per row and cutting batches at the target size. LIMIT
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

// seriesBatcher accumulates rows into size-bounded batches, one column vector
// per grouped tag (the shard entry's vector filled from the literal) and one
// column per requested aggregate.
type seriesBatcher struct {
	cols    seriesCols
	by      []byCol
	batches []tlstatshouse.StoreSeriesBatch
	cur     tlstatshouse.StoreSeriesBatch
	est     int
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

// add appends one row and flushes the batch when it passes the target size.
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

	b.est += b.rowBytes(r, pct, uniq)
	if b.est >= seriesBatchTargetBytes && len(b.cur.Time) > 0 {
		b.flush()
	}
}

// rowBytes estimates one row's serialized size, the number the batch target
// is judged against: 8 per number, a length prefix per string.
func (b *seriesBatcher) rowBytes(r *seriesScanRow, pct, uniq string) int {
	n := 8 + 8*len(b.by)
	for i := range b.by {
		if b.by[i].stag {
			n += 4 + len(r.stags[i])
		}
	}
	for _, present := range []struct {
		on  bool
		add int
	}{
		{b.cols.count, 8}, {b.cols.min, 8}, {b.cols.max, 8},
		{b.cols.sum, 8}, {b.cols.sumsquare, 8}, {b.cols.cardinality, 8},
		{b.cols.percentiles, 4 + len(pct)}, {b.cols.uniq, 4 + len(uniq)},
		{b.cols.minHost, 16 + 4 + len(r.minHostStag)},
		{b.cols.maxHost, 16 + 4 + len(r.maxHostStag)},
	} {
		if present.on {
			n += present.add
		}
	}
	return n
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
	b.est = 0
}

func (b *seriesBatcher) response(shardNum int32) tlstatshouse.StoreSeriesResponse {
	b.flush()
	return tlstatshouse.StoreSeriesResponse{ShardNum: shardNum, Batches: b.batches}
}

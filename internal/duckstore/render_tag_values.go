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
	"time"

	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/format"
)

// The DuckDB tag-values renderer: one structured storeQueryTagValues against
// this shard's store, answering the distinct values of one tag with the
// counts of the rows that carry them. It is the same read shape as the
// series renderer — a UNION ALL over the active delta and every served
// archive window of the tier, parameterized throughout — with the outer
// GROUP BY folding partial rows into one (value, count) pair per distinct
// value, so the answer is the same whether or not compaction has collapsed
// the rows it reads.
//
// The one rule the request's own top N plays no part in: a value ranked
// below N on every shard can still be globally top-N once counts are summed
// across shards, so applying N here would silently return wrong answers.
// The shard returns every value up to row_limit — a safety cap, never the
// user's N — and fails the whole call when truncation would occur, leaving
// the API to sum counts across shards and take the global top N itself.

// tagValuesPlan is the validated form of one storeQueryTagValues on top of
// its base.
type tagValuesPlan struct {
	*storeQueryPlan
	args tlstatshouse.StoreQueryTagValues
	tag  int32  // resolved tag index, the string top alias already folded to its slot
	expr string // SQL expression producing the tag's value
	// stag is set for a mapped tag outside ids-only mode: the value is the
	// (id, unmapped string) pair, exactly the ClickHouse builder's
	// buildTagValuesQuery projection. A raw tag's string half is unused and
	// the response's stag vector stays empty.
	stag bool
}

// planTagValuesQuery validates the request and resolves its tag. The two
// modes differ only in the projection: values mode reads the string half of
// a mapped tag, ids-only mode does not.
func planTagValuesQuery(args tlstatshouse.StoreQueryTagValues) (*tagValuesPlan, error) {
	bp, err := planStoreQuery(args.Base)
	if err != nil {
		return nil, err
	}
	// The shard pseudo-tag has no per-row storage to read — a tag-values
	// query over it is not a store question at all, and the API never builds
	// one (the tag comes from the metric's own entity).
	if args.TagIndex == format.ShardTagIndex {
		return nil, NewError(ErrCodeBadRequest, "tag values on tag %d: the shard tag is not a stored tag", args.TagIndex)
	}
	x, err := bp.layoutIndex(args.TagIndex, "tag values on")
	if err != nil {
		return nil, err
	}
	p := &tagValuesPlan{storeQueryPlan: bp, args: args, tag: x}
	switch bp.base.TagLayout.Kinds[x] {
	case tagKindRaw64:
		// the value is the whole 64 bits rebuilt from the two stored halves
		if int(x)+1 >= format.MaxTags || int(x)+1 >= len(bp.base.TagLayout.Kinds) {
			return nil, NewError(ErrCodeBadRequest, "raw64 tag %d has no high half in the tag layout", x)
		}
		p.expr = raw64ValueExpr(x)
	case tagKindMapped:
		p.expr = fmt.Sprintf("tag%d::BIGINT", x)
		if !args.IsSetIdsOnly() {
			p.stag = true
		}
	default:
		p.expr = fmt.Sprintf("tag%d::BIGINT", x)
	}
	return p, nil
}

// buildTagValuesSQL renders the plan into parameterized SQL over the given
// sources: an empty qualifier addresses the delta (the connection's own
// database), anything else is an attached archive window's alias. The shape
// mirrors the ClickHouse builder's buildTagValuesQueryEx — group by the tag
// (and its string half in values mode), count by summed `count`, zero-count
// groups dropped — with the user's ORDER BY ... LIMIT N+1 replaced by the
// safety cap and a deterministic order, because the global top N belongs to
// the API, after the shards' counts are summed.
func buildTagValuesSQL(p *tagValuesPlan, sources []string) (*seriesQuerySQL, error) {
	var args []any
	param := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("$%d", len(args))
	}

	var sel []string
	sel = append(sel, p.expr+" AS _tag")
	if p.stag {
		sel = append(sel, fmt.Sprintf("stag%d AS _stag", p.tag))
	}
	sel = append(sel, "sum(count) AS _count")

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

	group := []string{"_tag"}
	if p.stag {
		group = append(group, "_stag")
	}
	order := append([]string{"_count DESC"}, group...)

	var sb strings.Builder
	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(sel, ", "))
	sb.WriteString(" FROM (")
	sb.WriteString(strings.Join(arms, " UNION ALL "))
	sb.WriteString(") WHERE ")
	sb.WriteString(where)
	sb.WriteString(" GROUP BY ")
	sb.WriteString(strings.Join(group, ", "))
	sb.WriteString(" HAVING _count > 0")
	sb.WriteString(" ORDER BY ")
	sb.WriteString(strings.Join(order, ", "))
	sb.WriteString(" LIMIT " + param(int64(p.rowLimit+1)))
	return &seriesQuerySQL{sql: sb.String(), args: args}, nil
}

// RenderTagValues executes one structured tag-values query against the store.
// Failures that are the request's fault come back as structured store errors
// (bad_request, row_limit); infrastructure failures come back plain for the
// server to map onto the remaining codes.
func (s *Store) RenderTagValues(ctx context.Context, args tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
	start := time.Now()
	resp, err := s.renderTagValues(ctx, args)
	recordQuery(s.cfg.Metrics, QueryTagValues, start, err)
	return resp, err
}

func (s *Store) renderTagValues(ctx context.Context, args tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
	p, err := planTagValuesQuery(args)
	if err != nil {
		return tlstatshouse.StoreTagValuesResponse{}, err
	}
	var resp tlstatshouse.StoreTagValuesResponse
	err = s.withQuerySources(ctx, p.tier, p.from, p.to, func(ctx context.Context, conn *sql.Conn, sources []string) error {
		q, err := buildTagValuesSQL(p, sources)
		if err != nil {
			return err
		}
		rows, err := conn.QueryContext(ctx, q.sql, q.args...)
		if err != nil {
			return fmt.Errorf("duck-store: tag values query: %w", err)
		}
		defer rows.Close()
		resp, err = scanTagValuesRows(rows, p)
		return err
	})
	if err != nil {
		return tlstatshouse.StoreTagValuesResponse{}, err
	}
	return resp, nil
}

// scanTagValuesRows consumes the query's rows into the response. LIMIT
// row_limit+1 ran query-side: an extra row means truncation, and the whole
// call fails with row_limit rather than return — and let the API take a top
// N over — a partial value set.
func scanTagValuesRows(rows *sql.Rows, p *tagValuesPlan) (tlstatshouse.StoreTagValuesResponse, error) {
	var tag int64
	var stag string
	var count float64
	dest := []any{&tag}
	if p.stag {
		dest = append(dest, &stag)
	}
	dest = append(dest, &count)

	var resp tlstatshouse.StoreTagValuesResponse
	for rows.Next() {
		if err := rows.Scan(dest...); err != nil {
			return tlstatshouse.StoreTagValuesResponse{}, fmt.Errorf("duck-store: tag values query row: %w", err)
		}
		if len(resp.Tag) >= p.rowLimit {
			return tlstatshouse.StoreTagValuesResponse{}, NewError(ErrCodeRowLimit,
				"tag values query produced at least %d rows, above the %d-row limit", p.rowLimit+1, p.rowLimit)
		}
		resp.Tag = append(resp.Tag, tag)
		if p.stag {
			resp.Stag = append(resp.Stag, stag)
		}
		resp.Count = append(resp.Count, count)
	}
	if err := rows.Err(); err != nil {
		return tlstatshouse.StoreTagValuesResponse{}, fmt.Errorf("duck-store: tag values query: %w", err)
	}
	return resp, nil
}

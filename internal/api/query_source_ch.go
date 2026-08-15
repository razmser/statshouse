// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
)

// chQuerySource serves semantic queries from ClickHouse exactly as the API did
// before the QuerySource seam: it rebuilds today's queryBuilder from the
// semantic request, generates SQL with buildSeriesQuery/buildTagValuesQuery
// and executes it through requestHandler.doSelect over the existing client
// pool. SQL text, query metadata, error mapping and row decoding are the
// pre-seam code, moved rather than rewritten.
type chQuerySource struct {
	// selectFn, when set, replaces requestHandler.doSelect. Tests use it to
	// capture the generated SQL and feed synthetic result blocks; production
	// leaves it nil.
	selectFn func(ctx context.Context, h *requestHandler, meta chutil.QueryMetaInto, query ch.Query) error
}

func (s chQuerySource) doSelect(ctx context.Context, h *requestHandler, meta chutil.QueryMetaInto, query ch.Query) error {
	if s.selectFn != nil {
		return s.selectFn(ctx, h, meta, query)
	}
	return h.doSelect(ctx, meta, query)
}

// queryBuilder rebuilds the pre-seam queryBuilder for a semantic series
// request, restoring the fields loadPoints/loadPoint used to set directly.
func (q *seriesDataQuery) queryBuilder() *queryBuilder {
	return &queryBuilder{
		user:        q.user,
		metric:      q.metric,
		what:        q.what,
		by:          q.by,
		filterIn:    q.filterIn,
		filterNotIn: q.filterNotIn,
		sort:        q.sort,
		minMaxHost:  q.minMaxHost,
		point:       q.point,
		utcOffset:   q.utcOffset,
	}
}

// queryBuilder rebuilds the pre-seam queryBuilder for a semantic tag-values
// request.
func (q *tagValuesDataQuery) queryBuilder() *queryBuilder {
	return &queryBuilder{
		user:        q.user,
		metric:      q.metric,
		filterIn:    q.filterIn,
		filterNotIn: q.filterNotIn,
		tag:         q.tag,
		numResults:  q.numResults,
		utcOffset:   q.utcOffset,
	}
}

// seriesQueryMeta is the query metadata loadPoints/loadPoint/the tag-values
// handlers attached to every doSelect call, unchanged.
func seriesQueryMeta(h *requestHandler, pq *queryBuilder, lod *data_model.LOD, isLight, isHardware bool) chutil.QueryMetaInto {
	sharded := pq.metric.Sharded()
	return chutil.QueryMetaInto{
		IsFast:         lod.IsFast(),
		IsLight:        isLight,
		IsHardware:     isHardware,
		User:           pq.user,
		Metric:         pq.metric,
		Table:          lod.Table(sharded),
		Sharded:        sharded,
		DisableCHAddrs: h.disabledCHAddrs(),
	}
}

func (s chQuerySource) querySeries(ctx context.Context, h *requestHandler, q *seriesDataQuery, lod data_model.LOD, onRow func(tsSelectRow) error) error {
	pq := q.queryBuilder()
	query, err := pq.buildSeriesQuery(lod, h.getSelectSettings())
	if err != nil {
		return err
	}
	start := time.Now()
	err = s.doSelect(ctx, h, seriesQueryMeta(h, pq, &lod, query.isLight(), query.isHardware()), ch.Query{
		Body:   query.body,
		Result: query.res,
		OnResult: func(_ context.Context, block proto.Block) error {
			select {
			case <-ctx.Done():
				return nil // no client. Clickhouse still process query. Just ignore it
			default:
			}
			for i := 0; i < block.Rows; i++ {
				var err error
				if q.point {
					// point rows decode without time and string tags, exactly
					// as the pre-seam loadPoint did
					p := query.rowAtPoint(i)
					err = onRow(tsSelectRow{tsTags: p.tsTags, tsValues: p.tsValues})
				} else {
					err = onRow(query.rowAt(i))
				}
				if err != nil {
					return err
				}
			}
			return nil
		}})
	h.reportQueryDuration(query.body, time.Since(start))
	return err
}

func (s chQuerySource) queryTagValues(ctx context.Context, h *requestHandler, q *tagValuesDataQuery, lod data_model.LOD, onRow func(selectRow) error) error {
	pq := q.queryBuilder()
	var query *tagValuesQuery
	if q.idsOnly {
		query = pq.buildTagValueIDsQuery(lod, h.getSelectSettings())
	} else {
		query = pq.buildTagValuesQuery(lod, h.getSelectSettings())
	}
	return s.doSelect(ctx, h, seriesQueryMeta(h, pq, &lod, true, false), ch.Query{
		Body:   query.body,
		Result: query.res,
		OnResult: func(_ context.Context, block proto.Block) error {
			for i := 0; i < block.Rows; i++ {
				if err := onRow(query.rowAt(i)); err != nil {
					return err
				}
			}
			return nil
		}})
}

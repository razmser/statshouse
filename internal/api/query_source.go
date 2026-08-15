// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"context"
	"fmt"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
)

// QuerySource is the seam between the API's request handling and the storage
// backend that serves metric data. It sits above SQL generation: both methods
// take a semantic request plus an LOD and deliver decoded rows, so the two
// backends share semantics, not SQL. The ClickHouse implementation
// (query_source_ch.go) rebuilds today's queryBuilder and runs it through the
// existing client pool unchanged; the duck implementation fans the request out
// across aggregator shards over the structured query RPC.
type QuerySource interface {
	// querySeries executes a series, table-view or point query and hands
	// every decoded row to onRow, streaming. It returns after the last row
	// or the first error, whichever comes first.
	querySeries(ctx context.Context, h *requestHandler, q *seriesDataQuery, lod data_model.LOD, onRow func(tsSelectRow) error) error
	// queryTagValues executes a tag-values query in either mode — full
	// values or ids-only (q.idsOnly, today's buildTagValueIDsQuery) — and
	// hands every decoded (tag value, count) row to onRow.
	queryTagValues(ctx context.Context, h *requestHandler, q *tagValuesDataQuery, lod data_model.LOD, onRow func(selectRow) error) error
}

// seriesDataQuery is the semantic series request: everything the storage
// backend needs to know about a series/table/point query except the queried
// time window, which the LOD carries. It maps one-to-one onto the structured
// query RPC's series verb.
type seriesDataQuery struct {
	user        string
	metric      *format.MetricMetaValue // nil for multi-metric PromQL queries, which carry metrics in filterIn
	what        tsWhat                  // empty is legal: timestamps and grouped tags only
	by          []int
	filterIn    data_model.TagFilters
	filterNotIn data_model.TagFilters
	sort        querySort // table view ordering; sortNone for plain series
	minMaxHost  [2]bool   // "min" at [0], "max" at [1]
	point       bool      // point query: rows carry values and tags only, no time
	utcOffset   int64
}

// tagValuesDataQuery is the semantic tag-values request, covering both of
// today's tag-values query modes.
type tagValuesDataQuery struct {
	user        string
	metric      *format.MetricMetaValue
	tag         format.MetricMetaTag
	numResults  int // user's top N; the source applies only its own safety caps
	filterIn    data_model.TagFilters
	filterNotIn data_model.TagFilters
	idsOnly     bool
	utcOffset   int64
}

// newQuerySource picks the QuerySource for the configured storage backend.
func newQuerySource(backend duckstore.StorageBackend) QuerySource {
	if backend == duckstore.BackendDuck {
		// Replaced by the fan-out implementation over the structured query
		// RPC; until then duck is selected but not yet servable.
		return duckQuerySourcePending{}
	}
	return chQuerySource{}
}

// duckQuerySourcePending rejects every query until the duck source lands.
type duckQuerySourcePending struct{}

func (duckQuerySourcePending) querySeries(ctx context.Context, h *requestHandler, q *seriesDataQuery, lod data_model.LOD, onRow func(tsSelectRow) error) error {
	return errDuckQuerySourcePending
}

func (duckQuerySourcePending) queryTagValues(ctx context.Context, h *requestHandler, q *tagValuesDataQuery, lod data_model.LOD, onRow func(selectRow) error) error {
	return errDuckQuerySourcePending
}

var errDuckQuerySourcePending = fmt.Errorf("the duck storage backend does not serve API queries in this build yet")

// querySource returns the storage backend this API process reads metric data
// from. Handlers constructed without one (tests) fall back to ClickHouse,
// which preserves the pre-seam behaviour of bare handlers.
func (h *requestHandler) querySource() QuerySource {
	if h.Handler != nil && h.Handler.querySource != nil {
		return h.Handler.querySource
	}
	return chQuerySource{}
}

// seriesQueryFromBuilder lifts a queryBuilder — the request currency the
// loaders and their callers still speak — into the semantic series request.
// Every field that reaches SQL or row decoding is carried over; cache-key and
// cache2 play-mode fields stay behind because no storage backend sees them.
func seriesQueryFromBuilder(b *queryBuilder) *seriesDataQuery {
	return &seriesDataQuery{
		user:        b.user,
		metric:      b.metric,
		what:        b.what,
		by:          b.by,
		filterIn:    b.filterIn,
		filterNotIn: b.filterNotIn,
		sort:        b.sort,
		minMaxHost:  b.minMaxHost,
		point:       b.point,
		utcOffset:   b.utcOffset,
	}
}

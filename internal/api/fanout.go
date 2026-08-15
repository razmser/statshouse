// Copyright 2025 V Kontate LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/VKCOM/tl/pkg/rpc"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
)

// The duck backend's shard fan-out: the structured query RPC the aggregators
// serve has no Distributed-table equivalent, so the API issues the same
// request to every shard the query may touch in parallel and merges the
// per-shard partial aggregates in Go — with the cross-LOD state merge the API
// already runs (tsValues.merge), not a new one.
//
// Everything here is pure Go: the API never embeds DuckDB, it only speaks the
// TL protocol, so no duckdb build tag guards this file.

// fanoutRowCap is the global post-merge row cap, matching the per-shard cap
// (duckstore.MaxSeriesRowLimit) the aggregators clamp every request to. A
// merged result above it fails the whole query, exactly as one shard tripping
// its own cap does — no partial result is ever cached. It is a variable only
// so tests can shrink it; production leaves it alone.
var fanoutRowCap = int(duckstore.MaxSeriesRowLimit)

// storeShardClient is one aggregator shard as the fan-out sees it: its 1-based
// shard number, its store-query address, and the two query verbs. Production
// rides the TL client; tests supply fakes.
type storeShardClient interface {
	shardNum() uint32
	addr() string
	querySeries(ctx context.Context, args tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error)
	queryTagValues(ctx context.Context, args tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error)
}

// rpcStoreShardClient serves the two store-query verbs over a real TL
// connection. All shards share one rpc.Client (its connection pool is
// per-address); each shard keeps its own address.
type rpcStoreShardClient struct {
	shard uint32
	where string
	tl    *tlstatshouse.Client
}

// newRPCStoreShardClients builds one client per configured shard, sharing a
// single rpc connection pool. cryptoKey is presented at every handshake: the
// aggregator's store-query listener sits in another container/process, so the
// nonce exchange requires encryption and rejects a keyless client. Clients
// are returned sorted by shard number — the order the merge relies on for
// its deterministic tie-breaking.
func newRPCStoreShardClients(addrs map[uint32]string, cryptoKey string) []*rpcStoreShardClient {
	shards := make([]uint32, 0, len(addrs))
	for shard := range addrs {
		shards = append(shards, shard)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	rpcClient := rpc.NewClient(rpc.ClientWithProtocolVersion(rpc.LatestProtocolVersion), rpc.ClientWithCryptoKey(cryptoKey))
	clients := make([]*rpcStoreShardClient, len(shards))
	for i, shard := range shards {
		clients[i] = &rpcStoreShardClient{
			shard: shard,
			where: addrs[shard],
			tl:    &tlstatshouse.Client{Client: rpcClient, Network: "tcp4", Address: addrs[shard]},
		}
	}
	return clients
}

func (c *rpcStoreShardClient) shardNum() uint32 { return c.shard }
func (c *rpcStoreShardClient) addr() string     { return c.where }

func (c *rpcStoreShardClient) querySeries(ctx context.Context, args tlstatshouse.StoreQuerySeries) (tlstatshouse.StoreSeriesResponse, error) {
	var resp tlstatshouse.StoreSeriesResponse
	err := c.tl.StoreQuerySeries(ctx, args, nil, &resp)
	return resp, err
}

func (c *rpcStoreShardClient) queryTagValues(ctx context.Context, args tlstatshouse.StoreQueryTagValues) (tlstatshouse.StoreTagValuesResponse, error) {
	var resp tlstatshouse.StoreTagValuesResponse
	err := c.tl.StoreQueryTagValues(ctx, args, nil, &resp)
	return resp, err
}

// fanoutCall runs fn against every client in parallel and collects each
// result. The first failure cancels the others' context — a query with one
// dead shard is already dead — and the collected error is the first failure
// by shard order, wrapped with the shard's number and address.
func fanoutCall[T any](ctx context.Context, clients []storeShardClient, fn func(ctx context.Context, c storeShardClient) (T, error)) ([]T, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]T, len(clients))
	errs := make([]error, len(clients))
	var wg sync.WaitGroup
	for i, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := fn(ctx, c)
			results[i] = res
			errs[i] = err
			if err != nil {
				cancel() // one dead shard kills the query; stop the rest
			}
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return nil, fmt.Errorf("duck shard %d (%s): %w", clients[i].shardNum(), clients[i].addr(), err)
		}
	}
	return results, nil
}

// decodeSeriesResponse lowers one shard's columnar batches into decoded rows,
// in batch order. The response's optional columns are exactly the ones the
// request's what and host flags selected; a missing column leaves the
// corresponding tsValues field zero, like the ClickHouse decode does.
func decodeSeriesResponse(q *seriesDataQuery, resp tlstatshouse.StoreSeriesResponse) ([]tsSelectRow, error) {
	var rows []tsSelectRow
	for bi := range resp.Batches {
		b := &resp.Batches[bi]
		n := int(b.Rows)
		if n < 0 {
			return nil, fmt.Errorf("duck shard %d: batch holds %d rows", resp.ShardNum, n)
		}
		if len(b.Tag) != len(q.by) {
			return nil, fmt.Errorf("duck shard %d: batch holds %d tag columns for %d grouped tags",
				resp.ShardNum, len(b.Tag), len(q.by))
		}
		for i := 0; i < n; i++ {
			row := tsSelectRow{what: q.what, time: at(b.Time, i)}
			for j, x := range q.by {
				v := at(b.Tag[j], i)
				switch x {
				case format.ShardTagIndex:
					row.shardNum = uint32(v)
				case format.StringTopTagIndex:
					row.tag[format.StringTopTagIndexV3] = v
					row.stag[format.StringTopTagIndexV3] = atStag(b.Stag, j, i)
				default:
					row.tag[x] = v
					row.stag[x] = atStag(b.Stag, j, i)
				}
			}
			if len(b.Count) != 0 {
				row.count = at(b.Count, i)
			}
			if len(b.Min) != 0 {
				row.min = at(b.Min, i)
			}
			if len(b.Max) != 0 {
				row.max = at(b.Max, i)
			}
			if len(b.Sum) != 0 {
				row.sum = at(b.Sum, i)
			}
			if len(b.Sumsquare) != 0 {
				row.sumsquare = at(b.Sumsquare, i)
			}
			if len(b.Cardinality) != 0 {
				row.cardinality = at(b.Cardinality, i)
			}
			if len(b.Percentiles) != 0 {
				td, err := duckstore.DecodeTDigestState([]byte(at(b.Percentiles, i)))
				if err != nil {
					return nil, fmt.Errorf("duck shard %d: percentiles of the row at %d: %w", resp.ShardNum, row.time, err)
				}
				row.percentile = td
			}
			if len(b.UniqState) != 0 {
				var u data_model.ChUnique
				buf := bytes.NewBuffer([]byte(at(b.UniqState, i)))
				if err := u.MergeRead(buf); err != nil {
					return nil, fmt.Errorf("duck shard %d: uniq_state of the row at %d: %w", resp.ShardNum, row.time, err)
				}
				if buf.Len() != 0 {
					return nil, fmt.Errorf("duck shard %d: uniq_state of the row at %d holds %d trailing bytes", resp.ShardNum, row.time, buf.Len())
				}
				row.unique = u
			}
			if len(b.MinHostValue) != 0 {
				row.minHost = data_model.ArgMinInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{
					Arg: at(b.MinHostTag, i), Val: float32(at(b.MinHostValue, i)),
				}}
				row.minHostStr = data_model.ArgMinStringFloat32{ArgMinMaxStringFloat32: data_model.ArgMinMaxStringFloat32{
					AsInt32: at(b.MinHostTag, i), AsString: at(b.MinHostStag, i), Val: float32(at(b.MinHostValue, i)),
				}}
			}
			if len(b.MaxHostValue) != 0 {
				row.maxHost = data_model.ArgMaxInt32Float32{ArgMinMaxInt32Float32: data_model.ArgMinMaxInt32Float32{
					Arg: at(b.MaxHostTag, i), Val: float32(at(b.MaxHostValue, i)),
				}}
				row.maxHostStr = data_model.ArgMaxStringFloat32{ArgMinMaxStringFloat32: data_model.ArgMinMaxStringFloat32{
					AsInt32: at(b.MaxHostTag, i), AsString: at(b.MaxHostStag, i), Val: float32(at(b.MaxHostValue, i)),
				}}
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}

// mergeShardRows merges the per-shard decoded rows into one row per
// (time, tags, shard-tag) key, folding the partial aggregates with the
// existing cross-LOD state merge. Rows arrive grouped per shard in shard
// order, and are merged in that order, so every source row participates in
// exactly one merge and nothing is finalized mid-way: the merged states are
// only handed to the caller — and from there to percentile/unique
// evaluation — after every shard's contribution is folded in. Host ties
// resolve to the lower shard's row, deterministically.
//
// The merged rows come back ordered by (time, tags, shard number), matching
// the ClickHouse builder's ascending ORDER BY shape.
// tsMergeKey is a merged row's identity: the timestamp plus the grouped
// tags. tsTags alone is not enough — it does not carry time, so rows of the
// same series at different timestamps would fold into one.
type tsMergeKey struct {
	time int64
	tags tsTags
}

func mergeShardRows(perShard [][]tsSelectRow, rowCap int) ([]tsSelectRow, error) {
	var total int
	for _, rows := range perShard {
		total += len(rows)
	}
	merged := make([]tsSelectRow, 0, total)
	index := make(map[tsMergeKey]int)
	for _, rows := range perShard {
		for i := range rows {
			key := tsMergeKey{time: rows[i].time, tags: rows[i].tsTags}
			ix, ok := index[key]
			if !ok {
				if len(merged) >= rowCap {
					return nil, fmt.Errorf("duck fan-out merged at least %d rows, above the %d-row cap", len(merged)+1, rowCap)
				}
				index[key] = len(merged)
				merged = append(merged, rows[i])
				continue
			}
			merged[ix].tsValues.merge(rows[i].tsValues)
		}
	}
	sort.Slice(merged, func(i, j int) bool { return lessTsSelectRow(&merged[i], &merged[j]) })
	return merged, nil
}

// lessTsSelectRow orders rows by (time, tags, shard number) — the comparator
// behind the deterministic merged-row order.
func lessTsSelectRow(l, r *tsSelectRow) bool {
	if l.time != r.time {
		return l.time < r.time
	}
	for x := 0; x < format.MaxTags; x++ {
		if l.tag[x] != r.tag[x] {
			return l.tag[x] < r.tag[x]
		}
	}
	for x := 0; x < format.MaxTags; x++ {
		if l.stag[x] != r.stag[x] {
			return l.stag[x] < r.stag[x]
		}
	}
	return l.shardNum < r.shardNum
}

// decodeTagValuesResponse lowers one shard's tag-values response into
// decoded (value, count) rows.
func decodeTagValuesResponse(resp tlstatshouse.StoreTagValuesResponse) ([]selectRow, error) {
	if len(resp.Tag) != len(resp.Count) || len(resp.Stag) > len(resp.Count) {
		return nil, fmt.Errorf("duck shard response holds %d tag, %d stag and %d count columns",
			len(resp.Tag), len(resp.Stag), len(resp.Count))
	}
	rows := make([]selectRow, len(resp.Count))
	for i := range resp.Count {
		rows[i] = selectRow{
			valID: at(resp.Tag, i),
			val:   at(resp.Stag, i),
			cnt:   resp.Count[i],
		}
	}
	return rows, nil
}

// tagValueKey is a tag value's identity for the cross-shard count sum: the
// mapped id and the unmapped string, never the per-shard count.
type tagValueKey struct {
	valID int64
	val   string
}

// mergeTagValueRows sums the per-shard tag-value counts into one row per
// distinct (id, string) value — the global counts the top N is taken over.
// Per-shard top-N would be silently wrong: a value ranked below N on every
// shard can still be globally top-N, which is why the shards return every
// value up to their safety cap and only this merge sees N. The merged rows
// come back ordered by count descending, ties by value, so the caller's top N
// is deterministic; a merged set above rowCap fails the whole query, matching
// the per-shard cap's all-or-nothing semantics.
func mergeTagValueRows(perShard [][]selectRow, rowCap int) ([]selectRow, error) {
	counts := make(map[tagValueKey]float64)
	var order []selectRow
	for _, rows := range perShard {
		for _, r := range rows {
			key := tagValueKey{valID: r.valID, val: r.val}
			if _, ok := counts[key]; !ok {
				if len(order) >= rowCap {
					return nil, fmt.Errorf("duck fan-out merged at least %d tag values, above the %d-row cap", len(order)+1, rowCap)
				}
				order = append(order, r)
			}
			counts[key] += r.cnt
		}
	}
	for i := range order {
		order[i].cnt = counts[tagValueKey{valID: order[i].valID, val: order[i].val}]
	}
	sort.Slice(order, func(i, j int) bool {
		if order[i].cnt != order[j].cnt {
			return order[i].cnt > order[j].cnt
		}
		if order[i].valID != order[j].valID {
			return order[i].valID < order[j].valID
		}
		return order[i].val < order[j].val
	})
	return order, nil
}

// at returns v[i], treating a short column as all-zero — the contract keeps
// absent columns nil, and a present column shorter than Rows is a malformed
// response worth reading as zeros rather than panicking on.
func at[T any](v []T, i int) (zero T) {
	if i < len(v) {
		return v[i]
	}
	return zero
}

// atStag is at for the per-grouped-tag string columns: the outer slice is
// parallel to `by` and the contract may omit entries (raw tags emit no stag
// column) or the whole slice.
func atStag(v [][]string, j, i int) string {
	if j < len(v) && i < len(v[j]) {
		return v[j][i]
	}
	return ""
}

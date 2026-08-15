// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"context"
	"encoding/binary"
	"math"
	"net/http"
	"time"

	"pgregory.net/rand"

	"github.com/VKCOM/statshouse/internal/chutil"
	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/metajournal"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

// hostPair is one resolved arg_min_max host column: the host tag plus the skewed
// tie-break value it is inserted with. An empty tag means the empty aggregate.
type hostPair struct {
	tag   data_model.TagUnion
	value float32
}

// insertRow is one fully resolved row of an insert round, the form every storage
// backend must see. Row resolution — sampling factors, per-metric skips and
// unknown-tag bookkeeping — happens once, in shared code, so backends cannot
// disagree about what a row is. Sketch columns carry ClickHouse aggregate state
// bytes verbatim, which is also what duck-store keeps in its BLOB columns.
type insertRow struct {
	key         data_model.Key      // metric, time and tag0..tag15; the string top slot stays empty
	top         data_model.TagUnion // value written into the string top slot (usually empty)
	count       float64             // count and max_count, sampling factor applied
	min         float64
	max         float64
	sum         float64 // sampling factor applied
	sumSquare   float64 // sampling factor applied, zeroed when the metric skips it
	percentiles []byte  // tdigest state bytes, sampling factor applied; empty state when none
	unique      []byte  // uniq state bytes; empty state when none

	minHost      hostPair
	maxHost      hostPair
	maxCountHost hostPair
}

// InsertSink is the storage seam of the insert conveyor: it receives the resolved
// rows of one insert round and hands the whole round to storage. Rows arrive in
// conveyor order, the same order the pre-seam RowBinary body was built in.
type InsertSink interface {
	// AppendRow adds one resolved row to the pending round and returns the row's
	// RowBinary size in bytes, which feeds the insertSize accounting. The row's
	// sketch slices are valid only until the next AppendRow call.
	AppendRow(row *insertRow) int
	// Send delivers the pending round, returning the status, exception code,
	// elapsed time and error of the insert — the quadruple the conveyor reacts to.
	Send(ctx context.Context) (status int, exception int, elapsed time.Duration, err error)
	// RoundSize is the byte size of the pending round, as reported by the
	// agg insert size metric.
	RoundSize() int
	// Reset drops the pending round, keeping buffers for the next one.
	Reset()
}

// duckStoreHandle is the aggregator's lifecycle handle on its duck-store: it
// owns the single writer goroutine all insert threads share, produces their
// sinks and serves store queries. The concrete type exists only in
// duckdb-tagged builds; without the tag the duck backend is rejected during
// config validation and the handle stays nil.
type duckStoreHandle interface {
	NewSink() InsertSink
	// QueryExecutor serves the store query listener from the shard's store,
	// validating requests against the metrics journal and answering with this
	// shard's number.
	QueryExecutor(storage *metajournal.MetricsStorage, shardNum int32) storeQueryExecutor
	Close() error
}

// newInsertSink returns the sink one insert thread writes its rounds through:
// the duck-store writer when the duck backend is selected, today's
// ClickHouse inserter otherwise.
func (a *Aggregator) newInsertSink(httpClient *http.Client) InsertSink {
	if a.duckStore != nil {
		return a.duckStore.NewSink()
	}
	return newClickhouseSink(httpClient, a.config.KHAddr, a.config.KHUser, a.config.KHPassword, getTableDesc(),
		func() string { // insert settings can change through the remote config
			a.configMu.RLock()
			defer a.configMu.RUnlock()
			return a.configR.V3InsertSettings
		})
}

// rowBinarySize returns the number of bytes appendRowBinary produces for row,
// computed without building them. Every sink reports this per row, so the
// insertSize accounting stays identical regardless of the storage backend.
func rowBinarySize(row *insertRow) int {
	n := 1 + 4 + 4 // index_type, metric, time
	for ki := 0; ki < format.MaxTags; ki++ {
		if ki == format.StringTopTagIndexV3 {
			continue // the string top pair below takes this slot
		}
		n += tagBinarySize(row.key.STags[ki], row.key.Tags[ki])
	}
	n += tagBinarySize(row.top.S, row.top.I)
	n += 6 * 8 // count, max_count, min, max, sum, sumsquare
	n += len(row.percentiles) + len(row.unique)
	n += argMinMaxTagBinarySize(row.minHost)
	n += argMinMaxTagBinarySize(row.maxHost)
	n += argMinMaxTagBinarySize(row.maxCountHost)
	return n
}

// tagBinarySize is the size of appendTagBinary's output: the uint32 id plus a
// RowBinary string — always empty when an id is set.
func tagBinarySize(s string, i int32) int {
	if i != 0 || s == "" {
		return 4 + 1
	}
	var tmp [10]byte
	return 4 + binary.PutUvarint(tmp[:], uint64(len(s))) + len(s)
}

// argMinMaxTagBinarySize is the size of appendArgMinMaxTag's output for one
// host pair: the empty encoding, or AppendArgMinMaxBytesFloat32's framing —
// the uint32 size, the packed tag argument, the bool terminator and the
// float. The 4-byte placeholder appendArgMinMaxTag writes first is
// overwritten by that framing (it exists to keep the append aliasing-safe),
// so the tag bytes are counted exactly once.
func argMinMaxTagBinarySize(h hostPair) int {
	if h.tag.Empty() {
		return 5 // AppendArgMinMaxStringEmpty
	}
	argLen := 1 + len(h.tag.S) // string-kind discriminator + bytes
	if h.tag.I != 0 {
		argLen = 1 + 4 // int-kind discriminator + uint32
	}
	return 4 + argLen + 2 + 4
}

// clickhouseSink posts each round as one RowBinary insert over HTTP, byte for
// byte the way the aggregator inserted before the seam existed.
type clickhouseSink struct {
	httpClient *http.Client
	khAddr     string
	khUser     string
	khPassword string
	table      string
	settings   func() string // insert settings can change through the remote config
	body       []byte
}

func newClickhouseSink(httpClient *http.Client, khAddr, khUser, khPassword, table string, settings func() string) *clickhouseSink {
	return &clickhouseSink{
		httpClient: httpClient,
		khAddr:     khAddr,
		khUser:     khUser,
		khPassword: khPassword,
		table:      table,
		settings:   settings,
	}
}

func (s *clickhouseSink) AppendRow(row *insertRow) int {
	pos := len(s.body)
	s.body = appendRowBinary(s.body, row)
	return len(s.body) - pos
}

func (s *clickhouseSink) Send(ctx context.Context) (int, int, time.Duration, error) {
	return sendToClickhouse(ctx, s.httpClient, s.khAddr, s.khUser, s.khPassword, s.table, s.body, s.settings())
}

func (s *clickhouseSink) RoundSize() int { return len(s.body) }

func (s *clickhouseSink) Reset() { s.body = s.body[:0] }

// appendRowBinary encodes one resolved row in ClickHouse RowBinary format, in the
// column order of the incoming table.
func appendRowBinary(res []byte, row *insertRow) []byte {
	res = append(res, 0) // index_type
	res = binary.LittleEndian.AppendUint32(res, uint32(row.key.Metric))
	res = binary.LittleEndian.AppendUint32(res, row.key.Timestamp)
	for ki := 0; ki < format.MaxTags; ki++ {
		if ki == format.StringTopTagIndexV3 {
			continue
		}
		res = appendTagBinary(res, row.key.STags[ki], row.key.Tags[ki])
	}
	res = appendTagBinary(res, row.top.S, row.top.I) // string top
	res = binary.LittleEndian.AppendUint64(res, math.Float64bits(row.count))
	res = binary.LittleEndian.AppendUint64(res, math.Float64bits(row.count)) // max_count
	res = binary.LittleEndian.AppendUint64(res, math.Float64bits(row.min))
	res = binary.LittleEndian.AppendUint64(res, math.Float64bits(row.max))
	res = binary.LittleEndian.AppendUint64(res, math.Float64bits(row.sum))
	res = binary.LittleEndian.AppendUint64(res, math.Float64bits(row.sumSquare))
	res = append(res, row.percentiles...)
	res = append(res, row.unique...)
	res = appendArgMinMaxTag(res, row.minHost.tag, row.minHost.value)
	res = appendArgMinMaxTag(res, row.maxHost.tag, row.maxHost.value)
	res = appendArgMinMaxTag(res, row.maxCountHost.tag, row.maxCountHost.value)
	return res
}

func appendTagBinary(res []byte, S string, I int32) []byte {
	if I != 0 || S == "" { // if we somehow have both I and S, we prefer I
		res = binary.LittleEndian.AppendUint32(res, uint32(I))
		return rowbinary.AppendString(res, "")
	}
	res = binary.LittleEndian.AppendUint32(res, 0)
	return rowbinary.AppendString(res, S)
}

// resolveKeyTags records unknown string tags of the row's key (including the
// string top) so mappings are created after sampling, even for skipped rows.
func resolveKeyTags(row *insertRow, appendCtx appendContext) {
	for ki := 0; ki < format.MaxTags; ki++ {
		if ki == format.StringTopTagIndexV3 {
			continue
		}
		if row.key.Tags[ki] == 0 && row.key.STags[ki] != "" {
			processUnknownTag(row.key.STags[ki], appendCtx)
		}
	}
	if row.top.I == 0 && row.top.S != "" {
		processUnknownTag(row.top.S, appendCtx)
	}
}

// resolveHosts resolves the three host columns, drawing the skew values in the
// same order and under the same conditions as the conveyor always has.
func resolveHosts(rng *rand.Rand, row *insertRow, count float64, v *data_model.ItemValue, skipMaxHost bool, skipMinHost bool, appendCtx appendContext) {
	// min_host
	processUnknownTag(v.MinHostTag.S, appendCtx) // even if skip, to optimize agent-aggregator traffic
	if v.ValueSet && !skipMinHost && !v.MinHostTag.Empty() {
		row.minHost = hostPair{v.MinHostTag, float32(data_model.SkewMinMaxHost(rng, v.ValueMin))} // explanation is in Skew function
	}
	// max_host
	processUnknownTag(v.MaxHostTag.S, appendCtx) // even if skip, to optimize agent-aggregator traffic
	if v.ValueSet && !skipMaxHost && !v.MaxHostTag.Empty() {
		row.maxHost = hostPair{v.MaxHostTag, float32(data_model.SkewMinMaxHost(rng, v.ValueMax))} // explanation is in Skew function
	}
	// max_count_host
	processUnknownTag(v.MaxCounterHostTag.S, appendCtx) // even if skip, to optimize agent-aggregator traffic
	if !v.MaxCounterHostTag.Empty() {
		row.maxCountHost = hostPair{v.MaxCounterHostTag, float32(data_model.SkewMaxCounterHost(rng, count))} // explanation is in Skew function
	}
}

// resolveMultiValueRow resolves one conveyor row — the tail or one string top of
// an item — applying the sampling factor and the per-metric skips. scratch is
// the reusable buffer the sketch state bytes are encoded into; the returned
// buffer is the same storage rewound for the next row, so the row's sketch
// slices stay valid only until the next resolve.
func resolveMultiValueRow(rng *rand.Rand, key *data_model.Key, top data_model.TagUnion, value *data_model.MultiValue, sf float64, appendCtx appendContext, scratch []byte) (insertRow, []byte) {
	row := insertRow{key: *key, top: top}
	resolveKeyTags(&row, appendCtx)
	skipMaxHost, skipMinHost, skipSumSquare := appendCtx.metricCache.skips(key.Metric)
	row.count = value.Value.Count() * sf
	if value.Value.ValueSet {
		row.min = value.Value.ValueMin
		row.max = value.Value.ValueMax
		row.sum = value.Value.ValueSum * sf
		row.sumSquare = zeroIfTrue(value.Value.ValueSumSquare*sf, skipSumSquare)
	}
	row.percentiles = rowbinary.AppendCentroids(scratch[:0], value.ValueTDigest, sf)
	row.unique = value.HLL.MarshallAppend(row.percentiles[len(row.percentiles):][:0]) // both sketches share one buffer, laid out back to back
	resolveHosts(rng, &row, row.count, &value.Value, skipMaxHost, skipMinHost, appendCtx)
	return row, row.percentiles[:0]
}

// resolveValueStatRow resolves one value-stat row (badges and counts that are
// not sampled). ok is false for the counters that are normally 0 — those are not
// inserted at all, and resolution then has no side effects either.
func resolveValueStatRow(rng *rand.Rand, key *data_model.Key, v data_model.ItemValue, appendCtx appendContext, scratch []byte) (insertRow, []byte, bool) {
	count := v.Count()
	if count <= 0 { // We have lots of built-in counters which are normally 0
		return insertRow{}, scratch, false
	}
	row := insertRow{key: *key}
	resolveKeyTags(&row, appendCtx)
	skipMaxHost, skipMinHost, skipSumSquare := appendCtx.metricCache.skips(key.Metric)
	row.count = count
	if v.ValueSet {
		row.min = v.ValueMin
		row.max = v.ValueMax
		row.sum = v.ValueSum
		row.sumSquare = zeroIfTrue(v.ValueSumSquare, skipSumSquare)
	} else {
		row.max = count
	}
	row.percentiles = rowbinary.AppendEmptyCentroids(scratch[:0])
	row.unique = rowbinary.AppendEmptyUnique(row.percentiles[len(row.percentiles):][:0])
	resolveHosts(rng, &row, count, &v, skipMaxHost, skipMinHost, appendCtx)
	return row, row.percentiles[:0], true
}

func appendArgMinMaxTag(res []byte, tag data_model.TagUnion, value float32) []byte {
	if tag.Empty() {
		res = rowbinary.AppendArgMinMaxStringEmpty(res)
		return res
	}
	wasLen := len(res)
	// this is important, do not remove
	// without it AppendArgMinMaxBytesFloat32 will corrupt data because len and res are parts of the same slice
	res = append(res, 0, 0, 0, 0)
	if tag.I != 0 {
		res = append(res, 0)
		res = binary.LittleEndian.AppendUint32(res, uint32(tag.I))
		res = chutil.AppendArgMinMaxBytesFloat32(res[:wasLen], res[wasLen+4:], value)
		return res
	}
	res = append(res, 1)
	res = append(res, tag.S...)
	res = chutil.AppendArgMinMaxBytesFloat32(res[:wasLen], res[wasLen+4:], value)
	return res
}

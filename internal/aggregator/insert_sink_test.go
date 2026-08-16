// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"bytes"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"pgregory.net/rand"

	"github.com/hrissan/tdigest"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlmetadata"
	"github.com/VKCOM/statshouse/internal/format"
	"github.com/VKCOM/statshouse/internal/metajournal"
	"github.com/VKCOM/statshouse/internal/vkgo/kittenhouseclient/rowbinary"
)

// The golden files under testdata freeze the RowBinary output of the insert
// conveyor as it was before the InsertSink seam existed. The seam (and any
// future storage backend behind it) must keep producing byte-identical bodies.
// Regenerate with: go test ./internal/aggregator -run Golden -update-goldens
var updateGoldens = flag.Bool("update-goldens", false, "rewrite golden files under testdata")

const (
	goldenSeed   = uint64(0x6475636B73746F72) // "duckstor"
	goldenNow    = uint32(1740000000)         // whole minute, so badge buckets coincide with row seconds
	goldenHostID = int32(777)

	goldenMetricValues   = int32(10) // not in the journal: no skips
	goldenMetricTopOnly  = int32(11)
	goldenMetricCounter  = int32(12)
	goldenMetricSkips    = int32(20) // in the journal with all three skip flags
	goldenAccountMetric  = int32(5)  // ingestion status is accounted to its metric
	goldenSkipMetricName = "golden_skip_metric"
)

func goldenAggregator(t *testing.T, configR ConfigAggregatorRemote) *Aggregator {
	t.Helper()
	return &Aggregator{
		configR:           configR,
		metricStorage:     goldenMetricStorage(t),
		tagsMapper3:       &tagsMapper3{unknownTags: map[string]unknownTag{}, createTags: map[string]createMappingExtra{}},
		aggregatorHostTag: data_model.TagUnion{I: goldenHostID},
	}
}

func goldenMetricStorage(t *testing.T) *metajournal.MetricsStorage {
	t.Helper()
	storage := metajournal.MakeMetricsStorage(func(int32, string) {})
	// metric with all three skip flags, so the golden rows cover the skip paths
	event, err := metajournal.EventFromMetricMeta(format.MetricMetaValue{
		Name:          goldenSkipMetricName,
		MetricID:      goldenMetricSkips,
		SkipMinHost:   true,
		SkipMaxHost:   true,
		SkipSumSquare: true,
	}, "")
	if err != nil {
		t.Fatalf("failed to build metric event: %v", err)
	}
	storage.ApplyEvent([]tlmetadata.Event{event})
	if m := storage.GetMetaMetric(goldenMetricSkips); m == nil || !m.SkipSumSquare || !m.SkipMinHost || !m.SkipMaxHost {
		t.Fatalf("skip metric %d not applied to storage", goldenMetricSkips)
	}
	return storage
}

// One item per bucket and at most one string top per item, and every row in one
// second: map iteration order (which the pre-seam conveyor never sorted) then
// cannot reorder the body, so the golden bytes are stable.
func goldenBuckets(t *testing.T) []*aggregatorBucket {
	t.Helper()
	frnd := rand.New(goldenSeed) // fixture rng, separate from the conveyor rng
	newItemBucket := func(build func(item *data_model.MultiItem)) *aggregatorBucket {
		b := newAggregatorBucket(goldenNow)
		item := &data_model.MultiItem{}
		build(item)
		b.shards[0].MultiItems = map[string]*data_model.MultiItem{"item": item}
		return b
	}

	// recent: values with percentiles, uniques, string tag and all host kinds, plus a string top
	bValues := newItemBucket(func(item *data_model.MultiItem) {
		item.Key = data_model.Key{Timestamp: goldenNow, Metric: goldenMetricValues}
		item.Key.Tags[0] = 5
		item.Key.STags[1] = "golden unmapped tag"
		item.Tail.AddValueCounterHost(frnd, 1.5, 3, data_model.TagUnion{I: 7})
		item.Tail.AddValueCounterHost(frnd, 9.75, 4, data_model.TagUnion{S: "golden max host"})
		td := tdigest.NewWithCompression(rowbinary.TDigestCompression)
		td.Add(1.5, 3)
		td.Add(9.75, 4)
		item.Tail.ValueTDigest = td
		for _, u := range []uint64{11, 22, 33, 44} {
			item.Tail.HLL.Insert(u)
		}
		top := &data_model.MultiValue{}
		top.AddCounterHost(frnd, 3, data_model.TagUnion{})
		item.Top = map[data_model.TagUnion]*data_model.MultiValue{{S: "golden top"}: top}
	})

	// historic: ingestion status of a metric, the only built-in that carries a badge
	bIngestion := newItemBucket(func(item *data_model.MultiItem) {
		item.Key = data_model.Key{Timestamp: goldenNow, Metric: format.BuiltinMetricIDIngestionStatus}
		item.Key.Tags[1] = goldenAccountMetric
		item.Key.Tags[2] = format.TagValueIDSrcIngestionStatusWarnMapTagSetTwice
		item.Tail.AddCounterHost(frnd, 2, data_model.TagUnion{I: goldenHostID})
	})

	// historic: string top only, no tail
	bTopOnly := newItemBucket(func(item *data_model.MultiItem) {
		item.Key = data_model.Key{Timestamp: goldenNow, Metric: goldenMetricTopOnly}
		top := &data_model.MultiValue{}
		top.AddCounterHost(frnd, 1, data_model.TagUnion{})
		item.Top = map[data_model.TagUnion]*data_model.MultiValue{{I: 3}: top}
	})

	// historic: pure counter, no hosts at all
	bCounter := newItemBucket(func(item *data_model.MultiItem) {
		item.Key = data_model.Key{Timestamp: goldenNow, Metric: goldenMetricCounter}
		item.Key.Tags[15] = 9
		item.Tail.AddCounterHost(frnd, 4, data_model.TagUnion{})
	})

	// historic: journal metric with all skip flags
	bSkips := newItemBucket(func(item *data_model.MultiItem) {
		item.Key = data_model.Key{Timestamp: goldenNow, Metric: goldenMetricSkips}
		item.Tail.AddValueCounterHost(frnd, 2.25, 2, data_model.TagUnion{I: 9})
	})

	return []*aggregatorBucket{bValues, bIngestion, bTopOnly, bCounter, bSkips}
}

func goldenConfig() ConfigAggregatorRemote {
	configR := DefaultConfigAggregator().RemoteInitial
	configR.MinInsertBudget = 10_000_000 // keep everything, no sampling: sample factors are covered per-row below
	return configR
}

func goldenFilePath(t *testing.T, name string) string {
	t.Helper()
	return filepath.Join("testdata", name)
}

func loadGolden(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(goldenFilePath(t, name))
	if err != nil {
		t.Fatalf("failed to read golden file %s (run with -update-goldens to create): %v", name, err)
	}
	return data
}

func saveGolden(t *testing.T, name string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join("testdata"), 0o755); err != nil {
		t.Fatalf("failed to create testdata dir: %v", err)
	}
	if err := os.WriteFile(goldenFilePath(t, name), data, 0o644); err != nil {
		t.Fatalf("failed to write golden file %s: %v", name, err)
	}
}

func requireGoldenEqual(t *testing.T, name string, got []byte) {
	t.Helper()
	if *updateGoldens {
		saveGolden(t, name, got)
		return
	}
	want := loadGolden(t, name)
	if !bytes.Equal(want, got) {
		t.Fatalf("golden %s mismatch:\n want %x\n  got %x", name, want, got)
	}
}

// TestInsertRoundGoldenBody drives one full insert round (with the conveyor
// sampler, badges and the contributors log) through the sink and requires the
// RowBinary body and the per-bucket size accounting to stay byte-identical to
// the pre-seam output.
func TestInsertRoundGoldenBody(t *testing.T) {
	a := goldenAggregator(t, goldenConfig())
	buckets := goldenBuckets(t)
	sink := newClickhouseSink(nil, "", "", "", getTableDesc(), func() string { return "" })

	_, stats, _ := a.rowDataMarshalAppendPositions(buckets, data_model.SamplerBuffers{}, rand.New(goldenSeed), sink, goldenNow)
	if len(sink.body) == 0 {
		t.Fatal("golden round produced empty body")
	}
	requireGoldenEqual(t, "insert_round_golden.body", sink.body)
	requireGoldenEqual(t, "insert_round_golden.sizes", []byte(dumpInsertStats(stats)))
}

func dumpInsertStats(stats insertStats) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "recentTs=%d historicTag=%d contributors=%d samplingBudget=%d samplingMetricCount=%d\n",
		stats.recentTs, stats.historicTag, stats.contributors, stats.samplingBudget, stats.samplingMetricCount)
	tss := make([]int, 0, len(stats.sizes))
	for ts := range stats.sizes {
		tss = append(tss, int(ts))
	}
	sort.Ints(tss)
	for _, ts := range tss {
		is := stats.sizes[uint32(ts)]
		fmt.Fprintf(&sb, "ts=%d counters=%d values=%d percentiles=%d uniques=%d stringTops=%d builtin=%d\n",
			ts, is.counters, is.values, is.percentiles, is.uniques, is.stringTops, is.builtin)
	}
	return sb.String()
}

type goldenRowCase struct {
	name string
	run  func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte
}

// goldenRowCases covers every row flavour of the conveyor: value-stat rows
// (badges, contributors log, sampling factors), conveyor rows with and without
// sampling factors, skip flags, empty aggregate states and every tag encoding branch.
func goldenRowCases() []goldenRowCase {
	valueKey := func(metric int32) *data_model.Key {
		return &data_model.Key{Timestamp: goldenNow, Metric: metric}
	}
	richTail := func() *data_model.MultiValue {
		v := &data_model.MultiValue{}
		v.AddValueCounterHost(rand.New(goldenSeed), 1.5, 3, data_model.TagUnion{I: 7})
		v.AddValueCounterHost(rand.New(goldenSeed), 9.75, 4, data_model.TagUnion{S: "max host"})
		td := tdigest.NewWithCompression(rowbinary.TDigestCompression)
		td.Add(1.5, 3)
		td.Add(9.75, 4)
		v.ValueTDigest = td
		v.HLL.Insert(11)
		v.HLL.Insert(22)
		return v
	}
	counterTail := func() *data_model.MultiValue {
		v := &data_model.MultiValue{}
		v.AddCounterHost(rand.New(goldenSeed), 6, data_model.TagUnion{I: 4})
		return v
	}
	appendMulti := func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink, key *data_model.Key, top data_model.TagUnion, value *data_model.MultiValue, sf float64) []byte {
		sink.Reset()
		var row insertRow
		row, _ = resolveMultiValueRow(rnd, key, top, value, sf, appendCtx, nil)
		sink.AppendRow(&row)
		return append([]byte(nil), sinkRoundBody(sink)...)
	}
	appendValue := func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink, key *data_model.Key, v data_model.ItemValue) []byte {
		sink.Reset()
		var row insertRow
		row, _, _ = resolveValueStatRow(rnd, key, v, appendCtx, nil)
		sink.AppendRow(&row)
		return append([]byte(nil), sinkRoundBody(sink)...)
	}
	return []goldenRowCase{
		{"counter_sf1", func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte {
			return appendMulti(rnd, appendCtx, sink, valueKey(goldenMetricValues), data_model.TagUnion{}, counterTail(), 1)
		}},
		{"values_sf1", func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte {
			return appendMulti(rnd, appendCtx, sink, valueKey(goldenMetricValues), data_model.TagUnion{}, richTail(), 1)
		}},
		{"values_sf2.5", func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte {
			return appendMulti(rnd, appendCtx, sink, valueKey(goldenMetricValues), data_model.TagUnion{}, richTail(), 2.5)
		}},
		{"values_skips", func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte {
			return appendMulti(rnd, appendCtx, sink, valueKey(goldenMetricSkips), data_model.TagUnion{}, richTail(), 1)
		}},
		{"counter_novalue_sf4", func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte {
			v := &data_model.MultiValue{}
			v.AddCounterHost(rand.New(goldenSeed), 6, data_model.TagUnion{I: 4})
			return appendMulti(rnd, appendCtx, sink, valueKey(goldenMetricValues), data_model.TagUnion{}, v, 4)
		}},
		{"string_top_s", func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte {
			return appendMulti(rnd, appendCtx, sink, valueKey(goldenMetricValues), data_model.TagUnion{S: "top string"}, counterTail(), 1)
		}},
		{"string_top_id", func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte {
			return appendMulti(rnd, appendCtx, sink, valueKey(goldenMetricValues), data_model.TagUnion{I: 1234}, counterTail(), 1)
		}},
		{"stags_and_both_halves", func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte {
			key := valueKey(goldenMetricValues)
			key.STags[3] = "unmapped three"
			key.Tags[4] = 8
			key.STags[4] = "ignored when id set"
			return appendMulti(rnd, appendCtx, sink, key, data_model.TagUnion{}, counterTail(), 1)
		}},
		{"value_stat_values", func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte {
			v := data_model.SimpleItemValue(3.5, 2, data_model.TagUnion{I: goldenHostID})
			return appendValue(rnd, appendCtx, sink, valueKey(format.BuiltinMetricIDBadges), v)
		}},
		{"value_stat_counter", func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte {
			v := data_model.SimpleItemCounter(9, data_model.TagUnion{I: goldenHostID})
			return appendValue(rnd, appendCtx, sink, valueKey(format.BuiltinMetricIDContributorsLog), v)
		}},
		{"value_stat_zero_count", func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte {
			v := data_model.SimpleItemCounter(0, data_model.TagUnion{})
			row, _, ok := resolveValueStatRow(rnd, valueKey(format.BuiltinMetricIDBadges), v, appendCtx, nil)
			if ok || row.count != 0 {
				return []byte{1} // marker: zero-count rows must not resolve into anything
			}
			return nil
		}},
		{"value_stat_skips", func(rnd *rand.Rand, appendCtx appendContext, sink InsertSink) []byte {
			v := data_model.SimpleItemValue(3.5, 2, data_model.TagUnion{I: goldenHostID})
			return appendValue(rnd, appendCtx, sink, valueKey(goldenMetricSkips), v)
		}},
	}
}

// TestInsertRowsGoldenEncoding freezes the RowBinary encoding of individual rows
// — every aggregate, aggregate-state and host branch of the conveyor — as produced before
// the seam. Row resolution and encoding must keep matching these bytes.
func TestInsertRowsGoldenEncoding(t *testing.T) {
	storage := goldenMetricStorage(t)
	sink := newClickhouseSink(nil, "", "", "", getTableDesc(), func() string { return "" })
	cases := goldenRowCases()
	var buf bytes.Buffer
	for i, c := range cases {
		appendCtx := appendContext{
			metricCache:       makeMetricCache(storage),
			unknownTags:       map[string]createMappingExtra{},
			bucketUnknownTags: map[string]createMappingExtra{}, // no bucket context: unmapped strings are not recorded
		}
		body := c.run(rand.New(goldenSeed+uint64(i)), appendCtx, sink)
		if len(appendCtx.unknownTags) != 0 {
			t.Fatalf("case %s recorded unknown tags without bucket context", c.name)
		}
		var lenPrefix [4]byte
		binary.LittleEndian.PutUint32(lenPrefix[:], uint32(len(body)))
		buf.Write(lenPrefix[:])
		buf.Write(body)
	}
	requireGoldenEqual(t, "insert_rows_golden.bin", buf.Bytes())
}

func sinkRoundBody(sink InsertSink) []byte {
	return sink.(*clickhouseSink).body
}

// TestRowBinarySizeMatchesEncoding pins rowBinarySize to appendRowBinary's
// actual output, so the per-row size accounting of every sink (not just the
// one that really encodes the bytes) reports what ClickHouse would have sent.
func TestRowBinarySizeMatchesEncoding(t *testing.T) {
	storage := goldenMetricStorage(t)
	rnd := rand.New(goldenSeed)

	// rows resolved through the conveyor's own resolution cover every branch
	// of the encoders: values, counters, skips, sampling factors, string tops
	// in both encodings, raw string tags, both host encodings and both aggregate states
	valueKey := func(metric int32) *data_model.Key {
		return &data_model.Key{Timestamp: goldenNow, Metric: metric}
	}
	appendCtx := appendContext{
		metricCache:       makeMetricCache(storage),
		unknownTags:       map[string]createMappingExtra{},
		bucketUnknownTags: map[string]createMappingExtra{},
	}
	richTail := func() *data_model.MultiValue {
		v := &data_model.MultiValue{}
		v.AddValueCounterHost(rnd, 1.5, 3, data_model.TagUnion{I: 7})
		v.AddValueCounterHost(rnd, 9.75, 4, data_model.TagUnion{S: "max host"})
		td := tdigest.NewWithCompression(rowbinary.TDigestCompression)
		td.Add(1.5, 3)
		v.ValueTDigest = td
		v.HLL.Insert(11)
		return v
	}
	var rows []insertRow
	for _, sf := range []float64{1, 2.5, 17} {
		row, _ := resolveMultiValueRow(rnd, valueKey(goldenMetricValues), data_model.TagUnion{}, richTail(), sf, appendCtx, nil)
		rows = append(rows, row)
		row, _ = resolveMultiValueRow(rnd, valueKey(goldenMetricSkips), data_model.TagUnion{S: "string top"}, richTail(), 1, appendCtx, nil)
		rows = append(rows, row)
		// value-stat rows (badges, contributors log) have no aggregate states of their own
		row, _, _ = resolveValueStatRow(rnd, valueKey(format.BuiltinMetricIDBadges), data_model.SimpleItemValue(3.5, 2, data_model.TagUnion{I: goldenHostID}), appendCtx, nil)
		rows = append(rows, row)
	}
	// and randomized rows exercising arbitrary tag strings, ids, hosts and
	// aggregate-state lengths (including empty states and empty hosts)
	randStr := func(maxLen int) string {
		b := make([]byte, rnd.Intn(maxLen))
		_, _ = rnd.Read(b)
		return string(b)
	}
	for i := 0; i < 2000; i++ {
		row := insertRow{
			key: data_model.Key{
				Timestamp: rnd.Uint32(),
				Metric:    int32(rnd.Int31()),
			},
			top:       data_model.TagUnion{I: int32(rnd.Int31()), S: randStr(200)},
			count:     rnd.Float64() * 100,
			min:       rnd.Float64(),
			max:       rnd.Float64() * 100,
			sum:       rnd.Float64() * 1e6,
			sumSquare: rnd.Float64() * 1e9,
		}
		for ki := 0; ki < format.MaxTags; ki++ {
			if rnd.Intn(2) == 0 {
				row.key.Tags[ki] = int32(rnd.Int31())
			} else {
				row.key.STags[ki] = randStr(300)
			}
		}
		row.percentiles = make([]byte, rnd.Intn(600))
		row.unique = make([]byte, rnd.Intn(600))
		if rnd.Intn(2) == 0 {
			row.minHost = hostPair{data_model.TagUnion{I: int32(rnd.Int31())}, rnd.Float32()}
		} else if rnd.Intn(2) == 0 {
			row.minHost = hostPair{data_model.TagUnion{S: randStr(250)}, rnd.Float32()}
		}
		if rnd.Intn(2) == 0 {
			row.maxHost = hostPair{data_model.TagUnion{S: randStr(250)}, rnd.Float32()}
		}
		if rnd.Intn(2) == 0 {
			row.maxCountHost = hostPair{data_model.TagUnion{I: int32(rnd.Int31()), S: randStr(50)}, rnd.Float32()}
		}
		rows = append(rows, row)
	}

	for i := range rows {
		row := &rows[i]
		if got, want := rowBinarySize(row), len(appendRowBinary(nil, row)); got != want {
			t.Fatalf("row %d: rowBinarySize=%d but appendRowBinary produces %d bytes (row %+v)", i, got, want, row)
		}
	}
}

// fakeDuckHandle drives the sink selection without any DuckDB.
type fakeDuckHandle struct{ sink InsertSink }

func (f *fakeDuckHandle) NewSink() InsertSink { return f.sink }
func (f *fakeDuckHandle) QueryExecutor(_ *metajournal.MetricsStorage, _ int32) storeQueryExecutor {
	return nil // the sink tests never serve queries
}
func (f *fakeDuckHandle) Close() error { return nil }

// TestNewInsertSinkSelectsBackend pins the wiring of goInsert's sink: the duck
// handle when the duck backend opened one, the ClickHouse inserter otherwise.
func TestNewInsertSinkSelectsBackend(t *testing.T) {
	a := &Aggregator{}
	if _, ok := a.newInsertSink(nil).(*clickhouseSink); !ok {
		t.Fatal("without a duck store the sink must be the ClickHouse inserter")
	}

	sentinel := newClickhouseSink(nil, "", "", "", "", func() string { return "" }) // any distinct sink works
	a.duckStore = &fakeDuckHandle{sink: sentinel}
	if got := a.newInsertSink(nil); got != InsertSink(sentinel) {
		t.Fatalf("with a duck store the sink must come from the store handle, got %T", got)
	}
}

// TestClickhouseSinkSend drives the sink's HTTP insert against a fake
// ClickHouse: the request must carry the same query prefix, settings, headers
// and RowBinary body the pre-seam sendToClickhouse built, and the status,
// exception code and error must flow back to the conveyor unchanged.
func TestClickhouseSinkSend(t *testing.T) {
	row := insertRow{key: data_model.Key{Timestamp: goldenNow, Metric: goldenMetricValues}}
	row.count = 2
	row.percentiles = []byte{0}
	row.unique = []byte{0, 0}

	t.Run("ok", func(t *testing.T) {
		var gotQuery, gotUser, gotKey, gotAggregation string
		var gotBody []byte
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotQuery = r.URL.Query().Get("query")
			gotUser = r.Header.Get("X-ClickHouse-User")
			gotKey = r.Header.Get("X-ClickHouse-Key")
			gotAggregation = r.Header.Get("X-Kittenhouse-Aggregation")
			gotBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		sink := newClickhouseSink(server.Client(), server.Listener.Addr().String(), "u", "p", getTableDesc(), func() string { return "SETTINGS x=1" })
		if n := sink.AppendRow(&row); n <= 0 || n != sink.RoundSize() {
			t.Fatalf("AppendRow size %d does not match round size %d", n, sink.RoundSize())
		}
		status, exception, _, err := sink.Send(context.Background())
		if err != nil || status != http.StatusOK || exception != 0 {
			t.Fatalf("unexpected send result: status %d exception %d err %v", status, exception, err)
		}
		if want := "INSERT INTO " + getTableDesc() + " SETTINGS x=1 FORMAT RowBinary"; gotQuery != want {
			t.Fatalf("unexpected query:\n want %q\n  got %q", want, gotQuery)
		}
		if gotUser != "u" || gotKey != "p" {
			t.Fatalf("unexpected auth headers: %q %q", gotUser, gotKey)
		}
		if gotAggregation != "0" {
			t.Fatalf("unexpected aggregation header %q", gotAggregation)
		}
		if !bytes.Equal(gotBody, sinkRoundBody(sink)) {
			t.Fatalf("unexpected body:\n want %x\n  got %x", sinkRoundBody(sink), gotBody)
		}

		sink.Reset()
		if sink.RoundSize() != 0 {
			t.Fatalf("round not reset: %d bytes", sink.RoundSize())
		}
	})

	t.Run("clickhouse_error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.ReadAll(r.Body)
			w.Header().Set("X-ClickHouse-Exception-Code", "62")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("no such column"))
		}))
		defer server.Close()

		sink := newClickhouseSink(server.Client(), server.Listener.Addr().String(), "", "", getTableDesc(), func() string { return "" })
		sink.AppendRow(&row)
		status, exception, _, err := sink.Send(context.Background())
		if err == nil || status != http.StatusBadRequest || exception != 62 {
			t.Fatalf("unexpected send result: status %d exception %d err %v", status, exception, err)
		}
	})

	t.Run("local_mode", func(t *testing.T) {
		sink := newClickhouseSink(nil, "", "", "", getTableDesc(), func() string { return "" })
		sink.AppendRow(&row)
		status, exception, elapsed, err := sink.Send(context.Background())
		if err != nil || status != 0 || exception != 0 || elapsed != 1 {
			t.Fatalf("unexpected local-mode result: status %d exception %d elapsed %v err %v", status, exception, elapsed, err)
		}
	})
}

// TestInsertRowScratchIsolation guards the sink contract that a row's
// aggregate-state slices are valid only until the next append: the clickhouse sink copies them
// into the round body, so reusing one scratch buffer must not corrupt earlier
// rows.
func TestInsertRowScratchIsolation(t *testing.T) {
	appendCtx := appendContext{
		metricCache:       makeMetricCache(goldenMetricStorage(t)),
		unknownTags:       map[string]createMappingExtra{},
		bucketUnknownTags: map[string]createMappingExtra{},
	}
	rnd := rand.New(goldenSeed)
	scratch := make([]byte, 0, 1024) // pre-sized, so both rows provably resolve in one backing array
	sink := newClickhouseSink(nil, "", "", "", getTableDesc(), func() string { return "" })

	// two rows with different aggregate states, so overwriting the first row's bytes would show
	rich := func(value float64, unique uint64) *data_model.MultiValue {
		v := &data_model.MultiValue{}
		v.AddValueCounterHost(rand.New(goldenSeed), value, 3, data_model.TagUnion{I: 7})
		td := tdigest.NewWithCompression(rowbinary.TDigestCompression)
		td.Add(value, 3)
		v.ValueTDigest = td
		v.HLL.Insert(unique)
		return v
	}
	key := &data_model.Key{Timestamp: goldenNow, Metric: goldenMetricValues}
	var row insertRow
	row, scratch = resolveMultiValueRow(rnd, key, data_model.TagUnion{}, rich(1.5, 11), 1, appendCtx, scratch)
	firstP := append([]byte(nil), row.percentiles...)
	firstU := append([]byte(nil), row.unique...)
	firstArray := &row.percentiles[0]
	sink.AppendRow(&row)

	// a second row through the same scratch rewrites the aggregate-state slices
	row, _ = resolveMultiValueRow(rnd, key, data_model.TagUnion{}, rich(9.75, 99), 1, appendCtx, scratch)
	sink.AppendRow(&row)

	if &row.percentiles[0] != firstArray {
		t.Fatal("scratch buffer was not reused between rows")
	}
	if bytes.Equal(row.percentiles, firstP) || bytes.Equal(row.unique, firstU) {
		t.Fatal("second row did not rewrite the scratch buffer")
	}
	body := sinkRoundBody(sink)
	if !bytes.Contains(body, firstP) || !bytes.Contains(body, firstU) {
		t.Fatal("first row's aggregate-state bytes were corrupted by the second append")
	}
}

// TestProcessUnknownTag covers the shared unknown-string accounting that row
// resolution performs for tags and hosts, with and without bucket context.
func TestProcessUnknownTag(t *testing.T) {
	withContext := func() appendContext {
		return appendContext{
			unknownTags:       map[string]createMappingExtra{},
			bucketUnknownTags: map[string]createMappingExtra{"known": {MetricID: 4}}, // bucket total is always 0
		}
	}
	t.Run("empty_and_unmapped_are_free", func(t *testing.T) {
		appendCtx := withContext()
		processUnknownTag("", appendCtx)
		processUnknownTag("never seen", appendCtx) // no bucket context for this string
		if len(appendCtx.unknownTags) != 0 {
			t.Fatalf("unexpected unknown tags recorded: %v", appendCtx.unknownTags)
		}
	})
	t.Run("counts_per_insert", func(t *testing.T) {
		appendCtx := withContext()
		processUnknownTag("known", appendCtx)
		processUnknownTag("known", appendCtx)
		got := appendCtx.unknownTags["known"]
		if got.MetricID != 4 || got.total != 2 { // one per row that inserts the string
			t.Fatalf("unexpected accounting: %+v", got)
		}
		if len(appendCtx.unknownTags) != 1 {
			t.Fatalf("unexpected unknown tags recorded: %v", appendCtx.unknownTags)
		}
	})
}

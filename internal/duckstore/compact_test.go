// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hrissan/tdigest"
	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/format"
)

// partialRow builds one partial row carrying real aggregate states, so it
// survives the fold (testRow's placeholder bytes would not).
func partialRow(t *testing.T, metric int32, ts uint32) Row {
	t.Helper()
	r := testRow(metric, ts)
	r.Percentiles = pctState(t, 1)
	r.Unique = uniqState(t, 1)
	return r
}

// tagColList is the 48 tag pairs as a SELECT list.
func tagColList() string {
	var b strings.Builder
	for i := 0; i < format.MaxTags; i++ {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "tag%d, stag%d", i, i)
	}
	return b.String()
}

// rawRow is one stored row exactly as it sits in a tier table.
type rawRow struct {
	metric int32
	time   int64
	tags   [format.MaxTags]int32
	stags  [format.MaxTags]string

	count, min, max, maxCount, sum, sumSquare float64

	minHostID         int32
	minHostS          string
	minHostValue      float64
	maxHostID         int32
	maxHostS          string
	maxHostValue      float64
	maxCountHostID    int32
	maxCountHostS     string
	maxCountHostValue float64

	percentiles []byte
	uniq        []byte
}

// tierColumnCount is the number of columns in a tier table: metric and time,
// 48 tag pairs, six numerics, three host triples and the two aggregate-state
// columns.
const tierColumnCount = 2 + 2*format.MaxTags + 6 + 9 + 2

// scanTableRows reads every row of one tier table on one database.
func scanTableRows(t *testing.T, db *sql.DB, table string) []rawRow {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(
		"SELECT metric, time, %s, count, min, max, max_count, sum, sumsquare, "+
			"min_host, min_shost, min_host_value, max_host, max_shost, max_host_value, max_count_host, max_count_shost, max_count_host_value, percentiles, uniq_state FROM %s",
		tagColList(), table))
	require.NoError(t, err)
	defer rows.Close()
	var out []rawRow
	for rows.Next() {
		var r rawRow
		scan := make([]any, 0, tierColumnCount)
		scan = append(scan, &r.metric, &r.time)
		for i := range r.tags {
			scan = append(scan, &r.tags[i], &r.stags[i])
		}
		scan = append(scan,
			&r.count, &r.min, &r.max, &r.maxCount, &r.sum, &r.sumSquare,
			&r.minHostID, &r.minHostS, &r.minHostValue,
			&r.maxHostID, &r.maxHostS, &r.maxHostValue,
			&r.maxCountHostID, &r.maxCountHostS, &r.maxCountHostValue,
			&r.percentiles, &r.uniq)
		require.NoError(t, rows.Scan(scan...))
		out = append(out, r)
	}
	require.NoError(t, rows.Err())
	return out
}

// decodedKey is the full row key: what a query groups by.
type decodedKey struct {
	metric int32
	time   int64
	tags   [format.MaxTags]int32
	stags  [format.MaxTags]string
}

// hostPairDecoded is one resolved host column.
type hostPairDecoded struct {
	id int32
	s  string
}

func (h hostPairDecoded) empty() bool { return h.id == 0 && h.s == "" }

// decodedGroup is one key's aggregates the way a query decodes them: the
// numerics folded, the hosts resolved by their skewed state values with empty
// hosts losing the way ClickHouse's empty argMin/argMax states do, and the
// aggregate states merged from their decoded states.
type decodedGroup struct {
	rows int

	count, min, max, maxCount, sum, sumSquare float64

	minHost, maxHost, maxCountHost          hostPairDecoded
	minHostVal, maxHostVal, maxCountHostVal float64

	pctCount float64
	uniqSize uint64
}

// decodeRows folds raw rows in Go — the reference "uncollapsed read". Test
// data must keep skew ordering unambiguous (no ties), because ties are
// deliberately implementation-ordered.
func decodeRows(t *testing.T, rows []rawRow) map[decodedKey]*decodedGroup {
	t.Helper()
	groups := map[decodedKey]*decodedGroup{}
	digests := map[decodedKey]*tdigest.TDigest{}
	uniques := map[decodedKey]*data_model.ChUnique{}
	for _, r := range rows {
		k := decodedKey{metric: r.metric, time: r.time, tags: r.tags, stags: r.stags}
		g := groups[k]
		if g == nil {
			g = &decodedGroup{}
			groups[k] = g
		}
		g.rows++
		g.count += r.count
		g.sum += r.sum
		g.sumSquare += r.sumSquare
		if g.rows == 1 || r.min < g.min {
			g.min = r.min
		}
		if r.max > g.max {
			g.max = r.max
		}
		if r.maxCount > g.maxCount {
			g.maxCount = r.maxCount
		}
		if hp := (hostPairDecoded{r.minHostID, r.minHostS}); !hp.empty() {
			if g.minHost.empty() || r.minHostValue < g.minHostVal {
				g.minHost, g.minHostVal = hp, r.minHostValue
			}
		}
		if hp := (hostPairDecoded{r.maxHostID, r.maxHostS}); !hp.empty() {
			if g.maxHost.empty() || r.maxHostValue > g.maxHostVal {
				g.maxHost, g.maxHostVal = hp, r.maxHostValue
			}
		}
		if hp := (hostPairDecoded{r.maxCountHostID, r.maxCountHostS}); !hp.empty() {
			if g.maxCountHost.empty() || r.maxCountHostValue > g.maxCountHostVal {
				g.maxCountHost, g.maxCountHostVal = hp, r.maxCountHostValue
			}
		}
		if td, err := DecodeTDigestState(r.percentiles); err != nil {
			t.Fatalf("decode percentiles of metric %d: %v", r.metric, err)
		} else if td != nil {
			if d := digests[k]; d == nil {
				digests[k] = td
			} else {
				d.Merge(td)
			}
		}
		if len(r.uniq) > 0 {
			var u data_model.ChUnique
			if err := u.ReadFrom(bytes.NewReader(r.uniq)); err != nil {
				t.Fatalf("decode uniq of metric %d: %v", r.metric, err)
			}
			if d := uniques[k]; d == nil {
				uniques[k] = &u
			} else {
				d.Merge(u)
			}
		}
	}
	for k, d := range digests {
		groups[k].pctCount = d.Count()
	}
	for k, u := range uniques {
		groups[k].uniqSize = u.Size(false)
	}
	return groups
}

// requireSameDecoded compares a collapsed read against the uncollapsed
// reference: exact for the numerics, banded for the aggregate states.
func requireSameDecoded(t *testing.T, want, got map[decodedKey]*decodedGroup) {
	t.Helper()
	require.Len(t, got, len(want), "every key must survive the collapse")
	for k, w := range want {
		g, ok := got[k]
		require.True(t, ok, "key metric %d time %d lost by the collapse", k.metric, k.time)
		require.Equal(t, w.count, g.count, "count of metric %d", k.metric)
		require.Equal(t, w.sum, g.sum, "sum of metric %d", k.metric)
		require.Equal(t, w.min, g.min, "min of metric %d", k.metric)
		require.Equal(t, w.max, g.max, "max of metric %d", k.metric)
		require.Equal(t, w.maxCount, g.maxCount, "max_count of metric %d", k.metric)
		require.Equal(t, w.sumSquare, g.sumSquare, "sumsquare of metric %d", k.metric)
		require.Equal(t, w.minHost, g.minHost, "min_host of metric %d", k.metric)
		require.Equal(t, w.maxHost, g.maxHost, "max_host of metric %d", k.metric)
		require.Equal(t, w.maxCountHost, g.maxCountHost, "max_count_host of metric %d", k.metric)
		require.Equal(t, w.minHostVal, g.minHostVal, "min_host_value of metric %d", k.metric)
		require.Equal(t, w.maxHostVal, g.maxHostVal, "max_host_value of metric %d", k.metric)
		require.Equal(t, w.maxCountHostVal, g.maxCountHostVal, "max_count_host_value of metric %d", k.metric)
		require.InDelta(t, w.pctCount, g.pctCount, 1e-9, "percentile weight of metric %d", k.metric)
		if w.uniqSize > 0 || g.uniqSize > 0 {
			require.InDelta(t, float64(w.uniqSize), float64(g.uniqSize), 0.02*float64(w.uniqSize)+1,
				"unique size of metric %d", k.metric)
		}
	}
}

// compactFixture writes three partial rows of one key and one row of another,
// all with real aggregate states, and returns nothing: the expected decoded view
// is read off the delta before compaction runs.
func writeCollapseFixture(t *testing.T, s *Store, w *Writer) {
	t.Helper()
	now := uint32(writerNow.Unix())

	// the skews deliberately disagree with the true extremes: a2 holds the
	// smallest min but a1 the smallest skew, a3 the largest max but a1 the
	// largest skew — the states, not the values, pick the hosts
	a1 := partialRow(t, testMetricID, now-5)
	a1.Count, a1.Min, a1.Max, a1.Sum, a1.SumSquare = 3, 1.5, 9.75, 21, 101.25
	a1.Percentiles = pctState(t, 10, 20, 30)
	a1.Unique = uniqState(t, seq(1, 10)...)
	a1.MinHost = HostPair{Tag: HostTag{ID: 7}, Value: 0.4}
	a1.MaxHost = HostPair{Tag: HostTag{S: "hostB"}, Value: 10}
	a1.MaxCountHost = HostPair{Tag: HostTag{ID: 3}, Value: 1.5}

	a2 := partialRow(t, testMetricID, now-5)
	a2.Count, a2.Min, a2.Max, a2.Sum, a2.SumSquare = 4, 0.5, 5, 40, 200
	a2.Percentiles = pctState(t, 40, 50)
	a2.Unique = uniqState(t, seq(5, 15)...)
	a2.MinHost = HostPair{Tag: HostTag{ID: 9}, Value: 0.9}
	a2.MaxHost = HostPair{Tag: HostTag{S: "hostC"}, Value: 6}
	a2.MaxCountHost = HostPair{Tag: HostTag{ID: 4}, Value: 3.5}

	a3 := partialRow(t, testMetricID, now-5)
	a3.Count, a3.Min, a3.Max, a3.Sum, a3.SumSquare = 5, 2, 12, 60, 300
	a3.Percentiles = pctState(t, 60, 70, 80, 90)
	a3.Unique = uniqState(t, seq(10, 20)...)
	a3.MinHost = HostPair{}
	a3.MaxHost = HostPair{Tag: HostTag{S: "hostD"}, Value: 7}
	a3.MaxCountHost = HostPair{}

	// a value-stat row: empty aggregate states, no hosts
	b := partialRow(t, testMetricID2, now-5)
	b.Count, b.Min, b.Max, b.Sum, b.SumSquare = 2, 1, 2, 3, 5
	b.Percentiles = nil
	b.Unique = nil
	b.MinHost, b.MaxHost, b.MaxCountHost = HostPair{}, HostPair{}, HostPair{}

	require.NoError(t, w.WriteRound(context.Background(), []Row{a1, a2, a3}))
	require.NoError(t, w.WriteRound(context.Background(), []Row{b}))
}

// seq is the half-open range lo..hi.
func seq(lo, hi uint64) []uint64 {
	out := make([]uint64, 0, hi-lo+1)
	for v := lo; v <= hi; v++ {
		out = append(out, v)
	}
	return out
}

// writeGoldenSet writes the deterministic golden row set the collapse paths are
// byte-compared against: keys with tags in both encodings and a string top,
// real percentile and unique states of varied sizes (including none), hosts in
// all three columns — empty ones included, skews disagreeing with the true
// extremes — several partial runs per key across rounds, and rows spanning two
// 1s-tier windows whose times share 1m and 1h buckets with the fresh ones.
// Tasks 1, 2 and 9 compare their rewrites byte-for-byte against what this set
// collapses to.
func writeGoldenSet(t *testing.T, w *Writer) {
	t.Helper()
	now := uint32(writerNow.Unix())

	// key A: three partial rows in one 1s bucket, the skews deliberately
	// disagreeing with the true extremes — the states, not the values, pick
	// the hosts
	a1 := partialRow(t, testMetricID, now-5)
	a1.Count, a1.Min, a1.Max, a1.Sum, a1.SumSquare = 3, 1.5, 9.75, 21, 101.25
	a1.Percentiles = pctState(t, 10, 20, 30)
	a1.Unique = uniqState(t, seq(1, 10)...)
	a1.MinHost = HostPair{Tag: HostTag{ID: 7}, Value: 0.4}
	a1.MaxHost = HostPair{Tag: HostTag{S: "hostB"}, Value: 10}
	a1.MaxCountHost = HostPair{Tag: HostTag{ID: 3}, Value: 1.5}

	a2 := partialRow(t, testMetricID, now-5)
	a2.Count, a2.Min, a2.Max, a2.Sum, a2.SumSquare = 4, 0.5, 5, 40, 200
	a2.Percentiles = pctState(t, 40, 50)
	a2.Unique = uniqState(t, seq(5, 15)...)
	a2.MinHost = HostPair{Tag: HostTag{ID: 9}, Value: 0.9}
	a2.MaxHost = HostPair{Tag: HostTag{S: "hostC"}, Value: 6}
	a2.MaxCountHost = HostPair{Tag: HostTag{ID: 4}, Value: 3.5}

	a3 := partialRow(t, testMetricID, now-5)
	a3.Count, a3.Min, a3.Max, a3.Sum, a3.SumSquare = 5, 2, 12, 60, 300
	a3.Percentiles = pctState(t, 60, 70, 80, 90)
	a3.Unique = uniqState(t, seq(10, 20)...)
	a3.MinHost = HostPair{}
	a3.MaxHost = HostPair{Tag: HostTag{S: "hostD"}, Value: 7}
	a3.MaxCountHost = HostPair{}

	// key A once more, five seconds earlier: a 1s row of its own that shares
	// key A's 1m and 1h buckets, so the coarser tiers collapse it in while
	// the 1s tier keeps it apart
	a4 := partialRow(t, testMetricID, now-10)
	a4.Count, a4.Min, a4.Max, a4.Sum, a4.SumSquare = 2, 0.25, 3, 5, 12.5
	a4.Percentiles = pctState(t, 5)
	a4.Unique = uniqState(t, seq(90, 99)...)
	a4.MaxHost = HostPair{Tag: HostTag{S: "hostE"}, Value: 2}

	// key B: a value-stat row — no aggregate states, no hosts
	b := partialRow(t, testMetricID2, now-5)
	b.Count, b.Min, b.Max, b.Sum, b.SumSquare = 2, 1, 2, 3, 5
	b.Percentiles = nil
	b.Unique = nil
	b.MinHost, b.MaxHost, b.MaxCountHost = HostPair{}, HostPair{}, HostPair{}

	// key C: two partial runs in the previous 1s-tier window, overlapping
	// unique states
	c1 := partialRow(t, testMetricID2+1, now-3700)
	c1.Count, c1.Sum = 4, 44
	c1.Percentiles = pctState(t, 500, 600)
	c1.Unique = uniqState(t, seq(200, 240)...)
	c1.MinHost = HostPair{Tag: HostTag{ID: 21}, Value: 0.1}
	c2 := partialRow(t, testMetricID2+1, now-3700)
	c2.Count, c2.Sum = 6, 66
	c2.Percentiles = pctState(t, 700)
	c2.Unique = uniqState(t, seq(220, 260)...)

	// key D: a different tag shape and the largest unique, one run
	d := partialRow(t, testMetricID2+2, now-5)
	d.Count = 1
	d.Tags[5] = 55
	d.STags[6] = "golden raw"
	d.Top = HostTag{ID: 99}
	d.Unique = uniqState(t, seq(1000, 1127)...)
	d.MaxCountHost = HostPair{Tag: HostTag{ID: 12}, Value: 7}

	require.NoError(t, w.WriteRound(context.Background(), []Row{a1, a2, a3, d}))
	require.NoError(t, w.WriteRound(context.Background(), []Row{a4, b, c1}))
	require.NoError(t, w.WriteRound(context.Background(), []Row{c2}))
}

// seedGoldenGeneration writes the golden set into generation 0 of a fresh store
// in dir, rolls to generation 1 and closes everything: the on-disk state a
// process that died right after the roll leaves, for whoever opens the
// directory next to consume.
func seedGoldenGeneration(t *testing.T, dir string) {
	t.Helper()
	s, _ := openTestStore(t, dir)
	w, err := NewWriter(s, WriterConfig{NowFunc: func() time.Time { return writerNow }})
	require.NoError(t, err)
	writeGoldenSet(t, w)
	require.NoError(t, w.Close())
	require.NoError(t, s.RollGeneration())
	require.NoError(t, s.Close())
}

// copyTree copies a directory tree byte-for-byte, so two stores can consume
// the very same generation file.
func copyTree(t *testing.T, src, dst string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		out := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(out, b, 0o644)
	}))
}

// expectedRows reads a tier's rows off a database and folds them in Go.
func expectedRows(t *testing.T, db *sql.DB, tier string) map[decodedKey]*decodedGroup {
	t.Helper()
	return decodeRows(t, scanTableRows(t, db, tierTables[tier]))
}

// TestCompactionCollapseMatchesUncollapsedRead proves the load-bearing
// property: a collapsed archive window decodes to exactly what the uncollapsed
// delta rows decoded to — every partial row counted once, hosts resolved by
// their skewed state values, aggregate states folded — in all three tiers.
func TestCompactionCollapseMatchesUncollapsedRead(t *testing.T) {
	s, w := newTestWriter(t)
	writeCollapseFixture(t, s, w)

	// the uncollapsed reference, read off the delta before anything moves
	want := map[string]map[decodedKey]*decodedGroup{}
	for _, tier := range tiers {
		want[tier] = expectedRows(t, s.Delta(), tier)
	}
	// golden spot checks on the 1s tier
	ts := int64(writerNow.Unix() - 5)
	keyA := decodedKey{metric: testMetricID, time: ts}
	keyA.tags[0] = 11
	keyA.stags[1] = "raw tag"
	keyA.tags[2] = 13
	keyA.stags[47] = "string top"
	g := want[Tier1s][keyA]
	require.NotNil(t, g)
	require.Equal(t, 12.0, g.count)
	require.Equal(t, 121.0, g.sum)
	require.Equal(t, 0.5, g.min)
	require.Equal(t, 12.0, g.max)
	require.Equal(t, 5.0, g.maxCount)
	require.Equal(t, 601.25, g.sumSquare)
	require.Equal(t, hostPairDecoded{7, ""}, g.minHost, "the smallest skew's host wins, not the smallest min's")
	require.Equal(t, 0.4, g.minHostVal)
	require.Equal(t, hostPairDecoded{0, "hostB"}, g.maxHost, "the largest skew's host wins, not the largest max's")
	require.Equal(t, 10.0, g.maxHostVal)
	require.Equal(t, hostPairDecoded{4, ""}, g.maxCountHost)
	require.Equal(t, 3.5, g.maxCountHostVal)
	require.Equal(t, 9.0, g.pctCount)
	require.EqualValues(t, 20, g.uniqSize)

	require.NoError(t, w.Close())
	c := NewCompactor(s, CompactorConfig{})
	require.NoError(t, c.CompactOnce(context.Background()))

	require.Equal(t, []int64{1}, s.DeltaGenerations(), "the sealed generation is consumed")
	wins := s.Windows()
	require.Len(t, wins, 3, "one window per tier")
	for _, tier := range tiers {
		path := filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(tier, testWindowStart(tier, ts)))
		db, err := openStoreFile(path, true, ResourcesConfig{})
		require.NoError(t, err)
		rows := scanTableRows(t, db, tierTables[tier])
		require.Len(t, rows, 2, "%s: one row per key after the collapse", tier)
		requireSameDecoded(t, want[tier], decodeRows(t, rows))
		require.NoError(t, db.Close())
	}
}

// TestCompactionHostTieKeepsHalvesTogether drives skew ties — where two
// separate arg_min calls could resolve the integer and string halves to
// different rows — and proves the packed struct keeps each host whole, that an
// empty host loses to a real one, and that a group with no host at all stays
// empty.
func TestCompactionHostTieKeepsHalvesTogether(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())

	tieA := partialRow(t, testMetricID, now)
	tieA.Count, tieA.Min = 2, 1.5
	tieA.MinHost = HostPair{Tag: HostTag{ID: 7}, Value: 1.5}
	tieB := partialRow(t, testMetricID, now)
	tieB.Count, tieB.Min = 3, 0.5 // smaller min, tied skew
	tieB.MinHost = HostPair{Tag: HostTag{S: "hostB"}, Value: 1.5}

	emptyLoses1 := partialRow(t, testMetricID2, now)
	emptyLoses1.Count, emptyLoses1.Min = 1, 1 // smallest skew, empty host
	emptyLoses1.MinHost = HostPair{Value: 0.5}
	emptyLoses2 := partialRow(t, testMetricID2, now)
	emptyLoses2.Count, emptyLoses2.Min = 1, 2
	emptyLoses2.MinHost = HostPair{Tag: HostTag{ID: 9}, Value: 0.9}

	noHost1 := partialRow(t, testMetricID2+1, now)
	noHost1.Count = 1
	noHost1.MinHost, noHost1.MaxHost, noHost1.MaxCountHost = HostPair{}, HostPair{}, HostPair{}
	noHost2 := partialRow(t, testMetricID2+1, now)
	noHost2.Count = 1
	noHost2.MinHost, noHost2.MaxHost, noHost2.MaxCountHost = HostPair{}, HostPair{}, HostPair{}

	maxTieA := partialRow(t, testMetricID2+2, now)
	maxTieA.Count, maxTieA.Max = 1, 8
	maxTieA.MaxHost = HostPair{Tag: HostTag{ID: 5}, Value: 8}
	maxTieB := partialRow(t, testMetricID2+2, now)
	maxTieB.Count, maxTieB.Max = 1, 4 // smaller max, tied skew
	maxTieB.MaxHost = HostPair{Tag: HostTag{S: "maxS"}, Value: 8}

	require.NoError(t, w.WriteRound(context.Background(), []Row{tieA, emptyLoses1, noHost1, maxTieA}))
	require.NoError(t, w.WriteRound(context.Background(), []Row{tieB, emptyLoses2, noHost2, maxTieB}))
	require.NoError(t, w.Close())

	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(context.Background()))

	path := filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(Tier1s, testWindowStart(Tier1s, int64(now))))
	db, err := openStoreFile(path, true, ResourcesConfig{})
	require.NoError(t, err)
	defer db.Close()
	host := func(metric int32) hostPairDecoded {
		var id int32
		var sh string
		require.NoError(t, db.QueryRow(`SELECT min_host, min_shost FROM s1 WHERE metric = $1`, metric).Scan(&id, &sh))
		return hostPairDecoded{id, sh}
	}

	// a tie resolves to one whole input row, never a mix of two — and the
	// winning skew comes back with it
	var tieVal float64
	require.NoError(t, db.QueryRow(`SELECT min_host_value FROM s1 WHERE metric = $1`, testMetricID).Scan(&tieVal))
	require.Equal(t, 1.5, tieVal)
	h := host(testMetricID)
	require.Contains(t, []hostPairDecoded{{7, ""}, {0, "hostB"}}, h,
		"both halves must come from the same row")

	// the empty host loses even though its row holds the smallest skew
	require.Equal(t, hostPairDecoded{9, ""}, host(testMetricID2))
	// a group with no host at all stays empty
	require.Equal(t, hostPairDecoded{}, host(testMetricID2+1))

	var maxID int32
	var maxS string
	require.NoError(t, db.QueryRow(`SELECT max_host, max_shost FROM s1 WHERE metric = $1`, testMetricID2+2).Scan(&maxID, &maxS))
	require.Contains(t, []hostPairDecoded{{5, ""}, {0, "maxS"}}, hostPairDecoded{maxID, maxS},
		"max_host keeps its halves together on a tie too")
}

// TestCompactionRoutesRowsToWindowsByTimestamp proves each row lands in the
// archive window its own timestamp belongs to, across all three tiers.
func TestCompactionRoutesRowsToWindowsByTimestamp(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())

	newer := partialRow(t, testMetricID, now)
	older := partialRow(t, testMetricID2, now-3700) // the previous 1s-tier window
	require.NoError(t, w.WriteRound(context.Background(), []Row{newer, older}))
	require.NoError(t, w.Close())

	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(context.Background()))

	nowWindow := testWindowStart(Tier1s, int64(now))
	oldWindow := testWindowStart(Tier1s, int64(now)-3700)
	require.NotEqual(t, nowWindow, oldWindow)

	// two 1s windows, one each for 1m and 1h (the 3700s span stays inside both)
	wins := s.Windows()
	require.Len(t, wins, 4)
	require.Equal(t, WindowFile{Tier: Tier1s, WindowStart: oldWindow, Path: filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(Tier1s, oldWindow))}, wins[0])
	require.Equal(t, WindowFile{Tier: Tier1s, WindowStart: nowWindow, Path: filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(Tier1s, nowWindow))}, wins[1])

	olderTS := int64(now) - 3700
	for _, tc := range []struct {
		tier   string
		window int64
		metric int32
		rowTS  int64 // the tier-truncated time the row must sit at
	}{
		{Tier1s, oldWindow, testMetricID2, olderTS},
		{Tier1s, nowWindow, testMetricID, int64(now)},
		{Tier1m, testWindowStart(Tier1m, olderTS), testMetricID2, olderTS / 60 * 60},
		{Tier1m, testWindowStart(Tier1m, olderTS), testMetricID, int64(now) / 60 * 60},
		{Tier1h, testWindowStart(Tier1h, olderTS), testMetricID2, olderTS / 3600 * 3600},
		{Tier1h, testWindowStart(Tier1h, olderTS), testMetricID, int64(now) / 3600 * 3600},
	} {
		db, err := openStoreFile(filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(tc.tier, tc.window)), true, ResourcesConfig{})
		require.NoError(t, err)
		var n int
		require.NoError(t, db.QueryRow(
			fmt.Sprintf(`SELECT count(*) FROM %s WHERE metric = $1 AND time = $2`, tierTables[tc.tier]),
			tc.metric, tc.rowTS).Scan(&n), "%s window %d metric %d", tc.tier, tc.window, tc.metric)
		require.NotZero(t, n, "%s window %d must hold metric %d at its truncated time", tc.tier, tc.window, tc.metric)
		if tc.tier == Tier1s {
			var other int
			require.NoError(t, db.QueryRow(
				fmt.Sprintf(`SELECT count(*) FROM %s WHERE metric <> $1`, tierTables[tc.tier]), tc.metric).Scan(&other))
			require.Zero(t, other, "a 1s window holds only its own timestamps' rows")
		}
		require.NoError(t, db.Close())
	}
}

// TestCompactionWritesSortedArchive checks the point of the exercise: the
// appended run is physically ordered by (metric, time) — insert order never is.
func TestCompactionWritesSortedArchive(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())

	// deliberately unordered: metric and time both zig-zag
	var rows []Row
	for _, m := range []int32{7, 5, 6, 5} {
		r := partialRow(t, m, now-37)
		r.Count = 1
		rows = append(rows, r)
	}
	rows[3].Time = now - 5 // the second metric-5 row is newer
	require.NoError(t, w.WriteRound(context.Background(), rows))
	require.NoError(t, w.Close())

	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(context.Background()))

	wins := s.Windows()
	require.Len(t, wins, 3)
	db, err := openStoreFile(wins[2].Path, true, ResourcesConfig{}) // the 1h window, largest truncation
	require.NoError(t, err)
	defer db.Close()
	rr, err := db.Query(`SELECT metric, time FROM s1h`) // no ORDER BY: physical order
	require.NoError(t, err)
	defer rr.Close()
	var lastM int32 = -1 << 30
	var lastT int64 = -1 << 62
	for rr.Next() {
		var m int32
		var ts int64
		require.NoError(t, rr.Scan(&m, &ts))
		require.LessOrEqual(t, lastM, m, "metric order must not regress")
		if m == lastM {
			require.LessOrEqual(t, lastT, ts, "time order must not regress within a metric")
		}
		lastM, lastT = m, ts
	}
	require.NoError(t, rr.Err())
}

// TestCompactorRunsAlongsideIngestion drives the compactor against a writer
// hammering rounds and proves the two contracts: every acknowledged round
// lands exactly once across the delta and the windows, and ingestion is never
// blocked by a pass — the rounds finish while compaction is running.
func TestCompactorRunsAlongsideIngestion(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := NewCompactor(s, CompactorConfig{Interval: 10 * time.Millisecond})
	done := make(chan struct{})
	go func() { defer close(done); _ = c.Run(ctx) }()

	const rounds = 50
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds; i++ {
			row := partialRow(t, testMetricID, now-uint32(i%10))
			row.Count, row.Sum = 1, 2
			if err := w.WriteRound(context.Background(), []Row{row}); err != nil {
				t.Errorf("round %d failed: %v", i, err)
				return
			}
		}
	}()

	// ingestion must not stall behind compaction passes
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(10 * time.Second):
		t.Fatal("ingestion stalled behind compaction")
	}

	cancel()
	<-done
	require.NoError(t, w.Close())
	require.NoError(t, c.CompactOnce(context.Background())) // flush what is left

	require.Equal(t, []int64{s.ActiveDeltaGeneration()}, s.DeltaGenerations(),
		"every sealed generation must be consumed")

	// every row lands in all three tiers, so totalling the 1s tier over the
	// delta plus the 1s windows conserves the rounds
	var count, sum float64
	var c2, s2 float64
	require.NoError(t, s.Delta().QueryRow(
		`SELECT coalesce(sum(count), 0), coalesce(sum(sum), 0) FROM s1 WHERE metric = $1`,
		testMetricID).Scan(&c2, &s2))
	count, sum = count+c2, sum+s2
	for _, wf := range s.Windows() {
		if wf.Tier != Tier1s {
			continue
		}
		db, err := openStoreFile(wf.Path, true, ResourcesConfig{})
		require.NoError(t, err)
		require.NoError(t, db.QueryRow(
			`SELECT coalesce(sum(count), 0), coalesce(sum(sum), 0) FROM s1 WHERE metric = $1`,
			testMetricID).Scan(&c2, &s2))
		_ = db.Close()
		count, sum = count+c2, sum+s2
	}
	require.EqualValues(t, rounds, count, "every acknowledged round must land exactly once")
	require.EqualValues(t, 2*rounds, sum)
}

// TestCompactorCrashedCollapseResumesExactlyOnce crashes a collapsing consume
// after one window's append (before its commit) and proves the resumed
// compaction lands every value exactly once — the collapse rides the same
// crash protocol as the plain copy.
func TestCompactorCrashedCollapseResumesExactlyOnce(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTestStore(t, dir)
	w, err := NewWriter(s, WriterConfig{NowFunc: func() time.Time { return writerNow }})
	require.NoError(t, err)
	now := uint32(writerNow.Unix())

	a := partialRow(t, testMetricID, now-3700)
	a.Count, a.Sum = 2, 20
	b := partialRow(t, testMetricID2, now)
	b.Count, b.Sum = 3, 30
	require.NoError(t, w.WriteRound(context.Background(), []Row{a, b}))
	require.NoError(t, w.Close())

	gen := s.ActiveDeltaGeneration()
	require.NoError(t, s.RollGeneration())
	err = s.ConsumeGeneration(context.Background(), gen, ConsumeOptions{
		AppendWindow: collapseWindowRows,
		Fault:        crashAt(CrashAfterAppendBeforeCommit, 2),
	})
	require.Error(t, err)
	require.NoError(t, s.Close())

	s2, _ := openTestStore(t, dir)
	defer func() { _ = s2.Close() }()
	require.Equal(t, []int64{0, 1}, s2.DeltaGenerations(), "the crashed generation must be resumed")

	require.NoError(t, NewCompactor(s2, CompactorConfig{}).CompactOnce(context.Background()))
	require.Equal(t, []int64{1}, s2.DeltaGenerations())
	require.Len(t, s2.Windows(), 4)
	require.Equal(t, map[string]consumeTotals{
		"s1/" + fmt.Sprint(testMetricID):   {count: 2, sum: 20, min: 1.5, max: 9.75, sumsquare: 101.25},
		"s1/" + fmt.Sprint(testMetricID2):  {count: 3, sum: 30, min: 1.5, max: 9.75, sumsquare: 101.25},
		"s1m/" + fmt.Sprint(testMetricID):  {count: 2, sum: 20, min: 1.5, max: 9.75, sumsquare: 101.25},
		"s1m/" + fmt.Sprint(testMetricID2): {count: 3, sum: 30, min: 1.5, max: 9.75, sumsquare: 101.25},
		"s1h/" + fmt.Sprint(testMetricID):  {count: 2, sum: 20, min: 1.5, max: 9.75, sumsquare: 101.25},
		"s1h/" + fmt.Sprint(testMetricID2): {count: 3, sum: 30, min: 1.5, max: 9.75, sumsquare: 101.25},
	}, readerTotals(t, s2))
}

// The retired Go collapse round-trip, kept verbatim as the byte-identity
// reference for the one-statement SQL fold: the collapse SELECT emitted the
// two state columns as LIST(BLOB)s, Go scanned the groups out and folded the
// lists with foldPercentiles/foldUniques, and the folded rows went back
// through a prepared-statement (then Appender) insert. Task 2 replaced the
// whole pipeline with collapseInsert + the fold UDFs; these copies are what
// its output is byte-compared against.

// queryer is the query surface the retired round-trip's query needed,
// satisfied by both a checked-out connection and a database/sql transaction.
type queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// collapsedGroup is one key's folded contribution to an archive window: the
// collapse query's output row with both blob lists already folded.
type collapsedGroup struct {
	metric int32
	time   int64
	tags   [format.MaxTags]int32
	stags  [format.MaxTags]string

	count, min, max, maxCount, sum, sumSquare float64

	minHostID         int32
	minHostS          string
	minHostValue      float64
	maxHostID         int32
	maxHostS          string
	maxHostValue      float64
	maxCountHostID    int32
	maxCountHostS     string
	maxCountHostValue float64

	percentiles []byte
	uniq        []byte
}

// collapsedGroupScan builds the scan target for one collapse query row, in the
// SELECT list's order. The blob lists land in *any and are normalized by
// blobList afterwards.
func collapsedGroupScan(g *collapsedGroup, pctList, uniqList *any) []any {
	scan := make([]any, 0, tierColumnCount)
	scan = append(scan, &g.metric, &g.time)
	for i := range g.tags {
		scan = append(scan, &g.tags[i], &g.stags[i])
	}
	scan = append(scan,
		&g.count, &g.min, &g.max, &g.maxCount, &g.sum, &g.sumSquare,
		&g.minHostID, &g.minHostS, &g.minHostValue,
		&g.maxHostID, &g.maxHostS, &g.maxHostValue,
		&g.maxCountHostID, &g.maxCountHostS, &g.maxCountHostValue,
		pctList, uniqList)
	return scan
}

// collapseQuery is the retired collapse SELECT, verbatim from compact.go
// before the SQL-fold rewrite: every column but the two state columns folded
// by SQL aggregates, the state columns as LIST(BLOB)s for Go to fold.
func collapseQuery(src, table string) string {
	var cols []string
	cols = append(cols, "metric, time")
	for i := 0; i < format.MaxTags; i++ {
		cols = append(cols, fmt.Sprintf("tag%d, stag%d", i, i))
	}
	cols = append(cols,
		"sum(count) AS count",
		"min(min) AS min",
		"max(max) AS max",
		"max(max_count) AS max_count",
		"sum(sum) AS sum",
		"sum(sumsquare) AS sumsquare")
	hostCols := []struct{ fn, id, s, val string }{
		{"arg_min", "min_host", "min_shost", "min_host_value"},
		{"arg_max", "max_host", "max_shost", "max_host_value"},
		{"arg_max", "max_count_host", "max_count_shost", "max_count_host_value"},
	}
	for _, h := range hostCols {
		agg := fmt.Sprintf("%s(%s, %s) FILTER (WHERE %s <> 0 OR %s <> '')",
			h.fn, hostStruct(h.id, h.s, h.val), h.val, h.id, h.s)
		cols = append(cols,
			fmt.Sprintf("coalesce((%s).i, 0) AS %s", agg, h.id),
			fmt.Sprintf("coalesce((%s).s, '') AS %s", agg, h.s),
			fmt.Sprintf("coalesce((%s).v, 0) AS %s", agg, h.val))
	}
	cols = append(cols,
		"list(percentiles) AS percentiles_list",
		"list(uniq_state) AS uniq_state_list")
	return fmt.Sprintf(
		"SELECT %s FROM %s.%s WHERE time >= $1 AND time < $2 GROUP BY ALL ORDER BY metric, time",
		strings.Join(cols, ", "), src, table)
}

// queryCollapsedGroups is the retired round-trip's query-and-fold step,
// verbatim from compact.go: run the collapse SELECT and fold both state
// columns in Go, one group at a time.
func queryCollapsedGroups(ctx context.Context, q queryer, src, table string, windowStart, windowEnd int64) ([]collapsedGroup, error) {
	rows, err := q.QueryContext(ctx, collapseQuery(src, table), windowStart, windowEnd)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var g collapsedGroup
	var pctList, uniqList any
	scan := collapsedGroupScan(&g, &pctList, &uniqList)
	var groups []collapsedGroup
	for rows.Next() {
		if err := rows.Scan(scan...); err != nil {
			return nil, err
		}
		pctBlobs, err := blobList(pctList)
		if err != nil {
			return nil, fmt.Errorf("percentiles of metric %d at %d: %w", g.metric, g.time, err)
		}
		g.percentiles, err = foldPercentiles(pctBlobs)
		if err != nil {
			return nil, fmt.Errorf("percentiles of metric %d at %d: %w", g.metric, g.time, err)
		}
		uniqBlobs, err := blobList(uniqList)
		if err != nil {
			return nil, fmt.Errorf("uniq_state of metric %d at %d: %w", g.metric, g.time, err)
		}
		g.uniq, err = foldUniques(uniqBlobs)
		if err != nil {
			return nil, fmt.Errorf("uniq_state of metric %d at %d: %w", g.metric, g.time, err)
		}
		groups = append(groups, g)
	}
	return groups, rows.Err()
}

// goldenReferenceInsert is the retired prepared-statement append, verbatim from
// insertCollapsedGroups before the Appender rewrite: one INSERT per group
// through a prepared 116-parameter statement.
func goldenReferenceInsert(tx *sql.Tx, table string, groups []collapsedGroup) error {
	if len(groups) == 0 {
		return nil
	}
	placeholders := make([]string, tierColumnCount)
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s VALUES (%s)", table, strings.Join(placeholders, ", ")))
	if err != nil {
		return err
	}
	defer stmt.Close()
	args := make([]any, 0, tierColumnCount)
	for i := range groups {
		g := &groups[i]
		args = args[:0]
		args = append(args, g.metric, g.time)
		for ti := range g.tags {
			args = append(args, g.tags[ti], g.stags[ti])
		}
		args = append(args,
			g.count, g.min, g.max, g.maxCount, g.sum, g.sumSquare,
			g.minHostID, g.minHostS, g.minHostValue,
			g.maxHostID, g.maxHostS, g.maxHostValue,
			g.maxCountHostID, g.maxCountHostS, g.maxCountHostValue,
			g.percentiles, g.uniq)
		if _, err := stmt.Exec(args...); err != nil {
			return fmt.Errorf("insert metric %d at %d: %w", g.metric, g.time, err)
		}
	}
	return nil
}

// goldenReferenceConsume is the retired consume protocol — a database/sql
// transaction plus the prepared-statement insert loop — kept verbatim as the
// byte-identity reference for the Appender rewrite: the same window plan, the
// same collapse query and fold, the same insert order, the same consumption
// record. This is the shape consumeWindow and collapseWindowRows had before
// the connection-level transaction landed.
func goldenReferenceConsume(t *testing.T, s *Store, gen int64) {
	t.Helper()
	ctx := context.Background()
	deltaPath := filepath.Join(s.cfg.Dir, deltaFileName(gen))
	windows, err := generationWindows(deltaPath, s.cfg.Resources)
	require.NoError(t, err)
	for _, k := range sortedWindowKeys(windows) {
		path := filepath.Join(s.cfg.Dir, archiveSubdir, archiveFileName(k.tier, k.start))
		if _, err := os.Stat(path); os.IsNotExist(err) {
			require.NoError(t, createArchiveWindow(path, k.tier, s.currentStamp(), s.cfg.Resources))
		}
		db, err := openStoreFile(path, false, s.cfg.Resources)
		require.NoError(t, err)
		recorded, err := readConsumed(db)
		require.NoError(t, err)
		if _, done := recorded[gen]; done {
			require.NoError(t, db.Close())
			continue
		}
		conn, err := db.Conn(ctx)
		require.NoError(t, err)
		_, err = conn.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS %s (READ_ONLY)", sqlString(deltaPath), deltaSrcAlias))
		require.NoError(t, err)
		tx, err := conn.BeginTx(ctx, nil)
		require.NoError(t, err)
		table := tierTables[k.tier]
		groups, err := queryCollapsedGroups(ctx, tx, deltaSrcAlias, table, k.start, k.start+tierWindowSecs[k.tier])
		require.NoError(t, err)
		require.NoError(t, goldenReferenceInsert(tx, table, groups))
		_, err = tx.Exec("INSERT INTO "+ConsumedTable+" VALUES ($1)", gen)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
		_, _ = conn.ExecContext(ctx, "DETACH "+deltaSrcAlias)
		require.NoError(t, conn.Close())
		require.NoError(t, db.Close())
		s.mu.Lock()
		s.recordWindowLocked(k, path)
		if s.consumed[k] == nil {
			s.consumed[k] = map[int64]struct{}{}
		}
		s.consumed[k][gen] = struct{}{}
		s.mu.Unlock()
	}
	s.mu.Lock()
	s.deltas = removeGeneration(s.deltas, gen)
	s.mu.Unlock()
	require.True(t, s.unlinkDelta(deltaPath, gen), "the reference consume must finish like the real one")
}

// TestCollapseSQLMatchesGoFoldReference pins the SQL-fold rewrite's byte
// identity: the same golden generation consumed by the retired Go round-trip —
// collapse SELECT, fold in Go, prepared-statement insert — and by the
// production one-statement SQL fold (collapseInsert plus the fold UDFs)
// produces window files holding the same rows — every column, the two
// aggregate-state blobs included — in the same physical order, with the same
// consumption records. This golden set is the reference Task 9 keeps
// comparing against.
func TestCollapseSQLMatchesGoFoldReference(t *testing.T) {
	refDir := t.TempDir()
	seedGoldenGeneration(t, refDir)
	sqlDir := t.TempDir()
	copyTree(t, refDir, sqlDir)

	ref, _ := openTestStore(t, refDir)
	defer func() { _ = ref.Close() }()
	goldenReferenceConsume(t, ref, 0)

	prod, _ := openTestStore(t, sqlDir)
	defer func() { _ = prod.Close() }()
	require.NoError(t, prod.ConsumeGeneration(context.Background(), 0, ConsumeOptions{AppendWindow: collapseWindowRows}))

	require.Equal(t, []int64{1}, prod.DeltaGenerations())
	require.NoFileExists(t, filepath.Join(sqlDir, deltaFileName(0)))
	require.NoFileExists(t, filepath.Join(refDir, deltaFileName(0)))

	wins := prod.Windows()
	require.Len(t, wins, 4, "two 1s windows, one 1m window, one 1h window")
	for _, wf := range wins {
		refDB, err := openStoreFile(filepath.Join(refDir, archiveSubdir, archiveFileName(wf.Tier, wf.WindowStart)), true, ResourcesConfig{})
		require.NoError(t, err)
		prodDB, err := openStoreFile(wf.Path, true, ResourcesConfig{})
		require.NoError(t, err)
		refRows := scanTableRows(t, refDB, tierTables[wf.Tier])
		require.Equal(t, refRows, scanTableRows(t, prodDB, tierTables[wf.Tier]),
			"%s window %d: every column byte-identical, aggregate-state blobs and physical order included", wf.Tier, wf.WindowStart)
		refRecorded, err := readConsumed(refDB)
		require.NoError(t, err)
		prodRecorded, err := readConsumed(prodDB)
		require.NoError(t, err)
		require.Equal(t, refRecorded, prodRecorded)
		require.NoError(t, refDB.Close())
		require.NoError(t, prodDB.Close())
	}

	// the golden set really exercised the collapse: the current 1s window
	// holds four keys (A's three runs collapsed into one, A again at its own
	// second, B, D), the previous one holds C's two runs collapsed into one
	now := uint32(writerNow.Unix())
	for _, tc := range []struct {
		tier  string
		start int64
		rows  int
	}{
		{Tier1s, testWindowStart(Tier1s, int64(now)-3700), 1},
		{Tier1s, testWindowStart(Tier1s, int64(now)-5), 4},
		{Tier1m, testWindowStart(Tier1m, int64(now)-5), 4},
		{Tier1h, testWindowStart(Tier1h, int64(now)-5), 4},
	} {
		db, err := openStoreFile(filepath.Join(sqlDir, archiveSubdir, archiveFileName(tc.tier, tc.start)), true, ResourcesConfig{})
		require.NoError(t, err)
		require.Len(t, scanTableRows(t, db, tierTables[tc.tier]), tc.rows,
			"%s window %d", tc.tier, tc.start)
		require.NoError(t, db.Close())
	}
}

// TestConsumeCancelledMidAppendHoldsNeitherRowsNorRecord cancels the consume's
// context after a window's collapse statement lands its rows in the open
// transaction — before the consumption record — and proves the rollback leaves
// the window holding neither the rows nor the record: rows that escaped the
// transaction would survive its rollback and double-count on the retry. The
// resumed consume must then land every value exactly once.
func TestConsumeCancelledMidAppendHoldsNeitherRowsNorRecord(t *testing.T) {
	crashDir := t.TempDir()
	seedGoldenGeneration(t, crashDir)
	// the same generation consumed cleanly, to compare the resume against
	cleanDir := t.TempDir()
	copyTree(t, crashDir, cleanDir)

	s, _ := openTestStore(t, crashDir)
	defer func() { _ = s.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	appends := 0
	err := s.ConsumeGeneration(ctx, 0, ConsumeOptions{
		AppendWindow: func(ctx context.Context, conn *sql.Conn, tier string, windowStart, windowEnd int64) error {
			if err := collapseWindowRows(ctx, conn, tier, windowStart, windowEnd); err != nil {
				return err
			}
			appends++
			cancel() // rows flushed into the open transaction; the context dies before the record
			return nil
		},
	})
	require.Error(t, err)
	require.Equal(t, 1, appends)

	// the aborted window — the plan's first — holds neither rows nor record
	deltaPath := filepath.Join(crashDir, deltaFileName(0))
	windows, err := generationWindows(deltaPath, s.cfg.Resources)
	require.NoError(t, err)
	first := sortedWindowKeys(windows)[0]
	aborted, err := openStoreFile(filepath.Join(crashDir, archiveSubdir, archiveFileName(first.tier, first.start)), true, ResourcesConfig{})
	require.NoError(t, err)
	var rows int
	require.NoError(t, aborted.QueryRow("SELECT count(*) FROM " + tierTables[first.tier]).Scan(&rows))
	require.Zero(t, rows, "appender rows flushed into the open transaction must not survive its rollback")
	recorded, err := readConsumed(aborted)
	require.NoError(t, err)
	require.Empty(t, recorded, "an aborted append must leave no consumption record")
	require.NoError(t, aborted.Close())

	// the generation survived, unlinked nowhere; resuming lands everything the
	// clean consume landed, exactly once
	require.Equal(t, []int64{0, 1}, s.DeltaGenerations())
	require.NoError(t, s.ConsumeGeneration(context.Background(), 0, ConsumeOptions{AppendWindow: collapseWindowRows}))
	require.Equal(t, []int64{1}, s.DeltaGenerations())
	require.NoFileExists(t, deltaPath)

	clean, _ := openTestStore(t, cleanDir)
	defer func() { _ = clean.Close() }()
	require.NoError(t, clean.ConsumeGeneration(context.Background(), 0, ConsumeOptions{AppendWindow: collapseWindowRows}))
	require.Len(t, clean.Windows(), len(s.Windows()))
	for _, wf := range clean.Windows() {
		cleanDB, err := openStoreFile(wf.Path, true, ResourcesConfig{})
		require.NoError(t, err)
		resumedDB, err := openStoreFile(filepath.Join(crashDir, archiveSubdir, archiveFileName(wf.Tier, wf.WindowStart)), true, ResourcesConfig{})
		require.NoError(t, err)
		require.Equal(t, scanTableRows(t, cleanDB, tierTables[wf.Tier]), scanTableRows(t, resumedDB, tierTables[wf.Tier]),
			"%s window %d: the resumed consume must land exactly what a clean one does", wf.Tier, wf.WindowStart)
		require.NoError(t, cleanDB.Close())
		require.NoError(t, resumedDB.Close())
	}
}

// TestCompactOnceIdleStoreKeepsItsGeneration pins the idle path: no rows, no
// roll, no windows.
func TestCompactOnceIdleStoreKeepsItsGeneration(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir())
	require.NoError(t, NewCompactor(s, CompactorConfig{}).CompactOnce(context.Background()))
	require.Equal(t, []int64{0}, s.DeltaGenerations())
	require.Empty(t, s.Windows())
}

// TestNewCompactorDefaults checks the config defaults land.
func TestNewCompactorDefaults(t *testing.T) {
	c := NewCompactor(&Store{}, CompactorConfig{})
	require.Equal(t, DefaultCompactorInterval, c.cfg.Interval)
	require.NotNil(t, c.cfg.Logf)
}

// TestBlobListCoversDriverVariants feeds blobList every LIST(BLOB) shape
// duckdb-go can hand a Scan: nil, one []byte, a [][]byte, an []any of blobs,
// and the two shapes that must error. These arms exist precisely because the
// driver's scan type varies across versions; an upgrade would otherwise run
// code no test ever executed, failing every compaction and seal.
func TestBlobListCoversDriverVariants(t *testing.T) {
	out, err := blobList(nil)
	require.NoError(t, err)
	require.Nil(t, out)

	got, err := blobList([][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, got)

	got, err = blobList([]byte("single"))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("single")}, got)

	got, err = blobList([]any{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, got)

	_, err = blobList([]any{[]byte("a"), "not a blob"})
	require.Error(t, err)
	_, err = blobList(42)
	require.Error(t, err)
}

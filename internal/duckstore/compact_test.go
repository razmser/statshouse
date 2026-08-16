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

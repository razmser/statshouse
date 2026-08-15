// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writerNowUnix is the frozen clock writer tests run under; rows near it are
// inside the ingestion guard, rows far from it are not. Not minute- or
// hour-aligned, so tier truncation is observable.
const (
	writerNowUnix = int64(1740000000 + 100) // % 60 == 40, % 3600 == 1240
	testMetricID  = int32(5)
	testMetricID2 = int32(6)
)

var writerNow = time.Unix(writerNowUnix, 0)

// newTestWriter opens a writer on a fresh store under the frozen clock.
func newTestWriter(t *testing.T) (*Store, *Writer) {
	t.Helper()
	return newTestWriterCfg(t, WriterConfig{NowFunc: func() time.Time { return writerNow }})
}

func newTestWriterCfg(t *testing.T, cfg WriterConfig) (*Store, *Writer) {
	t.Helper()
	s, _ := openTestStore(t, t.TempDir())
	if cfg.NowFunc == nil {
		cfg.NowFunc = func() time.Time { return writerNow }
	}
	w, err := NewWriter(s, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = w.Close() })
	return s, w
}

// testRow builds one fully populated row: every aggregate, both sketches, all
// three host kinds and tags in both encodings, so a landed row exercises the
// whole column mapping.
func testRow(metric int32, ts uint32) Row {
	r := Row{
		Metric:      metric,
		Time:        ts,
		Count:       3,
		Min:         1.5,
		Max:         9.75,
		Sum:         21,
		SumSquare:   101.25,
		Percentiles: []byte{1, 2, 3, 4},
		Unique:      []byte{5, 6, 7},
		MinHost:     HostTag{ID: 7},
		MaxHost:     HostTag{S: "max host"},
	}
	r.Tags[0] = 11
	r.STags[1] = "raw tag"
	r.Tags[2] = 13
	r.STags[2] = "ignored when id set"
	r.Top = HostTag{S: "string top"}
	return r
}

// tierRow reads the single row a landed test row produces in one tier, with
// its time truncated to the tier, and asserts the aggregates travelled with
// it. Returns the row's stored time.
func tierRow(t *testing.T, s *Store, tier string, metric int32, ts int64) int64 {
	t.Helper()
	var count, sum, min, max, sumsquare float64
	var gotTS int64
	err := s.Delta().QueryRow(
		fmt.Sprintf(`SELECT time, count, sum, min, max, sumsquare FROM %s WHERE metric = $1 AND time = $2`, TierTable(tier)),
		metric, ts).Scan(&gotTS, &count, &sum, &min, &max, &sumsquare)
	require.NoError(t, err, "%s row for metric %d", tier, metric)
	require.EqualValues(t, 3, count)
	require.EqualValues(t, 21, sum)
	require.EqualValues(t, 1.5, min)
	require.EqualValues(t, 9.75, max)
	require.EqualValues(t, 101.25, sumsquare)
	return gotTS
}

// tierCount counts the rows one metric has in one tier.
func tierCount(t *testing.T, s *Store, tier string, metric int32) int {
	t.Helper()
	var n int
	require.NoError(t, s.Delta().QueryRow(
		fmt.Sprintf(`SELECT count(*) FROM %s WHERE metric = $1`, TierTable(tier)),
		metric).Scan(&n))
	return n
}

// TestWriterRoundLandsInAllTiersTruncated writes one round and proves the row
// reached all three tiers with time truncated to each tier, that it is
// committed (visible to a connection other than the writer's) as soon as
// WriteRound returns, and that the tag, host and sketch columns survived the
// mapping.
func TestWriterRoundLandsInAllTiersTruncated(t *testing.T) {
	s, w := newTestWriter(t)
	ts := uint32(writerNow.Unix()) - 37 // inside the guard, second 63 of its minute
	row := testRow(testMetricID, ts)
	require.NoError(t, w.WriteRound(context.Background(), []Row{row}))

	// the row is in all three tiers, each with its own time truncation
	for _, tc := range []struct {
		tier   string
		wantTS int64
	}{
		{Tier1s, int64(ts)},
		{Tier1m, int64(ts) / 60 * 60},
		{Tier1h, int64(ts) / 3600 * 3600},
	} {
		gotTS := tierRow(t, s, tc.tier, testMetricID, tc.wantTS)
		require.EqualValues(t, tc.wantTS, gotTS, "%s time must be truncated to the tier", tc.tier)
		require.Equal(t, 1, tierCount(t, s, tc.tier, testMetricID), "%s must hold exactly one row", tc.tier)
	}

	// tag mapping: id tags landed in tagN, raw strings in stagN, the string
	// top in slot 47, and an id with a string kept only its id half
	var tag0, tag2, tag47 int32
	var stag1, stag2, stag47 string
	require.NoError(t, s.Delta().QueryRow(
		`SELECT tag0, stag1, tag2, stag2, tag47, stag47 FROM s1 WHERE metric = $1`, testMetricID).
		Scan(&tag0, &stag1, &tag2, &stag2, &tag47, &stag47))
	require.EqualValues(t, 11, tag0)
	require.Equal(t, "raw tag", stag1)
	require.EqualValues(t, 13, tag2)
	require.Empty(t, stag2, "a tag with an id must not store its string half")
	require.Zero(t, tag47)
	require.Equal(t, "string top", stag47)

	// hosts and sketches are stored verbatim
	var minHost, maxHost int32
	var minShost, maxShost string
	var percentiles, uniq []byte
	require.NoError(t, s.Delta().QueryRow(
		`SELECT min_host, min_shost, max_host, max_shost, percentiles, uniq_state FROM s1 WHERE metric = $1`, testMetricID).
		Scan(&minHost, &minShost, &maxHost, &maxShost, &percentiles, &uniq))
	require.EqualValues(t, 7, minHost)
	require.Empty(t, minShost)
	require.Zero(t, maxHost)
	require.Equal(t, "max host", maxShost)
	require.Equal(t, testRow(testMetricID, ts).Percentiles, percentiles)
	require.Equal(t, testRow(testMetricID, ts).Unique, uniq)
}

// TestWriterDropsRowsOutsideIngestGuard checks the ClickHouse matview guard's
// counterpart, including both boundaries.
func TestWriterDropsRowsOutsideIngestGuard(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())

	rows := []Row{
		testRow(testMetricID, now-ingestGuardOldSecs),      // exactly three days old: kept
		testRow(testMetricID, now-ingestGuardOldSecs-1),    // one second older: dropped
		testRow(testMetricID, now-4*86400),                 // far past: dropped
		testRow(testMetricID, now+ingestGuardFutureSecs-1), // just under an hour ahead: kept
		testRow(testMetricID, now+ingestGuardFutureSecs),   // exactly an hour ahead: dropped
	}
	for i := range rows { // distinct metrics so tier counts identify the survivors
		if i > 0 {
			rows[i].Metric = testMetricID + int32(i)
		}
	}
	require.NoError(t, w.WriteRound(context.Background(), rows))

	// the two survivors are in every tier; the three dropped rows in none
	for _, tier := range allTiers() {
		require.Equal(t, 1, tierCount(t, s, tier, testMetricID), "%s: boundary-old row", tier)
		require.Equal(t, 0, tierCount(t, s, tier, testMetricID+1), "%s: one-second-too-old row", tier)
		require.Equal(t, 0, tierCount(t, s, tier, testMetricID+2), "%s: far-past row", tier)
		require.Equal(t, 1, tierCount(t, s, tier, testMetricID+3), "%s: boundary-future row", tier)
		require.Equal(t, 0, tierCount(t, s, tier, testMetricID+4), "%s: too-future row", tier)
	}
}

// TestWriterFailedRoundSurfacesError proves a forced write failure fails the
// round the way a storage error must, and that the writer recovers: the next
// round lands.
func TestWriterFailedRoundSurfacesError(t *testing.T) {
	s, w := newTestWriterCfg(t, WriterConfig{
		NowFunc: func() time.Time { return writerNow },
		FlushFault: func(round int64) error {
			if round != 2 {
				return nil // only the second round hits the fault
			}
			return fmt.Errorf("round %d: simulated disk failure", round)
		},
	})

	now := uint32(writerNow.Unix())
	require.NoError(t, w.WriteRound(context.Background(), []Row{testRow(testMetricID, now)}))
	require.Equal(t, 1, tierCount(t, s, Tier1s, testMetricID))

	err := w.WriteRound(context.Background(), []Row{testRow(testMetricID2, now)})
	require.Error(t, err, "a forced write failure must surface as a failed round")
	require.Contains(t, err.Error(), "simulated disk failure")

	// the failed round left nothing behind, and the writer still takes rounds
	require.Equal(t, 0, tierCount(t, s, Tier1s, testMetricID2), "the failed round must not land")
	require.NoError(t, w.WriteRound(context.Background(), []Row{testRow(testMetricID2, now)}))
	for _, tier := range allTiers() {
		require.Equal(t, 1, tierCount(t, s, tier, testMetricID2), "%s must hold the recovered round", tier)
	}
}

// TestWriterFlushJoinsRoundTransaction pins the duckdb-go behaviour the whole
// round atomicity rests on: an appender flush executes inside the connection's
// open transaction rather than auto-committing, so a ROLLBACK discards rows a
// flush already pushed. If a driver upgrade ever breaks this, rounds are no
// longer all-or-nothing and this test must fail before anything in production
// notices double counts.
func TestWriterFlushJoinsRoundTransaction(t *testing.T) {
	s, _ := newTestWriter(t)
	ctx := context.Background()
	conn, err := s.Delta().Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	appenders, err := createTierAppenders(conn)
	require.NoError(t, err)
	wa := &Writer{appenders: appenders, conn: conn}

	row := testRow(testMetricID, uint32(writerNow.Unix()))
	_, err = conn.ExecContext(ctx, "BEGIN TRANSACTION")
	require.NoError(t, err)
	require.NoError(t, wa.appendTierRow(Tier1s, &row))
	require.NoError(t, appenders[Tier1s].FlushWithCancel(ctx))
	var n int
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT count(*) FROM s1 WHERE metric = $1", testMetricID).Scan(&n))
	require.Equal(t, 1, n, "the flushed row must be visible inside the transaction")
	_, err = conn.ExecContext(ctx, "ROLLBACK")
	require.NoError(t, err)
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT count(*) FROM s1 WHERE metric = $1", testMetricID).Scan(&n))
	require.Zero(t, n, "ROLLBACK must discard rows the appender flushed — round atomicity depends on it")
}

// TestWriterFailedMidRoundCommitsNothing fails a round BETWEEN the tiers'
// flushes — the shape a real storage error takes — and proves the failed round
// is absent from every tier (not just the unflushed ones), so the conveyor's
// resend cannot double-count, and that the writer takes the next round.
func TestWriterFailedMidRoundCommitsNothing(t *testing.T) {
	s, w := newTestWriterCfg(t, WriterConfig{
		NowFunc: func() time.Time { return writerNow },
		FlushTierFault: func(round int64, tier string) error {
			if round == 1 && tier == Tier1m { // 1s already flushed when this fires
				return fmt.Errorf("round %d: simulated %s flush failure", round, tier)
			}
			return nil
		},
	})
	now := uint32(writerNow.Unix())

	err := w.WriteRound(context.Background(), []Row{testRow(testMetricID, now)})
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated 1m flush failure")

	// the failed round must be absent from ALL three tiers: 1s flushed fine,
	// but the round rolled back with it
	for _, tier := range allTiers() {
		require.Zero(t, tierCount(t, s, tier, testMetricID), "%s must not hold any of the failed round", tier)
	}

	// the writer recovers and the resent round lands exactly once
	require.NoError(t, w.WriteRound(context.Background(), []Row{testRow(testMetricID, now)}))
	for _, tier := range allTiers() {
		require.Equal(t, 1, tierCount(t, s, tier, testMetricID), "%s must hold the recovered round once", tier)
	}
}

// TestWriterClosedRefusesRounds checks the closed-writer path.
func TestWriterClosedRefusesRounds(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())
	require.NoError(t, w.WriteRound(context.Background(), []Row{testRow(testMetricID, now)}))
	require.NoError(t, w.Close())
	require.Error(t, w.WriteRound(context.Background(), []Row{testRow(testMetricID2, now)}))
	require.NoError(t, w.Close(), "second Close must be a no-op")
	// the store keeps serving reads after its writer is gone
	require.Equal(t, 1, tierCount(t, s, Tier1s, testMetricID))
}

// TestWriterDataSurvivesReopen closes the store with everything it wrote only
// in the write-ahead log, reopens it and proves the acknowledged round is
// still there in every tier — acknowledgement meant durable.
func TestWriterDataSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, _ := openTestStore(t, dir)
	w, err := NewWriter(s, WriterConfig{NowFunc: func() time.Time { return writerNow }})
	require.NoError(t, err)

	now := uint32(writerNow.Unix())
	row := testRow(testMetricID, now-5)
	require.NoError(t, w.WriteRound(context.Background(), []Row{row}))
	require.NoError(t, w.Close())
	require.NoError(t, s.Close())

	s2, _ := openTestStore(t, dir)
	defer func() { _ = s2.Close() }()
	for _, tc := range []struct {
		tier   string
		wantTS int64
	}{
		{Tier1s, int64(now - 5)},
		{Tier1m, int64(now-5) / 60 * 60},
		{Tier1h, int64(now-5) / 3600 * 3600},
	} {
		tierRow(t, s2, tc.tier, testMetricID, tc.wantTS)
	}
}

// TestWriterCancelledContextFailsRound proves a caller that gives up gets an
// error rather than a silent partial acknowledgement.
func TestWriterCancelledContextFailsRound(t *testing.T) {
	_, w := newTestWriter(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := w.WriteRound(ctx, []Row{testRow(testMetricID, uint32(writerNow.Unix()))})
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

// TestWriterSerializesConcurrentRounds drives several round submitters at the
// single writer goroutine at once and proves every acknowledged round landed,
// exactly once, in every tier.
func TestWriterSerializesConcurrentRounds(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())

	const submitters = 4
	const rounds = 10
	var wg sync.WaitGroup
	for g := 0; g < submitters; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				row := testRow(testMetricID+int32(g), now-uint32(i))
				row.Count = 1
				row.Sum = 1
				if err := w.WriteRound(context.Background(), []Row{row}); err != nil {
					t.Errorf("round %d of submitter %d failed: %v", i, g, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	for g := 0; g < submitters; g++ {
		var count, sum float64
		require.NoError(t, s.Delta().QueryRow(
			`SELECT sum(count), sum(sum) FROM s1 WHERE metric = $1`, testMetricID+int32(g)).Scan(&count, &sum))
		require.EqualValues(t, rounds, count, "every acknowledged round must land exactly once")
		require.EqualValues(t, rounds, sum)
	}
}

// TestWithinIngestGuard pins the guard bounds to the ClickHouse matview
// predicate they mirror.
func TestWithinIngestGuard(t *testing.T) {
	const now = int64(1740000000)
	require.True(t, withinIngestGuard(now, now))
	require.True(t, withinIngestGuard(now-ingestGuardOldSecs, now), "exactly three days old is kept")
	require.False(t, withinIngestGuard(now-ingestGuardOldSecs-1, now))
	require.True(t, withinIngestGuard(now+ingestGuardFutureSecs-1, now), "just under an hour ahead is kept")
	require.False(t, withinIngestGuard(now+ingestGuardFutureSecs, now))
}

// TestWriterEmptyRoundIsNoop keeps the never-empty conveyor honest: an empty
// round succeeds and lands nothing.
func TestWriterEmptyRoundIsNoop(t *testing.T) {
	s, w := newTestWriter(t)
	require.NoError(t, w.WriteRound(context.Background(), nil))
	for _, tier := range allTiers() {
		require.Equal(t, 0, tierCount(t, s, tier, testMetricID))
	}
}

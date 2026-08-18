// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
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

// testRow builds one fully populated row: every aggregate, both aggregate
// states, all
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
		MinHost:     HostPair{Tag: HostTag{ID: 7}, Value: 0.7},
		MaxHost:     HostPair{Tag: HostTag{S: "max host"}, Value: 8.25},
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

// TestWriterRoundLandsIn1sTier writes one round and proves the row reached
// the delta's one table — the 1s tier, at its raw second — that it is
// committed (visible to a connection other than the writer's) as soon as
// WriteRound returns, that the coarser tiers' tables are gone from the delta
// entirely, and that the tag, host and aggregate-state columns survived the
// mapping.
func TestWriterRoundLandsIn1sTier(t *testing.T) {
	s, w := newTestWriter(t)
	ts := uint32(writerNow.Unix()) - 37 // inside the guard, second 63 of its minute
	row := testRow(testMetricID, ts)
	require.NoError(t, w.WriteRound(context.Background(), []Row{row}))

	// the row sits in the 1s table at its raw second, exactly once
	gotTS := tierRow(t, s, Tier1s, testMetricID, int64(ts))
	require.EqualValues(t, ts, gotTS, "1s time is the row's own second")
	require.Equal(t, 1, tierCount(t, s, Tier1s, testMetricID))

	// the delta carries no coarser-tier table to append to: those tiers
	// derive from the 1s rows at compaction and read time
	var coarse int
	require.NoError(t, s.Delta().QueryRow(
		`SELECT count(*) FROM duckdb_tables() WHERE table_name IN ('s1m', 's1h')`).Scan(&coarse))
	require.Zero(t, coarse, "the delta must hold the 1s table alone")

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

	// hosts and aggregate states are stored verbatim, skew values included
	var minHost, maxHost int32
	var minShost, maxShost string
	var minHostValue, maxHostValue float64
	var percentiles, uniq []byte
	require.NoError(t, s.Delta().QueryRow(
		`SELECT min_host, min_shost, min_host_value, max_host, max_shost, max_host_value, percentiles, uniq_state FROM s1 WHERE metric = $1`, testMetricID).
		Scan(&minHost, &minShost, &minHostValue, &maxHost, &maxShost, &maxHostValue, &percentiles, &uniq))
	require.EqualValues(t, 7, minHost)
	require.Empty(t, minShost)
	require.Zero(t, maxHost)
	require.Equal(t, "max host", maxShost)
	require.Equal(t, 0.7, minHostValue, "the skewed state value travels with its host")
	require.Equal(t, 8.25, maxHostValue)
	require.Equal(t, testRow(testMetricID, ts).Percentiles, percentiles)
	require.Equal(t, testRow(testMetricID, ts).Unique, uniq)
}

// TestWriterDropsRowsOutsideIngestGuard checks the ingest guard's
// counterpart, including both boundaries.
func TestWriterDropsRowsOutsideIngestGuard(t *testing.T) {
	s, w := newTestWriter(t)
	now := uint32(writerNow.Unix())

	rows := []Row{
		testRow(testMetricID, now-ingestGuardOldSecs),      // exactly the historic window old: kept
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

	// the two survivors are stored; the three dropped rows are not
	require.Equal(t, 1, tierCount(t, s, Tier1s, testMetricID), "boundary-old row")
	require.Equal(t, 0, tierCount(t, s, Tier1s, testMetricID+1), "one-second-too-old row")
	require.Equal(t, 0, tierCount(t, s, Tier1s, testMetricID+2), "far-past row")
	require.Equal(t, 1, tierCount(t, s, Tier1s, testMetricID+3), "boundary-future row")
	require.Equal(t, 0, tierCount(t, s, Tier1s, testMetricID+4), "too-future row")
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
	require.Equal(t, 1, tierCount(t, s, Tier1s, testMetricID2), "the recovered round must land")
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
	appender, err := createAppender(conn)
	require.NoError(t, err)
	wa := &Writer{appender: appender, conn: conn}

	row := testRow(testMetricID, uint32(writerNow.Unix()))
	_, err = conn.ExecContext(ctx, "BEGIN TRANSACTION")
	require.NoError(t, err)
	require.NoError(t, wa.appendRow(&row))
	require.NoError(t, appender.FlushWithCancel(ctx))
	var n int
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT count(*) FROM s1 WHERE metric = $1", testMetricID).Scan(&n))
	require.Equal(t, 1, n, "the flushed row must be visible inside the transaction")
	_, err = conn.ExecContext(ctx, "ROLLBACK")
	require.NoError(t, err)
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT count(*) FROM s1 WHERE metric = $1", testMetricID).Scan(&n))
	require.Zero(t, n, "ROLLBACK must discard rows the appender flushed — round atomicity depends on it")
}

// TestWriterFailedMidRoundCommitsNothing fails a round BETWEEN the append
// and the flush — the shape a real storage error takes — and proves the
// failed round is absent from the delta even though its rows were already
// appended, so the conveyor's resend cannot double-count, and that the writer
// takes the next round.
func TestWriterFailedMidRoundCommitsNothing(t *testing.T) {
	s, w := newTestWriterCfg(t, WriterConfig{
		NowFunc: func() time.Time { return writerNow },
		FlushTierFault: func(round int64) error {
			if round == 1 { // the round's rows are already appended when this fires
				return fmt.Errorf("round %d: simulated %s flush failure", round, Tier1s)
			}
			return nil
		},
	})
	now := uint32(writerNow.Unix())

	err := w.WriteRound(context.Background(), []Row{testRow(testMetricID, now)})
	require.Error(t, err)
	require.Contains(t, err.Error(), "simulated 1s flush failure")

	// the failed round must be absent: its rows were appended, but the
	// rollback discarded the flush and the buffered leftovers with it
	require.Zero(t, tierCount(t, s, Tier1s, testMetricID), "the failed round must not land")

	// the writer recovers and the resent round lands exactly once
	require.NoError(t, w.WriteRound(context.Background(), []Row{testRow(testMetricID, now)}))
	require.Equal(t, 1, tierCount(t, s, Tier1s, testMetricID), "the recovered round must land once")
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
// still there — acknowledgement meant durable.
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
	tierRow(t, s2, Tier1s, testMetricID, int64(now-5))
}

// TestWriterCancelledContextFailsRound proves a caller that gives up gets an
// error rather than a silent partial acknowledgement. The select in
// WriteRound may still hand the round to the writer goroutine before noticing
// the cancellation, and duckdb-go starts an already-cancelled flush anyway —
// one that may beat its own interrupt and commit — so the two honest outcomes
// are a cancellation error (the round rolled back) or a true acknowledgement
// (the row really landed). Anything else, including an error with the row
// committed, is a false report.
func TestWriterCancelledContextFailsRound(t *testing.T) {
	s, w := newTestWriter(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	now := uint32(writerNow.Unix())
	err := w.WriteRound(ctx, []Row{testRow(testMetricID, now)})
	if err == nil {
		tierRow(t, s, Tier1s, testMetricID, int64(now)) // the ack must be true
	} else {
		require.ErrorIs(t, err, context.Canceled)
		require.Zero(t, tierCount(t, s, Tier1s, testMetricID), "a failed round must not land")
	}
}

// TestWriterCancelledContextKeepsRowOwnership pins the ownership half of the
// cancellation contract: the caller may reuse its row slice the moment
// WriteRound returns, so WriteRound must NOT return while the writer
// goroutine is still reading the round's rows — the regression this guards
// against returned ctx.Err() on cancellation and let the next round's AppendRow
// overwrite rows the writer was concurrently appending into the delta.
func TestWriterCancelledContextKeepsRowOwnership(t *testing.T) {
	inRound := make(chan struct{})
	release := make(chan struct{})
	_, w := newTestWriterCfg(t, WriterConfig{
		FlushTierFault: func(round int64) error {
			select {
			case <-inRound:
			default:
				close(inRound)
			}
			<-release
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- w.WriteRound(ctx, []Row{testRow(testMetricID, uint32(writerNow.Unix()))})
	}()
	<-inRound // the writer goroutine is mid-round, holding the rows
	cancel()  // the caller gives up
	select {
	case err := <-done:
		t.Fatalf("WriteRound returned %v while the round was still in flight", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release) // let the round finish
	// now it returns, with the round's true outcome (nil or the cancellation)
	err := <-done
	require.True(t, err == nil || errors.Is(err, context.Canceled), "unexpected round outcome: %v", err)
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

// TestWithinIngestGuard pins the guard bounds, and TestIngestGuardHorizon
// below pins the old bound to the seal horizon they must not cross.
func TestWithinIngestGuard(t *testing.T) {
	const now = int64(1740000000)
	require.True(t, withinIngestGuard(now, now))
	require.True(t, withinIngestGuard(now-ingestGuardOldSecs, now), "exactly the historic window old is kept")
	require.False(t, withinIngestGuard(now-ingestGuardOldSecs-1, now))
	require.True(t, withinIngestGuard(now+ingestGuardFutureSecs-1, now), "just under an hour ahead is kept")
	require.False(t, withinIngestGuard(now+ingestGuardFutureSecs, now))
}

// TestIngestGuardHorizon pins the guard's old bound to the historic window:
// windows seal at their end plus data_model.MaxHistoricWindow, so a row
// older than that could only target an already-sealed window. Widening this
// bound past the historic window re-opens the wedge consumeWindow guards
// against; change the two together or not at all.
func TestIngestGuardHorizon(t *testing.T) {
	require.EqualValues(t, data_model.MaxHistoricWindow, ingestGuardOldSecs)
}

// TestWriterEmptyRoundIsNoop keeps the never-empty conveyor honest: an empty
// round succeeds and lands nothing.
func TestWriterEmptyRoundIsNoop(t *testing.T) {
	s, w := newTestWriter(t)
	require.NoError(t, w.WriteRound(context.Background(), nil))
	require.Equal(t, 0, tierCount(t, s, Tier1s, testMetricID))
}

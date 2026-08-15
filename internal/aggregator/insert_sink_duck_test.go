// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package aggregator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/duckstore"
	"github.com/VKCOM/statshouse/internal/format"
)

// sinkNowUnix is the frozen clock the duck sink tests run under — the writer's
// age guard needs a NowFunc, which openDuckStore cannot inject.
const (
	sinkNowUnix      = int64(1740000000 + 100) // % 60 == 40: truncation is observable
	sinkTestMetricID = int32(10)
)

var sinkNow = time.Unix(sinkNowUnix, 0)

// newTestDuckSink opens a store and writer directly, the way openDuckStore
// does but under the frozen clock, keeping the store handle for assertions.
func newTestDuckSink(t *testing.T, cfg duckstore.WriterConfig) (*duckstore.Store, *duckSink) {
	t.Helper()
	s, err := duckstore.OpenStore(duckstore.StoreConfig{Dir: t.TempDir(), Logf: t.Logf})
	require.NoError(t, err)
	cfg.NowFunc = func() time.Time { return sinkNow }
	w, err := duckstore.NewWriter(s, cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = w.Close()
		_ = s.Close()
	})
	return s, newDuckSink(w)
}

// sinkTestInsertRow builds one fully populated insertRow, the conveyor-side
// counterpart of duckstore's testRow: every aggregate, both sketches, both
// host encodings and tags in both encodings.
func sinkTestInsertRow(metric int32, ts uint32) insertRow {
	row := insertRow{
		key:         data_model.Key{Timestamp: ts, Metric: metric},
		top:         data_model.TagUnion{S: "duck top"},
		count:       3,
		min:         1.5,
		max:         9.75,
		sum:         21,
		sumSquare:   101.25,
		percentiles: []byte{1, 2, 3, 4},
		unique:      []byte{5, 6, 7},
		minHost:     hostPair{tag: data_model.TagUnion{I: 7}, value: 0.5},
		maxHost:     hostPair{tag: data_model.TagUnion{S: "max host"}, value: 0.5},
	}
	row.key.Tags[0] = 11
	row.key.STags[1] = "raw tag"
	row.key.Tags[2] = 13
	row.key.STags[2] = "ignored when id set"
	return row
}

// sinkTierCount counts the rows one metric has in one tier.
func sinkTierCount(t *testing.T, s *duckstore.Store, tier string, metric int32) int {
	t.Helper()
	var n int
	require.NoError(t, s.Delta().QueryRow(
		fmt.Sprintf(`SELECT count(*) FROM %s WHERE metric = $1`, duckstore.TierTable(tier)),
		metric).Scan(&n))
	return n
}

// TestDuckSinkRoundLandsInStore drives one append/send cycle end to end and
// checks the conversion: the resolved row's tags, string top, hosts, sketches
// and aggregates must land decoded in the store, in all three tiers with time
// truncated, and the per-row size accounting must match the ClickHouse
// encoder's.
func TestDuckSinkRoundLandsInStore(t *testing.T) {
	s, sink := newTestDuckSink(t, duckstore.WriterConfig{})
	ts := uint32(sinkNowUnix - 37) // inside the guard, second 63 of its minute
	row := sinkTestInsertRow(sinkTestMetricID, ts)

	require.Equal(t, rowBinarySize(&row), sink.AppendRow(&row))
	require.Equal(t, rowBinarySize(&row), sink.RoundSize(), "one row's size must be the round's size")

	status, exception, _, err := sink.Send(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Zero(t, exception)

	// all three tiers, each with its own time truncation
	for _, tc := range []struct {
		tier   string
		wantTS int64
	}{
		{duckstore.Tier1s, int64(ts)},
		{duckstore.Tier1m, int64(ts) / 60 * 60},
		{duckstore.Tier1h, int64(ts) / 3600 * 3600},
	} {
		require.Equal(t, 1, sinkTierCount(t, s, tc.tier, sinkTestMetricID), "%s must hold the row", tc.tier)
		var gotTS int64
		require.NoError(t, s.Delta().QueryRow(
			fmt.Sprintf(`SELECT time FROM %s WHERE metric = $1`, duckstore.TierTable(tc.tier)),
			sinkTestMetricID).Scan(&gotTS))
		require.EqualValues(t, tc.wantTS, gotTS, "%s time must be truncated to the tier", tc.tier)
	}

	// the conversion: tags in both encodings, the string top in slot 47, and
	// an id with a string keeping only its id half
	var tag0, tag2, tag47 int32
	var stag1, stag2, stag47 string
	var count, min, max, sum, sumsquare float64
	require.NoError(t, s.Delta().QueryRow(
		`SELECT tag0, stag1, tag2, stag2, tag47, stag47, count, min, max, sum, sumsquare FROM s1 WHERE metric = $1`,
		sinkTestMetricID).Scan(&tag0, &stag1, &tag2, &stag2, &tag47, &stag47, &count, &min, &max, &sum, &sumsquare))
	require.EqualValues(t, 11, tag0)
	require.Equal(t, "raw tag", stag1)
	require.EqualValues(t, 13, tag2)
	require.Empty(t, stag2, "a tag with an id must not store its string half")
	require.Zero(t, tag47, "the string top must land through its own columns")
	require.Equal(t, "duck top", stag47)
	require.EqualValues(t, 3, count)
	require.EqualValues(t, 1.5, min)
	require.EqualValues(t, 9.75, max)
	require.EqualValues(t, 21, sum)
	require.EqualValues(t, 101.25, sumsquare)

	// hosts and sketches are stored verbatim
	var minHost, maxHost int32
	var minShost, maxShost string
	var percentiles, uniq []byte
	require.NoError(t, s.Delta().QueryRow(
		`SELECT min_host, min_shost, max_host, max_shost, percentiles, uniq_state FROM s1 WHERE metric = $1`,
		sinkTestMetricID).Scan(&minHost, &minShost, &maxHost, &maxShost, &percentiles, &uniq))
	require.EqualValues(t, 7, minHost)
	require.Empty(t, minShost)
	require.Zero(t, maxHost)
	require.Equal(t, "max host", maxShost)
	require.Equal(t, []byte{1, 2, 3, 4}, percentiles)
	require.Equal(t, []byte{5, 6, 7}, uniq)
}

// TestDuckSinkSendMapsFailure maps a writer failure onto the conveyor's
// quadruple — zero status and the error, the shape that keeps contributors
// unacked — and proves a later round succeeds again.
func TestDuckSinkSendMapsFailure(t *testing.T) {
	s, sink := newTestDuckSink(t, duckstore.WriterConfig{
		FlushFault: func(round int64) error {
			if round != 1 {
				return nil // only the first round fails
			}
			return fmt.Errorf("round %d: simulated disk failure", round)
		},
	})
	row := sinkTestInsertRow(sinkTestMetricID, uint32(sinkNowUnix))
	sink.AppendRow(&row)

	status, exception, _, err := sink.Send(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "duck-store insert round failed")
	require.Contains(t, err.Error(), "simulated disk failure")
	require.Zero(t, status)
	require.Zero(t, exception)
	require.Equal(t, 0, sinkTierCount(t, s, duckstore.Tier1s, sinkTestMetricID), "the failed round must not land")

	// the conveyor would reset and retry the round through a fresh append
	sink.Reset()
	row2 := sinkTestInsertRow(sinkTestMetricID+1, uint32(sinkNowUnix))
	sink.AppendRow(&row2)
	status, exception, _, err = sink.Send(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
	require.Zero(t, exception)
	require.Equal(t, 1, sinkTierCount(t, s, duckstore.Tier1s, sinkTestMetricID+1))
}

// TestDuckSinkRoundSizeAndReset checks the size accounting and the reset
// contract across multiple rows and rounds.
func TestDuckSinkRoundSizeAndReset(t *testing.T) {
	_, sink := newTestDuckSink(t, duckstore.WriterConfig{})
	now := uint32(sinkNowUnix)

	row1 := sinkTestInsertRow(sinkTestMetricID, now)
	row2 := sinkTestInsertRow(sinkTestMetricID+1, now)
	n1 := sink.AppendRow(&row1)
	n2 := sink.AppendRow(&row2)
	require.Equal(t, rowBinarySize(&row1), n1)
	require.Equal(t, rowBinarySize(&row2), n2)
	require.Equal(t, n1+n2, sink.RoundSize())

	sink.Reset()
	require.Zero(t, sink.RoundSize())

	// a reset round is a successful no-op write, and new rows accumulate from zero
	row3 := sinkTestInsertRow(sinkTestMetricID, now)
	n3 := sink.AppendRow(&row3)
	require.Equal(t, n1, n3)
	require.Equal(t, n3, sink.RoundSize())
	status, _, _, err := sink.Send(context.Background())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, status)
}

// TestDuckSinkCopiesSketchBytes guards the duck sink's side of the scratch
// contract: AppendRow must copy the sketch bytes, because the conveyor reuses
// the scratch they were encoded into before Send ever runs.
func TestDuckSinkCopiesSketchBytes(t *testing.T) {
	s, sink := newTestDuckSink(t, duckstore.WriterConfig{})
	row := sinkTestInsertRow(sinkTestMetricID, uint32(sinkNowUnix))
	sink.AppendRow(&row)

	// the conveyor's next resolve overwrites the scratch under the same slices
	for i := range row.percentiles {
		row.percentiles[i] = 0xAA
	}
	for i := range row.unique {
		row.unique[i] = 0xBB
	}

	_, _, _, err := sink.Send(context.Background())
	require.NoError(t, err)
	var percentiles, uniq []byte
	require.NoError(t, s.Delta().QueryRow(
		`SELECT percentiles, uniq_state FROM s1 WHERE metric = $1`, sinkTestMetricID).
		Scan(&percentiles, &uniq))
	require.Equal(t, []byte{1, 2, 3, 4}, percentiles, "AppendRow must have copied the percentiles")
	require.Equal(t, []byte{5, 6, 7}, uniq, "AppendRow must have copied the uniques")
}

// TestOpenDuckStore covers the plumbing: the dir must be set, and a set dir
// yields a handle that produces working sinks and closes cleanly. A nil
// metrics agent is allowed — the store and its maintenance run without
// observability in that case.
func TestOpenDuckStore(t *testing.T) {
	t.Run("empty_dir_is_rejected", func(t *testing.T) {
		_, err := openDuckStore(ConfigAggregator{}, nil)
		require.Error(t, err)
		require.Contains(t, err.Error(), "store directory is not set")
	})

	t.Run("opens_and_closes", func(t *testing.T) {
		handle, err := openDuckStore(ConfigAggregator{DuckStoreDir: t.TempDir()}, nil)
		require.NoError(t, err)
		require.NotNil(t, handle)

		sink, ok := handle.NewSink().(*duckSink)
		require.True(t, ok, "the handle's sinks must be duck sinks")
		_, _, _, err = sink.Send(context.Background())
		require.NoError(t, err, "an empty round is a no-op success")

		require.NoError(t, handle.Close())
	})
}

// TestDuckMetricsTagMappings proves the duck*Tag helpers name every event
// value the store emits with the constant its metric's value comments
// document, and collapse anything unknown to 0 — the value no comment names.
func TestDuckMetricsTagMappings(t *testing.T) {
	require.Equal(t, int32(format.TagValueIDStatusOK), duckStatusTag(nil))
	require.Equal(t, int32(format.TagValueIDStatusError), duckStatusTag(errors.New("boom")))

	require.Equal(t, int32(format.TagValueIDDuckMaintenanceCompaction), duckMaintenanceTag(duckstore.MaintenanceCompaction))
	require.Equal(t, int32(format.TagValueIDDuckMaintenanceSealing), duckMaintenanceTag(duckstore.MaintenanceSealing))
	require.Equal(t, int32(format.TagValueIDDuckMaintenanceRetention), duckMaintenanceTag(duckstore.MaintenanceRetention))
	require.Zero(t, duckMaintenanceTag(duckstore.MaintenanceKind("other")))

	require.Equal(t, int32(format.TagValueIDDuckWindowSealed), duckWindowEventTag(duckstore.WindowSealed))
	require.Equal(t, int32(format.TagValueIDDuckWindowUnlinked), duckWindowEventTag(duckstore.WindowUnlinked))
	require.Equal(t, int32(format.TagValueIDDuckWindowEarlyEvicted), duckWindowEventTag(duckstore.WindowEarlyEvicted))
	require.Equal(t, int32(format.TagValueIDDuckWindowLeaseDeferred), duckWindowEventTag(duckstore.WindowLeaseDeferred))
	require.Zero(t, duckWindowEventTag(duckstore.WindowEventKind("other")))

	require.Equal(t, int32(format.TagValueIDDuckTier1s), duckTierTag(duckstore.Tier1s))
	require.Equal(t, int32(format.TagValueIDDuckTier1m), duckTierTag(duckstore.Tier1m))
	require.Equal(t, int32(format.TagValueIDDuckTier1h), duckTierTag(duckstore.Tier1h))
	require.Zero(t, duckTierTag("no such tier"))

	require.Equal(t, int32(format.TagValueIDDuckQuarantineSchema), duckQuarantineAxisTag(duckstore.QuarantineSchema))
	require.Equal(t, int32(format.TagValueIDDuckQuarantineStorage), duckQuarantineAxisTag(duckstore.QuarantineStorage))
	require.Equal(t, int32(format.TagValueIDDuckQuarantineStatshouse), duckQuarantineAxisTag(duckstore.QuarantineStatshouse))
	require.Equal(t, int32(format.TagValueIDDuckQuarantineUnreadable), duckQuarantineAxisTag(duckstore.QuarantineUnreadable))
	require.Zero(t, duckQuarantineAxisTag(duckstore.QuarantineAxis("other")))

	require.Equal(t, int32(format.TagValueIDDuckQuerySeries), duckQueryVerbTag(duckstore.QuerySeries))
	require.Equal(t, int32(format.TagValueIDDuckQueryTagValues), duckQueryVerbTag(duckstore.QueryTagValues))
	require.Zero(t, duckQueryVerbTag(duckstore.QueryVerb("other")))

	require.Equal(t, int32(format.TagValueIDDuckSizeDelta), duckSizeLocationTag(duckstore.SizeDelta))
	require.Equal(t, int32(format.TagValueIDDuckSizeArchive), duckSizeLocationTag(duckstore.SizeArchive))
	require.Zero(t, duckSizeLocationTag(duckstore.SizeLocation("other")))
}

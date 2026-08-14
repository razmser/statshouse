// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package aggregator

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/data_model"
	"github.com/VKCOM/statshouse/internal/data_model/gen2/tlstatshouse"
	"github.com/VKCOM/statshouse/internal/duckstore"
)

// Ingestion must never yield to queries: with both admission slots held by
// running queries, a third query is refused as overloaded while insert rounds
// keep landing in the real store behind the real wiring (openDuckStore's
// writer, its sink, the resource-bounded store files). WriteRound returns only
// after the round is flushed and fsynced, so answered rounds mean ingestion
// genuinely progressed under query saturation — the refusal path touches no
// store file and takes no lock the insert path needs.
func TestStoreQueryServerIngestKeepsRunningWhileQueriesRefused(t *testing.T) {
	config := DefaultConfigAggregator()
	config.DuckStoreDir = t.TempDir()
	handle, err := openDuckStore(config)
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })
	sink := handle.NewSink()

	f := newGatedQueryExecutor()
	cl := startTestQueryServer(t, f, 2)

	// Hold both admission slots with queries that run until released.
	for i := 0; i < 2; i++ {
		go func() {
			var ret tlstatshouse.StoreSeriesResponse
			_ = cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
		}()
	}
	waitStarted(t, f, 2)

	// The slots are busy: the next query is refused as overloaded, promptly.
	var ret tlstatshouse.StoreSeriesResponse
	err = cl.StoreQuerySeries(context.Background(), tlstatshouse.StoreQuerySeries{}, nil, &ret)
	requireErrorCode(t, err, duckstore.ErrCodeOverloaded, "third concurrent query")

	// While queries hold every slot, two insert rounds land back to back.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for round := uint32(0); round < 2; round++ {
		sink.Reset()
		row := insertRow{key: data_model.Key{Timestamp: goldenNow + round, Metric: goldenMetricCounter}}
		row.count = 4
		sink.AppendRow(&row)
		status, _, _, err := sink.Send(ctx)
		require.NoError(t, err, "insert round %d must land while queries hold every slot", round)
		require.Equal(t, http.StatusOK, status, "the duck sink must report the round as a successful insert")
	}

	f.openGate()
}

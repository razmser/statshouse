// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"flag"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/duckstore"
)

func TestConfigStorageBackend(t *testing.T) {
	cfg := DefaultConfig()
	require.Equal(t, duckstore.BackendClickHouse, cfg.StorageBackend)
	require.NoError(t, cfg.ValidateConfig())

	// the flag must round-trip through the API's own flag surface, including
	// the remote-config line-by-line reparse done by the config listener
	f := flag.NewFlagSet("test", flag.ContinueOnError)
	f.SetOutput(io.Discard)
	cfg.Bind(f, DefaultConfig())
	require.NoError(t, f.Parse([]string{"--storage-backend=duck"}))
	require.Equal(t, duckstore.BackendDuck, cfg.StorageBackend)
	// the API never embeds DuckDB (it queries the aggregators over RPC), so
	// duck is a valid backend for an ordinary API binary — but only with the
	// shard query addresses every query fans out over
	err := cfg.ValidateConfig()
	require.Error(t, err, "duck without shard query addresses cannot serve anything")
	require.Contains(t, err.Error(), "--duck-shard-query-addrs")
	require.Contains(t, err.Error(), "--storage-backend=duck")

	require.NoError(t, f.Parse([]string{"--duck-shard-query-addrs=1=10.0.0.1:9900,2=10.0.0.2:9900", "--shard-by-metric-shards=2"}))
	require.NoError(t, cfg.ValidateConfig())
	require.Equal(t, map[uint32]string{1: "10.0.0.1:9900", 2: "10.0.0.2:9900"}, cfg.DuckShardQueryAddrs)

	// the by-metric-id routing shards by --shard-by-metric-shards (the copy
	// of the aggregator cluster's modulus), so a partial address list — a
	// contiguous prefix of a bigger count, say shards 1-2 of 16 — must fail
	// at startup: the query side would prune by-metric-id queries by
	// metric_id % 2 while the data lives by metric_id % 16, silently
	// misrouting most of them
	require.NoError(t, f.Parse([]string{"--shard-by-metric-shards=16"}))
	err = cfg.ValidateConfig()
	require.Error(t, err, "shards 1-2 of 16 silently misroute by-metric-id queries")
	require.Contains(t, err.Error(), "--shard-by-metric-shards")
	require.Contains(t, err.Error(), "16")
	require.Contains(t, err.Error(), "2 shards")

	// the count must be covered, not matched: a cluster can hold more shards
	// than the modulus pins by-metric-id data to, and fixed-shard metrics and
	// fan-outs still read the extras
	require.NoError(t, f.Parse([]string{"--duck-shard-query-addrs=1=10.0.0.1:9900,2=10.0.0.2:9900,3=10.0.0.3:9900", "--shard-by-metric-shards=2"}))
	require.NoError(t, cfg.ValidateConfig(), "three addresses cover a count of two")
	require.Equal(t, 2, cfg.ShardByMetricShards, "the flag must round-trip")

	// zero keeps the ClickHouse-pool meaning of deriving the modulus from
	// what is configured — the highest address
	require.NoError(t, f.Parse([]string{"--shard-by-metric-shards=0"}))
	require.NoError(t, cfg.ValidateConfig())

	// a negative count can only be a typo
	require.NoError(t, f.Parse([]string{"--shard-by-metric-shards=-1"}))
	err = cfg.ValidateConfig()
	require.Error(t, err, "a negative routing modulus routes nothing")
	require.Contains(t, err.Error(), "--shard-by-metric-shards")
	require.NoError(t, f.Parse([]string{"--shard-by-metric-shards=2"}))

	// the by-metric-id routing numbers the shards 1..N contiguously, so a
	// gap would route some metric ids to a shard with no address —
	// validation must refuse the gap
	require.NoError(t, f.Parse([]string{"--duck-shard-query-addrs=1=10.0.0.1:9900,3=10.0.0.3:9900"}))
	err = cfg.ValidateConfig()
	require.Error(t, err, "a gap in the shard numbering routes some metrics nowhere")
	require.Contains(t, err.Error(), "--duck-shard-query-addrs")
	require.Contains(t, err.Error(), "shard 3 breaks the numbering")

	require.NoError(t, f.Parse([]string{"--duck-shard-query-addrs=3=10.0.0.3:9900,1=10.0.0.1:9900,2=10.0.0.2:9900"}))
	require.NoError(t, cfg.ValidateConfig(), "unsorted-but-contiguous is fine")

	// the addresses without the duck backend are as nonsensical as the
	// backend without them
	cfgNoDuck := DefaultConfig()
	cfgNoDuck.DuckShardQueryAddrsStr = "1=10.0.0.1:9900"
	err = cfgNoDuck.ValidateConfig()
	require.Error(t, err)
	require.Contains(t, err.Error(), "--duck-shard-query-addrs")

	f2 := flag.NewFlagSet("test", flag.ContinueOnError)
	f2.SetOutput(io.Discard)
	cfg2 := DefaultConfig()
	cfg2.Bind(f2, DefaultConfig())
	require.Error(t, f2.Parse([]string{"--storage-backend=duckdb"}), "the build tag value must not silently pass as a backend value")
	require.Equal(t, duckstore.BackendClickHouse, cfg2.StorageBackend)
}

// TestParseDuckShardQueryAddrs covers the parser's error arms: a missing =,
// a non-numeric or zero shard, an empty address and a repeated shard — the
// duplicate check especially, because a dropped check would let one shard's
// address silently overwrite another's and send its queries to the wrong
// shard.
func TestParseDuckShardQueryAddrs(t *testing.T) {
	addrs, err := parseDuckShardQueryAddrs(" 1 = 10.0.0.1:9900 , 2=10.0.0.2:9900 , , ")
	require.NoError(t, err)
	require.Equal(t, map[uint32]string{1: "10.0.0.1:9900", 2: "10.0.0.2:9900"}, addrs, "whitespace and empty entries are tolerated")

	for _, tc := range []struct {
		in   string
		want string
	}{
		{"1", "expected shard=host:port"},
		{"10.0.0.1:9900", "expected shard=host:port"},
		{"=10.0.0.1:9900", "shard must be a positive number"},
		{"0=10.0.0.1:9900", "shard must be a positive number"},
		{"x=10.0.0.1:9900", "shard must be a positive number"},
		{"1=", "address is empty"},
		{"1=   ", "address is empty"},
		{"1=10.0.0.1:9900,1=10.0.0.2:9900", "shard 1 is listed twice"},
	} {
		_, err := parseDuckShardQueryAddrs(tc.in)
		require.Error(t, err, tc.in)
		require.Contains(t, err.Error(), tc.want, "%q must be rejected", tc.in)
	}
}

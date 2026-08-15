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

	require.NoError(t, f.Parse([]string{"--duck-shard-query-addrs=1=10.0.0.1:9900,2=10.0.0.2:9900"}))
	require.NoError(t, cfg.ValidateConfig())
	require.Equal(t, map[uint32]string{1: "10.0.0.1:9900", 2: "10.0.0.2:9900"}, cfg.DuckShardQueryAddrs)

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

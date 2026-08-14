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
	// duck must stay a valid backend for an ordinary API binary
	require.NoError(t, cfg.ValidateConfig())

	f2 := flag.NewFlagSet("test", flag.ContinueOnError)
	f2.SetOutput(io.Discard)
	cfg2 := DefaultConfig()
	cfg2.Bind(f2, DefaultConfig())
	require.Error(t, f2.Parse([]string{"--storage-backend=duckdb"}), "the build tag value must not silently pass as a backend value")
	require.Equal(t, duckstore.BackendClickHouse, cfg2.StorageBackend)
}

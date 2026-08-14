// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/duckstore"
)

func TestValidateConfigAggregatorStorageBackend(t *testing.T) {
	c := DefaultConfigAggregator()
	require.Equal(t, duckstore.BackendClickHouse, c.StorageBackend, "clickhouse must be the default backend")
	require.NoError(t, ValidateConfigAggregator(&c))

	// The query listener's defaults: two admission slots and the smallest
	// viable DuckDB memory limit.
	require.Equal(t, DefaultQueryConcurrency, c.DuckQueryConcurrency)
	require.Equal(t, int64(duckstore.DefaultMemoryLimitBytes), c.DuckMemoryLimit)

	if duckstore.Available {
		// A duck shard without a query address is a misconfiguration: the
		// shard would be a write-only sink no API can read.
		c.StorageBackend = duckstore.BackendDuck
		err := ValidateConfigAggregator(&c)
		require.Error(t, err)
		require.Contains(t, err.Error(), "--duck-query-addr")

		c.DuckQueryAddr = "127.0.0.1:9900"
		require.NoError(t, ValidateConfigAggregator(&c))
		return
	}
	c.StorageBackend = duckstore.BackendDuck
	err := ValidateConfigAggregator(&c)
	require.Error(t, err, "an untagged binary must refuse to start with the duck backend")
	require.Contains(t, err.Error(), "--storage-backend=duck")
	require.Contains(t, err.Error(), duckstore.BuildTag)
}

func TestValidateConfigAggregatorDuckQuery(t *testing.T) {
	for _, tt := range []struct {
		name string
		set  func(*ConfigAggregator)
		flag string
	}{
		{
			name: "zero query concurrency",
			set:  func(c *ConfigAggregator) { c.DuckQueryConcurrency = 0 },
			flag: "--duck-query-concurrency",
		},
		{
			name: "negative query concurrency",
			set:  func(c *ConfigAggregator) { c.DuckQueryConcurrency = -3 },
			flag: "--duck-query-concurrency",
		},
		{
			name: "zero memory limit",
			set:  func(c *ConfigAggregator) { c.DuckMemoryLimit = 0 },
			flag: "--duck-memory-limit",
		},
		{
			name: "negative memory limit",
			set:  func(c *ConfigAggregator) { c.DuckMemoryLimit = -1 },
			flag: "--duck-memory-limit",
		},
		{
			name: "query address without the duck backend",
			set:  func(c *ConfigAggregator) { c.DuckQueryAddr = "127.0.0.1:9900" },
			flag: "--duck-query-addr",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := DefaultConfigAggregator()
			tt.set(&c)
			err := ValidateConfigAggregator(&c)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.flag, "the error must name the offending flag")
		})
	}
}

func TestValidateConfigAggregatorDuckRetention(t *testing.T) {
	c := DefaultConfigAggregator()
	require.Equal(t, duckstore.DefaultRetention1s, c.DuckRetention1s)
	require.Equal(t, duckstore.DefaultRetention1m, c.DuckRetention1m)
	require.Equal(t, duckstore.DefaultRetention1h, c.DuckRetention1h)
	require.Equal(t, int64(duckstore.DefaultFreeSpaceWatermark), c.DuckFreeSpaceWatermark)
	require.NoError(t, ValidateConfigAggregator(&c))

	for _, tt := range []struct {
		name string
		set  func(*ConfigAggregator)
		flag string
	}{
		{
			name: "negative 1s retention",
			set:  func(c *ConfigAggregator) { c.DuckRetention1s = -time.Second },
			flag: "--duck-retention-1s",
		},
		{
			name: "negative 1m retention",
			set:  func(c *ConfigAggregator) { c.DuckRetention1m = -time.Second },
			flag: "--duck-retention-1m",
		},
		{
			name: "negative 1h retention",
			set:  func(c *ConfigAggregator) { c.DuckRetention1h = -time.Second },
			flag: "--duck-retention-1h",
		},
		{
			name: "negative watermark",
			set:  func(c *ConfigAggregator) { c.DuckFreeSpaceWatermark = -1 },
			flag: "--duck-free-space-watermark",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := DefaultConfigAggregator()
			tt.set(&c)
			err := ValidateConfigAggregator(&c)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.flag, "the error must name the offending flag")
		})
	}
}

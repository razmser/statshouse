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

	if duckstore.Available {
		t.Skip("built with the duckdb tag; duck is a valid backend there")
	}
	c.StorageBackend = duckstore.BackendDuck
	err := ValidateConfigAggregator(&c)
	require.Error(t, err, "an untagged binary must refuse to start with the duck backend")
	require.Contains(t, err.Error(), "--storage-backend=duck")
	require.Contains(t, err.Error(), duckstore.BuildTag)
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

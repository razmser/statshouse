// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package aggregator

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/duckstore"
)

// validClickHouseAggConfig is the smallest ConfigAggregator that passes
// validation on the clickhouse backend: the defaults plus the ClickHouse
// addresses the backend requires.
func validClickHouseAggConfig() ConfigAggregator {
	c := DefaultConfigAggregator()
	c.KHAddr = "127.0.0.1:8123"
	return c
}

// validDuckAggConfig is the smallest ConfigAggregator that passes validation
// on the duck backend: no ClickHouse anywhere, the store directory and query
// address the shard needs, and the shard/replica the local flags supply in
// place of the ClickHouse cluster autodetect.
func validDuckAggConfig() ConfigAggregator {
	c := DefaultConfigAggregator()
	c.StorageBackend = duckstore.BackendDuck
	c.KHAddr = ""
	c.DuckStoreDir = "/var/lib/statshouse/duck"
	c.DuckQueryAddr = "0.0.0.0:9900"
	c.LocalShard = 1
	c.LocalReplica = 1
	return c
}

// TestValidateConfigAggregatorValidBackends pins the two configurations that
// must start: clickhouse with its addresses, and (on a binary that embeds
// DuckDB) duck with everything the shard needs and nothing ClickHouse-shaped.
func TestValidateConfigAggregatorValidBackends(t *testing.T) {
	for _, tt := range []struct {
		name string
		make func() ConfigAggregator
	}{
		{name: "clickhouse with addresses", make: validClickHouseAggConfig},
		{name: "duck complete", make: validDuckAggConfig},
		{name: "duck shard two replica three", make: func() ConfigAggregator {
			c := validDuckAggConfig()
			c.LocalShard = 4
			c.LocalReplica = 3
			return c
		}},
		{name: "clickhouse with a migration range", make: func() ConfigAggregator {
			c := validClickHouseAggConfig()
			c.RemoteInitial.MigrationTimeRange = "2000000000-1000000000"
			return c
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.make()
			if c.StorageBackend == duckstore.BackendDuck && !duckstore.Available {
				t.Skip("duck combinations only validate on a binary built with the duckdb build tag")
			}
			require.NoError(t, ValidateConfigAggregator(&c))
		})
	}
}

// TestValidateConfigAggregatorBackendCombinations is the table over every
// nonsensical backend/flag combination, asserting the error names the
// offending flags. Duck rows behave differently per build: an untagged binary
// refuses the duck backend outright (the build-tag gate fires first), while a
// tagged one reaches the combination checks.
func TestValidateConfigAggregatorBackendCombinations(t *testing.T) {
	for _, tt := range []struct {
		name string
		make func() ConfigAggregator
		// flags the error must name once the duck combination actually
		// reaches validation (always on a tagged build; on an untagged build
		// only for clickhouse-side rows)
		wantFlags []string
	}{
		{
			name:      "duck with clickhouse addresses",
			make:      func() ConfigAggregator { c := validDuckAggConfig(); c.KHAddr = "127.0.0.1:8123"; return c },
			wantFlags: []string{"--kh", "--storage-backend=duck"},
		},
		{
			name:      "clickhouse without clickhouse addresses",
			make:      func() ConfigAggregator { c := validClickHouseAggConfig(); c.KHAddr = ""; return c },
			wantFlags: []string{"--kh", "--storage-backend=clickhouse"},
		},
		{
			name:      "duck without a store directory",
			make:      func() ConfigAggregator { c := validDuckAggConfig(); c.DuckStoreDir = ""; return c },
			wantFlags: []string{"--duck-store-dir", "--storage-backend=duck"},
		},
		{
			name:      "duck without a query address",
			make:      func() ConfigAggregator { c := validDuckAggConfig(); c.DuckQueryAddr = ""; return c },
			wantFlags: []string{"--duck-query-addr", "--storage-backend=duck"},
		},
		{
			name: "duck with a migration time range",
			make: func() ConfigAggregator {
				c := validDuckAggConfig()
				c.RemoteInitial.MigrationTimeRange = "2000000000-1000000000"
				return c
			},
			wantFlags: []string{"--migration", "--storage-backend=duck"},
		},
		{
			name:      "duck without a local replica",
			make:      func() ConfigAggregator { c := validDuckAggConfig(); c.LocalReplica = 0; return c },
			wantFlags: []string{"--local-replica", "--storage-backend=duck"},
		},
		{
			name:      "duck with an out-of-range local replica",
			make:      func() ConfigAggregator { c := validDuckAggConfig(); c.LocalReplica = 4; return c },
			wantFlags: []string{"--local-replica", "--storage-backend=duck"},
		},
		{
			name:      "duck with a zero local shard",
			make:      func() ConfigAggregator { c := validDuckAggConfig(); c.LocalShard = 0; return c },
			wantFlags: []string{"--local-shard", "--storage-backend=duck"},
		},
		{
			name: "store directory without the duck backend",
			make: func() ConfigAggregator {
				c := validClickHouseAggConfig()
				c.DuckStoreDir = "/var/lib/statshouse/duck"
				return c
			},
			wantFlags: []string{"--duck-store-dir"},
		},
		{
			name:      "query address without the duck backend",
			make:      func() ConfigAggregator { c := validClickHouseAggConfig(); c.DuckQueryAddr = "0.0.0.0:9900"; return c },
			wantFlags: []string{"--duck-query-addr"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.make()
			err := ValidateConfigAggregator(&c)
			require.Error(t, err)
			if c.StorageBackend == duckstore.BackendDuck && !duckstore.Available {
				// The untagged binary refuses the duck backend before any
				// combination is examined, with the build-tag error.
				require.Contains(t, err.Error(), "--storage-backend=duck")
				require.Contains(t, err.Error(), duckstore.BuildTag)
				return
			}
			for _, flag := range tt.wantFlags {
				require.Contains(t, err.Error(), flag, "the error must name the offending flag")
			}
		})
	}
}

// TestValidateConfigAggregatorKHRequiredByDefault pins that the clickhouse
// backend no longer has implicit default addresses: the defaults alone are a
// configuration error naming --kh, so an operator finds out at startup rather
// than through inserts going to a phantom local ClickHouse.
func TestValidateConfigAggregatorKHRequiredByDefault(t *testing.T) {
	c := DefaultConfigAggregator()
	require.Equal(t, "", c.KHAddr, "the config must not carry default clickhouse addresses")
	err := ValidateConfigAggregator(&c)
	require.Error(t, err)
	require.Contains(t, err.Error(), "--kh")
	require.Contains(t, err.Error(), "--storage-backend=clickhouse")
}

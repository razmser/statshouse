// Copyright 2025 V Kontate LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//go:build duckdb

package duckstore

import (
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

// currentSetting reads one DuckDB setting off db, normalized the way DuckDB
// reports it (sizes round to a human-readable unit).
func currentSetting(t *testing.T, db *sql.DB, name string) string {
	t.Helper()
	var value string
	require.NoError(t, db.QueryRow(`SELECT current_setting($1)`, name).Scan(&value))
	return value
}

// The resource bounds must hold from the first connection of every store file
// the store opens: one thread, the memory limit, the temp-directory bound.
// They ride as DSN options, so nothing — not the writer, not compaction, not
// a query — can run outside them.
func TestOpenStoreAppliesResourceBounds(t *testing.T) {
	s, _ := openTestStore(t, t.TempDir())
	require.Equal(t, DefaultResources(), s.cfg.Resources, "a store without explicit resources takes the defaults")

	db := s.Delta()
	require.Equal(t, "1", currentSetting(t, db, "threads"), "DuckDB must be single-threaded")
	require.Equal(t, "256.0 MiB", currentSetting(t, db, "memory_limit"))
	require.Equal(t, "256.0 MiB", currentSetting(t, db, "max_temp_directory_size"))
}

// Explicit resources override the defaults, file-wide: every tier table and
// metadata read through the same connection sees them.
func TestOpenStoreHonoursExplicitResources(t *testing.T) {
	var logs []string
	s, err := OpenStore(StoreConfig{
		Dir: t.TempDir(),
		Logf: func(format string, args ...any) {
			logs = append(logs, format)
		},
		Resources: ResourcesConfig{Threads: 1, MemoryLimitBytes: 128 << 20, MaxTempDirBytes: 64 << 20},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	require.Empty(t, logs)

	db := s.Delta()
	require.Equal(t, "128.0 MiB", currentSetting(t, db, "memory_limit"))
	require.Equal(t, "64.0 MiB", currentSetting(t, db, "max_temp_directory_size"))
}

// The zero ResourcesConfig is the defaults, and a partially set one keeps its
// explicit values.
func TestResourcesWithDefaults(t *testing.T) {
	require.Equal(t, DefaultResources(), ResourcesConfig{}.WithDefaults())
	require.Equal(t, ResourcesConfig{Threads: 2, MemoryLimitBytes: 1 << 30, MaxTempDirBytes: DefaultMaxTempDirBytes},
		ResourcesConfig{Threads: 2, MemoryLimitBytes: 1 << 30}.WithDefaults())
	require.Equal(t, ResourcesConfig{Threads: DefaultDuckDBThreads, MemoryLimitBytes: 1 << 30, MaxTempDirBytes: 1 << 20},
		ResourcesConfig{MemoryLimitBytes: 1 << 30, MaxTempDirBytes: 1 << 20}.WithDefaults())
}

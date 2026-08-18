// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/VKCOM/statshouse/internal/format"
)

func TestTierTableDDLMatchesResolvedSchema(t *testing.T) {
	for _, tier := range tiers {
		t.Run(tier, func(t *testing.T) {
			// whitespace-normalized, so the DDL's column alignment is free to
			// change without breaking the schema contract
			ddl := strings.Join(strings.Fields(strings.ToLower(tierTableDDL(TierTable(tier)))), " ")

			// the anchor columns of the transliterated 03-schema-ddl.sql
			require.Contains(t, ddl, "metric integer not null")
			require.Contains(t, ddl, "time bigint not null")
			for _, agg := range []string{"count", "min", "max", "max_count", "sum", "sumsquare"} {
				require.Contains(t, ddl, fmt.Sprintf("%s double not null default 0", agg))
			}
			require.Contains(t, ddl, "percentiles blob not null default ''::blob")
			require.Contains(t, ddl, "uniq_state blob not null default ''::blob")
			for _, host := range []string{"min_host", "max_host", "max_count_host"} {
				require.Contains(t, ddl, fmt.Sprintf("%s integer not null default 0", host))
			}
			for _, shost := range []string{"min_shost", "max_shost", "max_count_shost"} {
				require.Contains(t, ddl, fmt.Sprintf("%s varchar not null default ''", shost))
			}
			// the skewed argMin/argMax state values the hosts merge and serve by
			for _, val := range []string{"min_host_value", "max_host_value", "max_count_host_value"} {
				require.Contains(t, ddl, fmt.Sprintf("%s double not null default 0", val))
			}

			// all MaxTags tag pairs, flat and wide, with zero-value defaults
			for i := 0; i < format.MaxTags; i++ {
				require.Contains(t, ddl, fmt.Sprintf("tag%d integer not null default 0", i))
				require.Contains(t, ddl, fmt.Sprintf("stag%d varchar not null default ''", i))
			}

			// no key, no constraint, no index: partial rows repeat the key
			// and an index would cost Appender throughput
			for _, banned := range []string{"primary key", "unique", "index", "references"} {
				require.NotContains(t, ddl, banned)
			}

			// column count: key anchors + 2*MaxTags tags + 6 aggregates +
			// 3 host triples + 2 aggregate-state blobs
			wantColumns := 2 + 2*format.MaxTags + 6 + 9 + 2
			gotColumns := strings.Count(ddl, "not null")
			require.Equal(t, wantColumns, gotColumns, "every column must be NOT NULL")
		})
	}
}

func TestTierTableDDLDistinctPerTier(t *testing.T) {
	seen := map[string]string{}
	for _, tier := range tiers {
		table := TierTable(tier)
		require.NotEmpty(t, table)
		require.Equal(t, 1, strings.Count(tierTableDDL(table), "CREATE TABLE"))
		seen[table] = tier
	}
	require.Equal(t, map[string]string{"s1": Tier1s, "s1m": Tier1m, "s1h": Tier1h}, seen)
}

func TestTierSeconds(t *testing.T) {
	require.EqualValues(t, 1, tierSeconds[Tier1s])
	require.EqualValues(t, 60, tierSeconds[Tier1m])
	require.EqualValues(t, 3600, tierSeconds[Tier1h])
}

// TestSchemaVersionAxesAreScopedByFileKind pins the file-kind taxonomy the two
// schema-version axes hang on (DeltaSchemaVersion / ArchiveSchemaVersion) —
// in the untagged build, where this file is the axes' only visible surface.
// The labels are load-bearing: they name the axis in the quarantine reason a
// stamp mismatch produces, and the metric tag values the aggregator reports.
func TestSchemaVersionAxesAreScopedByFileKind(t *testing.T) {
	require.Equal(t, "delta", fileKindDelta.label())
	require.Equal(t, "archive", fileKindArchive.label())
	require.Positive(t, DeltaSchemaVersion)
	require.Positive(t, ArchiveSchemaVersion)
}

func TestDeltaFileNames(t *testing.T) {
	for _, gen := range []int64{0, 1, 42, 1 << 40} {
		name := deltaFileName(gen)
		got, ok := parseDeltaFileName(name)
		require.True(t, ok, name)
		require.Equal(t, gen, got)
	}

	for _, name := range []string{
		"delta.duckdb",         // generationless, the legacy pre-generation name
		"delta-.duckdb",        // empty generation
		"delta--1.duckdb",      // negative generation
		"delta-1.duckdb.wal",   // the write-ahead log
		"delta-x.duckdb",       // not a number
		"delta-1.txt",          // wrong extension
		"archive",              // directory
		"1s-1700000000.duckdb", // an archive window, not a delta
	} {
		_, ok := parseDeltaFileName(name)
		require.False(t, ok, name)
	}
}

func TestArchiveFileNames(t *testing.T) {
	for _, tier := range tiers {
		name := archiveFileName(tier, 1700000000)
		gotTier, gotStart, ok := parseArchiveFileName(name)
		require.True(t, ok, name)
		require.Equal(t, tier, gotTier)
		require.EqualValues(t, 1700000000, gotStart)
	}

	for _, name := range []string{
		"1s-.duckdb",     // empty window start
		"1s--5.duckdb",   // negative window start
		"1s-x.duckdb",    // not a number
		"1s-5.txt",       // wrong extension
		"1x-5.duckdb",    // unknown tier
		"5.duckdb",       // no tier
		"-5.duckdb",      // no tier
		"1s-5-6.duckdb",  // window start must be a plain number
		"delta-0.duckdb", // a delta, not an archive window
		"archive.duckdb", // junk
	} {
		_, _, ok := parseArchiveFileName(name)
		require.False(t, ok, name)
	}
}

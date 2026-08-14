// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package duckstore

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/VKCOM/statshouse/internal/format"
)

// SchemaVersion is the duck-store on-disk schema version. Every store file is
// stamped with the version that wrote it; a file whose stamp does not match
// the running binary is quarantined, never upgraded in place and never read
// through a compatibility shim.
const SchemaVersion = 1

// VersionTable is the version-stamp table written into every store file
// (delta generations and archive windows alike). It carries the three version
// axes the store verifies on open: the duck-store schema version, the DuckDB
// version that wrote the file and the StatsHouse version that wrote it.
const VersionTable = "duck_store_version"

// VersionTableDDL creates the version-stamp table (see VersionTable).
const VersionTableDDL = "CREATE TABLE IF NOT EXISTS " + VersionTable + " (" +
	"schema_version INTEGER NOT NULL, " +
	"storage_version VARCHAR NOT NULL, " +
	"statshouse_version VARCHAR NOT NULL)"

// Tier names, used both as archive file-name prefixes and to map a tier to its
// table. Delta files hold all three tier tables; an archive window file holds
// exactly one.
const (
	Tier1s = "1s"
	Tier1m = "1m"
	Tier1h = "1h"
)

// tiers is the canonical tier order.
var tiers = []string{Tier1s, Tier1m, Tier1h}

var tierTables = map[string]string{
	Tier1s: "s1",
	Tier1m: "s1m",
	Tier1h: "s1h",
}

var tierSeconds = map[string]int64{
	Tier1s: 1,
	Tier1m: 60,
	Tier1h: 3600,
}

// Tiers returns the tier names in canonical order.
func Tiers() []string {
	return append([]string(nil), tiers...)
}

// TierTable returns the table name a tier is stored in.
func TierTable(tier string) string {
	return tierTables[tier]
}

// TierSeconds returns the length of a tier's bucket interval in seconds.
func TierSeconds(tier string) int64 {
	return tierSeconds[tier]
}

// TierTableDDL returns the CREATE TABLE statement for one tier table: the
// transliteration of .scratch/duck-store/03-schema-ddl.sql. Every column is
// NOT NULL with a zero-value default (the API compares empty string tags
// directly and NULLs would make three-valued logic silently drop rows), time
// is BIGINT unix seconds, sketch states stay opaque ClickHouse bytes in BLOB,
// and there is deliberately no primary key, unique constraint or index:
// partial rows repeat the key and an index would cost Appender throughput.
func TierTableDDL(table string) string {
	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS " + table + " (\n")
	b.WriteString("    metric INTEGER NOT NULL,\n")
	b.WriteString("    time   BIGINT  NOT NULL, -- unix seconds, already truncated to the tier\n")
	for i := 0; i < format.MaxTags; i++ {
		fmt.Fprintf(&b, "    tag%d  INTEGER NOT NULL DEFAULT 0,\n", i)
		fmt.Fprintf(&b, "    stag%d VARCHAR NOT NULL DEFAULT '',\n", i)
	}
	for _, c := range []string{"count", "min", "max", "max_count", "sum", "sumsquare"} {
		fmt.Fprintf(&b, "    %s DOUBLE NOT NULL DEFAULT 0,\n", c)
	}
	b.WriteString("    min_host        INTEGER NOT NULL DEFAULT 0,\n")
	b.WriteString("    min_shost       VARCHAR NOT NULL DEFAULT '',\n")
	b.WriteString("    max_host        INTEGER NOT NULL DEFAULT 0,\n")
	b.WriteString("    max_shost       VARCHAR NOT NULL DEFAULT '',\n")
	b.WriteString("    max_count_host  INTEGER NOT NULL DEFAULT 0,\n")
	b.WriteString("    max_count_shost VARCHAR NOT NULL DEFAULT '',\n")
	b.WriteString("    percentiles BLOB NOT NULL DEFAULT ''::BLOB,\n")
	b.WriteString("    uniq_state  BLOB NOT NULL DEFAULT ''::BLOB\n)")
	return b.String()
}

// deltaFileName returns the name of delta generation N's file.
func deltaFileName(generation int64) string {
	return fmt.Sprintf("delta-%d.duckdb", generation)
}

// parseDeltaFileName parses a delta file name. ok is false for anything that
// is not a delta generation, so unknown files are simply ignored rather than
// misinterpreted.
func parseDeltaFileName(name string) (generation int64, ok bool) {
	s, ok := strings.CutPrefix(name, "delta-")
	if !ok {
		return 0, false
	}
	s, ok = strings.CutSuffix(s, ".duckdb")
	if !ok {
		return 0, false
	}
	gen, err := strconv.ParseInt(s, 10, 64)
	if err != nil || gen < 0 {
		return 0, false
	}
	return gen, true
}

// archiveFileName returns the name of an archive window file.
func archiveFileName(tier string, windowStart int64) string {
	return fmt.Sprintf("%s-%d.duckdb", tier, windowStart)
}

// parseArchiveFileName parses an archive window file name into its tier and
// window start. ok is false for anything that is not an archive window, so
// unknown files are simply ignored rather than misinterpreted.
func parseArchiveFileName(name string) (tier string, windowStart int64, ok bool) {
	ext, isDuck := strings.CutSuffix(name, ".duckdb")
	if !isDuck {
		return "", 0, false
	}
	prefix, rest, found := strings.Cut(ext, "-")
	if !found {
		return "", 0, false
	}
	if _, knownTier := tierTables[prefix]; !knownTier {
		return "", 0, false
	}
	start, err := strconv.ParseInt(rest, 10, 64)
	if err != nil || start < 0 {
		return "", 0, false
	}
	return prefix, start, true
}

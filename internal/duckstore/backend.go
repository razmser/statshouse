// Copyright 2025 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

// Package duckstore implements the DuckDB storage backend for StatsHouse:
// DuckDB files embedded in the aggregator and read by the API over the
// structured query RPC. Everything that touches the DuckDB driver sits
// behind the "duckdb" build tag, so binaries built without it stay pure Go.
package duckstore

import (
	"fmt"
	"time"
)

// BuildTag is the Go build tag that compiles DuckDB support into a binary.
const BuildTag = "duckdb"

// Default retention per tier, mirroring ClickHouse's TTLs so switching
// backends does not change how long data lives: an archive window file is
// unlinked once the window it covers ended this long ago. Zero keeps the
// tier's windows forever.
const (
	DefaultRetention1s = 52 * time.Hour
	DefaultRetention1m = 33 * 24 * time.Hour

	DefaultRetention1h time.Duration = 0
)

// DefaultFreeSpaceWatermark is the free-space low watermark's default: the
// safety net is off until an operator sets it, because disk is bounded
// upstream by the sampling budget and there is no required disk-cap flag.
const DefaultFreeSpaceWatermark uint64 = 0

// Resource bounds for every DuckDB database instance the store opens, so an
// embedded DuckDB cannot grow into the CPU and RAM the aggregator's own
// conveyor needs. Defaults target the smallest viable node, not the available
// envelope: one execution thread, a 256 MB memory limit and a temp directory
// bounded to the same 256 MB. The memory limit bounds intermediate state only
// (a measured 100 MB limit still produced a 382 MB result — nothing but the
// row cap bounds result size), which is why it sits alongside the row limit
// and the query admission control rather than instead of them.
const (
	// DefaultDuckDBThreads is DuckDB's thread count per store file: the spec
	// fixes one, so compaction, sealing and queries all run single-threaded
	// and never compete for CPU with ingestion.
	DefaultDuckDBThreads = 1
	// DefaultMemoryLimitBytes is DuckDB's memory_limit per store file.
	DefaultMemoryLimitBytes int64 = 256 << 20
	// DefaultMaxTempDirBytes is DuckDB's max_temp_directory_size per store
	// file when nothing is set at all: the bound that makes "spill to disk
	// instead of OOM" terminate with an out-of-memory error instead of
	// filling the volume. An unset bound tracks the memory limit rather than
	// DuckDB's own 90%-of-disk — including an operator-set memory limit, so
	// raising --duck-memory-limit does not silently leave the spill bound
	// pinned to this default.
	DefaultMaxTempDirBytes int64 = 256 << 20
)

// ResourcesConfig carries the DuckDB resource bounds applied when a store file
// is opened. The zero value means the defaults above.
type ResourcesConfig struct {
	// Threads is DuckDB's threads setting per opened store file.
	Threads int
	// MemoryLimitBytes is DuckDB's memory_limit per opened store file.
	MemoryLimitBytes int64
	// MaxTempDirBytes is DuckDB's max_temp_directory_size per opened store
	// file. Each file spills into its own temp directory next to itself.
	// Unset (0) tracks MemoryLimitBytes, whatever it resolved to.
	MaxTempDirBytes int64
}

// WithDefaults returns res with unset fields filled from the defaults, so a
// zero ResourcesConfig behaves like the defaults and a partially set one
// keeps its explicit values — except MaxTempDirBytes, whose unset value tracks
// the memory limit rather than a fixed default.
func (res ResourcesConfig) WithDefaults() ResourcesConfig {
	if res.Threads <= 0 {
		res.Threads = DefaultDuckDBThreads
	}
	if res.MemoryLimitBytes <= 0 {
		res.MemoryLimitBytes = DefaultMemoryLimitBytes
	}
	if res.MaxTempDirBytes <= 0 {
		res.MaxTempDirBytes = res.MemoryLimitBytes
	}
	return res
}

// StorageBackend selects which storage backend metric data is written to and
// read from. Parsed from --storage-backend by the aggregator and the API.
type StorageBackend int8

const (
	// BackendClickHouse is the default: all writes and reads go to the
	// ClickHouse cluster, behaviourally identical to the pre-seam code.
	BackendClickHouse StorageBackend = iota
	// BackendDuck stores metric data in the local duck-store files owned by
	// each aggregator shard.
	BackendDuck
)

// ParseStorageBackend parses a --storage-backend flag value. The empty string
// selects ClickHouse, the historical default.
func ParseStorageBackend(s string) (StorageBackend, error) {
	switch s {
	case "", "clickhouse":
		return BackendClickHouse, nil
	case "duck":
		return BackendDuck, nil
	default:
		return BackendClickHouse, fmt.Errorf("invalid --storage-backend value %q, must be %q or %q", s, "clickhouse", "duck")
	}
}

// String implements flag.Value.
func (b StorageBackend) String() string {
	switch b {
	case BackendDuck:
		return "duck"
	default:
		return "clickhouse"
	}
}

// Set implements flag.Value.
func (b *StorageBackend) Set(s string) error {
	v, err := ParseStorageBackend(s)
	if err != nil {
		return err
	}
	*b = v
	return nil
}

// Validate reports whether this binary can actually run backend b. A binary
// built without the "duckdb" build tag has no DuckDB compiled in and must
// refuse --storage-backend=duck at startup instead of failing later with an
// obscure error. Backends that do not embed DuckDB (the API talks to the
// aggregators over RPC) must not gate on this.
func (b StorageBackend) Validate() error {
	if b == BackendDuck && !Available {
		return fmt.Errorf("--storage-backend=duck is not supported by this binary: it was built without the %q build tag that embeds DuckDB", BuildTag)
	}
	return nil
}

# CLAUDE.md

Agent guidance for this repository. See also [AGENTS.md](./AGENTS.md) (issue
tracker, triage labels, domain docs) and [CONTEXT.md](./CONTEXT.md).

## Build configurations

- The default build is pure Go and must stay that way
  (`CGO_ENABLED=0 go build ./...`; the only failures allowed are the
  pre-existing cgo-only `internal/sqlite/sqlite0` packages, which fail on
  pristine master too).
- Every import of `github.com/duckdb/duckdb-go/v2` sits behind the `duckdb`
  build tag (`//go:build duckdb`), mostly in `internal/duckstore`. Run both
  suites: `go test ./...` and `go test -tags duckdb ./...`. The DuckDB-tagged
  aggregator is built by `make build-agg-duckdb`, which carries the verified
  static link flags — a naive static link of DuckDB produces a binary that
  segfaults on first use.

## The storage-backend seam

Metric data storage is behind exactly two seams; ClickHouse and duck-store
(DuckDB embedded in the aggregator) are the two implementations, selected per
process by `--storage-backend=clickhouse|duck` (parsed by
`duckstore.StorageBackend`). One backend per process — no dual-write, no
read-comparison mode.

- **Write seam — `InsertSink`** (`internal/aggregator`): receives the sampled
  in-memory row set plus a `Send(ctx)` returning status/exception/elapsed, so
  insert budgeting, sampling and ingestion-status machinery are shared and
  untouched. ClickHouse sink: `aggregator_insert.go` (RowBinary bytes);
  duck sink: `internal/duckstore/writer.go`.
- **Read seam — `QuerySource`** (`internal/api/query_source.go`): series and
  tag-values methods taking a semantic request plus an LOD. ClickHouse:
  `query_source_ch.go` (today's SQL builder through `doSelect`). Duck:
  `query_source_duck.go` + `fanout.go` — the request is serialized over the
  structured store-query RPC to every relevant aggregator shard
  (`internal/aggregator/store_query_server.go`) and merged in Go through the
  existing cross-LOD state merge.

Ground rules when touching either seam:

- The ClickHouse paths must stay behaviourally identical; the insert refactor
  is verified byte-identical.
- The API never links DuckDB — everything crosses the RPC. The aggregator
  gates duck on `duckstore.Available` (false in untagged builds); the API
  accepts `duck` in any build.
- Cross-backend agreement is enforced by the differential conformance run
  (`e2e/conformance_test.go`) and the e2e suite, whose assertions and input
  matrix are frozen. Backend comparisons are always by decoded value — never
  by generated SQL, state bytes, or file lists.
- The operator-facing surface (flags, retention, disk formula, quarantine,
  backup policy) is documented in `docs/duck-store.md`, kept in sync with the
  code by `internal/duckstore/docs_test.go`.

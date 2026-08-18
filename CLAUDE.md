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
  duck sink: `internal/duckstore/writer.go` — the delta is single-tier: the
  writer appends only `rows_1s`, and compaction derives the 1m/1h tiers by
  timestamp truncation.
- **Read seam — `QuerySource`** (`internal/api/query_source.go`): series and
  tag-values methods taking a semantic request plus an LOD. ClickHouse:
  `query_source_ch.go` (today's SQL builder through `doSelect`). Duck:
  `query_source_duck.go` + `fanout.go` — the request is serialized over the
  structured store-query RPC to every relevant aggregator shard
  (`internal/aggregator/store_query_server.go`) and merged in Go through the
  existing cross-LOD state merge. Inside duckstore the read runs over a
  query-source snapshot (`internal/duckstore/query_snapshot.go`):
  per-source descriptors carrying each file's kind, tier table and exact
  time range — the active delta, rolled-but-unconsumed generations and
  archive windows — so a concurrent roll or consume can neither lose nor
  double-count a generation.

Ground rules when touching either seam:

- The ClickHouse paths must stay behaviourally identical; the insert refactor
  is verified byte-identical.
- duck-store's compaction and sealing transact through the writer's
  connection-level protocol — explicit `BEGIN`/`COMMIT` via
  `conn.ExecContext`, never `sql.Tx`, which a live `duckdb.Appender` cannot
  participate in — so `ConsumeOptions.AppendWindow` carries the `*sql.Conn`
  and the appended rows plus the consumption record commit, or roll back, as
  one.
- The API never links DuckDB — everything crosses the RPC. The aggregator
  gates duck on `duckstore.Available` (false in untagged builds); the API
  accepts `duck` in any build.
- Cross-backend agreement is enforced by the differential conformance run —
  `e2e/conformance.go`, a mode of the e2e binary (`go run ./e2e
  --conformance`, or `bash e2e/lima.sh --conformance` in the Lima VM) that
  boots ClickHouse plus both daemon stacks over one shared metadata, seeds
  the identical deterministic stream to both and compares the two APIs'
  decoded answers to every query shape, with CH as the reference. The e2e
  suite's client assertions and input matrix are frozen. Backend comparisons
  are always by decoded value — never by generated SQL, state bytes, or file
  lists. The same suite also runs the duck stack alone (`go run ./e2e
  --storage-backend=duck`, or `bash e2e/lima.sh --storage-backend=duck`):
  no ClickHouse container, the same client assertions.
- The operator-facing surface (flags, retention, disk formula, quarantine,
  backup policy) is documented in `docs/duck-store.md`, kept in sync with the
  code by `internal/duckstore/docs_test.go`.

#!/bin/sh
set -e

# Duck-storage counterpart of run-aggregator.sh: the binary is the duckdb-tagged
# build (make build-agg-duckdb) with the store embedded, so there is no
# ClickHouse to talk to and --kh must not be set. --duck-query-addr opens the
# query-only listener that run-api-duck.sh fans out to (13339, because the
# agent already owns 13338 in the localdebug layout). See the "minimal
# single-shard setup" in docs/duck-store.md.

mkdir -p cache/aggregator/
# --deny-old-agents=false so agent built in debugger (with commit timestamp 0) will be accepted
../target/statshouse-agg-duckdb --agg-addr=127.0.0.1:13336 --cluster=statlogs2 \
   --auto-create --auto-create-default-namespace \
   --deny-old-agents=false --metadata-addr "127.0.0.1:2442" --cache-dir=cache/aggregator \
   --storage-backend=duck \
   --duck-store-dir=cache/aggregator/duck-store \
   --duck-query-addr=127.0.0.1:13339 \
   --local-shard=1 --local-replica=1 \
   "$@"

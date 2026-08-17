#!/bin/sh
set -e

# Duck-storage counterpart of run-api.sh: no --clickhouse-v2-addrs — every read
# fans out to the aggregator's store-query listener instead (127.0.0.1:13339 is
# --duck-query-addr in run-aggregator-duck.sh; shard 1 matches --local-shard=1
# there). --listen-addr stays overridable so run-in-herdr.sh can dodge a busy
# 10888 (e.g. a Lima e2e stack forwarding it).

mkdir -p cache/api
../target/statshouse-api --local-mode --insecure-mode --access-log \
  --listen-rpc-addr=localhost:10889 \
  --verbose --listen-addr localhost:10888 --static-dir ../statshouse-ui/build/ \
  --metadata-addr "127.0.0.1:2442" --available-shards "1" --cache-dir=cache/api --announcement=Крокозяблик \
  --storage-backend=duck \
  --duck-shard-query-addrs=1=127.0.0.1:13339 \
  --shard-by-metric-shards=1 \
  "$@"

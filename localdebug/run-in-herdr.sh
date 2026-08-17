#!/bin/bash
set -euo pipefail

# Herdr counterpart of run-in-tmux.sh, on the duck storage backend instead of
# ClickHouse: no ClickHouse container — the aggregator is the duckdb-tagged
# build (make build-agg-duckdb) with the store embedded, and the api reads
# through the aggregator's store-query RPC. The backend flags live in
# run-aggregator-duck.sh / run-api-duck.sh; this script only builds, opens one
# herdr workspace with a tab per service and points the browser at the api.
#
# Run it from localdebug/ inside a herdr pane (the herdr CLI manages the
# session it runs in). The api port defaults to 10888; if it is busy on the
# host (a running Lima e2e stack forwards it), pick another:
#   API_PORT=10890 ./run-in-herdr.sh

WORKSPACE_LABEL="sh-local-duck"
API_PORT="${API_PORT:-10888}"
API_URL="http://localhost:${API_PORT}/"
VIEW_URL="http://localhost:${API_PORT}/view?live=1&f=-300&t=0&s=__contributors_log_rev"
LOCALDEBUG_DIR="$(pwd)"

if [[ "${HERDR_ENV:-}" != 1 ]]; then
  echo "ERROR: not inside a herdr pane — the herdr CLI manages the session of the pane it runs in" >&2
  exit 1
fi
command -v jq >/dev/null || { echo "ERROR: jq is required (herdr replies are JSON)" >&2; exit 1; }
if lsof -nP -iTCP:"${API_PORT}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "ERROR: port ${API_PORT} is busy — rerun with API_PORT=<free port>" >&2
  exit 1
fi

# Start one labeled tab in the workspace and run cmd in it
# (the tmux new-window + send-keys pair). Sets LAST_TAB_ID.
start_service() { # <label> <command>
  local label="$1" cmd="$2" resp
  resp="$(herdr tab create --workspace "$WORKSPACE_ID" --label "$label" --cwd "$LOCALDEBUG_DIR" --no-focus)"
  herdr pane run "$(jq -r '.result.root_pane.pane_id' <<<"$resp")" "$cmd"
  LAST_TAB_ID="$(jq -r '.result.tab.tab_id' <<<"$resp")"
}

# First, build the binaries in the root directory
echo 'Building binaries...'
# statshouse-ui/build must exist BEFORE build-main-daemons: the api's embed
# build embeds it, but build.sh only builds the UI after the daemons — too
# late on a fresh checkout
if [[ ! -f ../statshouse-ui/build/index.html ]]; then
  (cd .. && make build-sh-ui)
fi
./build.sh
# the duckdb-tagged aggregator (target/statshouse-agg-duckdb) that
# run-aggregator-duck.sh runs; the default build stays pure Go
(cd .. && make build-agg-duckdb)
echo 'Build complete! Starting services...'

# No ClickHouse to wait for under duck — the aggregator owns storage.

WORKSPACE_ID="$(herdr workspace list \
  | jq -r --arg lbl "$WORKSPACE_LABEL" '.result.workspaces[] | select(.label == $lbl) | .workspace_id')"
if [[ -n "$WORKSPACE_ID" ]]; then
  echo "ERROR: herdr workspace '$WORKSPACE_LABEL' already exists — close it first: herdr workspace close $WORKSPACE_ID" >&2
  exit 1
fi
WORKSPACE_ID="$(herdr workspace create --label "$WORKSPACE_LABEL" --no-focus \
  | jq -r '.result.workspace.workspace_id')"

# Tab 1: Metadata service
start_service metadata "./run-metadata.sh"
# Tab 2: Aggregator service (duck storage)
start_service aggregator "./run-aggregator-duck.sh"
# Tab 3: Agent service (--agg-addr override: run-agent.sh lists the multi-shard
# CH-layout aggregators 13336/13346/13356, but this stack runs a single duck
# aggregator, and the agent would retry the dead two forever; the agent needs
# 3 replicas per shard, so all three point at the one aggregator — the same
# single-host triplet the aggregator advertises for itself)
start_service agent "./run-agent.sh --p=:13338 --agg-addr=127.0.0.1:13336,127.0.0.1:13336,127.0.0.1:13336"
AGENT_TAB_ID="$LAST_TAB_ID"
# Tab 4: API service (duck storage)
start_service api "./run-api-duck.sh --listen-addr=localhost:${API_PORT}"
# Tab 5: Load generator (STATSHOUSE_API_URL: loadgen's ensure-metrics/dashboard
# calls default to 10888, which is not this stack's api port; env(1) because
# the pane shell is fish and has no VAR=val cmd prefix)
start_service loadgen "cd .. && env STATSHOUSE_API_URL=http://localhost:${API_PORT} go run ./cmd/loadgen client"
# Tab 6: Balancer service
start_service balancer "./run-balancer.sh"

echo -n "Waiting for API" && until curl --output /dev/null --silent --head --fail "$API_URL"; do echo -n .; sleep 1; done

# copypasted https://stackoverflow.com/questions/54995983/how-to-detect-availability-of-gui-in-bash-shell
check_macos_gui() {
  command -v swift >/dev/null && swift <(cat <<"EOF"
import Security
var attrs = SessionAttributeBits(rawValue:0)
let result = SessionGetInfo(callerSecuritySession, nil, &attrs)
exit((result == 0 && attrs.contains(.sessionHasGraphicAccess)) ? 0 : 1)
EOF
)
}
case "$OSTYPE" in
  darwin*)  if check_macos_gui ; then open "$VIEW_URL" ; fi ;;
  linux*) if [[ -n "$XDG_CURRENT_DESKTOP" ]] ; then xdg-open "$VIEW_URL" ; fi ;;
esac

# Select the agent tab and switch to the workspace (the tmux select-window + attach)
herdr tab focus "$AGENT_TAB_ID"
herdr workspace focus "$WORKSPACE_ID"

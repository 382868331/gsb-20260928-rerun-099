#!/usr/bin/env bash
set -euo pipefail

addr="127.0.0.1:19091"
token="local-demo-token"
tmp_dir="$(mktemp -d)"
server_log="$tmp_dir/server.log"
cleanup() {
  kill "$server_pid" 2>/dev/null || true
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

go run ./cmd/demo-server -addr "$addr" -token "$token" >"$server_log" 2>&1 &
server_pid=$!
ready=0
for _ in {1..50}; do
  if (exec 3<>/dev/tcp/127.0.0.1/19091) 2>/dev/null; then
    exec 3>&- 3<&-
    ready=1
    break
  fi
  sleep 0.05
done
if [[ "$ready" != 1 ]]; then
  cat "$server_log"
  exit 1
fi
go run ./cmd/demo-client -addr "$addr" -token "$token" -phase stream

kill -TERM "$server_pid"
wait "$server_pid" 2>/dev/null || true
go run ./cmd/demo-server -addr "$addr" -token "$token" >"$server_log" 2>&1 &
server_pid=$!
ready=0
for _ in {1..50}; do
  if (exec 3<>/dev/tcp/127.0.0.1/19091) 2>/dev/null; then
    exec 3>&- 3<&-
    ready=1
    break
  fi
  sleep 0.05
done
if [[ "$ready" != 1 ]]; then
  cat "$server_log"
  exit 1
fi
go run ./cmd/demo-client -addr "$addr" -token "$token" -phase reconnect
echo "local TCP round-trip, slow-reader backpressure, cancel, disconnect, and reconnect checks completed"

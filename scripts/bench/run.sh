#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright © 2025 The Dymes project authors
#
# SPDX-License-Identifier: Apache-2.0
#
# Runs benchmark scenarios against a node, each on its own copy of a generated store.
#
#   scripts/bench/run.sh <store dir> <manifest> <results dir> <scenario> [<scenario> ...]
#
# The store is copied per scenario (a copy-on-write clone where the filesystem has one), so every
# scenario starts from the same store and the generated one is never written to.
#
# Environment:
#   DYMES_BIN     directory holding dymes_node and dymes_bench (default: zig-out/bin)
#   DYMES_IMAGE   run the node from this container image instead (4 CPUs, DYMES_CPUS to change)
#   BENCH_PORT    HTTP port (default: 16510)
#   DYMES_CURSOR_IDLE_S  the native node's cursor idle timeout in seconds (default: the node's)
#   BENCH_ARGS    extra arguments for every `dymes_bench run`

set -euo pipefail

store=$(cd "$1" && pwd); manifest=$2; results=$3; shift 3
bin=${DYMES_BIN:-zig-out/bin}
port=${BENCH_PORT:-16510}
mkdir -p "$results"
work=$(dirname "$store")/.bench-work
url="http://127.0.0.1:$port"

clone() {
  rm -rf "$work"
  if [ "$(uname)" = Darwin ]; then cp -c -R "$store" "$work"; else cp --reflink=auto -R "$store" "$work"; fi
}

node_pid=""
start_node() {
  local began=$SECONDS
  if [ -n "${DYMES_IMAGE:-}" ]; then
    docker rm -f dymes-bench >/dev/null 2>&1 || true
    docker run -d --name dymes-bench --cpus "${DYMES_CPUS:-4}" -p "$port:6510" -p 6502:6502 \
      -v "$work:/var/dymes/data" "$DYMES_IMAGE" >/dev/null
  else
    # A node that died leaves its metrics exporter holding the metrics port; the next node then exits
    pkill -f "$bin/dymes_dme" 2>/dev/null || true
    cat > "$work.yaml" <<YAML
engine:
  logging:
    level: info
    format: json
  store:
    id: 1
    data_path: "$work"
  http:
    host: "127.0.0.1"
    port: $port
${DYMES_CURSOR_IDLE_S:+    cursor_idle_timeout_s: $DYMES_CURSOR_IDLE_S}
YAML
    "$bin/dymes_node" --config "$work.yaml" > "$results/$1.node.log" 2>&1 &
    node_pid=$!
  fi
  until curl -s -m 2 -o /dev/null "$url/observe/health/ready"; do
    if [ $((SECONDS - began)) -gt 900 ]; then echo "node did not become ready" >&2; return 1; fi
    if [ -n "$node_pid" ] && ! kill -0 "$node_pid" 2>/dev/null; then echo "node exited while starting" >&2; return 1; fi
    sleep 1
  done
  echo "{\"scenario\":\"$1\",\"node_start_s\":$((SECONDS - began))}" >> "$results/$1.jsonl"
}

stop_node() {
  if [ -n "${DYMES_IMAGE:-}" ]; then
    docker logs dymes-bench > "$results/$1.node.log" 2>&1 || true
    docker rm -f dymes-bench >/dev/null 2>&1 || true
  elif [ -n "$node_pid" ]; then
    kill "$node_pid" 2>/dev/null || true
    wait "$node_pid" 2>/dev/null || true
    node_pid=""
    pkill -f "$bin/dymes_dme" 2>/dev/null || true
  fi
}
trap 'stop_node aborted; rm -rf "$work" "$work.yaml"' EXIT

for scenario in "$@"; do
  echo "== $scenario" >&2
  : > "$results/$scenario.jsonl"
  clone
  start_node "$scenario"
  # shellcheck disable=SC2086
  "$bin/dymes_bench" run "$scenario" --url "$url" --manifest "$manifest" ${BENCH_ARGS:-} \
    >> "$results/$scenario.jsonl" 2> "$results/$scenario.bench.log" || echo "{\"scenario\":\"$scenario\",\"ok\":false,\"failed\":true}" >> "$results/$scenario.jsonl"
  stop_node "$scenario"
  cat "$results/$scenario.jsonl"
done

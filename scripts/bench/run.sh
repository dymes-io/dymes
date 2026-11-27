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
#   BENCH_SMALL_FS  for disk-full: an empty directory on a filesystem about 400 MB larger than the
#                 store (on macOS a disk image is made and removed when this is not set)

set -euo pipefail

store=$(cd "$1" && pwd); manifest=$2; results=$3; shift 3
bin=${DYMES_BIN:-zig-out/bin}
port=${BENCH_PORT:-16510}
mkdir -p "$results"
work_base=$(dirname "$store")/.bench-work
work=$work_base
url="http://127.0.0.1:$port"

# A new directory each time: a container runtime that shares the host's files can keep serving a
# directory that was removed and made again under the same name.
clone() {
  rm -rf "$work" "$work.yaml"
  work="$work_base.$1.$$"
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
    if [ -n "${DYMES_IMAGE:-}" ] && [ "$(docker inspect -f '{{.State.Running}}' dymes-bench 2>/dev/null)" != true ]; then
      docker logs dymes-bench 2>&1 | tail -5 >&2; echo "node container exited while starting" >&2; return 1
    fi
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
small_image=""
cleanup() {
  stop_node aborted
  rm -rf "$work" "$work.yaml"
  if [ -n "$small_image" ]; then hdiutil detach -force "$small_image.mnt" >/dev/null 2>&1 || true; rm -rf "$small_image" "$small_image.mnt"; fi
}
trap cleanup EXIT

# Kills the node (SIGKILL) while clients append and read, restarts it on the same store and checks
# that every acknowledged append is there. Native node only.
crash() {
  local rounds=${CRASH_ROUNDS:-3} round
  clone crash
  for round in $(seq 1 "$rounds"); do
    start_node crash
    "$bin/dymes_bench" run crash-load --url "$url" --manifest "$manifest" --state "$work.acked" \
      >> "$results/crash.jsonl" 2>> "$results/crash.bench.log" &
    local load_pid=$!
    sleep $((2 + RANDOM % 4))
    kill -9 "$node_pid"; wait "$node_pid" 2>/dev/null || true; node_pid=""
    pkill -f "$bin/dymes_dme" 2>/dev/null || true
    wait "$load_pid" || true
    cp "$results/crash.node.log" "$results/crash.node.$round.killed.log"
    start_node crash
    "$bin/dymes_bench" run crash-verify --url "$url" --manifest "$manifest" --state "$work.acked" \
      >> "$results/crash.jsonl" 2>> "$results/crash.bench.log" || echo '{"scenario":"crash-verify","ok":false,"failed":true}' >> "$results/crash.jsonl"
    stop_node crash
  done
  rm -f "$work.acked"
  cat "$results/crash.jsonl"
}

# Fills the store's disk while clients append, then frees space and restarts the node. A fit node
# refuses the appends it cannot store, keeps serving reads and says it is not ready; restarted with
# space, it has every append it acknowledged and appends again. Native node only.
disk_full() {
  local small=${BENCH_SMALL_FS:-} acked="$results/disk-full.acked"
  if [ -z "$small" ]; then
    [ "$(uname)" = Darwin ] || { echo "disk-full needs BENCH_SMALL_FS" >&2; return 1; }
    small_image="$work_base.small.$$.sparseimage"; small="$small_image.mnt"
    hdiutil create -quiet -type SPARSE -fs APFS -volname dymes-bench-small \
      -size $(( $(du -sm "$store" | cut -f1) + 400 ))m "$small_image"
    mkdir -p "$small"; hdiutil attach -quiet -nobrowse -mountpoint "$small" "$small_image"
  fi
  rm -rf "$work" "$work.yaml" "$acked"
  work="$small/store"
  cp -R "$store" "$work"
  start_node disk-full
  # Leave 48 MB: the node appends for a moment and then meets the full disk
  local free_mb=$(( $(df -k "$small" | awk 'NR==2 {print $4}') / 1024 ))
  dd if=/dev/zero of="$small/filler" bs=1048576 count=$(( free_mb - 48 )) 2>/dev/null
  "$bin/dymes_bench" run crash-load --url "$url" --manifest "$manifest" --state "$acked" \
    >> "$results/disk-full.jsonl" 2>> "$results/disk-full.bench.log" || true
  local alive=true; kill -0 "$node_pid" 2>/dev/null || alive=false
  echo "{\"scenario\":\"disk-full\",\"free_kb_when_refused\":$(df -k "$small" | awk 'NR==2 {print $4}'),\"node_running\":$alive}" >> "$results/disk-full.jsonl"
  "$bin/dymes_bench" run disk-full-check --url "$url" --manifest "$manifest" \
    >> "$results/disk-full.jsonl" 2>> "$results/disk-full.bench.log" || echo '{"scenario":"disk-full-check","ok":false,"failed":true}' >> "$results/disk-full.jsonl"
  rm -f "$small/filler"
  cp "$results/disk-full.node.log" "$results/disk-full.node.full.log"
  stop_node disk-full
  if start_node disk-full; then
    "$bin/dymes_bench" run crash-verify --url "$url" --manifest "$manifest" --state "$acked" \
      >> "$results/disk-full.jsonl" 2>> "$results/disk-full.bench.log" || echo '{"scenario":"crash-verify","ok":false,"failed":true}' >> "$results/disk-full.jsonl"
  else
    echo '{"scenario":"disk-full","starts_after_space_freed":false,"ok":false}' >> "$results/disk-full.jsonl"
  fi
  stop_node disk-full
  rm -f "$acked"
  cat "$results/disk-full.jsonl"
}

# Stops the node (SIGTERM) with cursors open, some with a long fetch in flight, and starts it again.
# A fit node stops within seconds, and afterwards refuses the old cursors. Native node
# only.
restart() {
  local state="$results/restart.cursors"
  clone restart
  start_node restart
  "$bin/dymes_bench" run cursors-hold --url "$url" --manifest "$manifest" --state "$state" \
    >> "$results/restart.jsonl" 2>> "$results/restart.bench.log" &
  local hold_pid=$!
  sleep 5
  local began=$(date +%s) stop_s=-1
  kill -TERM "$node_pid"
  while kill -0 "$node_pid" 2>/dev/null; do
    if [ $(( $(date +%s) - began )) -ge 60 ]; then kill -9 "$node_pid"; break; fi
    sleep 0.2
  done
  wait "$node_pid" 2>/dev/null && stop_s=$(( $(date +%s) - began )) || stop_s=$(( $(date +%s) - began ))
  local stopped=true; [ "$stop_s" -lt 60 ] || stopped=false
  node_pid=""
  pkill -f "$bin/dymes_dme" 2>/dev/null || true
  wait "$hold_pid" || true
  echo "{\"scenario\":\"restart\",\"stop_s\":$stop_s,\"stopped_on_sigterm\":$stopped,\"ok\":$( [ "$stop_s" -lt 10 ] && echo true || echo false )}" >> "$results/restart.jsonl"
  cp "$results/restart.node.log" "$results/restart.node.stopped.log"
  start_node restart
  "$bin/dymes_bench" run cursors-after --url "$url" --manifest "$manifest" --state "$state" \
    >> "$results/restart.jsonl" 2>> "$results/restart.bench.log" || echo '{"scenario":"cursors-after","ok":false,"failed":true}' >> "$results/restart.jsonl"
  stop_node restart
  rm -f "$state"
  cat "$results/restart.jsonl"
}

for scenario in "$@"; do
  echo "== $scenario" >&2
  : > "$results/$scenario.jsonl"
  if [ "$scenario" = crash ]; then crash; continue; fi
  if [ "$scenario" = disk-full ]; then disk_full; continue; fi
  if [ "$scenario" = restart ]; then restart; continue; fi
  clone "$scenario"
  start_node "$scenario"
  # shellcheck disable=SC2086
  "$bin/dymes_bench" run "$scenario" --url "$url" --manifest "$manifest" ${BENCH_ARGS:-} \
    >> "$results/$scenario.jsonl" 2> "$results/$scenario.bench.log" || echo "{\"scenario\":\"$scenario\",\"ok\":false,\"failed\":true}" >> "$results/$scenario.jsonl"
  stop_node "$scenario"
  cat "$results/$scenario.jsonl"
done

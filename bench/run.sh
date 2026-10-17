#!/usr/bin/env bash
# Inter-core ping-pong: cannes engine vs. two bare threads on the same rings, interleaved.
# usage: bench/run.sh [reps=5] [iterations=1000000] [ping_core=1] [pong_core=2]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPS=${1:-5}; ITER=${2:-1000000}; PING=${3:-1}; PONG=${4:-2}; WARMUP=100000

RUSTFLAGS="-C target-cpu=native" cargo build --release --quiet --example pingpong --manifest-path "$ROOT/Cargo.toml"
BIN="$ROOT/target/release/examples/pingpong"
for ((r = 1; r <= REPS; r++)); do
    "$BIN" "$ITER" "$WARMUP" "$PING" "$PONG" engine 2>/dev/null
    "$BIN" "$ITER" "$WARMUP" "$PING" "$PONG" raw
done

#!/usr/bin/env bash
# Builds both ping-pong benchmarks and runs them interleaved, REPS times each.
# usage: bench/run_pingpong.sh [reps=5] [iterations=1000000] [ping_core=1] [pong_core=2]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPS=${1:-5}; ITER=${2:-1000000}; PING=${3:-1}; PONG=${4:-2}; WARMUP=100000

mkdir -p "$ROOT/build"
cmake -S "$ROOT" -B "$ROOT/build" -DCMAKE_BUILD_TYPE=Release -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
      -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG -march=native -Wno-error" >/dev/null
make -C "$ROOT/build" -j"$(nproc)" pingpong_bench >/dev/null
cargo build --release --quiet --manifest-path "$ROOT/rust/Cargo.toml"

CPP="$ROOT/build/bench/pingpong_cpp/pingpong_bench"
RS="$ROOT/rust/target/release/pingpong_bench"
for ((r = 1; r <= REPS; r++)); do
    "$CPP" "$ITER" "$WARMUP" "$PING" "$PONG" spin
    "$CPP" "$ITER" "$WARMUP" "$PING" "$PONG" yield
    "$RS"  "$ITER" "$WARMUP" "$PING" "$PONG" engine
    "$RS"  "$ITER" "$WARMUP" "$PING" "$PONG" raw
done

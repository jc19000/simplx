#!/usr/bin/env bash
# Same ping-pong on Tredzone simplx (C++) and on cannes, interleaved.
# Clones simplx into a temporary directory, adds bench/simplx-cpp to its build and builds it in
# Release (gcc >= 13 needs -Wno-error: simplx builds with -Werror). The measured version is the
# `oss` branch of simplx (commit 0aff7eb), kept in the fork below; Tredzone/simplx `master` is an
# older layout this script does not build.
# usage: bench/compare_simplx.sh [reps=5] [iterations=1000000] [ping_core=1] [pong_core=2]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPS=${1:-5}; ITER=${2:-1000000}; PING=${3:-1}; PONG=${4:-2}; WARMUP=100000
WORK="${SIMPLX_DIR:-$(mktemp -d)}"

if [ ! -d "$WORK/simplx" ]; then
    git clone --depth 1 --branch oss https://github.com/jc19000/simplx "$WORK/simplx"
    cp -r "$ROOT/bench/simplx-cpp" "$WORK/simplx/pingpong_bench"
    printf '\nadd_subdirectory(pingpong_bench)\n' >> "$WORK/simplx/CMakeLists.txt"
fi
cmake -S "$WORK/simplx" -B "$WORK/build" -DCMAKE_BUILD_TYPE=Release -DCMAKE_POLICY_VERSION_MINIMUM=3.5 \
      -DCMAKE_CXX_FLAGS_RELEASE="-O3 -DNDEBUG -march=native -Wno-error" >/dev/null
make -C "$WORK/build" -j"$(nproc)" pingpong_bench >/dev/null
RUSTFLAGS="-C target-cpu=native" cargo build --release --quiet --example pingpong --manifest-path "$ROOT/Cargo.toml"

CPP="$WORK/build/pingpong_bench/pingpong_bench"
RS="$ROOT/target/release/examples/pingpong"
for ((r = 1; r <= REPS; r++)); do
    "$CPP" "$ITER" "$WARMUP" "$PING" "$PONG" spin
    "$CPP" "$ITER" "$WARMUP" "$PING" "$PONG" yield
    "$RS"  "$ITER" "$WARMUP" "$PING" "$PONG" engine 2>/dev/null
    "$RS"  "$ITER" "$WARMUP" "$PING" "$PONG" raw
done

# Simplx benchmarks
cmake_minimum_required(VERSION 3.7.2)

include("../common_simplx.cmake")

trz_set_cxx_flags()
trz_add_topdir(src/engine)

# Include path for common headers
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/common)

# Microbenchmark — hot path component cost isolation
add_executable(bench_microbench.bin microbench/main.cpp)
target_link_libraries(bench_microbench.bin engine)

# Flush demo — 6-actor pipeline showing flushPipe() parallelization benefit
add_executable(bench_flush_demo.bin flush_demo/main.cpp)
target_link_libraries(bench_flush_demo.bin engine)

cmake_minimum_required(VERSION 3.0)
set(TARGET_NAME pingpong_bench)

set(SOURCE_FILES pingpong_bench.cpp)

add_executable(${TARGET_NAME} ${SOURCE_FILES})
target_link_libraries(${TARGET_NAME} engine)

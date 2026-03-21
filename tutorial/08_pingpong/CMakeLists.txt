# single tuto
cmake_minimum_required(VERSION 3.7.2)

include("../../common_simplx.cmake")

trz_set_cxx_flags()

trz_add_topdir(src/engine)

# v1 — original pingpong
set(TARGET_V1 pingpong.bin)
set(SOURCE_V1 ${CMAKE_CURRENT_SOURCE_DIR}/pingpong.cpp)
add_executable(${TARGET_V1} ${SOURCE_V1})
target_link_libraries(${TARGET_V1} engine)

# v2 — static dispatch + cache optimized
set(TARGET_V2 pingpong_v2.bin)
set(SOURCE_V2 ${CMAKE_CURRENT_SOURCE_DIR}/pingpong_v2.cpp)
add_executable(${TARGET_V2} ${SOURCE_V2})
target_link_libraries(${TARGET_V2} engine)

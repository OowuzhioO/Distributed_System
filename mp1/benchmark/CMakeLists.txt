cmake_minimum_required(VERSION 2.8)
project(benchmark_grep)

set(sources
    main.cpp
    ../client/LogQueryClient.cpp
)

set(includes
    ../client/LogQueryClient.h
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -pthread")
FIND_PACKAGE(Boost REQUIRED COMPONENTS system)
include_directories(${Boost_INCLUDE_DIRS})

add_executable(benchmark_grep ${sources} ${includes})
target_link_libraries(benchmark_grep ${Boost_LIBRARIES})
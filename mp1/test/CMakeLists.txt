cmake_minimum_required(VERSION 2.8)
project(log_grep_test)

set(sources
    ../client/LogQueryClient.cpp
    LogGrepTests.cpp
)

set(includes
    ../client/LogQueryClient.h
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -pthread")
FIND_PACKAGE(Boost REQUIRED COMPONENTS system)
include_directories(${Boost_INCLUDE_DIRS})

add_executable(log_grep_test ${sources} ${includes})
target_link_libraries(log_grep_test ${Boost_LIBRARIES})
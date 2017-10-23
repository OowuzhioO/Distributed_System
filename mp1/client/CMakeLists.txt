cmake_minimum_required(VERSION 2.8)
project(log_grep)

set(sources
    LogQueryClient.cpp
    main.cpp
)

set(includes
    LogQueryClient.h
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -pthread")
FIND_PACKAGE(Boost REQUIRED COMPONENTS system)
include_directories(${Boost_INCLUDE_DIRS})

add_executable(log_grep ${sources} ${includes})
target_link_libraries(log_grep ${Boost_LIBRARIES})
cmake_minimum_required(VERSION 2.8)
project(log)

set(sources_client
	log_generator.cpp		
)

set(includes_client
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++1y -pthread")
FIND_PACKAGE(Boost REQUIRED COMPONENTS system)
include_directories(${Boost_INCLUDE_DIRS})

add_executable(logger ${sources_client} ${includes_client})
target_link_libraries(logger ${Boost_LIBRARIES})


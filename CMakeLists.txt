cmake_minimum_required (VERSION 2.6)
project (wordcount)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -Wall -Wextra -std=c++11")

include_directories ("${PROJECT_SOURCE_DIR}/MapReducer")
add_subdirectory(MapReducer)

add_executable (wordcount wordcount.cpp)
target_link_libraries (wordcount MapReducer)

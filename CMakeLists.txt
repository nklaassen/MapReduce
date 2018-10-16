cmake_minimum_required (VERSION 2.6)
project (MapReduce)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3 -Wall -Wextra -std=c++11")

include_directories ("${PROJECT_SOURCE_DIR}/MapReducer")
add_subdirectory(MapReducer)

add_executable (sort sort.cpp)
target_link_libraries (sort MapReducer)

add_executable (wordcount wordcount.cpp)
add_executable (matrixmult matrixmult.cpp)
target_link_libraries (wordcount MapReducer)
target_link_libraries (matrixmult MapReducer)

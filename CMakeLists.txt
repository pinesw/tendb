cmake_minimum_required(VERSION 3.27)
project(tendb VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)

# add_library(tendb tendb.cpp)
add_executable(${PROJECT_NAME} test/skip_list.cpp)

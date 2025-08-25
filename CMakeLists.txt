cmake_minimum_required(VERSION 3.27)
project(tendb VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

# Add include directories
include_directories(src)

# Find dependencies
find_package(Boost REQUIRED)

add_executable(${PROJECT_NAME} test/test_pbt.cpp src/pbt/pbt.cpp)
target_link_libraries(${PROJECT_NAME} boost::boost)

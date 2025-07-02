cmake_minimum_required(VERSION 3.27)
project(tendb VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

# Configure Conan
# set(CONAN_PATH "${CMAKE_CURRENT_SOURCE_DIR}/build")
# set(CMAKE_TOOLCHAIN_FILE "${CMAKE_CURRENT_SOURCE_DIR}/build/conan_toolchain.cmake")

# Find and include Conan dependencies
set(CMAKE_FIND_PACKAGE_PREFER_CONFIG TRUE)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/build")
list(APPEND CMAKE_PREFIX_PATH "${CMAKE_CURRENT_SOURCE_DIR}/build")
find_package(Tracy REQUIRED)

# Add include directories
include_directories(src)

# add_library(tendb tendb.cpp)
add_executable(${PROJECT_NAME} test/skip_list.cpp)
target_link_libraries(${PROJECT_NAME} Tracy::TracyClient)

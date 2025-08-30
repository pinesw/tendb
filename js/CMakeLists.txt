cmake_minimum_required(VERSION 3.27)
project(tendb VERSION 0.1.0 LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 20)

# Print arguments for debugging
message(CONAN_PATH="${CONAN_PATH}")
message(NODE_API_HEADERS="${NODE_API_HEADERS}")

# Set output directories to be consistent across all platforms
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
foreach(OUTPUTCONFIG ${CMAKE_CONFIGURATION_TYPES})
    string(TOUPPER ${OUTPUTCONFIG} OUTPUTCONFIG)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR})
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR})
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR})
endforeach(OUTPUTCONFIG CMAKE_CONFIGURATION_TYPES)

# Add include directories
include_directories(include)
include_directories(../src)

# Configure Node API
add_definitions(-DNAPI_VERSION=8)
include_directories(${NODE_API_HEADERS}/include)

# Find dependencies
list(APPEND CMAKE_MODULE_PATH ${CONAN_PATH})
list(APPEND CMAKE_PREFIX_PATH ${CONAN_PATH})
set(CMAKE_FIND_PACKAGE_PREFER_CONFIG TRUE)
find_package(Boost REQUIRED)

add_library(${PROJECT_NAME} SHARED binding.cpp ../src/pbt/pbt.cpp)
set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "" SUFFIX ".node")
target_link_libraries(${PROJECT_NAME} boost::boost)

# Link node_api.lib on Windows
if(MSVC)
    message("Linking node_api.lib on Windows")

    SET(CMAKE_CXX_FLAGS "/EHsc")
    execute_process(COMMAND ${CMAKE_AR} /def:${NODE_API_HEADERS}/def/node_api.def /out:${CMAKE_BINARY_DIR}/node_api.lib ${CMAKE_STATIC_LINKER_FLAGS})
    target_link_libraries(${PROJECT_NAME} node_api)
endif()

# Ignore undefined symbols on MacOS (for Node API)
if(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
    message("Ignoring undefined symbols on MacOS")

    set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS} -undefined dynamic_lookup")
endif()

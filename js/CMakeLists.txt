cmake_minimum_required(VERSION 3.27)
project(tendb VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)

# Add include directories
include_directories(include)
include_directories(../src)

# Find dependencies
find_package(Boost REQUIRED)

# Configure Node API
add_definitions(-DNAPI_VERSION=8)
include_directories(node_modules/node-api-headers/include)

add_library(${PROJECT_NAME} SHARED binding.cpp ../src/pbt/pbt.cpp)
set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "" SUFFIX ".node")
target_link_libraries(${PROJECT_NAME} boost::boost)

# Link node_api.lib on Windows
if(MSVC)
    message("Linking node_api.lib on Windows")

    SET(CMAKE_CXX_FLAGS "/EHsc")
    execute_process(COMMAND ${CMAKE_AR} /def:C:/Users/Wouter/workspace/tendb/js/node_modules/node-api-headers/def/node_api.def /out:${CMAKE_BINARY_DIR}/node_api.lib ${CMAKE_STATIC_LINKER_FLAGS})
    target_link_libraries(${PROJECT_NAME} node_api)
endif()

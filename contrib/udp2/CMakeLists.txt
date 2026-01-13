# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.11.0)
project(udp2)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Get the top-level project directory
set(TOP_DIR ${PROJECT_SOURCE_DIR}/../..)
set(CBDB_INCLUDE_DIR ${TOP_DIR}/src/include)

# CMAKE_INSTALL_PREFIX should be set by the calling Makefile
# If not set, we'll use a reasonable default but warn about it
if(NOT DEFINED CMAKE_INSTALL_PREFIX OR CMAKE_INSTALL_PREFIX STREQUAL "/usr/local")
    message(WARNING "CMAKE_INSTALL_PREFIX not set by parent build system, using default")
    set(CMAKE_INSTALL_PREFIX "/usr/local" CACHE PATH "Install prefix" FORCE)
endif()

# Check for debug/release configuration from main project
include(CheckSymbolExists)
set(PG_CONFIG_HEADER_FILE "${CBDB_INCLUDE_DIR}/pg_config.h")
if(EXISTS "${PG_CONFIG_HEADER_FILE}")
    CHECK_SYMBOL_EXISTS(USE_ASSERT_CHECKING "${PG_CONFIG_HEADER_FILE}" UDP2_USE_DEBUG)
    if(UDP2_USE_DEBUG)
        set(CMAKE_BUILD_TYPE "Debug" CACHE STRING "Choose the type of build." FORCE)
        message(STATUS "Setting CMAKE_BUILD_TYPE to 'Debug' based on main project configuration")
    else()
        set(CMAKE_BUILD_TYPE "Release" CACHE STRING "Choose the type of build." FORCE)
        message(STATUS "Setting CMAKE_BUILD_TYPE to 'Release' based on main project configuration")
    endif()
else()
    # Fallback to Release if pg_config.h is not found
    if(NOT CMAKE_BUILD_TYPE)
        set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
        message(STATUS "Setting default CMAKE_BUILD_TYPE to 'Release'")
    endif()
endif()

# First, build and install ic_common as a subdirectory
add_subdirectory(ic_common)

# Set up include directories
include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_INSTALL_PREFIX}/include/postgresql/)
include_directories(${CMAKE_INSTALL_PREFIX}/include/postgresql/udp2/)
include_directories(${CBDB_INCLUDE_DIR})
include_directories(${CBDB_INCLUDE_DIR}/server)

# Set up library directories
link_directories(${CMAKE_INSTALL_PREFIX}/lib/postgresql/)

# Source files for udp2 module
set(UDP2_SOURCES
    ic_udp2.c
    ic_modules.c
)

# Create the udp2 shared library
add_library(udp2 SHARED ${UDP2_SOURCES})

# Set compiler flags consistent with main project
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -D_GNU_SOURCE -Wall -Wpointer-arith -Wendif-labels -Wmissing-format-attribute -Wimplicit-fallthrough=3 -Wcast-function-type -Wformat-security -fno-strict-aliasing -fwrapv")

if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS} -g -ggdb")
    message(STATUS "Building udp2 in Debug mode")
elseif (${CMAKE_BUILD_TYPE} STREQUAL "Release")
    set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS} -O2 -DNDEBUG")
    message(STATUS "Building udp2 in Release mode")
endif()

# Link against ic_common library
target_link_libraries(udp2 ic_common)

# Make sure ic_common is built before udp2
add_dependencies(udp2 ic_common)

# Set output name and remove lib prefix
set_target_properties(udp2 PROPERTIES
    OUTPUT_NAME "udp2"
    PREFIX ""
)

# Install the udp2 library
install(TARGETS udp2
    LIBRARY DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/postgresql/"
    ARCHIVE DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/postgresql/"
)

# Install udp2 headers
install(FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/ic_udp2.h"
    "${CMAKE_CURRENT_SOURCE_DIR}/ic_modules.h"
    DESTINATION "${CMAKE_INSTALL_PREFIX}/include/postgresql/"
)

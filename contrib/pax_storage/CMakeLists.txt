cmake_minimum_required (VERSION 3.11.0)
project(Pax)
set(CMAKE_CXX_STANDARD 17)

set(TOP_DIR ${PROJECT_SOURCE_DIR}/../..)
set(CBDB_INCLUDE_DIR ${TOP_DIR}/src/include)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Werror -Wno-unused-function -Wno-error=ignored-qualifiers -Wuninitialized -Winit-self -Wstrict-aliasing -Wno-missing-field-initializers -Wno-unused-parameter -Wno-clobbered -Wno-sized-deallocation -g")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wno-unused-parameter -Wno-parameter-name")

option(USE_MANIFEST_API "Use manifest API" OFF)
option(USE_PAX_CATALOG "Use manifest API, by pax impl" ON)

# Build gtest options
option(BUILD_GTEST "Build with google test" ON)
option(BUILD_GBENCH "Build with google benchmark" OFF)

# Build pax tools
option(BUILD_TOOLS "Build with pax tools" OFF)

# Build with return dictionary in record batch
option(BUILD_RB_RET_DICT "Build with pax direct return dictionary in record batch." OFF)

if (USE_MANIFEST_API)
  ADD_DEFINITIONS(-DUSE_MANIFEST_API)
else()
  set(USE_PAX_CATALOG ON)
endif(USE_MANIFEST_API)

if (USE_PAX_CATALOG)
  ADD_DEFINITIONS(-DUSE_PAX_CATALOG)
endif()

include(CheckSymbolExists)
SET(PG_CONFIG_HEADER_FILE "${CBDB_INCLUDE_DIR}/pg_config.h")
message(STATUS "pg_config.h => ${PG_CONFIG_HEADER_FILE}")

CHECK_SYMBOL_EXISTS(USE_ASSERT_CHECKING "${PG_CONFIG_HEADER_FILE}" PAX_USE_DEBUG)
CHECK_SYMBOL_EXISTS(USE_LZ4 "${PG_CONFIG_HEADER_FILE}" PAX_USE_LZ4)
if (PAX_USE_DEBUG)
  ADD_DEFINITIONS(-DENABLE_DEBUG)
  # Use to build compile_commands.json
  SET(CMAKE_BUILD_TYPE "Debug")
  SET(CMAKE_CXX_FLAGS_DEBUG "$ENV{CXXFLAGS} -O0 -ggdb")
  SET(INSTALL_GTEST OFF) # disable gtest install() target
else()
  SET(CMAKE_BUILD_TYPE "Release")
  SET(CMAKE_CXX_FLAGS_RELEASE "$ENV{CXXFLAGS} -O3")
  # no need build gtest in release mode
  SET(BUILD_GTEST OFF)
endif()
message(STATUS "PAX BUILD TYPE: ${CMAKE_BUILD_TYPE}")

if(BUILD_GBENCH)
  SET(BUILD_GTEST ON)
  SET(BENCHMARK_ENABLE_INSTALL OFF) # disable benchmark install() target
endif(BUILD_GBENCH)

if (BUILD_GTEST)
  SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -no-pie -fno-stack-protector -Wall -Wno-unused-function  -Wno-unused-variable")
  SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-access-control -fno-inline -Wno-pmf-conversions -Wl,--allow-multiple-definition -no-pie -fno-stack-protector")
endif(BUILD_GTEST)

# Vec options
CHECK_SYMBOL_EXISTS(USE_VECTORIZATION "${PG_CONFIG_HEADER_FILE}" VEC_BUILD)
if (VEC_BUILD)
  set(VEC_HOME "${PROJECT_SOURCE_DIR}/../vectorization")
  set(INSTALL_HOME "${CMAKE_INSTALL_PREFIX}")

  if (BUILD_RB_RET_DICT)
    ADD_DEFINITIONS(-DBUILD_RB_RET_DICT)
  endif(BUILD_RB_RET_DICT)

  ADD_DEFINITIONS(-DVEC_BUILD)
  message(STATUS "Build pax with vectorization support, VEC_HOME=${VEC_HOME} INSTALL_HOME=${INSTALL_HOME}, BUILD_RB_RET_DICT=${BUILD_RB_RET_DICT}")
else()
  message(STATUS "Build pax without vectorization support")
endif(VEC_BUILD)

## find dependency
## depend on the value of the above option to check dependencies.
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/")
include(FindDependencies)
add_subdirectory(src/cpp)

if(USE_MANIFEST_API AND NOT USE_PAX_CATALOG)
    set(SAVED_BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS})
    set(SAVED_CMAKE_INSTALL_SO_NO_EXE ${CMAKE_INSTALL_SO_NO_EXE})

    set(BUILD_SHARED_LIBS ON)
    set(CMAKE_INSTALL_SO_NO_EXE OFF)
    add_subdirectory(${CMAKE_SOURCE_DIR}/../../dependency/yyjson ${CMAKE_BINARY_DIR}/yyjson_build)

    set(BUILD_SHARED_LIBS ${SAVED_BUILD_SHARED_LIBS})
    set(CMAKE_INSTALL_SO_NO_EXE ${SAVED_CMAKE_INSTALL_SO_NO_EXE})
endif()

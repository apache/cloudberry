cmake_minimum_required(VERSION 3.18)
# include_guard()           need 3.10
# file(ARCHIVE_EXTRACT foo) need 3.18

project(diskquota)

# generate 'compile_commands.json'
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Gpdb.cmake)

# set include directories for all sub-projects
include_directories(${PG_INCLUDE_DIR_SERVER})
include_directories(${PG_INCLUDE_DIR}) # for libpq
# set link flags for all sub-projects
set(CMAKE_SHARED_LINKER_FLAGS "${PG_LD_FLAGS}")
# set c and ld flags for all projects
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${PG_C_FLAGS}")
set(CMAKE_MODULE_LINKER_FLAGS_INIT "${CMAKE_EXE_LINKER_FLAGS} ${PG_LD_FLAGS}")

# generate version
if(NOT DEFINED DISKQUOTA_VERSION)
  file(STRINGS VERSION DISKQUOTA_VERSION)
endif()

string(REGEX REPLACE "^([0-9]+).[0-9]+.[0-9]+$" "\\1" DISKQUOTA_MAJOR_VERSION
                     ${DISKQUOTA_VERSION})
string(REGEX REPLACE "^[0-9]+.([0-9]+).[0-9]+$" "\\1" DISKQUOTA_MINOR_VERSION
                     ${DISKQUOTA_VERSION})
string(REGEX REPLACE "^[0-9]+.[0-9]+.([0-9]+)$" "\\1" DISKQUOTA_PATCH_VERSION
                     ${DISKQUOTA_VERSION})

if("${DISKQUOTA_MAJOR_VERSION}.${DISKQUOTA_MINOR_VERSION}" STREQUAL "1.0")
  # in special, version 1.0.x do not has suffix
  set(DISKQUOTA_BINARY_NAME "diskquota")
else()
  set(DISKQUOTA_BINARY_NAME
      "diskquota-${DISKQUOTA_MAJOR_VERSION}.${DISKQUOTA_MINOR_VERSION}")
endif()

set(CMAKE_C_FLAGS
    "${CMAKE_C_FLAGS} \
	-DDISKQUOTA_VERSION='\"${DISKQUOTA_VERSION}\"' \
	-DDISKQUOTA_MAJOR_VERSION='\"${DISKQUOTA_MAJOR_VERSION}\"' \
	-DDISKQUOTA_MINOR_VERSION='\"${DISKQUOTA_MINOR_VERSION}\"' \
	-DDISKQUOTA_PATCH_VERSION='\"${DISKQUOTA_PATCH_VERSION}\"' \
	-DDISKQUOTA_BINARY_NAME='\"${DISKQUOTA_BINARY_NAME}\"'")

list(
  APPEND
  diskquota_SRC
  diskquota.c
  diskquota_utility.c
  enforcement.c
  gp_activetable.c
  quotamodel.c
  relation_cache.c)

list(
  APPEND
  diskquota_DDL
  diskquota.control
  diskquota--1.0.sql
  diskquota--1.0--2.0.sql
  diskquota--2.0.sql
  diskquota--2.0--1.0.sql)

add_library(diskquota MODULE ${diskquota_SRC})

if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX "${PG_HOME}" CACHE PATH "default install prefix" FORCE)
endif()

set_target_properties(
  diskquota
  PROPERTIES OUTPUT_NAME ${DISKQUOTA_BINARY_NAME}
             PREFIX ""
             C_STANDARD 99
             LINKER_LANGUAGE "CXX")

# packing part, move to a separate file if this part is too large
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Distro.cmake)

if(DEFINED DISKQUOTA_PREVIOUS_INSTALLER)
  message(STATUS "Copy pervious installer from ${DISKQUOTA_PREVIOUS_INSTALLER}")
  file(ARCHIVE_EXTRACT INPUT ${DISKQUOTA_PREVIOUS_INSTALLER} PATTERNS "*.so")
  file(GLOB DISKQUOTA_PREVIOUS_LIBRARY
       "${CMAKE_BINARY_DIR}/lib/postgresql/*.so")
  install(PROGRAMS ${DISKQUOTA_PREVIOUS_LIBRARY} DESTINATION "lib/postgresql/")
endif()

set(CPACK_GENERATOR "TGZ")
set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY OFF)
set(CPACK_PACKAGE_FILE_NAME
    "diskquota-${DISKQUOTA_MAJOR_VERSION}.${DISKQUOTA_MINOR_VERSION}.${DISKQUOTA_PATCH_VERSION}-${DISTRO_NAME}_x86_64"
)
include(CPack)
# packing end

# NOTE: keep install part at the end of file, to overwrite previous binary
install(PROGRAMS "cmake/install_gpdb_component" DESTINATION ".")
install(FILES ${diskquota_DDL} DESTINATION "share/postgresql/extension/")
install(TARGETS diskquota DESTINATION "lib/postgresql/")

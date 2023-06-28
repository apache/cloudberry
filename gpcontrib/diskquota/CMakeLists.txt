cmake_minimum_required(VERSION 3.20)
# cmake_path requires 3.20

project(diskquota)

if(NOT CMAKE_BUILD_TYPE)
  message(STATUS "Setting build type to 'Debug' as none was specified.")
  set(CMAKE_BUILD_TYPE "Debug" CACHE
      STRING "Choose the type of build." FORCE)
endif()

# generate 'compile_commands.json'
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Retrieve repository information
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Git.cmake)
GitHash_Get(DISKQUOTA_GIT_HASH)

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Gpdb.cmake)


# set include directories for all sub-projects
include_directories(${PG_INCLUDE_DIR_SERVER})
include_directories(${PG_INCLUDE_DIR}) # for libpq
# Overwrite the default build type flags set by cmake.
# We don't want the '-O3 -DNDEBUG' from cmake. Instead, those will be set by the CFLAGS from pg_config.
# And, the good news is, GPDB release always have '-g'.
set(CMAKE_C_FLAGS_RELEASE "" CACHE
    STRING "Flags for RELEASE build" FORCE)
set(CMAKE_C_FLAGS_DEBUG "-DDISKQUOTA_DEBUG"
    CACHE STRING "Flags for DEBUG build" FORCE)
# set link flags for all sub-projects
set(CMAKE_MODULE_LINKER_FLAGS "${PG_LD_FLAGS}")
if (APPLE)
    set(CMAKE_MODULE_LINKER_FLAGS "${CMAKE_MODULE_LINKER_FLAGS} -bundle_loader ${PG_BIN_DIR}/postgres")
endif()
# set c and ld flags for all projects
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${PG_C_FLAGS}")

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

add_compile_definitions(
  DISKQUOTA_VERSION="${DISKQUOTA_VERSION}"
  DISKQUOTA_MAJOR_VERSION=${DISKQUOTA_MAJOR_VERSION}
  DISKQUOTA_MINOR_VERSION=${DISKQUOTA_MINOR_VERSION}
  DISKQUOTA_PATCH_VERSION=${DISKQUOTA_PATCH_VERSION}
  DISKQUOTA_BINARY_NAME="${DISKQUOTA_BINARY_NAME}")

set(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
file(GLOB diskquota_SRC "${SRC_DIR}/*.c")

set(DISKQUOTA_DDL_DIR "${CMAKE_CURRENT_SOURCE_DIR}/control/ddl")
file(GLOB diskquota_DDL "${DISKQUOTA_DDL_DIR}/*")

add_library(diskquota MODULE ${diskquota_SRC})

if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(CMAKE_INSTALL_PREFIX
      "${PG_HOME}"
      CACHE PATH "default install prefix" FORCE)
endif()

set_target_properties(
  diskquota
  PROPERTIES OUTPUT_NAME ${DISKQUOTA_BINARY_NAME}
             PREFIX ""
             C_STANDARD 99
             LINKER_LANGUAGE "C")

# packing part, move to a separate file if this part is too large
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/Distro.cmake)

if(DEFINED DISKQUOTA_LAST_RELEASE_PATH)
  message(STATUS "Copy pervious installer from ${DISKQUOTA_LAST_RELEASE_PATH}")
  file(ARCHIVE_EXTRACT INPUT ${DISKQUOTA_LAST_RELEASE_PATH} PATTERNS "*.so")
  file(GLOB so_files_list
       "${CMAKE_BINARY_DIR}/lib/postgresql/*.so")

  foreach(so_path IN LISTS so_files_list)
    get_filename_component(so_name ${so_path} NAME_WLE)
    # Replace 'diskquota-x.y' with 'x.y'. 'diskquota' won't be replaced, which belongs to 1.x release.
    string(REPLACE "diskquota-" "" so_ver ${so_name})
    # Install the previous so files. Those so files have versions less than current version.
    # diskqutoa.so doesn't have version string in the file name. It belongs to 1.x release.
    if((${so_ver} STREQUAL "diskquota") OR
      ${so_ver} VERSION_LESS ${DISKQUOTA_MAJOR_VERSION}.${DISKQUOTA_MINOR_VERSION})
      list(APPEND DISKQUOTA_PREVIOUS_LIBRARY ${so_path})
    endif()
  endforeach()

  install(PROGRAMS ${DISKQUOTA_PREVIOUS_LIBRARY} DESTINATION "lib/postgresql/")

  get_filename_component(
    DISKQUOTA_LAST_RELEASE_FILENAME ${DISKQUOTA_LAST_RELEASE_PATH} NAME CACHE
    "last release installer name")
  string(
    REGEX
    REPLACE "^diskquota-([0-9]+).[0-9]+.[0-9]+-.*$" "\\1"
            DISKQUOTA_LAST_MAJOR_VERSION ${DISKQUOTA_LAST_RELEASE_FILENAME})
  string(
    REGEX
    REPLACE "^diskquota-[0-9]+.([0-9]+).[0-9]+-.*$" "\\1"
            DISKQUOTA_LAST_MINOR_VERSION ${DISKQUOTA_LAST_RELEASE_FILENAME})
  string(
    REGEX
    REPLACE "^diskquota-[0-9]+.[0-9]+.([0-9]+)-.*$" "\\1"
            DISKQUOTA_LAST_PATCH_VERSION ${DISKQUOTA_LAST_RELEASE_FILENAME})

  set(DISKQUOTA_LAST_VERSION
      "${DISKQUOTA_LAST_MAJOR_VERSION}.${DISKQUOTA_LAST_MINOR_VERSION}.${DISKQUOTA_LAST_PATCH_VERSION}"
  )
endif()

set(tgz_NAME
    "diskquota-${DISKQUOTA_MAJOR_VERSION}.${DISKQUOTA_MINOR_VERSION}.${DISKQUOTA_PATCH_VERSION}-${DISTRO_NAME}_x86_64"
)
set(CPACK_GENERATOR "TGZ")
set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY OFF)
set(CPACK_PACKAGE_FILE_NAME ${tgz_NAME})
include(CPack)
# create_artifact target is used to tar the package with version into a version-less tarball to be
# used on concourse gcs resource. It will be uploaded to a gcs version file (no diskquota version
# string in the file name), and be retrieved in the release step. Then we don't have to firgure out
# a way to add the version string back to the release file name, just untar it.
set(artifact_NAME "diskquota.tar.gz")
add_custom_target(create_artifact
  COMMAND
  ${CMAKE_COMMAND} --build . --target package
  COMMAND
  ${CMAKE_COMMAND} -E tar czvf ${artifact_NAME} "${tgz_NAME}.tar.gz")
# packing end

# Create build-info
# The diskquota-build-info shouldn't be copied to GPDB release by install_gpdb_component
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/BuildInfo.cmake)
set(build_info_PATH ${CMAKE_CURRENT_BINARY_DIR}/diskquota-build-info)
BuildInfo_Create(${build_info_PATH}
  VARS
  DISKQUOTA_GIT_HASH
  DISKQUOTA_VERSION
  GP_MAJOR_VERSION
  GP_VERSION
  CMAKE_BUILD_TYPE)
# Create build-info end

# Add installcheck targets
add_subdirectory(tests)
add_subdirectory(upgrade_test)

# NOTE: keep install part at the end of file, to overwrite previous binary
install(PROGRAMS "cmake/install_gpdb_component" DESTINATION ".")
install(FILES ${diskquota_DDL} DESTINATION "share/postgresql/extension/")
install(TARGETS diskquota DESTINATION "lib/postgresql/")
install(FILES ${build_info_PATH} DESTINATION ".")

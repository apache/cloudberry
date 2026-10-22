# Use pg_config to detect postgres dependencies
#
# Variables:
#
# PG_CONFIG - the path to the pg_config executable to be used. this determines the
#             version to be built with.
# GP_MAJOR_VERSION - the major version parsed from gpdb source
# GP_VERSION - The GP_VERSION string
# PG_BIN_DIR - location of user executables
# PG_INCLUDE_DIR - location of C header files of the client
# PG_INCLUDE_DIR_SERVER - location of C header files for the server
# PG_LIBS - LIBS value used when PostgreSQL was built
# PG_LIB_DIR - location of object code libraries
# PG_PKG_LIB_DIR - location of dynamically loadable modules
# PG_SHARE_DIR - location of architecture-independent support files
# PG_PGXS - location of extension makefile
# PG_CPP_FLAGS - CPPFLAGS value used when PostgreSQL was built
# PG_C_FLAGS - CFLAGS value used when PostgreSQL was built
# PG_LD_FLAGS - LDFLAGS value used when PostgreSQL was built
# PG_HOME - The installation directory of Greenplum
# PG_SRC_DIR - The directory of the postgres/greenplum source code

include_guard()
find_program(PG_CONFIG pg_config)
if(PG_CONFIG)
    message(STATUS "Use '${PG_CONFIG}'")
else()
    message(FATAL_ERROR "Unable to find 'pg_config'")
endif()
# Query one pg_config value into 'var'.
#
# exec_program() used to do this, but it is deprecated and CMake 3.30 and
# newer warn about every call (CMP0153). execute_process() is the
# replacement; it differs in that it keeps the trailing newline, so
# OUTPUT_STRIP_TRAILING_WHITESPACE is required -- without it every path
# below would carry a newline into include_directories() and friends.
macro(pg_config_var var)
    execute_process(
        COMMAND ${PG_CONFIG} ${ARGN}
        OUTPUT_VARIABLE ${var}
        OUTPUT_STRIP_TRAILING_WHITESPACE)
endmacro()

pg_config_var(PG_INCLUDE_DIR        --includedir)
pg_config_var(PG_INCLUDE_DIR_SERVER --includedir-server)
pg_config_var(PG_PKG_LIB_DIR        --pkglibdir)
pg_config_var(PG_SHARE_DIR          --sharedir)
pg_config_var(PG_BIN_DIR            --bindir)
pg_config_var(PG_CPP_FLAGS          --cppflags)
pg_config_var(PG_C_FLAGS            --cflags)
pg_config_var(PG_LD_FLAGS           --ldflags)
pg_config_var(PG_LIBS               --libs)
pg_config_var(PG_LIB_DIR            --libdir)
pg_config_var(PG_PGXS               --pgxs)
get_filename_component(PG_HOME "${PG_BIN_DIR}/.." ABSOLUTE)

# If PG_SRC_DIR is provided (in-tree build), use source tree paths
# This is necessary because pg_config returns install paths,
# which don't exist yet during in-tree builds
if(PG_SRC_DIR)
    set(PG_INCLUDE_DIR "${PG_SRC_DIR}/src/include")
    set(PG_INCLUDE_DIR_SERVER "${PG_SRC_DIR}/src/include")
    # libpq headers and library are in src/interfaces/libpq in source tree
    set(PG_INCLUDE_DIR_LIBPQ "${PG_SRC_DIR}/src/interfaces/libpq")
    set(PG_LIB_DIR "${PG_SRC_DIR}/src/interfaces/libpq")
    message(STATUS "In-tree build: using source include path '${PG_INCLUDE_DIR}'")
else()
    # Standalone build: try to derive PG_SRC_DIR from Makefile.global (optional)
    get_filename_component(pgsx_SRC_DIR ${PG_PGXS} DIRECTORY)
    set(makefile_global ${pgsx_SRC_DIR}/../Makefile.global)
    if(EXISTS ${makefile_global})
        execute_process(
            COMMAND grep "^abs_top_builddir" ${makefile_global}
            COMMAND sed s/.*abs_top_builddir.*=\(.*\)/\\1/
            OUTPUT_VARIABLE PG_SRC_DIR OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_QUIET)
        if(PG_SRC_DIR)
            string(STRIP ${PG_SRC_DIR} PG_SRC_DIR)
        endif()
    endif()
endif()

# Get the GP_MAJOR_VERSION from header
file(READ ${PG_INCLUDE_DIR}/pg_config.h config_header)
string(REGEX MATCH "#define *GP_MAJORVERSION *\"[0-9]+\"" macrodef "${config_header}")
string(REGEX MATCH "[0-9]+" GP_MAJOR_VERSION "${macrodef}")
if (GP_MAJOR_VERSION)
    message(STATUS "Build extension for Cloudberry ${GP_MAJOR_VERSION}")
else()
    message(FATAL_ERROR "Cannot read GP_MAJORVERSION from '${PG_INCLUDE_DIR}/pg_config.h'")
endif()
string(REGEX MATCH "#define *GP_VERSION *\"[^\"]*\"" macrodef "${config_header}")
string(REGEX REPLACE ".*\"\(.*\)\".*" "\\1" GP_VERSION "${macrodef}")
if (GP_VERSION)
    message(STATUS "The exact Cloudberry version is '${GP_VERSION}'")
else()
    message(FATAL_ERROR "Cannot read GP_VERSION from '${PG_INCLUDE_DIR}/pg_config.h'")
endif()

# Check if PG_SRC_DIR is available (for source-dependent features like isolation2 tests)
if ("${PG_SRC_DIR}" STREQUAL "" OR NOT EXISTS "${PG_SRC_DIR}")
    message(STATUS "PG_SRC_DIR not found or empty, source-dependent features will be disabled")
    set(PG_SRC_DIR_AVAILABLE OFF CACHE BOOL "Whether PG_SRC_DIR is available")
else()
    message(STATUS "PG_SRC_DIR is '${PG_SRC_DIR}'")
    set(PG_SRC_DIR_AVAILABLE ON CACHE BOOL "Whether PG_SRC_DIR is available")
endif()

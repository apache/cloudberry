# Use pg_config to detect postgres dependencies
#
# Variables:
#
# PG_CONFIG - the path to the pg_config executable to be used. this determines the
#             version to be built with.
# GP_MAJOR_VERSION - the major version parsed from gpdb source
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

include_guard()
find_program(PG_CONFIG pg_config)
if(PG_CONFIG)
    message(STATUS "Use '${PG_CONFIG}'")
else()
    message(FATAL_ERROR "Unable to find 'pg_config'")
endif()
exec_program(${PG_CONFIG} ARGS --includedir OUTPUT_VARIABLE PG_INCLUDE_DIR)
exec_program(${PG_CONFIG} ARGS --includedir-server OUTPUT_VARIABLE PG_INCLUDE_DIR_SERVER)
exec_program(${PG_CONFIG} ARGS --pkglibdir OUTPUT_VARIABLE PG_PKG_LIB_DIR)
exec_program(${PG_CONFIG} ARGS --sharedir OUTPUT_VARIABLE PG_SHARE_DIR)
exec_program(${PG_CONFIG} ARGS --bindir OUTPUT_VARIABLE PG_BIN_DIR)
exec_program(${PG_CONFIG} ARGS --cppflags OUTPUT_VARIABLE PG_CPP_FLAGS)
exec_program(${PG_CONFIG} ARGS --cflags OUTPUT_VARIABLE PG_C_FLAGS)
exec_program(${PG_CONFIG} ARGS --ldflags OUTPUT_VARIABLE PG_LD_FLAGS)
exec_program(${PG_CONFIG} ARGS --libs OUTPUT_VARIABLE PG_LIBS)
exec_program(${PG_CONFIG} ARGS --libdir OUTPUT_VARIABLE PG_LIB_DIR)
exec_program(${PG_CONFIG} ARGS --pgxs OUTPUT_VARIABLE PG_PGXS)
get_filename_component(PG_HOME "${PG_BIN_DIR}/.." ABSOLUTE)

# Get the GP_MAJOR_VERSION from header
file(READ ${PG_INCLUDE_DIR}/pg_config.h config_header)
string(REGEX MATCH "#define *GP_MAJORVERSION *\"[0-9]+\"" macrodef "${config_header}")
string(REGEX MATCH "[0-9]+" GP_MAJOR_VERSION "${macrodef}")
if (GP_MAJOR_VERSION)
    message(STATUS "Build extension for GPDB ${GP_MAJOR_VERSION}")
else()
    message(FATAL_ERROR "Cannot read GP_MAJORVERSION from '${PG_INCLUDE_DIR}/pg_config.h'")
endif()

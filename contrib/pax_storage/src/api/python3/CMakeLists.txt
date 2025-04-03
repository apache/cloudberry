project(paypy)
cmake_minimum_required (VERSION 3.11.0)
set(CMAKE_CXX_STANDARD 17)

set(TOP_DIR ${PROJECT_SOURCE_DIR}/../../../../..)
set(CBDB_INCLUDE_DIR ${TOP_DIR}/src/include)

include(CheckSymbolExists)
SET(PG_CONFIG_HEADER_FILE "${CBDB_INCLUDE_DIR}/pg_config.h")
message(STATUS "pg_config.h => ${PG_CONFIG_HEADER_FILE}")

CHECK_SYMBOL_EXISTS(USE_VECTORIZATION "${PG_CONFIG_HEADER_FILE}" VEC_BUILD)

if (VEC_BUILD)

message(STATUS "Build paxpy with vectorization support")
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/paxpy_config_d.h
    COMMAND rm -rf ${CMAKE_CURRENT_SOURCE_DIR}/paxpy_config_d.h && echo "#define VEC_BUILD 1" >> ${CMAKE_CURRENT_SOURCE_DIR}/paxpy_config_d.h
    VERBATIM
)

else()

message(STATUS "Build paxpy without vectorization support")
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/paxpy_config_d.h
    COMMAND rm -rf ${CMAKE_CURRENT_SOURCE_DIR}/paxpy_config_d.h && echo "#undef VEC_BUILD" >> ${CMAKE_CURRENT_SOURCE_DIR}/paxpy_config_d.h
    VERBATIM
)

endif()

add_custom_target(
    build-paxpy ALL
    COMMAND python3 ${CMAKE_CURRENT_SOURCE_DIR}/setup.py bdist_wheel
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/paxpy_config_d.h
)

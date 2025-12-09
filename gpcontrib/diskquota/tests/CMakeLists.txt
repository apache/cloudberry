include(${CMAKE_SOURCE_DIR}/cmake/Regress.cmake)

list(APPEND isolation2_expected_DIR ${CMAKE_CURRENT_SOURCE_DIR}/isolation2/expected)
list(APPEND regress_expected_DIR ${CMAKE_CURRENT_SOURCE_DIR}/regress/expected)
# PLPYTHON_LANG_STR will be replaced by Regress.cmake
set(PLPYTHON_LANG_STR "plpython3u")
set(POSTMASTER_START_CMD "pg_ctl -D $COORDINATOR_DATA_DIRECTORY -w -o \"-c gp_role=dispatch\" start")

set(exclude_fault_injector OFF)
# GP7 release build doesn't support fault injector.
if (CMAKE_BUILD_TYPE STREQUAL "Release")
  message(WARNING "Fault injector test cases will be disabled.")
  set(exclude_fault_injector ON)
endif()

# Check if pg_isolation2_regress is available (either pre-built or can be built from source)
# In binary-only installations, PG_SRC_DIR may not exist or isolation2 may not be buildable
set(ENABLE_ISOLATION2_TESTS OFF)

# First, check if pg_isolation2_regress is already installed
find_program(PG_ISOLATION2_REGRESS pg_isolation2_regress HINTS ${PG_BIN_DIR})
if(PG_ISOLATION2_REGRESS)
  message(STATUS "Found pg_isolation2_regress: ${PG_ISOLATION2_REGRESS}")
  set(ENABLE_ISOLATION2_TESTS ON)
elseif(PG_SRC_DIR_AVAILABLE AND EXISTS "${PG_SRC_DIR}/src/test/isolation2/Makefile")
  # Can build from source
  message(STATUS "pg_isolation2_regress will be built from source: ${PG_SRC_DIR}/src/test/isolation2")
  set(ENABLE_ISOLATION2_TESTS ON)
else()
  message(WARNING "pg_isolation2_regress not found and cannot be built from source. Isolation2 tests will be disabled.")
endif()

RegressTarget_Add(regress
    INIT_FILE
    ${CMAKE_CURRENT_SOURCE_DIR}/init_file
    SQL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/regress/sql
    EXPECTED_DIR ${regress_expected_DIR}
    RESULTS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/regress/results
    DATA_DIR ${CMAKE_CURRENT_SOURCE_DIR}/data
    SCHEDULE_FILE ${CMAKE_CURRENT_SOURCE_DIR}/regress/diskquota_schedule
    EXCLUDE_FAULT_INJECT_TEST ${exclude_fault_injector}
    REGRESS_OPTS
    --load-extension=gp_inject_fault
    --load-extension=diskquota_test
    --dbname=contrib_regression)

if(ENABLE_ISOLATION2_TESTS)
  RegressTarget_Add(isolation2
      REGRESS_TYPE
      isolation2
      INIT_FILE
      ${CMAKE_CURRENT_SOURCE_DIR}/init_file
      SQL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/isolation2/sql
      EXPECTED_DIR ${isolation2_expected_DIR}
      RESULTS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/isolation2/results
      DATA_DIR ${CMAKE_CURRENT_SOURCE_DIR}/data
      SCHEDULE_FILE ${CMAKE_CURRENT_SOURCE_DIR}/isolation2/isolation2_schedule
      EXCLUDE_FAULT_INJECT_TEST ${exclude_fault_injector}
      REGRESS_OPTS
      --load-extension=gp_inject_fault
      --dbname=isolation2test)
endif()

add_custom_target(install_test_extension
  COMMAND
  cmake -E copy ${CMAKE_SOURCE_DIR}/control/test/diskquota_test.control ${CMAKE_INSTALL_PREFIX}/share/postgresql/extension
  COMMAND
  cmake -E copy ${CMAKE_SOURCE_DIR}/control/test/diskquota_test--1.0.sql ${CMAKE_INSTALL_PREFIX}/share/postgresql/extension
  )

add_custom_target(installcheck)
add_dependencies(regress install_test_extension)

if(ENABLE_ISOLATION2_TESTS)
  add_dependencies(isolation2 install_test_extension)
  add_dependencies(installcheck isolation2 regress)
else()
  add_dependencies(installcheck regress)
endif()

# Example to run test_truncate infinite times
# RegressTarget_Add(regress_config
#     INIT_FILE
#     ${CMAKE_CURRENT_SOURCE_DIR}/init_file
#     SQL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/regress/sql
#     EXPECTED_DIR ${CMAKE_CURRENT_SOURCE_DIR}/regress/expected
#     RESULTS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/regress/results
#     DATA_DIR ${CMAKE_CURRENT_SOURCE_DIR}/data
#     REGRESS
#     config test_create_extension
#     REGRESS_OPTS
#     --load-extension=gp_inject_fault
#     --dbname=contrib_regression)
# RegressTarget_Add(regress_truncate_loop
#     INIT_FILE
#     ${CMAKE_CURRENT_SOURCE_DIR}/init_file
#     SQL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/regress/sql
#     EXPECTED_DIR ${CMAKE_CURRENT_SOURCE_DIR}/regress/expected
#     RESULTS_DIR ${CMAKE_CURRENT_SOURCE_DIR}/regress/results
#     DATA_DIR ${CMAKE_CURRENT_SOURCE_DIR}/data
#     REGRESS
#     test_truncate
#     RUN_TIMES -1
#     REGRESS_OPTS
#     --load-extension=gp_inject_fault
#     --dbname=contrib_regression
#     --use-existing)
# add_dependencies(regress_truncate_loop regress_config)
# add_dependencies(installcheck regress_truncate_loop)

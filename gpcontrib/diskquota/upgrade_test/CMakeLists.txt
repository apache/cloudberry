include(${CMAKE_SOURCE_DIR}/cmake/Regress.cmake)

set(EXPECTED_DIR "${CMAKE_CURRENT_SOURCE_DIR}/expected")
list(APPEND schedule_files
  ${CMAKE_CURRENT_SOURCE_DIR}/schedule_2.2--2.3
  ${CMAKE_CURRENT_SOURCE_DIR}/schedule_2.3--2.2
)
regresstarget_add(
  upgradecheck
  INIT_FILE
  ${CMAKE_CURRENT_SOURCE_DIR}/init_file
  SQL_DIR
  ${CMAKE_CURRENT_SOURCE_DIR}/sql
  EXPECTED_DIR
  ${EXPECTED_DIR}
  RESULTS_DIR
  ${CMAKE_CURRENT_SOURCE_DIR}/results
  SCHEDULE_FILE
  ${schedule_files}
  REGRESS_OPTS
  --dbname=contrib_regression)

# NOTE: DDL change detection and upgrade version validation logic was removed
# as diskquota is now part of the Cloudberry source tree. Upgrade testing
# should be handled as part of the Cloudberry release process if needed.

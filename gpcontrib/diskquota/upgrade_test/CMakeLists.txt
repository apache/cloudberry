include(${CMAKE_SOURCE_DIR}/cmake/Regress.cmake)

RegressTarget_Add(upgrade
    INIT_FILE
    ${CMAKE_CURRENT_SOURCE_DIR}/init_file
    SQL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/sql
    EXPECTED_DIR ${CMAKE_CURRENT_SOURCE_DIR}/expected
    SCHEDULE_FILE
        ${CMAKE_CURRENT_SOURCE_DIR}/schedule_1.0--2.0
        ${CMAKE_CURRENT_SOURCE_DIR}/schedule_2.0--1.0
    REGRESS_OPTS --dbname=contrib_regression)

# not use `installcheck` target on purpose.
# upgrade test is not needed in feature development
add_custom_target(upgradecheck)

add_dependencies(upgradecheck upgrade)

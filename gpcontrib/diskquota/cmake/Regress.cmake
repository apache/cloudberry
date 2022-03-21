# CMake module for create regress test target.
#
# Usage:
# RegressTarget_Add(<name>
#   SQL_DIR <sql_dir>
#   EXPECTED_DIR <expected_dir>
#   RESULTS_DIR <results_dir>
#   [INIT_FILE <init_file_1> <init_file_2> ...]
#   [SCHEDULE_FILE <schedule_file_1> <schedule_file_2> ...]
#   [REGRESS <test1> <test2> ...]
#   [EXCLUDE <test1> <test2> ...]
#   [REGRESS_OPTS <opt1> <opt2> ...]
#   [REGRESS_TYPE isolation2/regress]
#   [RUN_TIMES <times>]
# )
# All the file path can be the relative path to ${CMAKE_CURRENT_SOURCE_DIR}.
# A bunch of diff targets will be created as well for comparing the regress results. The diff
# target names like diff_<regress_target_name>_<casename>
#
# Use RUN_TIMES to specify how many times the regress tests should be executed. A negative RUN_TIMES
# will run the test infinite times.
#
# NOTE: To use this cmake file in another project, below files needs to be placed alongside:
#  - regress_show_diff.sh
#  - regress_loop.sh
#
# Example:
# RegressTarget_Add(installcheck_avro_fmt
#    REGRESS ${avro_regress_TARGETS}
#    INIT_FILE init_file
#    DATA_DIR data
#    SQL_DIR sql
#    EXPECTED_DIR expected_${GP_MAJOR_VERSION})

# CMAKE_CURRENT_FUNCTION_LIST_DIR - 3.17
cmake_minimum_required(VERSION 3.17)

# pg_isolation2_regress was not shipped with GPDB release. It needs to be created from source.
function(_PGIsolation2Target_Add working_DIR)
    if(TARGET pg_isolation2_regress)
        return()
    endif()

    add_custom_target(
        pg_isolation2_regress
        COMMAND
        make -C ${PG_SRC_DIR}/src/test/isolation2 install
        COMMAND
        ${CMAKE_COMMAND} -E copy_if_different
        ${PG_SRC_DIR}/src/test/isolation2/sql_isolation_testcase.py ${working_DIR}
    )
endfunction()

function(RegressTarget_Add name)
    cmake_parse_arguments(
        arg
        ""
        "SQL_DIR;EXPECTED_DIR;RESULTS_DIR;DATA_DIR;REGRESS_TYPE;RUN_TIMES"
        "REGRESS;EXCLUDE;REGRESS_OPTS;INIT_FILE;SCHEDULE_FILE"
        ${ARGN})
    if (NOT arg_EXPECTED_DIR)
        message(FATAL_ERROR
            "'EXPECTED_DIR' needs to be specified.")
    endif()
    if (NOT arg_SQL_DIR)
        message(FATAL_ERROR
            "'SQL_DIR' needs to be specified.")
    endif()
    if (NOT arg_RESULTS_DIR)
        message(FATAL_ERROR "'RESULTS_DIR' needs to be specified")
    endif()

    set(working_DIR "${CMAKE_CURRENT_BINARY_DIR}/${name}")
    file(MAKE_DIRECTORY ${working_DIR})

    # Isolation2 test has different executable to run
    if(arg_REGRESS_TYPE STREQUAL isolation2)
        set(regress_BIN ${PG_SRC_DIR}/src/test/isolation2/pg_isolation2_regress)
        _PGIsolation2Target_Add(${working_DIR})
    else()
        set(regress_BIN ${PG_PKG_LIB_DIR}/pgxs/src/test/regress/pg_regress)
        if (NOT EXISTS ${regress_BIN})
            message(FATAL_ERROR
                "Cannot find 'pg_regress' executable by path '${regress_BIN}'. Is 'pg_config' in the $PATH?")
        endif()
    endif()

    # Set REGRESS test cases
    foreach(r IN LISTS arg_REGRESS)
        set(regress_arg ${regress_arg} ${r})
    endforeach()

    # Set REGRESS options
    foreach(o IN LISTS arg_INIT_FILE)
        get_filename_component(init_file_PATH ${o} ABSOLUTE)
        list(APPEND arg_REGRESS_OPTS "--init=${init_file_PATH}")
    endforeach()
    foreach(o IN LISTS arg_SCHEDULE_FILE)
        get_filename_component(schedule_file_PATH ${o} ABSOLUTE)
        list(APPEND arg_REGRESS_OPTS "--schedule=${schedule_file_PATH}")
    endforeach()
    foreach(o IN LISTS arg_EXCLUDE)
        list(APPEND to_exclude ${o})
    endforeach()
    if (to_exclude)
        set(exclude_arg "--exclude-tests=${to_exclude}")
        string(REPLACE ";" "," exclude_arg "${exclude_arg}")
        set(regress_opts_arg ${regress_opts_arg} ${exclude_arg})
    endif()
    foreach(o IN LISTS arg_REGRESS_OPTS)
        set(regress_opts_arg ${regress_opts_arg} ${o})
    endforeach()

    get_filename_component(sql_DIR ${arg_SQL_DIR} ABSOLUTE)
    get_filename_component(expected_DIR ${arg_EXPECTED_DIR} ABSOLUTE)
    get_filename_component(results_DIR ${arg_RESULTS_DIR} ABSOLUTE)
    if (arg_DATA_DIR)
        get_filename_component(data_DIR ${arg_DATA_DIR} ABSOLUTE)
        set(ln_data_dir_CMD ln -s ${data_DIR} data)
    endif()

    set(regress_command
        ${regress_BIN} --psqldir='${PG_BIN_DIR}' ${regress_opts_arg}  ${regress_arg})
    if (arg_RUN_TIMES)
        set(test_command
            ${CMAKE_CURRENT_FUNCTION_LIST_DIR}/regress_loop.sh
            ${arg_RUN_TIMES}
            ${regress_command})
    else()
        set(test_command ${regress_command})
    endif()

    # Create the target
    add_custom_target(
        ${name}
        WORKING_DIRECTORY ${working_DIR}
        COMMAND rm -f sql
        COMMAND ln -s ${sql_DIR} sql
        COMMAND rm -f expected
        COMMAND ln -s ${expected_DIR} expected
        COMMAND rm -f results
        COMMAND mkdir -p ${results_DIR}
        COMMAND ln -s ${results_DIR} results
        COMMAND rm -f data
        COMMAND ${ln_data_dir_CMD}
        COMMAND
        ${test_command}
        ||
        ${CMAKE_CURRENT_FUNCTION_LIST_DIR}/regress_show_diff.sh ${working_DIR}
    )

    if(arg_REGRESS_TYPE STREQUAL isolation2)
        add_dependencies(${name} pg_isolation2_regress)
    endif()

    # Add targets for easily showing results diffs
    FILE(GLOB expected_files ${expected_DIR}/*.out)
    foreach(f IN LISTS expected_files)
        get_filename_component(casename ${f} NAME_WE)
        set(diff_target_name diff_${name}_${casename})
        # Check if the diff target has been created before
        if(NOT TARGET ${diff_target_name})
            add_custom_target(${diff_target_name}
                COMMAND
                diff
                ${working_DIR}/expected/${casename}.out
                ${working_DIR}/results/${casename}.out || exit 0
                COMMAND
                echo ${working_DIR}/expected/${casename}.out
                COMMAND
                echo ${working_DIR}/results/${casename}.out
                )
        endif()
    endforeach()
endfunction()


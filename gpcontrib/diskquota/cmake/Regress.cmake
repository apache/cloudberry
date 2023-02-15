# CMake module for create regress test target.
#
# Usage:
# RegressTarget_Add(<name>
#   SQL_DIR <sql_dir> [<sql_dir_2> ...]
#   EXPECTED_DIR <expected_dir> [<expected_dir_2> ...]
#   RESULTS_DIR <results_dir>
#   [INIT_FILE <init_file_1> <init_file_2> ...]
#   [SCHEDULE_FILE <schedule_file_1> <schedule_file_2> ...]
#   [REGRESS <test1> <test2> ...]
#   [EXCLUDE <test1> <test2> ...]
#   [REGRESS_OPTS <opt1> <opt2> ...]
#   [REGRESS_TYPE isolation2/regress]
#   [RUN_TIMES <times>]
#   [EXCLUDE_FAULT_INJECT_TEST <ON/OFF>]
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
# NOTE: If the input sql file extension is ".in.sql" instead of ".sql", the "@VAR@" in the input
# file will be replaced by the corresponding cmake VAR before tests are executed.
#
# NOTE: The directory that comes later in the SQL_DIR/EXPECTED_DIR list has a higher priory. The
# test case with the same name will be overwritten by the case that comes after in the directory
# list.t
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

# Find all tests in the given directory which uses fault injector, and add them to
# fault_injector_test_list.
function(_Find_FaultInjector_Tests sql_DIR)
    if (NOT fault_injector_test_list)
        set(fault_injector_test_list "" PARENT_SCOPE)
    endif()
    set(test_list ${fault_injector_test_list})

    get_filename_component(sql_DIR ${sql_DIR} ABSOLUTE)
    file(GLOB files "${sql_DIR}/*.sql")
    foreach(f ${files})
        set(ret 1)
        execute_process(
            COMMAND
            grep gp_inject_fault ${f}
            OUTPUT_QUIET
            RESULT_VARIABLE ret)
        if(ret EQUAL 0)
            get_filename_component(test_name ${f} NAME_WE)
            if (NOT test_name IN_LIST test_list)
                list(APPEND test_list ${test_name})
            endif()
        endif()
    endforeach()

    set(fault_injector_test_list ${test_list} PARENT_SCOPE)
endfunction()

# Create symbolic links in the binary dir to input SQL files.
function(_Link_Test_Files src_DIR dest_DIR suffix)
    get_filename_component(src_DIR ${src_DIR} ABSOLUTE)
    file(MAKE_DIRECTORY ${dest_DIR})
    file(GLOB files "${src_DIR}/*.${suffix}")
    foreach(f ${files})
        get_filename_component(file_name ${f} NAME)
        file(CREATE_LINK ${f} ${dest_DIR}/${file_name} SYMBOLIC)
    endforeach()
    file(GLOB files "${src_DIR}/*.in.${suffix}")
    foreach(f ${files})
        get_filename_component(file_name ${f} NAME_WE)
        configure_file(${f} ${dest_DIR}/${file_name}.${suffix})
    endforeach()
endfunction()

function(RegressTarget_Add name)
    cmake_parse_arguments(
        arg
        ""
        "RESULTS_DIR;DATA_DIR;REGRESS_TYPE;RUN_TIMES;EXCLUDE_FAULT_INJECT_TEST"
        "SQL_DIR;EXPECTED_DIR;REGRESS;EXCLUDE;REGRESS_OPTS;INIT_FILE;SCHEDULE_FILE"
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

    # Link input sql files to the build dir
    foreach(sql_DIR IN LISTS arg_SQL_DIR)
        _Link_Test_Files(${sql_DIR} ${working_DIR}/sql sql)
        # Find all tests using fault injector
        if(arg_EXCLUDE_FAULT_INJECT_TEST)
            _Find_FaultInjector_Tests(${sql_DIR})
        endif()
    endforeach()

    # Link output out files to the build dir
    foreach(expected_DIR IN LISTS arg_EXPECTED_DIR)
        _Link_Test_Files(${expected_DIR} ${working_DIR}/expected out)
    endforeach()

    # Set REGRESS test cases
    foreach(r IN LISTS arg_REGRESS)
        if (arg_EXCLUDE_FAULT_INJECT_TEST AND (r IN_LIST fault_injector_test_list))
            continue()
        endif()
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
    if(arg_EXCLUDE_FAULT_INJECT_TEST)
        list(APPEND to_exclude ${fault_injector_test_list})
    endif()
    if (to_exclude)
        set(exclude_arg "--exclude-tests=${to_exclude}")
        string(REPLACE ";" "," exclude_arg "${exclude_arg}")
        set(regress_opts_arg ${regress_opts_arg} ${exclude_arg})
    endif()
    foreach(o IN LISTS arg_REGRESS_OPTS)
        # If the fault injection tests are excluded, ignore the --load-extension=gp_inject_fault as
        # well.
        if (arg_EXCLUDE_FAULT_INJECT_TEST AND (o MATCHES ".*inject_fault"))
            continue()
        endif()
        set(regress_opts_arg ${regress_opts_arg} ${o})
    endforeach()

    get_filename_component(results_DIR ${arg_RESULTS_DIR} ABSOLUTE)
    if (arg_DATA_DIR)
        get_filename_component(data_DIR ${arg_DATA_DIR} ABSOLUTE)
        set(ln_data_dir_CMD ln -s ${data_DIR} data)
    endif()

    set(regress_command
        ${regress_BIN}  ${regress_opts_arg}  ${regress_arg})
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


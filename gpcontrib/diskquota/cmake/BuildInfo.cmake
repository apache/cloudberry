# Create a build info file based on the given cmake variables
# For example:
# BuildInfo_Create(
# ${CMAKE_CURRENT_BINARY_DIR}/build-info
# VARS
# DISKQUOTA_GIT_HASH
# GP_MAJOR_VERSION)
# )
# will create a build info file:
# ❯ cat build-info
# DISKQUOTA_GIT_HASH = 151ed92
# GP_MAJOR_VERSION = 6

function(BuildInfo_Create path)
  cmake_parse_arguments(
    arg
    ""
    ""
    "VARS"
    ${ARGN})

  # Set REGRESS test cases
  foreach(key IN LISTS arg_VARS)
    get_property(val VARIABLE PROPERTY ${key})
    list(APPEND info_list "${key} = ${val}")
  endforeach()
  file(WRITE ${path} "")
  foreach(content IN LISTS info_list)
    file(APPEND ${path} "${content}\n")
  endforeach()
endfunction()


# get git hash
macro(GitHash_Get _git_hash)
  find_package(Git)
  execute_process(
    COMMAND ${GIT_EXECUTABLE} log -1 --pretty=format:%h
    OUTPUT_VARIABLE ${_git_hash}
    OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endmacro()

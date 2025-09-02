#!/bin/bash
# --------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership.  The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.
#
# --------------------------------------------------------------------
#
# Library: cloudberry-utils.sh
# Description: Common utility functions for Apache Cloudberry build
# and test scripts
#
# Required Environment Variables:
#   SRC_DIR - Root source directory
#
# Optional Environment Variables:
#   LOG_DIR - Directory for logs (defaults to ${SRC_DIR}/build-logs)
#
# Functions:
#   init_environment "Script Name" "Log File"
#     - Initialize logging and verify environment
#     - Parameters:
#       * script_name: Name of the calling script
#       * log_file: Path to log file
#     - Returns: 0 on success, 1 on failure
#
#   execute_cmd command [args...]
#     - Execute command with logging
#     - Parameters: Command and its arguments
#     - Returns: Command's exit code
#
#   run_psql_cmd "sql_command"
#     - Execute PostgreSQL command with logging
#     - Parameters: SQL command string
#     - Returns: psql command's exit code
#
#   source_cloudberry_env
#     - Source Cloudberry environment files
#     - Returns: 0 on success
#
#   log_section "section_name"
#     - Log section start
#     - Parameters: Name of the section
#
#   log_section_end "section_name"
#     - Log section end
#     - Parameters: Name of the section
#
#   log_completion "script_name" "log_file"
#     - Log script completion
#     - Parameters:
#       * script_name: Name of the calling script
#       * log_file: Path to log file
#
# Usage:
#   source ./cloudberry-utils.sh
#
# Example:
#   source ./cloudberry-utils.sh
#   init_environment "My Script" "${LOG_FILE}"
#   execute_cmd make clean
#   log_section "Build Process"
#   execute_cmd make -j$(nproc)
#   log_section_end "Build Process"
#   log_completion "My Script" "${LOG_FILE}"
#
# --------------------------------------------------------------------

DEFAULT_BUILD_DESTINATION=/usr/local/cloudberry-db

# Initialize logging and environment
init_environment() {
    local script_name=$1
    local log_file=$2

    init_build_destination_var

    echo "=== Initializing environment for ${script_name} ==="
    echo "${script_name} executed at $(date)" | tee -a "${log_file}"
    echo "Whoami: $(whoami)" | tee -a "${log_file}"
    echo "Hostname: $(hostname)" | tee -a "${log_file}"
    echo "Working directory: $(pwd)" | tee -a "${log_file}"
    echo "Source directory: ${SRC_DIR}" | tee -a "${log_file}"
    echo "Log directory: ${LOG_DIR}" | tee -a "${log_file}"
    echo "Build destination: ${BUILD_DESTINATION}" | tee -a "${log_file}"

    if [ -z "${SRC_DIR:-}" ]; then
        echo "Error: SRC_DIR environment variable is not set" | tee -a "${log_file}"
        exit 1
    fi

    mkdir -p "${LOG_DIR}"
}

# Function to echo and execute command with logging
execute_cmd() {
    local cmd_str="$*"
    local timestamp=$(date "+%Y.%m.%d-%H.%M.%S")
    echo "Executing at ${timestamp}: $cmd_str" | tee -a "${LOG_DIR}/commands.log"
    "$@" 2>&1 | tee -a "${LOG_DIR}/commands.log"
    return ${PIPESTATUS[0]}
}

# Function to run psql commands with logging
run_psql_cmd() {
    local cmd=$1
    local timestamp=$(date "+%Y.%m.%d-%H.%M.%S")
    echo "Executing psql at ${timestamp}: $cmd" | tee -a "${LOG_DIR}/psql-commands.log"
    psql -P pager=off template1 -c "$cmd" 2>&1 | tee -a "${LOG_DIR}/psql-commands.log"
    return ${PIPESTATUS[0]}
}

# Function to source Cloudberry environment
source_cloudberry_env() {
    echo "=== Sourcing Cloudberry environment ===" | tee -a "${LOG_DIR}/environment.log"
    source ${BUILD_DESTINATION}/cloudberry-env.sh
    source ${SRC_DIR}/../cloudberry/gpAux/gpdemo/gpdemo-env.sh
}

# Function to log section start
log_section() {
    local section_name=$1
    local timestamp=$(date "+%Y.%m.%d-%H.%M.%S")
    echo "=== ${section_name} started at ${timestamp} ===" | tee -a "${LOG_DIR}/sections.log"
}

# Function to log section end
log_section_end() {
    local section_name=$1
    local timestamp=$(date "+%Y.%m.%d-%H.%M.%S")
    echo "=== ${section_name} completed at ${timestamp} ===" | tee -a "${LOG_DIR}/sections.log"
}

# Function to log script completion
log_completion() {
    local script_name=$1
    local log_file=$2
    local timestamp=$(date "+%Y.%m.%d-%H.%M.%S")
    echo "${script_name} execution completed successfully at ${timestamp}" | tee -a "${log_file}"
}

# Function to get OS identifier
detect_os() {
  if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS_ID=$ID
    OS_VERSION=$VERSION_ID
  else
    echo "Unsupported system: cannot detect OS" >&2
    exit 99
  fi
}

# Init BUILD_DESTINATION default value if not set
init_build_destination_var() {

    if [ -z ${BUILD_DESTINATION+x} ]; then 
      export BUILD_DESTINATION=${DEFAULT_BUILD_DESTINATION}
      exec "$@"
    fi

}

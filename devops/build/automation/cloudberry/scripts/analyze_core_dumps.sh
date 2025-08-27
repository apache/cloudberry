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
# Script: analyze_core_dumps.sh
# Description: Automated analysis tool for core dump files using GDB
#
# This script automatically analyzes core dump files found in a
# specified directory, providing stack traces and register
# information. It's particularly useful for analyzing crashes in
# Postgres/Cloudberry executables and Python applications.
#
# Features:
# - Automatic detection of core dump files
# - Support for both compiled executables and interpreted languages
# - Detailed stack traces with GDB
# - Register state analysis
# - Assembly code context at crash point
# - Comprehensive logging of analysis results
#
# Usage: analyze_core_dumps.sh [test_id]
#   test_id: Optional identifier for the test configuration that generated cores
#
# Dependencies:
#   - GDB (GNU Debugger)
#   - file command
#
# Environment Variables:
#   SRC_DIR - Base directory for operations (defaults to /tmp)
#
# Return Codes:
#   0 - No core files were found
#   1 - Core files were found and all were processed successfully
#   2 - Error conditions:
#       - Missing required dependencies (gdb, file)
#       - Issues processing some or all core files
# --------------------------------------------------------------------

set -u

# Configuration
#-----------------------------------------------------------------------------
# Use SRC_DIR if defined, otherwise default to /tmp
SRC_DIR="${SRC_DIR:-/tmp}"
# Define log directory and files
LOG_DIR="${SRC_DIR}/build-logs"
# Create log directories if they don't exist
mkdir -p "${LOG_DIR}"

# Determine log file name based on test_id argument
if [ $# -ge 1 ]; then
    test_id="$1"
    log_file="${LOG_DIR}/core_analysis_${test_id}_$(date +%Y%m%d_%H%M%S).log"
else
    log_file="${LOG_DIR}/core_analysis_$(date +%Y%m%d_%H%M%S).log"
fi
echo "log_file: ${log_file}"

# Directory where core dumps are located
core_dir="/tmp/cloudberry-cores/"

# Pattern to match core dump files
core_pattern="core-*"

# Function Definitions
#-----------------------------------------------------------------------------
# Log messages to both console and log file
# Args:
#   $1 - Message to log
log_message() {
    local message="[$(date '+%Y-%m-%d %H:%M:%S')] $1"
    echo "$message"
    echo "$message" >> "$log_file"
}

# Analyze a single core file
# Args:
#   $1 - Path to core file
# Returns:
#   0 on success, 1 on failure
analyze_core_file() {
    local core_file="$1"
    local file_info

    log_message "Analyzing core file: $core_file"
    file_info=$(file "$core_file")
    log_message "Core file info: $file_info"

    # Extract the original command from the core file info
    if [[ "$file_info" =~ "from '([^']+)'" ]]; then
        local original_cmd="${BASH_REMATCH[1]}"
        log_message "Original command: $original_cmd"
    fi

    # Extract executable path from core file info
    if [[ "$file_info" =~ execfn:\ \'([^\']+)\' ]]; then
        local executable="${BASH_REMATCH[1]}"
        log_message "Executable path: $executable"

        # Convert relative path to absolute if needed
        if [[ "$executable" == "./"* ]]; then
            executable="$PWD/${executable:2}"
            log_message "Converted to absolute path: $executable"
        fi

        # Run GDB analysis
        log_message "Starting GDB analysis..."

        gdb -quiet \
            --batch \
            -ex 'set pagination off' \
            -ex 'info target' \
            -ex 'thread apply all bt' \
            -ex 'print $_siginfo' \
            -ex quit \
            "$executable" "$core_file" 2>&1 >> "$log_file"

        local gdb_rc=$?
        if [ $gdb_rc -eq 0 ] && [ -s "$log_file" ]; then
            log_message "GDB analysis completed successfully"
            return 0
        else
            log_message "Warning: GDB analysis failed or produced no output"
            return 1
        fi
    else
        log_message "Could not find executable path in core file"
        return 1
    fi
}

# Function to check required commands
check_dependencies() {
    local missing=0
    local required_commands=("gdb" "file")

    log_message "Checking required commands..."
    for cmd in "${required_commands[@]}"; do
        if ! command -v "$cmd" >/dev/null 2>&1; then
            log_message "Error: Required command '$cmd' not found"
            missing=1
        fi
    done

    if [ $missing -eq 1 ]; then
        log_message "Missing required dependencies. Please install them and try again."
        return 1
    fi

    log_message "All required commands found"
    return 0
}

# Main Execution
#-----------------------------------------------------------------------------
main() {
    local core_count=0
    local analyzed_count=0
    local return_code=0

    log_message "Starting core dump analysis"
    log_message "Using source directory: $SRC_DIR"
    log_message "Using log directory: $LOG_DIR"

    # Check dependencies first
    if ! check_dependencies; then
        return 2
    fi

    # Process all core files
    for core_file in "$core_dir"/$core_pattern; do
        if [[ -f "$core_file" ]]; then
            ((core_count++))
            if analyze_core_file "$core_file"; then
                ((analyzed_count++))
            fi
        fi
    done

    # Determine return code based on results
    if ((core_count == 0)); then
        log_message "No core files found matching pattern $core_pattern in $core_dir"
        return_code=0  # No cores found
    elif ((analyzed_count == core_count)); then
        log_message "Analysis complete. Successfully processed $analyzed_count core(s) files"
        return_code=1  # All cores processed successfully
    else
        log_message "Analysis complete with errors. Processed $analyzed_count of $core_count core files"
        return_code=2  # Some cores failed to process
    fi

    log_message "Log file: $log_file"

    return $return_code
}

# Script entry point
main
return_code=$?

if ((return_code == 0)); then
    rm -fv "${log_file}"
fi

exit $return_code

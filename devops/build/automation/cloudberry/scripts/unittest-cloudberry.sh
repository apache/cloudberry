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
# Script: unittest-cloudberry.sh
# Description: Executes unit tests for Apache Cloudberry from source
#             code.  Runs the 'unittest-check' make target and logs
#             results.  Tests are executed against the compiled source
#             without requiring a full installation.
#
# Required Environment Variables:
#   SRC_DIR - Root source directory
#
# Optional Environment Variables:
#   LOG_DIR - Directory for logs (defaults to ${SRC_DIR}/build-logs)
#
# Usage:
#   ./unittest-cloudberry.sh
#
# Exit Codes:
#   0 - All unit tests passed successfully
#   1 - Environment setup failed (missing SRC_DIR, LOG_DIR creation failed)
#   2 - Unit test execution failed
#
# --------------------------------------------------------------------

set -euo pipefail

# Source common utilities
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SCRIPT_DIR}/cloudberry-utils.sh"

# Define log directory and files
export LOG_DIR="${SRC_DIR}/build-logs"
UNITTEST_LOG="${LOG_DIR}/unittest.log"

# Initialize environment
init_environment "Cloudberry Unittest Script" "${UNITTEST_LOG}"

# Set environment
log_section "Environment Setup"
export LD_LIBRARY_PATH=${BUILD_DESTINATION}/lib:LD_LIBRARY_PATH
log_section_end "Environment Setup"

# Unittest process
log_section "Unittest Process"
execute_cmd make --directory ${SRC_DIR}/../cloudberry unittest-check || exit 2
log_section_end "Unittest Process"

# Log completion
log_completion "Cloudberry Unittest Script" "${UNITTEST_LOG}"
exit 0

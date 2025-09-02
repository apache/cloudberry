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
# Script: build-cloudberry.sh
# Description: Builds Apache Cloudberry from source code and installs
# it.
#             Performs the following steps:
#             1. Builds main Apache Cloudberry database components
#             2. Builds contrib modules
#             3. Installs both main and contrib components
#             Uses parallel compilation based on available CPU cores.
#
# Required Environment Variables:
#   SRC_DIR - Root source directory containing Apache Cloudberry
#   source code
#
# Optional Environment Variables:
#   LOG_DIR - Directory for logs (defaults to ${SRC_DIR}/build-logs)
#   NPROC   - Number of parallel jobs (defaults to all available cores)
#
# Usage:
#   Export required variables:
#     export SRC_DIR=/path/to/cloudberry/source
#   Then run:
#     ./build-cloudberry.sh
#
# Prerequisites:
#   - configure-cloudberry.sh must be run first
#   - Required build dependencies must be installed
#   - ${BUILD_DESTINATION}/lib (by default /usr/local/cloudberry-db/lib) must exist and be writable
#
# Exit Codes:
#   0 - Build and installation completed successfully
#   1 - Environment setup failed (missing SRC_DIR, LOG_DIR creation failed)
#   2 - Main component build failed
#   3 - Contrib build failed
#   4 - Installation failed
#
# --------------------------------------------------------------------

set -euo pipefail

# Source common utilities
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SCRIPT_DIR}/cloudberry-utils.sh"

# Define log directory and files
export LOG_DIR="${SRC_DIR}/build-logs"
BUILD_LOG="${LOG_DIR}/build.log"

# Initialize environment
init_environment "Cloudberry Build Script" "${BUILD_LOG}"

# Set environment
log_section "Environment Setup"
export LD_LIBRARY_PATH=${BUILD_DESTINATION}/lib:LD_LIBRARY_PATH
log_section_end "Environment Setup"

# Build process
log_section "Build Process"
execute_cmd make -j$(nproc) --directory ${SRC_DIR} || exit 2
execute_cmd make -j$(nproc) --directory ${SRC_DIR}/contrib || exit 3
log_section_end "Build Process"

# Installation
log_section "Installation"
execute_cmd make install --directory ${SRC_DIR} || exit 4
execute_cmd make install --directory ${SRC_DIR}/contrib || exit 4
log_section_end "Installation"

# Log completion
log_completion "Cloudberry Build Script" "${BUILD_LOG}"
exit 0

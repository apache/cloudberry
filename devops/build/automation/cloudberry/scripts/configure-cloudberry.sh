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
# Script: configure-cloudberry.sh
# Description: Configures Apache Cloudberry build environment and runs
#             ./configure with optimized settings. Performs the
#             following:
#             1. Prepares ${BUILD_DESTINATION} (by default /usr/local/cloudberry-db) directory
#             2. Sets up library dependencies
#             3. Configures build with required features enabled
#
# Configuration Features:
#   - Cloud Storage Integration (gpcloud)
#   - IC Proxy Support
#   - MapReduce Processing
#   - Oracle Compatibility (orafce)
#   - ORCA Query Optimizer
#   - PAX Access Method
#   - PXF External Table Access
#   - Test Automation Support (tap-tests)
#
# System Integration:
#   - GSSAPI Authentication
#   - LDAP Authentication
#   - XML Processing
#   - LZ4 Compression
#   - OpenSSL Support
#   - PAM Authentication
#   - Perl Support
#   - Python Support
#
# Required Environment Variables:
#   SRC_DIR - Root source directory
#   BUILD_DESTINATION - Directory to build binaries
#
# Optional Environment Variables:
#   LOG_DIR - Directory for logs (defaults to ${SRC_DIR}/build-logs)
#   ENABLE_DEBUG - Enable debug build options (true/false, defaults to
#                  false)
#
#                 When true, enables:
#                   --enable-debug
#                   --enable-profiling
#                   --enable-cassert
#                   --enable-debug-extensions
#
# Prerequisites:
#   - System dependencies must be installed:
#     * xerces-c development files
#     * OpenSSL development files
#     * Python development files
#     * Perl development files
#     * LDAP development files
#   - /usr/local must be writable
#   - User must have sudo privileges
#
# Usage:
#   Export required variables:
#     export SRC_DIR=/path/to/cloudberry/source
#   Then run:
#     ./configure-cloudberry.sh
#
# Exit Codes:
#   0 - Configuration completed successfully
#   1 - Environment setup failed
#   2 - Directory preparation failed
#   3 - Library setup failed
#   4 - Configure command failed
#
# --------------------------------------------------------------------

set -euo pipefail

# Source common utilities
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SCRIPT_DIR}/cloudberry-utils.sh"

# Call it before conditional logic
detect_os
echo "Detected OS: $OS_ID $OS_VERSION"

# Define log directory and files
export LOG_DIR="${SRC_DIR}/build-logs"
CONFIGURE_LOG="${LOG_DIR}/configure.log"

# Initialize environment
init_environment "Cloudberry Configure Script" "${CONFIGURE_LOG}"

# Check if BUILD_DESTINATION is set
if [ -z "${BUILD_DESTINATION}" ]; then
  log_completion "BUILD_DESTINATION is empty - error with exit"
  exit 1
fi

# Initial setup
log_section "Initial Setup"
execute_cmd sudo rm -rf ${BUILD_DESTINATION} || exit 2
execute_cmd sudo chmod a+w /usr/local || exit 2
execute_cmd sudo mkdir -p ${BUILD_DESTINATION}/lib || exit 2
if [[ "$OS_ID" == "rocky" && "$OS_VERSION" =~ ^(8|9) ]]; then
    execute_cmd sudo cp /usr/local/xerces-c/lib/libxerces-c.so \
                /usr/local/xerces-c/lib/libxerces-c-3.3.so \
                ${BUILD_DESTINATION}/lib || exit 3
fi
execute_cmd sudo chown -R gpadmin:gpadmin ${BUILD_DESTINATION} || exit 2
log_section_end "Initial Setup"

# Set environment
log_section "Environment Setup"
export LD_LIBRARY_PATH=${BUILD_DESTINATION}/lib:LD_LIBRARY_PATH
log_section_end "Environment Setup"

# Add debug options if ENABLE_DEBUG is set to "true"
CONFIGURE_DEBUG_OPTS=""

if [ "${ENABLE_DEBUG:-false}" = "true" ]; then
    CONFIGURE_DEBUG_OPTS="--enable-debug \
                          --enable-profiling \
                          --enable-cassert \
                          --enable-debug-extensions"
fi

# Configure build
log_section "Configure"
execute_cmd ./configure --prefix=${BUILD_DESTINATION} \
            --disable-external-fts \
            --enable-gpcloud \
            --enable-ic-proxy \
            --enable-mapreduce \
            --enable-orafce \
            --enable-orca \
            --enable-pax \
            --enable-pxf \
            --enable-tap-tests \
            ${CONFIGURE_DEBUG_OPTS} \
            --with-gssapi \
            --with-ldap \
            --with-libxml \
            --with-lz4 \
            --with-openssl \
            --with-pam \
            --with-perl \
            --with-pgport=5432 \
            --with-python \
            --with-pythonsrc-ext \
            --with-ssl=openssl \
            --with-openssl \
            --with-uuid=e2fs \
            --with-includes=/usr/local/xerces-c/include \
            --with-libraries=${BUILD_DESTINATION}/lib || exit 4
log_section_end "Configure"

# Capture version information
log_section "Version Information"
execute_cmd ag "GP_VERSION | GP_VERSION_NUM | PG_VERSION | PG_VERSION_NUM | PG_VERSION_STR" src/include/pg_config.h
log_section_end "Version Information"

# Log completion
log_completion "Cloudberry Configure Script" "${CONFIGURE_LOG}"
exit 0

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
# Script: destroy-cloudberry-demo-cluster.sh
# Description: Destroys and cleans up a demo Apache Cloudberry
# cluster.
#             Performs the following steps:
#             1. Sources required environment variables
#             2. Stops any running cluster processes
#             3. Removes cluster data directories and configuration
#             4. Cleans up any remaining cluster resources
#
# Required Environment Variables:
#   SRC_DIR - Root source directory
#
# Optional Environment Variables:
#   LOG_DIR - Directory for logs (defaults to ${SRC_DIR}/build-logs)
#
# Prerequisites:
#   - Apache Cloudberry environment must be available
#   - User must have permissions to remove cluster directories
#   - No active connections to the cluster
#
# Usage:
#   Export required variables:
#     export SRC_DIR=/path/to/cloudberry/source
#   Then run:
#     ./destroy-cloudberry-demo-cluster.sh
#
# Exit Codes:
#   0 - Cluster destroyed successfully
#   1 - Environment setup/sourcing failed
#   2 - Cluster destruction failed
#
# Related Scripts:
#   - create-cloudberry-demo-cluster.sh: Creates a new demo cluster
#
# Notes:
#   - This script will forcefully terminate all cluster processes
#   - All cluster data will be permanently deleted
#   - Make sure to backup any important data before running
#
# --------------------------------------------------------------------

set -euo pipefail

# Source common utilities
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SCRIPT_DIR}/cloudberry-utils.sh"

# Define log directory
export LOG_DIR="${SRC_DIR}/build-logs"
CLUSTER_LOG="${LOG_DIR}/destroy-cluster.log"

# Initialize environment
init_environment "Destroy Cloudberry Demo Cluster Script" "${CLUSTER_LOG}"

# Source Cloudberry environment
log_section "Environment Setup"
source_cloudberry_env || {
    echo "Failed to source Cloudberry environment" | tee -a "${CLUSTER_LOG}"
    exit 1
}
log_section_end "Environment Setup"

# Destroy demo cluster
log_section "Destroy Demo Cluster"
execute_cmd make destroy-demo-cluster --directory ${SRC_DIR}/../cloudberry || {
    echo "Failed to destroy demo cluster" | tee -a "${CLUSTER_LOG}"
    exit 2
}
log_section_end "Destroy Demo Cluster"

# Verify cleanup
log_section "Cleanup Verification"
if [ -d "${SRC_DIR}/../cloudberry/gpAux/gpdemo/data" ]; then
    echo "Warning: Data directory still exists after cleanup" | tee -a "${CLUSTER_LOG}"
fi
log_section_end "Cleanup Verification"

# Log completion
log_completion "Destroy Cloudberry Demo Cluster Script" "${CLUSTER_LOG}"
exit 0

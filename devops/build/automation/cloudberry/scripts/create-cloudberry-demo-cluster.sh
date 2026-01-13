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
# Script: create-cloudberry-demo-cluster.sh
# Description: Creates and configures a demo Apache Cloudbery cluster.
#             Performs the following steps:
#             1. Sets up required environment variables
#             2. Verifies SSH connectivity
#             3. Creates demo cluster using make
#             4. Initializes and starts the cluster
#             5. Performs comprehensive verification checks
#
# Required Environment Variables:
#   SRC_DIR - Root source directory
#
# Optional Environment Variables:
#   LOG_DIR - Directory for logs (defaults to ${SRC_DIR}/build-logs)
#
# Prerequisites:
#   - Apache Cloudberry must be installed (default location is /usr/local/cloudberry-db)
#   - SSH must be configured for passwordless access to localhost
#   - User must have permissions to create cluster directories
#   - PostgreSQL client tools (psql) must be available
#
# Usage:
#   Export required variables:
#     export SRC_DIR=/path/to/cloudberry/source
#   Then run:
#     ./create-cloudberry-demo-cluster.sh
#
# Verification Checks:
#   - Apache Cloudberry version
#   - Segment configuration
#   - Available extensions
#   - Active sessions
#   - Configuration history
#   - Replication status
#
# Exit Codes:
#   0 - Cluster created and verified successfully
#   1 - Environment setup failed
#   2 - SSH verification failed
#   3 - Cluster creation failed
#   4 - Cluster startup failed
#   5 - Verification checks failed
#
# --------------------------------------------------------------------

set -euo pipefail

# Source common utilities
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SCRIPT_DIR}/cloudberry-utils.sh"

# Define log directory
export LOG_DIR="${SRC_DIR}/build-logs"
CLUSTER_LOG="${LOG_DIR}/cluster.log"

# Initialize environment
init_environment "Cloudberry Demo Cluster Script" "${CLUSTER_LOG}"

# Setup environment
log_section "Environment Setup"
source ${BUILD_DESTINATION}/cloudberry-env.sh || exit 1
log_section_end "Environment Setup"

# Verify SSH access
log_section "SSH Verification"
execute_cmd ssh $(hostname) 'whoami; hostname' || exit 2
log_section_end "SSH Verification"

# Create demo cluster
log_section "Demo Cluster Creation"
execute_cmd make create-demo-cluster --directory ${SRC_DIR}/../cloudberry || exit 3
log_section_end "Demo Cluster Creation"

# Source demo environment
log_section "Source Environment"
source ${SRC_DIR}/../cloudberry/gpAux/gpdemo/gpdemo-env.sh || exit 1
log_section_end "Source Environment"

# Manage cluster state
log_section "Cluster Management"
execute_cmd gpstop -a || exit 4
execute_cmd gpstart -a || exit 4
execute_cmd gpstate || exit 4
log_section_end "Cluster Management"

# Verify installation
log_section "Installation Verification"
verification_failed=false
run_psql_cmd "SELECT version()" || verification_failed=true
run_psql_cmd "SELECT * from gp_segment_configuration" || verification_failed=true
run_psql_cmd "SELECT * FROM pg_available_extensions" || verification_failed=true
run_psql_cmd "SELECT * from pg_stat_activity" || verification_failed=true
run_psql_cmd "SELECT * FROM gp_configuration_history" || verification_failed=true
run_psql_cmd "SELECT * FROM gp_stat_replication" || verification_failed=true

if [ "$verification_failed" = true ]; then
    echo "One or more verification checks failed" | tee -a "${CLUSTER_LOG}"
    exit 5
fi
log_section_end "Installation Verification"

# Log completion
log_completion "Cloudberry Demo Cluster Script" "${CLUSTER_LOG}"
exit 0

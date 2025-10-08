#!/usr/bin/env bash
# --------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership. The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# --------------------------------------------------------------------
set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

# Cleanup function
cleanup_on_exit() {
    local exit_code=$?
    if [ $exit_code -ne 0 ]; then
        log_error "Script failed with exit code $exit_code"
        log_info "Cleaning up any partial deployments..."
        # Add cleanup logic here if needed
    fi
    exit $exit_code
}

# Set trap for cleanup
trap cleanup_on_exit EXIT

# Default values
DEFAULT_OS_VERSION="rockylinux9"
DEFAULT_TIMEZONE_VAR="America/Los_Angeles"
DEFAULT_PIP_INDEX_URL_VAR="https://pypi.org/simple"
BUILD_ONLY="false"
MULTINODE="false"

# Use environment variables if set, otherwise use default values
# Export set for some variables to be used referenced docker compose file
export OS_VERSION="${OS_VERSION:-$DEFAULT_OS_VERSION}"
BUILD_ONLY="${BUILD_ONLY:-false}"
export CODEBASE_VERSION="${CODEBASE_VERSION:-}"
TIMEZONE_VAR="${TIMEZONE_VAR:-$DEFAULT_TIMEZONE_VAR}"
PIP_INDEX_URL_VAR="${PIP_INDEX_URL_VAR:-$DEFAULT_PIP_INDEX_URL_VAR}"

# Function to display help message
function usage() {
    echo "Usage: $0 [-o <os_version>] [-c <codebase_version>] [-b] [-m]"
    echo "  -c  Codebase version (valid values: main, or other available version like 2.0.0)"
    echo "  -t  Timezone (default: America/Los_Angeles, or set via TIMEZONE_VAR environment variable)"
    echo "  -p  Python Package Index (PyPI) (default: https://pypi.org/simple, or set via PIP_INDEX_URL_VAR environment variable)"
    echo "  -b  Build only, do not run the container (default: false, or set via BUILD_ONLY environment variable)"
    echo "  -m  Multinode, this creates a multinode (multi-container) Cloudberry cluster using docker compose (requires compose to be installed)"
    exit 1
}

# Parse command-line options
while getopts "c:t:p:bmh" opt; do
    case "${opt}" in
        c)
            CODEBASE_VERSION=${OPTARG}
            ;;
        t)
            TIMEZONE_VAR=${OPTARG}
            ;;
        p)
            PIP_INDEX_URL_VAR=${OPTARG}
            ;;
        b)
            BUILD_ONLY="true"
            ;;
        m)
            MULTINODE="true"
            ;;
        h)
            usage
            ;;
        *)
            usage
            ;;
    esac
done

if [[ $MULTINODE == "true" ]] && ! docker compose version; then
        echo "Error: Multinode -m flag found in run arguments but calling docker compose failed. Please install Docker Compose by following the instructions at https://docs.docker.com/compose/install/. Exiting"
        exit 1
fi

if [[ "${MULTINODE}" == "true" && "${BUILD_ONLY}" == "true" ]]; then
    echo "Error: Cannot pass both multinode deployment [m] and build only [b] flags together"
    exit 1
fi

# CODEBASE_VERSION must be specified via -c argument or CODEBASE_VERSION environment variable
if [[ -z "$CODEBASE_VERSION" ]]; then
    echo "Error: CODEBASE_VERSION must be specified via environment variable or '-c' command line parameter."
    usage
fi

# Validate OS_VERSION and map to appropriate Docker image
case "${OS_VERSION}" in
    rockylinux9)
        OS_DOCKER_IMAGE="rockylinux9"
        ;;
    *)
        echo "Invalid OS version: ${OS_VERSION}"
        usage
        ;;
esac

# Validate CODEBASE_VERSION
if [[ "${CODEBASE_VERSION}" != "main" && ! "${CODEBASE_VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Invalid codebase version: ${CODEBASE_VERSION}"
    usage
fi

# Build image
if [[ "${CODEBASE_VERSION}" = "main"  ]]; then
    DOCKERFILE=Dockerfile.${CODEBASE_VERSION}.${OS_VERSION}

    # Single image build
    docker build --file ${DOCKERFILE} \
                 --build-arg TIMEZONE_VAR="${TIMEZONE_VAR}" \
                 --tag cbdb-${CODEBASE_VERSION}:${OS_VERSION} .

    # Prepare shared cluster-ssh volume for multinode (keys live in /opt/cbdb/cluster-ssh inside the volume)
    if [[ "${MULTINODE}" == "true" ]]; then
        TMP_CLUSTER_SSH_DIR="$(mktemp -d)"
        ssh-keygen -t rsa -b 4096 -N "" -f "${TMP_CLUSTER_SSH_DIR}/id_rsa" >/dev/null 2>&1
        docker volume create cbdb-cluster-ssh >/dev/null 2>&1 || true
        # Populate the volume using a one-off container
        docker run --rm \
          -v cbdb-cluster-ssh:/opt/cbdb/cluster-ssh \
          -v "${TMP_CLUSTER_SSH_DIR}":/tmp/keys:ro \
          cbdb-${CODEBASE_VERSION}:${OS_VERSION} bash -lc 'set -e; sudo mkdir -p /opt/cbdb/cluster-ssh; sudo cp /tmp/keys/id_rsa /opt/cbdb/cluster-ssh/id_rsa; sudo cp /tmp/keys/id_rsa.pub /opt/cbdb/cluster-ssh/id_rsa.pub; sudo cp /tmp/keys/id_rsa.pub /opt/cbdb/cluster-ssh/coordinator.pub; sudo chmod 700 /opt/cbdb/cluster-ssh; sudo chmod 600 /opt/cbdb/cluster-ssh/id_rsa; sudo chmod 644 /opt/cbdb/cluster-ssh/id_rsa.pub /opt/cbdb/cluster-ssh/coordinator.pub'
        rm -rf "${TMP_CLUSTER_SSH_DIR}"
    fi
else
    DOCKERFILE=Dockerfile.RELEASE.${OS_VERSION}

    docker build --file ${DOCKERFILE} \
                 --build-arg TIMEZONE_VAR="${TIMEZONE_VAR}" \
                 --build-arg CODEBASE_VERSION_VAR="${CODEBASE_VERSION}" \
                 --tag cbdb-${CODEBASE_VERSION}:${OS_VERSION} .

    # For release multinode, also prepare shared cluster-ssh volume (same as main)
    if [[ "${MULTINODE}" == "true" ]]; then
        TMP_CLUSTER_SSH_DIR="$(mktemp -d)"
        ssh-keygen -t rsa -b 4096 -N "" -f "${TMP_CLUSTER_SSH_DIR}/id_rsa" >/dev/null 2>&1
        docker volume create cbdb-cluster-ssh >/dev/null 2>&1 || true
        docker run --rm \
          -v cbdb-cluster-ssh:/opt/cbdb/cluster-ssh \
          -v "${TMP_CLUSTER_SSH_DIR}":/tmp/keys:ro \
          cbdb-${CODEBASE_VERSION}:${OS_VERSION} bash -lc 'set -e; sudo mkdir -p /opt/cbdb/cluster-ssh; sudo cp /tmp/keys/id_rsa /opt/cbdb/cluster-ssh/id_rsa; sudo cp /tmp/keys/id_rsa.pub /opt/cbdb/cluster-ssh/id_rsa.pub; sudo cp /tmp/keys/id_rsa.pub /opt/cbdb/cluster-ssh/coordinator.pub; sudo chmod 700 /opt/cbdb/cluster-ssh; sudo chmod 600 /opt/cbdb/cluster-ssh/id_rsa; sudo chmod 644 /opt/cbdb/cluster-ssh/id_rsa.pub /opt/cbdb/cluster-ssh/coordinator.pub'
        rm -rf "${TMP_CLUSTER_SSH_DIR}"
    fi
fi

# Check if build only flag is set
if [ "${BUILD_ONLY}" == "true" ]; then
    echo "Docker image built successfully with OS version ${OS_VERSION} and codebase version ${CODEBASE_VERSION}. Build only mode, not running the container."
    exit 0
fi

# Deploy container(s)
if [ "${MULTINODE}" == "true" ]; then
    docker compose -f docker-compose-$OS_VERSION.yml up --detach
else
    docker run --interactive \
           --tty \
           --name cbdb-cdw \
           --detach \
           --volume /sys/fs/cgroup:/sys/fs/cgroup:ro \
           --hostname cdw \
           --publish 122:22 \
           --publish 15432:5432 \
           cbdb-${CODEBASE_VERSION}:${OS_VERSION}
fi
#!/bin/bash
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
## ======================================================================
## Container initialization script for Apache Cloudberry Sandbox
## ======================================================================

# ----------------------------------------------------------------------
# Start SSH daemon and setup for SSH access
# ----------------------------------------------------------------------
# The SSH daemon is started to allow remote access to the container via
# SSH. This is useful for development and debugging purposes.
# ----------------------------------------------------------------------

# Ensure SSH directory exists (created at build time; ignore errors if any)
mkdir -p /run/sshd 2>/dev/null || true

# Start SSH daemon directly (binary is setuid-root in the image)
if ! /usr/sbin/sshd; then
    echo "Failed to start SSH daemon" >&2
    exit 1
fi

# Give SSH daemon time to start
sleep 5

# ----------------------------------------------------------------------
# Remove /run/nologin to allow logins
# ----------------------------------------------------------------------
# The /run/nologin file, if present, prevents users from logging into
# the system. This file is removed to ensure that users can log in via SSH.
# ----------------------------------------------------------------------
rm -f /run/nologin 2>/dev/null || true

# ----------------------------------------------------------------------
# Configure passwordless SSH access for 'gpadmin' user
# ----------------------------------------------------------------------
# SSH keys are already generated and configured in the Docker image at
# build time. All containers from the same image share the same keypair,
# which allows passwordless SSH between containers.
#
# This is ONLY suitable for sandbox/testing environments.
# DO NOT use this approach in production.
# ----------------------------------------------------------------------

# Verify SSH keys exist (they should be in the image already)
if [ ! -f /home/gpadmin/.ssh/id_rsa ]; then
    echo "ERROR: SSH keys not found in image. This should not happen."
    exit 1
fi

# Add container hostnames to the known_hosts file to avoid SSH warnings
if [[ "${MULTINODE:-false}" == "true" ]]; then
    ssh-keyscan -t rsa cdw scdw sdw1 sdw2 > /home/gpadmin/.ssh/known_hosts 2>/dev/null || true
else
    ssh-keyscan -t rsa cdw > /home/gpadmin/.ssh/known_hosts 2>/dev/null || true
fi
chmod 600 /home/gpadmin/.ssh/known_hosts
chown gpadmin:gpadmin /home/gpadmin/.ssh/known_hosts

# Load Cloudberry/Greenplum environment with fallback, then ensure PATH
if [ -f "/usr/local/cloudberry-db/cloudberry-env.sh" ]; then
  # shellcheck disable=SC1091
  . /usr/local/cloudberry-db/cloudberry-env.sh
elif [ -f "/usr/local/cloudberry-db/greenplum_path.sh" ]; then
  # shellcheck disable=SC1091
  . /usr/local/cloudberry-db/greenplum_path.sh
else
  # Fallback: minimal env to find gp* tools
  export GPHOME="/usr/local/cloudberry-db"
fi
# Ensure coordinator data dir variable is set
export COORDINATOR_DATA_DIRECTORY="${COORDINATOR_DATA_DIRECTORY:-/data0/database/coordinator/gpseg-1}"
# Ensure PATH includes Cloudberry bin
if [ -d "/usr/local/cloudberry-db/bin" ]; then
  case ":$PATH:" in
    *":/usr/local/cloudberry-db/bin:"*) : ;;
    *) export PATH="/usr/local/cloudberry-db/bin:$PATH" ;;
  esac
fi

# Initialize single node Cloudberry cluster
if [[ "${MULTINODE:-false}" == "false" && "$HOSTNAME" == "cdw" ]]; then
    gpinitsystem -a \
                 -c /tmp/gpinitsystem_singlenode \
                 -h /tmp/gpdb-hosts \
                 --max_connections=100
# Initialize multi node Cloudberry cluster
elif [[ "${MULTINODE:-false}" == "true" && "$HOSTNAME" == "cdw" ]]; then
    # Wait for other containers' SSH to become reachable (max 120s per host)
    for host in sdw1 sdw2 scdw; do
        MAX_WAIT=120
        WAITED=0
        until ssh -o StrictHostKeyChecking=no -o PasswordAuthentication=no -o ConnectTimeout=5 gpadmin@${host} "echo Connected to ${host}" 2>/dev/null; do
            if [ $WAITED -ge $MAX_WAIT ]; then
                echo "Timeout waiting for SSH on ${host}"
                exit 1
            fi
            sleep 5
            WAITED=$((WAITED+5))
        done
    done

    # Clean up any existing data directories to avoid conflicts
    rm -rf /data0/database/coordinator/* /data0/database/primary/* /data0/database/mirror/* 2>/dev/null || true

    # Ensure database directories exist with proper permissions
    mkdir -p /data0/database/coordinator /data0/database/primary /data0/database/mirror
    chmod -R 700 /data0/database

    gpinitsystem -a \
                 -c /tmp/gpinitsystem_multinode \
                 -h /tmp/multinode-gpinit-hosts \
                 --max_connections=100
    gpinitstandby -s scdw -a
    printf "sdw1\nsdw2\n" >> /tmp/gpdb-hosts
fi

# ----------------------------------------------------------------------
# Post-initialization configuration (applies to both single and multi-node)
# ----------------------------------------------------------------------
# Configure pg_hba.conf to allow passwordless access from any host,
# remove password requirement for gpadmin user, and display cluster info.
# This section runs on the coordinator node after cluster initialization.
# ----------------------------------------------------------------------
if [ "$HOSTNAME" == "cdw" ]; then
    ## Allow any host access the Cloudberry Cluster
    echo 'host all all 0.0.0.0/0 trust' >> /data0/database/coordinator/gpseg-1/pg_hba.conf
    gpstop -u

    cat <<-'EOF'

======================================================================
	  ____ _                 _ _
	 / ___| | ___  _   _  __| | |__   ___ _ __ _ __ _   _
	| |   | |/ _ \| | | |/ _` | '_ \ / _ \ '__| '__| | | |
	| |___| | (_) | |_| | (_| | |_) |  __/ |  | |  | |_| |
	 \____|_|\___/ \__,_|\__,_|_.__/ \___|_|  |_|   \__, |
	                                                |___/
======================================================================
EOF

    cat <<-'EOF'

======================================================================
Sandbox: Apache Cloudberry Cluster details
======================================================================

EOF

    echo "Current time: $(date)"
    source /etc/os-release
    echo "OS Version: ${NAME} ${VERSION}"

    ## Display version and cluster configuration
    psql -P pager=off -d template1 -c "SELECT VERSION()"
    psql -P pager=off -d template1 -c "SELECT * FROM gp_segment_configuration ORDER BY dbid"
    psql -P pager=off -d template1 -c "SHOW optimizer"
fi

echo """
===========================
=  DEPLOYMENT SUCCESSFUL  =
===========================
"""

# ----------------------------------------------------------------------
# Start an interactive bash shell
# ----------------------------------------------------------------------
# Finally, the script starts an interactive bash shell to keep the
# container running and allow the user to interact with the environment.
# ----------------------------------------------------------------------
/bin/bash

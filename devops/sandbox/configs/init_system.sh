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

# Ensure SSH directory exists
sudo mkdir -p /run/sshd

# Start SSH daemon (base image already handles most SSH setup)
sudo /usr/sbin/sshd

# Give SSH daemon time to start
sleep 5

# ----------------------------------------------------------------------
# Remove /run/nologin to allow logins
# ----------------------------------------------------------------------
# The /run/nologin file, if present, prevents users from logging into
# the system. This file is removed to ensure that users can log in via SSH.
# ----------------------------------------------------------------------
sudo rm -rf /run/nologin

# ## Set gpadmin ownership - Cloudberry install directory and supporting
# ## cluster creation files.
sudo chown -R gpadmin.gpadmin /usr/local/cloudberry-db \
                              /tmp/gpinitsystem_singlenode \
                              /tmp/gpinitsystem_multinode \
                              /tmp/gpdb-hosts \
                              /tmp/multinode-gpinit-hosts

# ----------------------------------------------------------------------
# Configure passwordless SSH access for 'gpadmin' user
# ----------------------------------------------------------------------
# The script sets up SSH key-based authentication for the 'gpadmin' user,
# allowing passwordless SSH access. It generates a new SSH key pair if one
# does not already exist, and configures the necessary permissions.
# ----------------------------------------------------------------------
mkdir -p /home/gpadmin/.ssh
chmod 700 /home/gpadmin/.ssh

# Prefer build-time cluster keypair on cdw; only generate if absent
if [[ "$HOSTNAME" == "cdw" && -f "/opt/cbdb/cluster-ssh/id_rsa" && -f "/opt/cbdb/cluster-ssh/id_rsa.pub" ]]; then
    cp /opt/cbdb/cluster-ssh/id_rsa /home/gpadmin/.ssh/id_rsa
    cp /opt/cbdb/cluster-ssh/id_rsa.pub /home/gpadmin/.ssh/id_rsa.pub
else
    if [ ! -f /home/gpadmin/.ssh/id_rsa ]; then
        ssh-keygen -t rsa -b 4096 -C gpadmin -f /home/gpadmin/.ssh/id_rsa -P "" > /dev/null 2>&1
    fi
fi

# Enforce strict permissions on key files
chmod 600 /home/gpadmin/.ssh/id_rsa
chmod 644 /home/gpadmin/.ssh/id_rsa.pub
chown gpadmin:gpadmin /home/gpadmin/.ssh/id_rsa /home/gpadmin/.ssh/id_rsa.pub

# Multi-node key distribution (HTTP removed):
# - Require embedded coordinator pubkey in segment/standby images (/opt/cbdb/cluster-ssh/coordinator.pub)
# - No network-based key distribution is performed.
if [[ $MULTINODE == "true" ]]; then
    # Ensure local authorized_keys exists with strict permissions
    touch /home/gpadmin/.ssh/authorized_keys
    chmod 600 /home/gpadmin/.ssh/authorized_keys
    chown gpadmin:gpadmin /home/gpadmin/.ssh/authorized_keys

    if [[ $HOSTNAME == "cdw" ]]; then
        # Coordinator: ensure its own pubkey is present idempotently (robust append using temp file)
        TMP_PUB="$(mktemp)"
        tr -d "\r" < /home/gpadmin/.ssh/id_rsa.pub > "${TMP_PUB}"
        touch /home/gpadmin/.ssh/authorized_keys
        chmod 600 /home/gpadmin/.ssh/authorized_keys
        chown gpadmin:gpadmin /home/gpadmin/.ssh/authorized_keys
        if ! grep -F -x -f "${TMP_PUB}" /home/gpadmin/.ssh/authorized_keys >/dev/null 2>&1; then
            cat "${TMP_PUB}" >> /home/gpadmin/.ssh/authorized_keys
        fi
        rm -f "${TMP_PUB}"
    else
        # Segments/standby: require embedded coordinator pubkey (robust append using temp file)
        if [[ -f "/opt/cbdb/cluster-ssh/coordinator.pub" ]]; then
            TMP_COORD_PUB="$(mktemp)"
            tr -d "\r" < /opt/cbdb/cluster-ssh/coordinator.pub > "${TMP_COORD_PUB}"
            touch /home/gpadmin/.ssh/authorized_keys
            chmod 600 /home/gpadmin/.ssh/authorized_keys
            chown gpadmin:gpadmin /home/gpadmin/.ssh/authorized_keys
            if ! grep -F -x -f "${TMP_COORD_PUB}" /home/gpadmin/.ssh/authorized_keys >/dev/null 2>&1; then
                cat "${TMP_COORD_PUB}" >> /home/gpadmin/.ssh/authorized_keys
            fi
            rm -f "${TMP_COORD_PUB}"
        else
            echo "Error: Missing embedded coordinator pubkey: /opt/cbdb/cluster-ssh/coordinator.pub"
            echo "Hint: Use main build with role-differentiated images (coord/seg) or provide the pubkey via a mounted file."
            exit 1
        fi
    fi
else
    # Single node: use local key
    cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys
    chmod 600 /home/gpadmin/.ssh/authorized_keys
    chown gpadmin:gpadmin /home/gpadmin/.ssh/authorized_keys
fi

# Add container hostnames to the known_hosts file to avoid SSH warnings
if [[ $MULTINODE == "true" ]]; then
    ssh-keyscan -t rsa cdw scdw sdw1 sdw2 > /home/gpadmin/.ssh/known_hosts 2>/dev/null || true
else
    ssh-keyscan -t rsa cdw > /home/gpadmin/.ssh/known_hosts 2>/dev/null || true
fi
chmod 600 /home/gpadmin/.ssh/known_hosts
chown gpadmin:gpadmin /home/gpadmin/.ssh/known_hosts

# Source Cloudberry environment variables and set
# COORDINATOR_DATA_DIRECTORY
source /usr/local/cloudberry-db/cloudberry-env.sh
export COORDINATOR_DATA_DIRECTORY=/data0/database/coordinator/gpseg-1

# Initialize single node Cloudberry cluster
if [[ $MULTINODE == "false" && $HOSTNAME == "cdw" ]]; then
    gpinitsystem -a \
                 -c /tmp/gpinitsystem_singlenode \
                 -h /tmp/gpdb-hosts \
                 --max_connections=100
# Initialize multi node Cloudberry cluster
elif [[ $MULTINODE == "true" && $HOSTNAME == "cdw" ]]; then
    # Wait for other containers' SSH to become reachable (max 300s per host)
    for host in sdw1 sdw2 scdw; do
        MAX_WAIT=300
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
    sudo rm -rf /data0/database/coordinator/* /data0/database/primary/* /data0/database/mirror/* 2>/dev/null || true

    # Ensure database directories exist with proper permissions
    sudo mkdir -p /data0/database/coordinator /data0/database/primary /data0/database/mirror
    sudo chown -R gpadmin:gpadmin /data0/database
    sudo chmod -R 700 /data0/database

    gpinitsystem -a \
                 -c /tmp/gpinitsystem_multinode \
                 -h /tmp/multinode-gpinit-hosts \
                 --max_connections=100
    gpinitstandby -s scdw -a
    printf "sdw1\nsdw2\n" >> /tmp/gpdb-hosts

    if [ $HOSTNAME == "cdw" ]; then
        ## Allow any host access the Cloudberry Cluster
        echo 'host all all 0.0.0.0/0 trust' >> /data0/database/coordinator/gpseg-1/pg_hba.conf
        gpstop -u

        # Remove password requirement for gpadmin user
        psql -d template1 \
             -c "ALTER USER gpadmin PASSWORD NULL"

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

     ## Set gpadmin password, display version and cluster configuration
     psql -P pager=off -d template1 -c "SELECT VERSION()"
     psql -P pager=off -d template1 -c "SELECT * FROM gp_segment_configuration ORDER BY dbid"
     psql -P pager=off -d template1 -c "SHOW optimizer"
fi

fi  # Close the main if/elif block

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

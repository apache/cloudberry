#!/bin/bash
## ======================================================================
## Container initialization script
## ======================================================================

# ----------------------------------------------------------------------
# Start SSH daemon and setup for SSH access
# ----------------------------------------------------------------------
# The SSH daemon is started to allow remote access to the container via
# SSH. This is useful for development and debugging purposes. If the SSH
# daemon fails to start, the script exits with an error.
# ----------------------------------------------------------------------
if ! sudo /usr/sbin/sshd; then
    echo "Failed to start SSH daemon" >&2
    exit 1
fi

# ----------------------------------------------------------------------
# Remove /run/nologin to allow logins
# ----------------------------------------------------------------------
# The /run/nologin file, if present, prevents users from logging into
# the system. This file is removed to ensure that users can log in via SSH.
# ----------------------------------------------------------------------
sudo rm -rf /run/nologin

# ## Set gpadmin ownership - Clouberry install directory and supporting
# ## cluster creation files.
sudo chown -R gpadmin.gpadmin /usr/local/cloudberry-db \
                              /tmp/gpinitsystem_singlenode \
                              /tmp/gpinitsystem_multinode \
                              /tmp/gpdb-hosts \
                              /tmp/multinode-gpinit-hosts \
                              /tmp/faa.tar.gz \
                              /tmp/smoke-test.sh

# ----------------------------------------------------------------------
# Configure passwordless SSH access for 'gpadmin' user
# ----------------------------------------------------------------------
# The script sets up SSH key-based authentication for the 'gpadmin' user,
# allowing passwordless SSH access. It generates a new SSH key pair if one
# does not already exist, and configures the necessary permissions.
# ----------------------------------------------------------------------
mkdir -p /home/gpadmin/.ssh
chmod 700 /home/gpadmin/.ssh

if [ ! -f /home/gpadmin/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -b 4096 -C gpadmin -f /home/gpadmin/.ssh/id_rsa -P "" > /dev/null 2>&1
fi

cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys
chmod 600 /home/gpadmin/.ssh/authorized_keys

# Add the container's hostname to the known_hosts file to avoid SSH warnings
ssh-keyscan -t rsa cdw > /home/gpadmin/.ssh/known_hosts 2>/dev/null

# Source Cloudberry environment variables and set
# COORDINATOR_DATA_DIRECTORY
source /usr/local/cloudberry-db/greenplum_path.sh
export COORDINATOR_DATA_DIRECTORY=/data0/database/coordinator/gpseg-1

# Initialize single node Cloudberry cluster
if [[ $MULTINODE == "false" && $HOSTNAME == "cdw" ]]; then
    gpinitsystem -a \
                 -c /tmp/gpinitsystem_singlenode \
                 -h /tmp/gpdb-hosts \
                 --max_connections=100
# Initialize multi node Cloudberry cluster
elif [[ $MULTINODE == "true" && $HOSTNAME == "cdw" ]]; then
    sshpass -p "cbdb@123" ssh-copy-id -o StrictHostKeyChecking=no sdw1
    sshpass -p "cbdb@123" ssh-copy-id -o StrictHostKeyChecking=no sdw2
    sshpass -p "cbdb@123" ssh-copy-id -o StrictHostKeyChecking=no scdw
    gpinitsystem -a \
                 -c /tmp/gpinitsystem_multinode \
                 -h /tmp/multinode-gpinit-hosts \
                 --max_connections=100
    gpinitstandby -s scdw -a
    printf "sdw1\nsdw2\n" >> /tmp/gpdb-hosts
fi

if [ $HOSTNAME == "cdw" ]; then
     ## Allow any host access the Cloudberry Cluster
     echo 'host all all 0.0.0.0/0 trust' >> /data0/database/coordinator/gpseg-1/pg_hba.conf
     gpstop -u

     psql -d template1 \
          -c "ALTER USER gpadmin PASSWORD 'cbdb@123'"

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

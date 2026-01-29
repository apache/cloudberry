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
# --------------------------------------------------------------------
# Container Initialization Script
# --------------------------------------------------------------------
# This script sets up the environment inside the Docker container for
# the Apache Cloudberry Build Environment. It performs the following
# tasks:
#
# 1. Verifies that the container is running with the expected hostname.
# 2. Starts the SSH daemon to allow SSH access to the container.
# 3. Configures passwordless SSH access for the 'gpadmin' user.
# 4. Displays a welcome banner and system information.
# 5. Starts an interactive bash shell.
#
# This script is intended to be used as an entrypoint or initialization
# script for the Docker container.
# --------------------------------------------------------------------

# --------------------------------------------------------------------
# Check if the hostname is 'cdw'
# --------------------------------------------------------------------
# The script checks if the container's hostname is set to 'cdw'. This is
# a requirement for this environment, and if the hostname does not match,
# the script will exit with an error message. This ensures consistency
# across different environments.
# --------------------------------------------------------------------
if [ "$(hostname)" != "cdw" ]; then
    echo "Error: This container must be run with the hostname 'cdw'."
    echo "Use the following command: docker run -h cdw ..."
    exit 1
fi

# --------------------------------------------------------------------
# Start SSH daemon and setup for SSH access
# --------------------------------------------------------------------
# The SSH daemon is started to allow remote access to the container via
# SSH. This is useful for development and debugging purposes. If the SSH
# daemon fails to start, the script exits with an error.
# --------------------------------------------------------------------
if ! sudo /usr/sbin/sshd; then
    echo "Failed to start SSH daemon" >&2
    exit 1
fi

# --------------------------------------------------------------------
# Remove /run/nologin to allow logins
# --------------------------------------------------------------------
# The /run/nologin file, if present, prevents users from logging into
# the system. This file is removed to ensure that users can log in via SSH.
# --------------------------------------------------------------------
sudo rm -rf /run/nologin

# --------------------------------------------------------------------
# Configure passwordless SSH access for 'gpadmin' user
# --------------------------------------------------------------------
# The script sets up SSH key-based authentication for the 'gpadmin' user,
# allowing passwordless SSH access. It generates a new SSH key pair if one
# does not already exist, and configures the necessary permissions.
# --------------------------------------------------------------------
mkdir -p /home/gpadmin/.ssh
chmod 700 /home/gpadmin/.ssh

if [ ! -f /home/gpadmin/.ssh/id_rsa ]; then
    ssh-keygen -t rsa -b 4096 -C gpadmin -f /home/gpadmin/.ssh/id_rsa -P "" > /dev/null 2>&1
fi

cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys
chmod 600 /home/gpadmin/.ssh/authorized_keys

# Add the container's hostname to the known_hosts file to avoid SSH warnings
ssh-keyscan -t rsa cdw > /home/gpadmin/.ssh/known_hosts 2>/dev/null

# Change to the home directory of the current user
cd $HOME

# --------------------------------------------------------------------
# Display a Welcome Banner
# --------------------------------------------------------------------
# The following ASCII art and welcome message are displayed when the
# container starts. This banner provides a visual indication that the
# container is running in the Apache Cloudberry Build Environment.
# --------------------------------------------------------------------
cat <<-'EOF'

======================================================================

                          ++++++++++       ++++++
                        ++++++++++++++   +++++++
                       ++++        +++++ ++++
                      ++++          +++++++++
                   =+====         =============+
                 ========       =====+      =====
                ====  ====     ====           ====
               ====    ===     ===             ====
               ====            === ===         ====
               ====            ===  ==--       ===
                =====          ===== --       ====
                 =====================     ======
                   ============================
                                     =-----=
     ____  _                    _  _
    / ___|| |  ___   _   _   __| || |__    ___  _ __  _ __  _   _
   | |    | | / _ \ | | | | / _` || '_ \  / _ \| '__|| '__|| | | |
   | |___ | || (_) || |_| || (_| || |_) ||  __/| |   | |   | |_| |
    \____||_| \____  \__,_| \__,_||_.__/  \___||_|   |_|    \__, |
                                                            |___/
----------------------------------------------------------------------

EOF

# --------------------------------------------------------------------
# Display System Information
# --------------------------------------------------------------------
# The script sources the /etc/os-release file to retrieve the operating
# system name and version. It then displays the following information:
# - OS name and version
# - Current user
# - Container hostname
# - IP address
# - CPU model name and number of cores
# - Total memory available
# This information is useful for users to understand the environment they
# are working in.
# --------------------------------------------------------------------
source /etc/os-release

# First, create the CPU info detection function
get_cpu_info() {
   ARCH=$(uname -m)
   if [ "$ARCH" = "x86_64" ]; then
       lscpu | grep 'Model name:' | awk '{print substr($0, index($0,$3))}'
   elif [ "$ARCH" = "aarch64" ]; then
       VENDOR=$(lscpu | grep 'Vendor ID:' | awk '{print $3}')
       if [ "$VENDOR" = "Apple" ] || [ "$VENDOR" = "0x61" ]; then
           echo "Apple Silicon ($ARCH)"
       else
           if [ -f /proc/cpuinfo ]; then
               IMPL=$(grep "CPU implementer" /proc/cpuinfo | head -1 | awk '{print $3}')
               PART=$(grep "CPU part" /proc/cpuinfo | head -1 | awk '{print $3}')
               if [ ! -z "$IMPL" ] && [ ! -z "$PART" ]; then
                   echo "ARM $ARCH (Implementer: $IMPL, Part: $PART)"
               else
                   echo "ARM $ARCH"
               fi
           else
               echo "ARM $ARCH"
           fi
       fi
   else
       echo "Unknown architecture: $ARCH"
   fi
}

cat <<-EOF
Welcome to the Apache Cloudberry Build Environment!

Container OS ........ : $NAME $VERSION
User ................ : $(whoami)
Container hostname .. : $(hostname)
IP Address .......... : $(hostname -I | awk '{print $1}')
CPU Info ............ : $(get_cpu_info)
CPU(s) .............. : $(nproc)
Memory .............. : $(free -h | grep Mem: | awk '{print $2}') total
======================================================================

EOF

# --------------------------------------------------------------------
# Start an interactive bash shell
# --------------------------------------------------------------------
# Finally, the script starts an interactive bash shell to keep the
# container running and allow the user to interact with the environment.
# --------------------------------------------------------------------
/bin/bash

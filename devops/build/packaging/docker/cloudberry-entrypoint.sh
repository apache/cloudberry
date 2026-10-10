#!/usr/bin/env bash
# --------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# --------------------------------------------------------------------
# Cloudberry Docker Entrypoint Script
# --------------------------------------------------------------------
# Standardized entrypoint for Apache Cloudberry Docker containers
# Provides consistent behavior across different deployment scenarios
# --------------------------------------------------------------------

set -euo pipefail

log() {
  echo "cloudberry-entrypoint: $*" >&2
}

die() {
  log "ERROR: $*"
  exit 1
}

# --------------------------------------------------------------------
# Command Handler
# --------------------------------------------------------------------
# Handle different command types for Docker standard behavior
# --------------------------------------------------------------------
if [[ $# -gt 0 ]]; then
  case "$1" in
    cloudberry)
      # Default behavior - start Cloudberry
      ;;
    bash|sh|/bin/bash|/bin/sh)
      exec "$@"
      ;;
    -*)
      echo "Error: unsupported options: $*" >&2
      echo "Hint: run an interactive shell with: docker run -it <image> bash" >&2
      exit 2
      ;;
    *)
      exec "$@"
      ;;
  esac
fi

# --------------------------------------------------------------------
# Environment Setup
# --------------------------------------------------------------------
# Set up Cloudberry environment variables and directories
# --------------------------------------------------------------------
GPHOME="${GPHOME:-/usr/local/cloudberry-db}"
COORDINATOR_DATA_DIRECTORY="${COORDINATOR_DATA_DIRECTORY:-/data0/database/coordinator/gpseg-1}"
export GPHOME COORDINATOR_DATA_DIRECTORY

if [[ ! -f "${GPHOME}/cloudberry-env.sh" ]]; then
  die "Missing ${GPHOME}/cloudberry-env.sh (GPHOME=${GPHOME})"
fi

# Optional: Postgres-compatible env vars for production-friendly auth defaults.
# - Default host auth method is md5 (like many database images).
# - If set to trust, no password is required (NOT recommended for production).
HOST_AUTH_METHOD="${POSTGRES_HOST_AUTH_METHOD:-${CLOUDBERRY_HOST_AUTH_METHOD:-trust}}"
DB_PASSWORD="${POSTGRES_PASSWORD:-${GPADMIN_PASSWORD:-${CLOUDBERRY_PASSWORD:-}}}"

case "${HOST_AUTH_METHOD}" in
  trust|md5|scram-sha-256) ;;
  *) die "Unsupported host auth method: ${HOST_AUTH_METHOD}. Use trust|md5|scram-sha-256." ;;
esac

host_auth_method_explicit=false
if [[ "${POSTGRES_HOST_AUTH_METHOD+x}" == "x" || "${CLOUDBERRY_HOST_AUTH_METHOD+x}" == "x" ]]; then
  host_auth_method_explicit=true
fi

db_password_explicit=false
if [[ "${POSTGRES_PASSWORD+x}" == "x" || "${GPADMIN_PASSWORD+x}" == "x" || "${CLOUDBERRY_PASSWORD+x}" == "x" ]]; then
  db_password_explicit=true
fi

# Source Cloudberry environment
source "${GPHOME}/cloudberry-env.sh"

# Create hostfile for cluster initialization (must match coordinator hostname)
CBDB_HOSTNAME="$(hostname)"
echo "${CBDB_HOSTNAME}" > /tmp/gpdb-hosts
chown gpadmin:gpadmin /tmp/gpdb-hosts

# --------------------------------------------------------------------
# SSH Setup
# --------------------------------------------------------------------
# Generate SSH keys at runtime for security (production-safe)
# --------------------------------------------------------------------
if [[ ! -f /home/gpadmin/.ssh/id_rsa ]]; then
  ssh-keygen -q -t rsa -b 4096 -N '' -C 'gpadmin@cloudberry' -f /home/gpadmin/.ssh/id_rsa >/dev/null
fi

touch /home/gpadmin/.ssh/authorized_keys
chmod 600 /home/gpadmin/.ssh/authorized_keys
if ! grep -qF "$(cat /home/gpadmin/.ssh/id_rsa.pub)" /home/gpadmin/.ssh/authorized_keys; then
  cat /home/gpadmin/.ssh/id_rsa.pub >> /home/gpadmin/.ssh/authorized_keys
fi

# --------------------------------------------------------------------
# SSH Daemon Startup
# --------------------------------------------------------------------
# Start SSH daemon for cluster communication
# --------------------------------------------------------------------
mkdir -p /run/sshd 2>/dev/null || true
# Host keys are generated at runtime (do not bake into image)
sudo ssh-keygen -A >/dev/null 2>&1 || true
if ! sudo /usr/sbin/sshd \
  -o PasswordAuthentication=no \
  -o PermitRootLogin=no \
  -o ChallengeResponseAuthentication=no \
  -o AllowUsers=gpadmin; then
    echo "Failed to start SSH daemon" >&2
    exit 1
fi
sleep 2
sudo rm -f /run/nologin 2>/dev/null || true

# Setup SSH known_hosts (optional; StrictHostKeyChecking is disabled in the image)
ssh-keyscan -t rsa "${CBDB_HOSTNAME}" localhost 127.0.0.1 > /home/gpadmin/.ssh/known_hosts 2>/dev/null || true
chmod 600 /home/gpadmin/.ssh/known_hosts
chown gpadmin:gpadmin /home/gpadmin/.ssh/known_hosts

# Prepare an effective gpinitsystem config with the runtime hostname.
GPINIT_CONF="/tmp/gpinitsystem_singlenode.runtime"
cp /tmp/gpinitsystem_singlenode "${GPINIT_CONF}"
sed -i -E "s/^COORDINATOR_HOSTNAME=.*/COORDINATOR_HOSTNAME=${CBDB_HOSTNAME}/" "${GPINIT_CONF}"

# --------------------------------------------------------------------
# Cluster Initialization
# --------------------------------------------------------------------
# Initialize Cloudberry cluster if not already initialized
# --------------------------------------------------------------------
echo "Starting Cloudberry initialization..."

ensure_access_config() {
  local is_first_init="${1:-false}"
  local coordinator_conf="${COORDINATOR_DATA_DIRECTORY}/postgresql.conf"
  local hba_file="${COORDINATOR_DATA_DIRECTORY}/pg_hba.conf"

  # On subsequent startups, only manage access config if explicitly requested.
  if [[ "${is_first_init}" != "true" && "${host_auth_method_explicit}" != "true" && "${db_password_explicit}" != "true" ]]; then
    return 0
  fi

  # Ensure coordinator listens on all interfaces for container port-mapping use cases.
  if [[ -f "${coordinator_conf}" ]]; then
    if grep -Eq '^[#[:space:]]*listen_addresses[[:space:]]*=' "${coordinator_conf}"; then
      sed -i -E "s/^[#[:space:]]*(listen_addresses)[[:space:]]*=.*/\\1 = '*'/" "${coordinator_conf}"
    else
      echo "listen_addresses = '*'" >> "${coordinator_conf}"
    fi
  fi

  # Manage a dedicated docker auth block idempotently.
  if [[ -f "${hba_file}" ]]; then
    if grep -Eq '^# BEGIN CLOUDBERRY DOCKER AUTH$' "${hba_file}"; then
      sed -i '/^# BEGIN CLOUDBERRY DOCKER AUTH$/, /^# END CLOUDBERRY DOCKER AUTH$/d' "${hba_file}"
    fi

    {
      echo ""
      echo "# BEGIN CLOUDBERRY DOCKER AUTH"
      echo "# Managed by devops/build/packaging/docker/cloudberry-entrypoint.sh"
      echo "host all all 0.0.0.0/0 ${HOST_AUTH_METHOD}"
      echo "host all all ::/0 ${HOST_AUTH_METHOD}"
      echo "# END CLOUDBERRY DOCKER AUTH"
    } >> "${hba_file}"
  fi

  if [[ "${HOST_AUTH_METHOD}" != "trust" ]]; then
    if [[ -z "${DB_PASSWORD}" ]]; then
      if [[ "${is_first_init}" == "true" ]]; then
        die "Database is uninitialized and POSTGRES_PASSWORD is not set (auth=${HOST_AUTH_METHOD}). Set POSTGRES_PASSWORD, or set POSTGRES_HOST_AUTH_METHOD=trust."
      fi
      log "POSTGRES_PASSWORD is not set; skipping password/auth reconfiguration."
      return 0
    fi

    log "Setting gpadmin password..."
    psql -v ON_ERROR_STOP=1 -d template1 -v gpadmin_pass="${DB_PASSWORD}" -c "ALTER USER gpadmin PASSWORD :'gpadmin_pass';"
  else
    log "WARNING: POSTGRES_HOST_AUTH_METHOD=trust disables password authentication. Do NOT use this in production."
  fi

  # Reload configuration (pg_hba.conf / postgresql.conf).
  gpstop -u || true
}

if [[ ! -f "${COORDINATOR_DATA_DIRECTORY}/PG_VERSION" ]]; then
    echo "Initializing Cloudberry cluster (first startup)..."

    if [[ "${HOST_AUTH_METHOD}" != "trust" && -z "${DB_PASSWORD}" ]]; then
      die "Database is uninitialized and POSTGRES_PASSWORD is not set (auth=${HOST_AUTH_METHOD}). Set POSTGRES_PASSWORD, or set POSTGRES_HOST_AUTH_METHOD=trust."
    fi
    
    # Clean up any existing data directories
    rm -rf /data0/database/coordinator/* /data0/database/primary/* /data0/database/mirror/* 2>/dev/null || true
    
    # Ensure database directories exist with proper permissions
    sudo mkdir -p /data0/database/coordinator /data0/database/primary /data0/database/mirror
    sudo chown -R gpadmin:gpadmin /data0/database
    chmod -R 700 /data0/database
    
    # Initialize cluster using standard configuration
    gpinitsystem -a \
                 -c "${GPINIT_CONF}" \
                 -h /tmp/gpdb-hosts \
                 --max_connections=100
    
    ensure_access_config true
    
    echo "Cluster initialization completed successfully!"
else
    echo "Cluster already initialized, starting..."
    gpstart -a
    ensure_access_config false
fi

# --------------------------------------------------------------------
# Deployment Success Message
# --------------------------------------------------------------------
cat <<-'EOF'

======================================================================
	  ____ _                 _ _
	 / ___| | ___  _   _  __| | |__   ___ _ __ _ __ _   _
	| |   | |/ _ \| | | |/ _` | '_ \ / _ \ '__| '__| | | |
	| |___| | (_) | |_| | (_| | |_) |  __/ |  | |  | |_| |
	 \____|_|\___/ \__,_|\__,_|_.__/ \___|_|  |_|   \__, |
	                                                |___/
======================================================================
=  DEPLOYMENT SUCCESSFUL                                           =
======================================================================

EOF

# --------------------------------------------------------------------
# Container Lifecycle Management
# --------------------------------------------------------------------
# Follow logs (users can `docker exec` for interactive shells)
# --------------------------------------------------------------------
stop_cluster() {
  log "Stopping Cloudberry..."
  gpstop -a -M fast >/dev/null 2>&1 || true
}

trap stop_cluster SIGTERM SIGINT SIGQUIT

log_dir="${COORDINATOR_DATA_DIRECTORY}/log"
log "Following coordinator logs in: ${log_dir}"

shopt -s nullglob
while [[ ! -d "${log_dir}" ]]; do
  sleep 1
done

while :; do
  log_files=( "${log_dir}"/*.log )
  if (( ${#log_files[@]} > 0 )); then
    break
  fi
  sleep 1
done

tail -n 0 -F "${log_files[@]}" &
tail_pid=$!

# Drop into psql if running interactively attached, otherwise wait indefinitely
# Note: If running detached but allocating a TTY (`docker run -d -t`), this
# will still trigger and exit. Please use `docker run -d` without `-t`/`-it`
# to keep the container running in the background.
if [ -t 0 ]; then
  log "Interactive terminal detected. Dropping into psql..."
  # Sleep briefly to ensure the cluster is ready
  sleep 2
  exec psql -d postgres
else
  log "Running in detached mode. Waiting indefinitely..."
  wait "${tail_pid}" || true
  sleep infinity
fi

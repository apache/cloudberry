<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Apache Cloudberry Bare-Metal Deployment via Ansible

This directory contains Ansible playbooks for deploying Apache Cloudberry on physical or
virtual machines. Two installation methods are supported:

- **RPM/DEB package** (`site.yml`) — installs from a pre-built binary package
- **Source build** (`site-from-source.yml`) — compiles and installs from a source tarball
  you provide

## Quick Start

### Install from RPM/DEB package

```bash
# 1. Edit inventory and variables
vi inventory/hosts        # set hostnames and IPs
vi group_vars/all.yml     # set password, disk, segments, etc.

# 2. Run the playbook
ansible-playbook site.yml -i inventory/hosts \
    -e package_path=/path/to/apache-cloudberry-db-incubating-2.1.0-1.el9.x86_64.rpm
```

### Install from source

```bash
# 1. Edit inventory and variables (same as above)
vi inventory/hosts
vi group_vars/all.yml

# 2. Run the playbook
ansible-playbook site-from-source.yml -i inventory/hosts \
    -e source_path=/path/to/apache-cloudberry-2.1.0-incubating-src.tar.gz \
    -e xerces_path=/path/to/xerces-c-3.3.0.tar.gz   # RHEL/Rocky only
```

Download and verify the following before running:
- Cloudberry source: [Apache Cloudberry Releases](https://cloudberry.apache.org/releases)
- Xerces-C source (RHEL/Rocky only): [Apache Xerces-C Downloads](https://xerces.apache.org/xerces-c/download.cgi)

Ubuntu uses the system `libxerces-c-dev` package, so `xerces_path` is not needed on Ubuntu.

#### Downloading Cloudberry source

Download the source tarball and verify its integrity:

```bash
# Download source tarball
curl -L -o apache-cloudberry-2.1.0-incubating-src.tar.gz \
  "https://www.apache.org/dyn/closer.lua/incubator/cloudberry/2.1.0-incubating/apache-cloudberry-2.1.0-incubating-src.tar.gz?action=download"

# Download checksum and signature files
curl -O https://downloads.apache.org/incubator/cloudberry/2.1.0-incubating/apache-cloudberry-2.1.0-incubating-src.tar.gz.sha512
curl -O https://downloads.apache.org/incubator/cloudberry/2.1.0-incubating/apache-cloudberry-2.1.0-incubating-src.tar.gz.asc

# Verify SHA512 checksum
sha512sum -c apache-cloudberry-2.1.0-incubating-src.tar.gz.sha512

# Verify GPG signature (optional but recommended)
curl https://downloads.apache.org/incubator/cloudberry/KEYS | gpg --import
gpg --verify apache-cloudberry-2.1.0-incubating-src.tar.gz.asc \
             apache-cloudberry-2.1.0-incubating-src.tar.gz
```

#### Downloading Xerces-C (RHEL/Rocky Linux only)

The playbook requires Xerces-C 3.3.0. Download and verify:

```bash
# Download source tarball
wget https://dlcdn.apache.org/xerces/c/3/sources/xerces-c-3.3.0.tar.gz

# Verify SHA256 checksum
echo "$(curl -sL https://dlcdn.apache.org/xerces/c/3/sources/xerces-c-3.3.0.tar.gz.sha256)" \
  | sha256sum -c -
```

## Cluster Layout (default)

| Host | Role |
|------|------|
| cdw  | Coordinator |
| scdw | Standby Coordinator |
| sdw1 | Segment Host 1 |
| sdw2 | Segment Host 2 |
| sdw3 | Segment Host 3 |

Each segment host runs 2 primary segments and 2 mirror segments (spread mirroring).

## Prerequisites

- Ansible installed on the control machine (tested with ansible-core 2.14+)
- Root SSH access from the control machine to all hosts
- All hosts running Rocky Linux 8/9, Ubuntu 22.04, or compatible OS

Ansible 2.10+ requires the following collections:

```bash
ansible-galaxy collection install ansible.posix community.general community.crypto
```

To suppress the Python `crypt` module deprecation warning:

```bash
pip3 install passlib
```

## Directory Structure

```
ansible/
├── site.yml                      # RPM/DEB installation entry point
├── site-from-source.yml          # Source build entry point
├── ansible.cfg                   # Disables host key checking
├── inventory/
│   └── hosts                     # Hostnames and IPs
├── group_vars/
│   └── all.yml                   # Deployment variables
└── roles/
    ├── common/                   # OS configuration (shared by both methods)
    │   ├── tasks/main.yml
    │   └── handlers/main.yml
    ├── install_package/          # RPM/DEB package installation
    │   └── tasks/main.yml
    ├── install_from_source/      # Source build and installation
    │   └── tasks/main.yml
    ├── ssh/                      # Passwordless SSH + data directories
    │   └── tasks/main.yml
    └── initialize/               # Cluster initialization (gpinitsystem)
        └── tasks/main.yml
```

## What the Playbooks Do

Both `site.yml` and `site-from-source.yml` share the same OS configuration, SSH setup,
and cluster initialization steps. Only the installation role differs.

### Shared steps (all hosts)

1. Disable SELinux / ufw firewall
2. Configure hostnames and `/etc/hosts`
3. Set kernel parameters (`sysctl`) — dynamically calculated per host based on RAM/swap
4. Set resource limits (`limits.conf`)
5. Configure XFS mount and disk I/O settings
6. Disable Transparent Huge Pages
7. Disable IPC object removal
8. Configure SSH thresholds
9. Synchronize system clocks (chronyd / chrony)
10. Create `gpadmin` user with sudo

### RPM/DEB installation (`install_package` role)

11. Copy package to each host and install via `dnf` / `apt`

### Source build (`install_from_source` role)

11. Install build dependencies (OS-specific)
12. Build and install Apache Xerces-C (RHEL/Rocky only; Ubuntu uses system package)
13. Copy source tarball to each host and extract
14. Compile and install using the project's build scripts

### Shared steps (coordinator only)

16. Configure N-N passwordless SSH for gpadmin via `gpssh-exkeys`
17. Create data storage directories on all nodes
18. Initialize the cluster with `gpinitsystem` (includes standby coordinator)
19. Set environment variables in `.bashrc`

## Key Variables (`group_vars/all.yml`)

| Variable | Default | Description |
|----------|---------|-------------|
| `cloudberry_version` | `2.1.0` | Cloudberry version (informational) |
| `cloudberry_admin_user` | `gpadmin` | Admin OS user |
| `cloudberry_admin_password` | `changeme` | Password for gpadmin |
| `data_disk` | `/dev/sdb` | Data disk device (run `lsblk` to verify) |
| `data_mount` | `/data` | Mount point for data disk |
| `segments_per_host` | `2` | Primary segment instances per host |
| `coordinator_port` | `5432` | Coordinator port |
| `database_name` | `warehouse` | Default database created at init |
| `xerces_version` | `3.3.0` | Xerces-C version (source build, RHEL/Rocky only) |

## After Deployment

```bash
su - gpadmin
psql -d warehouse    # connect to the database
gpstate -s           # check cluster status
gpstate -f           # check standby coordinator status
```

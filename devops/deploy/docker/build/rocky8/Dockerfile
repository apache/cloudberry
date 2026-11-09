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
#
# Apache Cloudberry (Incubating) is an effort undergoing incubation at
# the Apache Software Foundation (ASF), sponsored by the Apache
# Incubator PMC.
#
# Incubation is required of all newly accepted projects until a
# further review indicates that the infrastructure, communications,
# and decision making process have stabilized in a manner consistent
# with other successful ASF projects.
#
# While incubation status is not necessarily a reflection of the
# completeness or stability of the code, it does indicate that the
# project has yet to be fully endorsed by the ASF.
#
# --------------------------------------------------------------------
# Dockerfile for Apache Cloudberry Build Environment
# --------------------------------------------------------------------
# This Dockerfile sets up a Rocky Linux 9-based container for building
# and developing Apache Cloudberry. It installs necessary system
# utilities, development tools, and configures the environment for SSH
# access and systemd support.
#
# Key Features:
# - Locale setup for en_US.UTF-8
# - SSH daemon setup for remote access
# - Essential development tools and libraries installation
# - User configuration for 'gpadmin' with sudo privileges
#
# Usage:
#   docker build -t cloudberry-db-env .
#   docker run -h cdw -it cloudberry-db-env
# --------------------------------------------------------------------

# Base image: Rocky Linux 8
FROM rockylinux/rockylinux:8

# Argument for configuring the timezone
ARG TIMEZONE_VAR="America/Los_Angeles"

# Environment variables for locale and user
ENV container=docker
ENV LANG=en_US.UTF-8
ENV USER=gpadmin

# --------------------------------------------------------------------
# Install Development Tools and Utilities
# --------------------------------------------------------------------
# Install various development tools, system utilities, and libraries
# required for building and running Apache Cloudberry.
# - EPEL repository is enabled for additional packages.
# - Cleanup steps are added to reduce image size after installation.
# --------------------------------------------------------------------
RUN dnf makecache && \
    dnf install -y \
        epel-release \
        rocky-release-hpc \
        git && \
    dnf makecache && \
    dnf config-manager --disable epel && \
    dnf install -y -d0 --enablerepo=epel \
        the_silver_searcher \
        htop && \
    dnf install -y -d0 \
        apr-devel \
        autoconf \
        bison \
        bzip2-devel \
        cmake3 \
        diffutils \
        file \
        flex \
        gcc \
        gcc-c++ \
        gdb \
        glibc-langpack-en \
        glibc-locale-source \
        iproute \
        java-11-openjdk \
        java-11-openjdk-devel \
        krb5-devel \
        libcurl-devel \
        libevent-devel \
        libicu-devel \
        libssh2-devel \
        libuuid-devel \
        libxml2-devel \
        libzstd-devel \
        lsof \
        lz4 \
        lz4-devel \
        make \
        m4 \
        nc \
        net-tools \
        openldap-devel \
        openssh-clients \
        openssh-server \
        openssl-devel \
        pam-devel \
        passwd \
        perl-Env \
        perl-ExtUtils-Embed \
        perl-Test-Simple \
        procps-ng \
        python3 \
        python3-devel \
        readline-devel \
        rsync \
        sshpass \
        sudo \
        tar \
        unzip \
        util-linux-ng \
        wget \
        which \
        rpm-build \
        rpmdevtools \
        zlib-devel && \
    dnf install -y -d0 --enablerepo=devel \
        liburing-devel \
        libuv-devel \
        libyaml-devel \
        perl-IPC-Run \
        protobuf-devel && \
    dnf clean all && rm -rf /var/cache/dnf/* && \
    cd && XERCES_LATEST_RELEASE=3.3.0 && \
    wget -nv "https://archive.apache.org/dist/xerces/c/3/sources/xerces-c-${XERCES_LATEST_RELEASE}.tar.gz" && \
    echo "$(curl -sL https://archive.apache.org/dist/xerces/c/3/sources/xerces-c-${XERCES_LATEST_RELEASE}.tar.gz.sha256)" | sha256sum -c - && \
    tar xf "xerces-c-${XERCES_LATEST_RELEASE}.tar.gz"; rm "xerces-c-${XERCES_LATEST_RELEASE}.tar.gz" && \
    cd xerces-c-${XERCES_LATEST_RELEASE} && \
    ./configure --prefix=/usr/local/xerces-c && \
    make -j$(nproc) && \
    make install -C ~/xerces-c-${XERCES_LATEST_RELEASE} && \
    rm -rf ~/xerces-c* && \
    cd && GO_VERSION="go1.25.10" && \
    ARCH=$(uname -m) && \
    if [ "${ARCH}" = "aarch64" ]; then \
        GO_ARCH="arm64" && \
        GO_SHA256="654da1f9b50a5d1c2a85ccf8ed405aa89c06e94d18384628bf186f7712677b08"; \
    elif [ "${ARCH}" = "x86_64" ]; then \
        GO_ARCH="amd64" && \
        GO_SHA256="42d4f7a32316aa66591eca7e89867256057a4264451aca10570a715b3637ba70"; \
    else \
        echo "Unsupported architecture: ${ARCH}" && exit 1; \
    fi && \
    GO_URL="https://go.dev/dl/${GO_VERSION}.linux-${GO_ARCH}.tar.gz" && \
    wget -nv "${GO_URL}" && \
    echo "${GO_SHA256}  ${GO_VERSION}.linux-${GO_ARCH}.tar.gz" | sha256sum -c - && \
    tar xf "${GO_VERSION}.linux-${GO_ARCH}.tar.gz" && \
    mv go "/usr/local/${GO_VERSION}" && \
    ln -s "/usr/local/${GO_VERSION}" /usr/local/go && \
    rm -f "${GO_VERSION}.linux-${GO_ARCH}.tar.gz" && \
    echo 'export PATH=$PATH:/usr/local/go/bin' | tee -a /etc/profile.d/go.sh > /dev/null

# --------------------------------------------------------------------
# Install Apache Arrow and Parquet C++ for contrib/datalake_fdw
# --------------------------------------------------------------------
# From the Arrow project's own repository, pinned: datalake_fdw shares
# its ABI with Arrow, so the version moves only with a change here.
# 22.0.0 is the last release whose headers are C++17: from 23 on they
# need C++20, which this image's gcc 8 does not have.
# EPEL and PowerTools carry what the packages depend on (re2, thrift, ...).
# The repository is left disabled afterwards, as EPEL is.
# The release package needs nothing from a repository, so it goes in
# with them all disabled: loading their metadata under QEMU took eight
# minutes of an arm64 build.
# --------------------------------------------------------------------
ARG ARROW_VERSION=22.0.0
RUN dnf install -y --disablerepo='*' \
        https://packages.apache.org/artifactory/arrow/almalinux/8/apache-arrow-release-latest.rpm && \
    dnf install -y --enablerepo=epel --enablerepo=powertools \
        arrow-devel-${ARROW_VERSION}-1.el8 \
        parquet-devel-${ARROW_VERSION}-1.el8 && \
    dnf config-manager --disable 'apache-arrow-*' && \
    dnf clean all

# --------------------------------------------------------------------
# Copy Configuration Files and Setup the Environment
# --------------------------------------------------------------------
# - Copy custom configuration files from the build context to /tmp/.
# - Apply custom system limits and timezone.
# - Create and configure the 'gpadmin' user with sudo privileges.
# - Set up SSH for password-based authentication.
# - Generate locale and set the default locale to en_US.UTF-8.
# --------------------------------------------------------------------
COPY ./configs/* /tmp/

RUN cp /tmp/90-cbdb-limits /etc/security/limits.d/90-cbdb-limits && \
    sed -i.bak -r 's/^(session\s+required\s+pam_limits.so)/#\1/' /etc/pam.d/* && \
    cat /usr/share/zoneinfo/${TIMEZONE_VAR} > /etc/localtime && \
    chmod 777 /tmp/init_system.sh && \
    /usr/sbin/groupadd gpadmin && \
    /usr/sbin/useradd gpadmin -g gpadmin -G wheel && \
    setcap cap_net_raw+ep /usr/bin/ping && \
    echo 'gpadmin ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/90-gpadmin && \
    echo -e '\n# Add Cloudberry entries\nif [ -f /usr/local/cbdb/cloudberry-env.sh ]; then\n  source /usr/local/cbdb/cloudberry-env.sh\nfi' >> /home/gpadmin/.bashrc && \
    ssh-keygen -A && \
    echo "PasswordAuthentication yes" >> /etc/ssh/sshd_config && \
    localedef -i en_US -f UTF-8 en_US.UTF-8 && \
    echo "LANG=en_US.UTF-8" | tee /etc/locale.conf && \
    dnf clean all  # Final cleanup to remove unnecessary files

# --------------------------------------------------------------------
# Install Meson and Ninja
# --------------------------------------------------------------------
# For building with meson. Rocky 8 packages ninja-build 1.8.2, older
# than the 1.10 the generated build.ninja needs, so both come from
# PyPI instead. The system pip 9 predates the wheel tags ninja
# publishes, so pip is upgraded first; pip 21 is the last release that
# runs on this image's Python 3.6, which in turn caps meson at 0.61.5,
# still above the 0.54 the build requires.
# --------------------------------------------------------------------
RUN python3 -m pip install --upgrade "pip<22" && \
    python3 -m pip install meson ninja

# Install testinfra via pip
RUN pip3 install pytest-testinfra

# Example: Copying test files into the container
COPY tests /tests

# --------------------------------------------------------------------
# Set the Default User and Command
# --------------------------------------------------------------------
# The default user is set to 'gpadmin', and the container starts by
# running the init_system.sh script. The container also mounts the
# /sys/fs/cgroup volume for systemd compatibility.
# --------------------------------------------------------------------
USER gpadmin

VOLUME [ "/sys/fs/cgroup" ]
CMD ["bash","-c","/tmp/init_system.sh"]

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

# Base image: Rocky Linux 9
FROM rockylinux/rockylinux:9

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
        git && \
    dnf config-manager --disable epel-cisco-openh264 && \
    dnf makecache && \
    dnf config-manager --disable epel && \
    dnf install -y --enablerepo=epel \
        the_silver_searcher \
        bat \
        htop && \
    dnf install -y \
        bison \
        cmake3 \
        ed \
        file \
        flex \
        gcc \
        gcc-c++ \
        gdb \
        glibc-langpack-en \
        glibc-locale-source \
        initscripts \
        iproute \
        less \
        lsof \
        m4 \
        net-tools \
        openssh-clients \
        openssh-server \
        perl \
        rpm-build \
        rpmdevtools \
        rsync \
        sudo \
        tar \
        unzip \
        util-linux-ng \
        wget \
        sshpass \
        which && \
    dnf install -y \
        apr-devel \
        bzip2-devel \
        java-11-openjdk \
        java-11-openjdk-devel \
        krb5-devel \
        libcurl-devel \
        libevent-devel \
        libxml2-devel \
        libuuid-devel \
        libzstd-devel \
        lz4 \
        lz4-devel \
        openldap-devel \
        openssl-devel \
        pam-devel \
        perl-ExtUtils-Embed \
        perl-Test-Simple \
        perl-core \
        python3-devel \
        python3-pytest \
        readline-devel \
        zlib-devel && \
    dnf install -y --enablerepo=crb \
        libuv-devel \
        libssh2-devel \
        libyaml-devel \
        perl-IPC-Run \
        protobuf-devel && \
    dnf clean all && \
    cd && XERCES_LATEST_RELEASE=3.3.0 && \
    wget -nv "https://archive.apache.org/dist/xerces/c/3/sources/xerces-c-${XERCES_LATEST_RELEASE}.tar.gz" && \
    echo "$(curl -sL https://archive.apache.org/dist/xerces/c/3/sources/xerces-c-${XERCES_LATEST_RELEASE}.tar.gz.sha256)" | sha256sum -c - && \
    tar xf "xerces-c-${XERCES_LATEST_RELEASE}.tar.gz"; rm "xerces-c-${XERCES_LATEST_RELEASE}.tar.gz" && \
    cd xerces-c-${XERCES_LATEST_RELEASE} && \
    ./configure --prefix=/usr/local/xerces-c && \
    make -j$(nproc) && \
    make install -C ~/xerces-c-${XERCES_LATEST_RELEASE} && \
    rm -rf ~/xerces-c* && \
    cd && GO_VERSION="go1.23.4" && \
    ARCH=$(uname -m) && \
    if [ "${ARCH}" = "aarch64" ]; then \
        GO_ARCH="arm64" && \
        GO_SHA256="16e5017863a7f6071363782b1b8042eb12c6ca4f4cd71528b2123f0a1275b13e"; \
    elif [ "${ARCH}" = "x86_64" ]; then \
        GO_ARCH="amd64" && \
        GO_SHA256="6924efde5de86fe277676e929dc9917d466efa02fb934197bc2eba35d5680971"; \
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
# Copy Configuration Files and Setup the Environment
# --------------------------------------------------------------------
# - Copy custom configuration files from the build context to /tmp/.
# - Apply custom system limits and timezone.
# - Create and configure the 'gpadmin' user with sudo privileges.
# - Set up SSH for password-based authentication.
# - Generate locale and set the default locale to en_US.UTF-8.
# --------------------------------------------------------------------

# Copy configuration files from their respective locations
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

# Install testinfra via pip
RUN pip3 install pytest-testinfra

# Copying test files into the container
COPY ./tests /tests

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

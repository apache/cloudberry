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
# Apache Cloudberry (incubating) is an effort undergoing incubation at
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
# Dockerfile for Apache Cloudberry Base Environment
# --------------------------------------------------------------------
# This Dockerfile sets up a Ubuntu jammy 22.04 -based container to serve as
# a base environment for evaluating the Apache Cloudberry. It installs
# necessary system utilities, configures the environment for SSH access,
# and sets up a 'gpadmin' user with sudo privileges. The Apache Cloudberry
# DEB can be installed into this container for testing and
# functional verification.
#
# Key Features:
# - Locale setup for en_US.UTF-8
# - SSH daemon setup for remote access
# - Essential system utilities installation
# - Separate user creation and configuration steps
#
# Security Considerations:
# - This Dockerfile prioritizes ease of use for functional testing and
#   evaluation. It includes configurations such as passwordless sudo access
#   for the 'gpadmin' user and SSH access with password authentication.
# - These configurations are suitable for testing and development but
#   should NOT be used in a production environment due to potential security
#   risks.
#
# Usage:
#   docker build -t cloudberry-db-base-env .
#   docker run -h cdw -it cloudberry-db-base-env
# --------------------------------------------------------------------

FROM ubuntu:22.04

# Argument for configuring the timezone
ARG TIMEZONE_VAR="Europe/London"

# Environment variables for locale and user
ENV container=docker
ENV LANG=en_US.UTF-8
ENV USER=gpadmin
ENV TZ=${TIMEZONE_VAR}
ENV DEBIAN_FRONTEND=noninteractive

# --------------------------------------------------------------------
# Install Development Tools and Utilities
# --------------------------------------------------------------------

RUN apt-get update && \
    apt-get install -y -qq \
            htop \
            bat \
            silversearcher-ag \
            vim \
            wget && \
    apt-get install -y -qq locales && \
    locale-gen "en_US.UTF-8" && \
    update-locale LANG="en_US.UTF-8" && \
    apt-get install -y -qq \
            bison \
            build-essential \
            cmake \
            dpkg-dev \
            fakeroot \
            flex \
            g++-11 \
            gcc-11 \
            git \
            iproute2 \
            iputils-ping \
            libapr1-dev \
            libbz2-dev \
            libcurl4-gnutls-dev \
            libevent-dev \
            libipc-run-perl \
            libkrb5-dev \
            libldap-dev \
            liblz4-dev \
            libpam0g-dev \
            libperl-dev \
            libprotobuf-dev \
            libreadline-dev \
            libssl-dev \
            libuv1-dev \
            libxerces-c-dev \
            libxml2-dev \
            libyaml-dev \
            libzstd-dev \
            lsof \
            make \
            openssh-server \
            pkg-config \
            protobuf-compiler \
            python3-distutils \
            python3-pip \
            python3-setuptools \
            python3.10 \
            python3.10-dev \
            rsync \
            sudo \
            tzdata \
            zlib1g-dev && \
    apt-get install -y -qq \
            ca-certificates-java \
            cgroup-tools \
            curl \
            debhelper \
            libaprutil1-dev \
            libcgroup1 \
            ninja-build \
            quilt \
            unzip && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
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

RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100 && \
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 100 && \
    update-alternatives --install /usr/bin/x86_64-linux-gnu-gcc x86_64-linux-gnu-gcc /usr/bin/gcc-11 100 && \
    update-alternatives --set gcc /usr/bin/gcc-11 && \
    update-alternatives --set g++ /usr/bin/g++-11

# --------------------------------------------------------------------
# Copy Configuration Files and Setup the Environment
# --------------------------------------------------------------------

COPY ./configs/* /tmp/

RUN cp /tmp/90-cbdb-limits /etc/security/limits.d/90-cbdb-limits && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    chmod 755 /tmp/init_system.sh && \
    /usr/sbin/groupadd gpadmin && \
    /usr/sbin/useradd -m -g gpadmin gpadmin && \
    echo 'gpadmin ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/90-gpadmin && \
    chmod 0440 /etc/sudoers.d/90-gpadmin && \
    ssh-keygen -A && \
    mkdir /var/run/sshd && chmod 0755 /var/run/sshd

# Install testinfra via pip
RUN pip3 install pytest-testinfra

# Example: Copying test files into the container
COPY tests /tests

USER gpadmin
WORKDIR /home/gpadmin

CMD ["bash","-c","/tmp/init_system.sh"]

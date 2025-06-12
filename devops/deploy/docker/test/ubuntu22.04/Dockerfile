
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
ENV DEBCONF_NOWARNINGS="yes"

# --------------------------------------------------------------------
# Install Development Tools and Utilities
# --------------------------------------------------------------------

RUN apt-get update && \
    apt-get install -y -qq \
            htop \
            bat \
            silversearcher-ag \
            vim \
            wget \
            gdb \
            git \
            iputils-ping \
            lsof \
            openssh-server \
            pkg-config \
            python3.10 \
            python3-distutils \
            python3-pip \
            python3-setuptools \
            sudo \
            tzdata && \
    apt-get install -y -qq locales && \
    locale-gen ${LANG} && \
    update-locale LANG=${LANG} && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3.10 /usr/bin/python

# --------------------------------------------------------------------
# User Creation and Configuration
# --------------------------------------------------------------------
# - Create the 'gpadmin' user and group.
# - Configure the 'gpadmin' user with passwordless sudo privileges.
# - Add Cloudberry-specific entries to the gpadmin's .bashrc.
# --------------------------------------------------------------------
RUN /usr/sbin/groupadd gpadmin && \
    /usr/sbin/useradd -m -g gpadmin gpadmin -s /bin/bash && \
    echo 'gpadmin ALL=(ALL) NOPASSWD: ALL' > /etc/sudoers.d/90-gpadmin && \
    chmod 0440 /etc/sudoers.d/90-gpadmin && \
    echo '\n# Add Cloudberry entries\nif [ -f /usr/local/cloudberry/cloudberry-env.sh ]; then\n  source /usr/local/cloudberry/cloudberry-env.sh\n  export COORDINATOR_DATA_DIRECTORY=/data1/coordinator/gpseg-1\nfi' >> /home/gpadmin/.bashrc

# --------------------------------------------------------------------
# Copy Configuration Files and Setup the Environment
# --------------------------------------------------------------------
# - Copy custom configuration files from the build context to /tmp/.
# - Apply custom system limits and timezone.
# - Set up SSH for password-based authentication.
# - Generate locale and set the default locale to en_US.UTF-8.
# --------------------------------------------------------------------
COPY ./configs/* /tmp/

RUN cp /tmp/90-cbdb-limits /etc/security/limits.d/90-cbdb-limits && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && \
    echo $TZ > /etc/timezone && \
    chmod 755 /tmp/init_system.sh && \
    ssh-keygen -A

USER gpadmin
WORKDIR /home/gpadmin

CMD ["bash","-c","/tmp/init_system.sh"]


# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Validate required macros early so rpmbuild fails with a clear message
# before reaching the header.
%{!?version:%{error:The macro 'version' must be supplied as --define 'version ...'}}
%{!?release:%{error:The macro 'release' must be supplied as --define 'release ...'}}

%define cloudberry_base_dir /usr/local
%define cloudberry_name cloudberry-db
%define cloudberry_install_dir %{cloudberry_base_dir}/%{cloudberry_name}

# Major version, used to build a versioned package Name so that different
# major versions can be installed side by side (like greenplum-db-6 vs
# greenplum-db-7). Derived from the version macro by default (single source
# of truth), but can be overridden via --define 'cloudberry_major_version N'.
%{!?cloudberry_major_version:%define cloudberry_major_version %(echo %{version} | cut -d. -f1)}

# Suppress build-id links so they are not created outside the relocatable prefix.
%define _build_id_links none

# Do not expose bundled/private shared libraries as RPM Provides.
# (e.g., libpq.so.5 would conflict with system postgresql-libs.)
%global __provides_exclude_from ^%{cloudberry_install_dir}-%{version}/.*\.so

# Do not require these bundled libraries from the system;
# they are shipped inside the package and located via RPATH.
%global __requires_exclude ^(libpax\.so|libpaxformat\.so|libpostgres\.so|libpq\.so\.5|libxerces-c-3\.3\.so)

# Default to non-debug build
%bcond_with debug

# Conditional stripping based on debug flag
%if %{with debug}
%define __os_install_post %{nil}
%define __strip /bin/true
%endif

Name:           apache-cloudberry-db-incubating-%{cloudberry_major_version}
# Replace the previous unversioned package on upgrade. This targets only the
# old fixed name; it does NOT match apache-cloudberry-db-incubating-<major>,
# so different major versions still coexist.
Obsoletes:      apache-cloudberry-db-incubating < %{version}-%{release}
Version:        %{version}
# In the release definition section
%if %{with debug}
Release:        %{release}.debug%{?dist}
%else
Release:        %{release}%{?dist}
%endif
Summary:        High-performance, open-source data warehouse based on PostgreSQL/Greenplum

License:        Apache-2.0
URL:            https://cloudberry.apache.org
Vendor:         Apache Cloudberry (Incubating)
Group:          Applications/Databases
Prefix:         %{cloudberry_base_dir}

# Disabled as we are shipping GO programs (e.g. gpbackup)
%define _missing_build_ids_terminate_build 0

# Disable debugsource files
%define _debugsource_template %{nil}

# List runtime dependencies

Requires:       bash
Requires:       hostname
Requires:       iproute
Requires:       iputils
Requires:       less
Requires:       openssh
Requires:       openssh-clients
Requires:       openssh-server
Requires:       rsync
Requires:       which

# Scriptlet dependencies (ln, readlink, rm are from coreutils).
Requires(post): coreutils
Requires(postun): coreutils

%if 0%{?rhel} == 8
Requires:       apr
Requires:       audit
Requires:       bzip2
Requires:       keyutils
Requires:       libcurl
Requires:       libevent
Requires:       libidn2
Requires:       libselinux
Requires:       libstdc++
Requires:       libuuid
Requires:       libuv
Requires:       libxml2
Requires:       libyaml
Requires:       libzstd
Requires:       lz4
Requires:       openldap
Requires:       pam
Requires:       perl
Requires:       python3
Requires:       readline
%endif

%if 0%{?rhel} == 9
Requires:       apr
Requires:       bzip2
Requires:       glibc
Requires:       keyutils
Requires:       libcap
Requires:       libcurl
Requires:       libidn2
Requires:       libpsl
Requires:       libssh
Requires:       libstdc++
Requires:       libxml2
Requires:       libyaml
Requires:       libzstd
Requires:       lz4
Requires:       openldap
Requires:       pam
Requires:       pcre2
Requires:       perl
Requires:       readline
Requires:       python3
Requires:       xz
%endif

%description

Apache Cloudberry (Incubating) is an advanced, open-source, massively
parallel processing (MPP) data warehouse developed from PostgreSQL and
Greenplum. It is designed for high-performance analytics on
large-scale data sets, offering powerful analytical capabilities and
enhanced security features.

Key Features:

- Massively parallel processing for optimized performance
- Advanced analytics for complex data processing
- Integration with ETL and BI tools
- Compatibility with multiple data sources and formats
- Enhanced security features

Apache Cloudberry supports both batch processing and real-time data
warehousing, making it a versatile solution for modern data
environments.

Apache Cloudberry (Incubating) is an effort undergoing incubation at
the Apache Software Foundation (ASF), sponsored by the Apache
Incubator PMC.

Incubation is required of all newly accepted projects until a further
review indicates that the infrastructure, communications, and decision
making process have stabilized in a manner consistent with other
successful ASF projects.

While incubation status is not necessarily a reflection of the
completeness or stability of the code, it does indicate that the
project has yet to be fully endorsed by the ASF.

%prep
# No prep needed for binary RPM

%build
# No build needed for binary RPM

%install
rm -rf %{buildroot}

# Create the versioned directory
mkdir -p %{buildroot}%{cloudberry_install_dir}-%{version}

# Use cp -a with /. to include all the files
cp -a %{cloudberry_install_dir}/. %{buildroot}%{cloudberry_install_dir}-%{version}/

# Copy Apache mandatory compliance files from the SOURCES directory into the installation directory
cp %{_sourcedir}/LICENSE %{buildroot}%{cloudberry_install_dir}-%{version}/
cp %{_sourcedir}/NOTICE %{buildroot}%{cloudberry_install_dir}-%{version}/
cp %{_sourcedir}/DISCLAIMER %{buildroot}%{cloudberry_install_dir}-%{version}/
cp -a %{_sourcedir}/licenses %{buildroot}%{cloudberry_install_dir}-%{version}/

%files
%{cloudberry_install_dir}-%{version}
%config(noreplace) %{cloudberry_install_dir}-%{version}/cloudberry-env.sh

%debug_package

%post
# RPM_INSTALL_PREFIX is set dynamically by RPM to the actual --prefix value.
# Fall back to cloudberry_base_dir when --prefix was not used.
INSTALL_PREFIX="${RPM_INSTALL_PREFIX:-%{cloudberry_base_dir}}"
INSTALL_BASE="${INSTALL_PREFIX%/}"

LINK_PATH="${INSTALL_BASE}/%{cloudberry_name}"
VERSIONED_DIR="${INSTALL_BASE}/%{cloudberry_name}-%{version}"
LINK_TARGET_REL="%{cloudberry_name}-%{version}"

if [ ! -e "${LINK_PATH}" ] && [ ! -L "${LINK_PATH}" ]; then
    # Nothing at the symlink location yet — create it.
    ln -s "${LINK_TARGET_REL}" "${LINK_PATH}" || :
elif [ -L "${LINK_PATH}" ]; then
    # A symlink already exists. Update it when it points to a
    # recognized Cloudberry versioned directory.
    EXISTING_TARGET=$(readlink -f -- "${LINK_PATH}" 2>/dev/null || :)
    EXISTING_NAME=${EXISTING_TARGET##*/}

    case "${EXISTING_NAME}" in
        %{cloudberry_name}-*)
            EXISTING_VERSION=${EXISTING_NAME#%{cloudberry_name}-}
            EXISTING_MAJOR=${EXISTING_VERSION%%.*}
            if [ "${EXISTING_MAJOR}" = "%{cloudberry_major_version}" ]; then
                # Same major version: move the generic symlink to this build.
                ln -sfnT "${LINK_TARGET_REL}" "${LINK_PATH}" || :
            else
                # Different major version: leave the existing symlink untouched
                # so that multiple major versions can coexist under one prefix.
                echo "Warning: ${LINK_PATH} points to Cloudberry major version ${EXISTING_MAJOR}; leaving it unchanged so major versions can coexist" >&2
            fi
            ;;
        *)
            echo "Warning: ${LINK_PATH} does not point to a recognized Cloudberry installation; leaving it unchanged" >&2
            ;;
    esac
else
    echo "Warning: ${LINK_PATH} exists and is not a symbolic link; leaving it unchanged" >&2
fi

exit 0

%postun
INSTALL_PREFIX="${RPM_INSTALL_PREFIX:-%{cloudberry_base_dir}}"
INSTALL_BASE="${INSTALL_PREFIX%/}"

LINK_PATH="${INSTALL_BASE}/%{cloudberry_name}"
VERSIONED_DIR="${INSTALL_BASE}/%{cloudberry_name}-%{version}"

if [ -L "${LINK_PATH}" ]; then
    LINK_TARGET=$(readlink "${LINK_PATH}" 2>/dev/null || :)

    # Remove the symlink only when it still points to the version
    # being removed (handles both absolute and relative targets).
    case "${LINK_TARGET}" in
        "${VERSIONED_DIR}"|"%{cloudberry_name}-%{version}")
            rm -f -- "${LINK_PATH}" || :
            ;;
    esac
fi

exit 0

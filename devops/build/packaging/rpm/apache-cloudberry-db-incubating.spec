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

%define cloudberry_install_dir /usr/local/cloudberry-db

# Add at the top of the spec file
# Default to non-debug build
%bcond_with debug

# Conditional stripping based on debug flag
%if %{with debug}
%define __os_install_post %{nil}
%define __strip /bin/true
%endif

Name:           apache-cloudberry-db-incubating
Version:        %{version}
# In the release definition section
%if %{with debug}
Release:        %{release}.debug%{?dist}
%else
Release:        %{release}%{?dist}
%endif
Summary:        High-performance, open-source data warehouse based on PostgreSQL/Greenplum

License:        ASL 2.0
URL:            https://cloudberry.apache.org
Vendor:         Apache Cloudberry (Incubating)
Group:          Applications/Databases
Prefix:         %{cloudberry_install_dir}

# Disabled as we are shipping GO programs (e.g. gpbackup)
%define _missing_build_ids_terminate_build 0

# Disable debugsource files
%define _debugsource_template %{nil}

# List runtime dependencies

Requires:       bash
Requires:       iproute
Requires:       iputils
Requires:       openssh
Requires:       openssh-clients
Requires:       openssh-server
Requires:       rsync

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
# No prep needed for binary RPM

%install
rm -rf %{buildroot}

# Create the versioned directory
mkdir -p %{buildroot}%{cloudberry_install_dir}-%{version}

cp -R %{cloudberry_install_dir}/* %{buildroot}%{cloudberry_install_dir}-%{version}

# Create the symbolic link
ln -sfn %{cloudberry_install_dir}-%{version} %{buildroot}%{cloudberry_install_dir}

%files
%{prefix}-%{version}
%{prefix}

%license %{cloudberry_install_dir}-%{version}/LICENSE

%debug_package

%post
# Change ownership to gpadmin.gpadmin if the gpadmin user exists
if id "gpadmin" &>/dev/null; then
    chown -R gpadmin:gpadmin %{cloudberry_install_dir}-%{version}
    chown gpadmin:gpadmin %{cloudberry_install_dir}
fi

%postun
if [ $1 -eq 0 ] ; then
  if [ "$(readlink -f "%{cloudberry_install_dir}")" == "%{cloudberry_install_dir}-%{version}" ]; then
    unlink "%{cloudberry_install_dir}" || true
  fi
fi

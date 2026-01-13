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

%global cloudberry_version %{?_cloudberry_version}%{!?_cloudberry_version:1.6}
%global cloudberry_install_dir /usr/local/cloudberry-db

Name:           apache-cloudberry-hll-incubating
Version:        2.18.0
Release:        %{?release}%{!?release:1}
Summary:        HyperLogLog extension for Apache Cloudberry %{cloudberry_version}
License:        ASL 2.0
URL:            https://github.com/citusdata/postgresql-hll
Vendor:         Apache Cloudberry (Incubating)
Group:          Applications/Databases
BuildArch:      x86_64
Requires:       apache-cloudberry-db-incubating >= %{cloudberry_version}
Prefix:         %{cloudberry_install_dir}

%description
HLL is an open-source PostgreSQL extension (compatible with Apache
Cloudberry (Incubating) %{cloudberry_version}) adding HyperLogLog data
structures as a native data type. HyperLogLog is a fixed-size,
set-like structure used for distinct value counting with tunable
precision.

%prep
# No prep needed for binary RPM

%build
# No build needed for binary RPM

%install
mkdir -p %{buildroot}%{prefix}/lib/postgresql \
         %{buildroot}%{prefix}/share/postgresql/extension

cp -R %{cloudberry_install_dir}/lib/postgresql/hll.so \
      %{buildroot}%{prefix}/lib/postgresql/hll.so

cp -R %{cloudberry_install_dir}/share/postgresql/extension/hll* \
      %{buildroot}%{prefix}/share/postgresql/extension

%files
%{prefix}/lib/postgresql/hll.so
%{prefix}/share/postgresql/extension/hll--*.sql
%{prefix}/share/postgresql/extension/hll.control

%post
echo "HLL extension for Apache Cloudberry %{cloudberry_version} has been installed in %{prefix}."
echo "To enable it in a database, run:"
echo "  CREATE EXTENSION hll;"

%postun
echo "HLL extension for Apache Cloudberry %{cloudberry_version} has been removed from %{prefix}."
echo "You may need to manually clean up any database objects that were using the extension."

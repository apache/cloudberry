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

Name:           cloudberry-dev-repo
Version:        1.0
Release:        1%{?dist}
Summary:        Apache Cloudberry Repository Configuration
License:        ASL 2.0
Group:          Applications/Databases
URL:            https://cloudberry.apache.org
Vendor:         Cloudberry Open Source
BuildArch:      noarch

%description
This package configures the Apache Cloudberry repository on your
system. Apache Cloudberry is an open-source project aimed at
providing a scalable, high-performance SQL database for
analytics. This repository provides access to the latest RPM packages
for Apache Cloudberry, allowing you to easily install and stay
up-to-date with the latest developments.

%install
mkdir -p %{buildroot}%{_sysconfdir}/yum.repos.d/
cat > %{buildroot}%{_sysconfdir}/yum.repos.d/cloudberry-dev.repo <<EOF
[cloudberry-dev]
name=Apache Cloudberry Repository
baseurl=https://cloudberry-rpm-dev-bucket.s3.amazonaws.com/repo/el%{rhel}/x86_64/
enabled=1
gpgcheck=1
gpgkey=https://cloudberry-rpm-dev-bucket.s3.amazonaws.com/repo/el%{rhel}/x86_64/RPM-GPG-KEY-cloudberry
EOF

%files
%{_sysconfdir}/yum.repos.d/cloudberry-dev.repo

%changelog
* Thu Aug 21 2024 Ed Espino  eespino@gmail.com - 1.0-1
- Initial package with repository configuration

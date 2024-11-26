%define _rpmfilename %%{ARCH}/%%{NAME}-%{cbdb_version}-%%{RELEASE}.%%{ARCH}.rpm
%define bin_cbdb %{prefix}/%{name}-%{cbdb_version}

# Disable build-id in generated RPM
%define _build_id_links none

%{!?cbdb_install_prefix: %define cbdb_install_prefix /usr/local}

Name:		cloudberry-db
Version:	%{version}
Release:	1%{?dist}
Summary:	A High Performance Massively Parallel Data Warehouse

Group:		Applications/Databases
License:	Apache License Version 2.0
URL:		http://cloudberry.apache.org
Source0:	bin_cbdb.tar.gz
Prefix: 	%{cbdb_install_prefix}
AutoReqProv: 	no

Requires: apr apr-util
Requires: bash
Requires: bzip2
Requires: curl
Requires: iproute
Requires: krb5-devel
Requires: less
Requires: libcurl
Requires: libxml2
Requires: libyaml
Requires: openldap
Requires: openssh
Requires: openssh-clients
Requires: openssh-server
Requires: openssl
Requires: openssl-libs
Requires: perl
Requires: python3-devel
Requires: readline
Requires: rsync
Requires: sed
Requires: tar
Requires: which
Requires: zip
Requires: zlib

%description
A High Performance Massively Parallel Data Warehouse

%prep
%setup -q -c -n %{name}-%{cbdb_version}

%install
mkdir -p %{buildroot}/%{prefix}/%{name}-%{cbdb_version}
cp -R * %{buildroot}/%{prefix}/%{name}-%{cbdb_version}
exit 0

%files
%{prefix}/%{name}-%{cbdb_version}
%config(noreplace) %{prefix}/%{name}-%{cbdb_version}/greenplum_path.sh

%clean
rm -rf %{buildroot}

%post
ln -sT "${RPM_INSTALL_PREFIX}/%{name}-%{cbdb_version}" "${RPM_INSTALL_PREFIX}/%{name}" || true

%postun
if [ $1 -eq 0 ] ; then
  if [ "$(readlink -f "${RPM_INSTALL_PREFIX}/%{name}")" == "${RPM_INSTALL_PREFIX}/%{name}-%{cbdb_version}" ]; then
    unlink "${RPM_INSTALL_PREFIX}/%{name}" || true
  fi
fi

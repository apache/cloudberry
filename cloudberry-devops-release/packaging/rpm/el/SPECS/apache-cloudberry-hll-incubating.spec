%global cloudberry_version %{?_cloudberry_version}%{!?_cloudberry_version:1.6}
%global cloudberry_install_dir /usr/local/cloudberry-db

Name:           apache-cloudberry-hll-incubating
Version:        2.18.0
Release:        %{?release}%{!?release:1}
Summary:        HyperLogLog extension for Cloudberry Database %{cloudberry_version}
License:        ASL 2.0
URL:            https://github.com/citusdata/postgresql-hll
Vendor:         Apache Cloudberry (incubating)
Group:          Applications/Databases
BuildArch:      x86_64
Requires:       apache-cloudberry-db-incubating >= %{cloudberry_version}
Prefix:         %{cloudberry_install_dir}

%description
HLL is an open-source PostgreSQL extension (compatible with Apache
Cloudberry (incubating) %{cloudberry_version}) adding HyperLogLog data
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
echo "HLL extension for Cloudberry Database %{cloudberry_version} has been installed in %{prefix}."
echo "To enable it in a database, run:"
echo "  CREATE EXTENSION hll;"

%postun
echo "HLL extension for Cloudberry Database %{cloudberry_version} has been removed from %{prefix}."
echo "You may need to manually clean up any database objects that were using the extension."

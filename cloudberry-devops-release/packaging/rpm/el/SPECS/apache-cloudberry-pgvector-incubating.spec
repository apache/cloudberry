%global cloudberry_version %{?_cloudberry_version}%{!?_cloudberry_version:1.6}
%global cloudberry_install_dir /usr/local/cloudberry-db
%global pgvector_version %{?_pgvector_version}%{!?_pgvector_version:0.5.1}

Name:           cloudberry-pgvector
Version:        %{pgvector_version}
Release:        %{?release}%{!?release:1}
Summary:        pgvector extension for Cloudberry Database %{cloudberry_version}
License:        PostgreSQL
URL:            https://github.com/pgvector/pgvector
Vendor:         Cloudberry Open Source
Group:          Applications/Databases
BuildArch:      x86_64
Requires:       cloudberry-db >= %{cloudberry_version}
Prefix:         %{cloudberry_install_dir}

%description
pgvector is an open-source vector similarity search extension for
PostgreSQL and Cloudberry Database %{cloudberry_version}.  It provides
vector data types and vector similarity search functions, allowing for
efficient similarity search operations on high-dimensional data.

%prep
# No prep needed for binary RPM

%build
# No build needed for binary RPM

%install
mkdir -p %{buildroot}%{prefix}/include/postgresql/server/extension/vector \
         %{buildroot}%{prefix}/lib/postgresql                             \
         %{buildroot}%{prefix}/share/postgresql/extension
cp -R %{cloudberry_install_dir}/include/postgresql/server/extension/vector/* \
      %{buildroot}%{prefix}/include/postgresql/server/extension/vector
cp -R %{cloudberry_install_dir}/lib/postgresql/vector.so \
      %{buildroot}%{prefix}/lib/postgresql/vector.so
cp -R %{cloudberry_install_dir}/share/postgresql/extension/vector* \
      %{buildroot}%{prefix}/share/postgresql/extension

%files
%{prefix}/include/postgresql/server/extension/vector/*
%{prefix}/lib/postgresql/vector.so
%{prefix}/share/postgresql/extension/vector--*.sql
%{prefix}/share/postgresql/extension/vector.control

%post
echo "pgvector extension version %{version} for Cloudberry Database %{cloudberry_version} has been installed in %{prefix}."
echo "To enable it in a database, run:"
echo "  CREATE EXTENSION vector;"

%postun
echo "pgvector extension version %{version} for Cloudberry Database %{cloudberry_version} has been removed from %{prefix}."
echo "You may need to manually clean up any database objects that were using the extension."

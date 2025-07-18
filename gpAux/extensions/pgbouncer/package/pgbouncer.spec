Summary:        Connection pool for Greengage Database 
License:        BSD License        
Name:           pgbouncer
Version:        %{pgbouncer_ver}
Release:        %{pgbouncer_rel}
Group:          Development/Tools
Prefix:         /temp
AutoReq:        no
AutoProv:       no
Provides:       pgbouncer = %{pgbouncer_ver} 

%description
The Pgbouncer package provides connection pool for the Greengage Database.

%install
mkdir -p %{buildroot}/temp/bin %{buildroot}/temp/lib
cp -f %{pgbouncer_dir}/source/pgbouncer %{buildroot}/temp/bin
#cp -f %{pgbouncer_dir}/install/lib/libevent-2.0.so.5 %{buildroot}/temp/lib
#cp -f %{pgbouncer_dir}/stunnel/bin/stunnel %{buildroot}/temp/bin

%files
/temp

--
-- Test CONNECTION LIMITs on databases.
--

drop database if exists limitdb;
drop database if exists "limit_evil_'""_db";
drop user if exists connlimit_test_user;

create database limitdb connection limit 1;
alter database limitdb connection limit 2;
select datconnlimit from pg_database where datname='limitdb';

-- Test that the limit works.
alter database limitdb connection limit 0;

create user connlimit_test_user; -- superusers are exempt from limits

-- should fail, because the connection limit is 0
\! psql limitdb -c "select 'connected'" -U connlimit_test_user

drop user connlimit_test_user;

-- Test ALTER DATABASE with funny characters. (There used to be a quoting
-- bug in dispatching ALTER DATABASE .. CONNECTION LIMIT.)
alter database limitdb rename to "limit_evil_'""_db";
alter database "limit_evil_'""_db" connection limit 3;
select datname, datconnlimit from pg_database where datname like 'limit%db';

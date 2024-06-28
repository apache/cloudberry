create extension perfmon;
select count(*) from pg_extension where extname = 'perfmon';
\c gpperfmon
select count(*) from pg_extension where extname = 'perfmon';

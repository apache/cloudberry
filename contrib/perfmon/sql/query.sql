select sess_id from pg_stat_activity where pg_backend_pid()=pid;
\gset
create table test(a int);
select * from test;
select pg_sleep(18);
\c gpperfmon
select ssid, pid, ccnt, status, query_text from queries_now where ssid = :sess_id;
\c contrib_regression
drop table test;

create extension diskquota;
select pg_sleep(1);
\! pg_ctl -D /tmp/pg_diskquota_test/data reload
\! cp data/csmall.txt /tmp/csmall.txt
select pg_sleep(5);

-- prepare a schema that has reached quota limit
create schema badquota;
select diskquota.set_schema_quota('badquota', '1 MB');
create role testbody;
create table badquota.t1(i int);
alter table badquota.t1 owner to testbody;
insert into badquota.t1 select generate_series(0, 50000);
select pg_sleep(5);
insert into badquota.t1 select generate_series(0, 10);

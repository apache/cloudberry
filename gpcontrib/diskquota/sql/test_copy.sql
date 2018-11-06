-- Test copy
create schema s3;
select diskquota.set_schema_quota('s3', '1 MB');
set search_path to s3;

create table c (i int);
copy c from '/tmp/csmall.txt';
-- expect failed 
insert into c select generate_series(1,100000000);
select pg_sleep(5);
-- select pg_total_table_size('c');
-- expect copy fail
copy c from '/tmp/csmall.txt';

drop table c;
reset search_path;
drop schema s3;

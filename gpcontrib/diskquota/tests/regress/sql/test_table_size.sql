-- Test tablesize table

create table a(i text) DISTRIBUTED BY (i);

insert into a select * from generate_series(1,10000);

SELECT diskquota.wait_for_worker_new_epoch();
select pg_table_size('a') as table_size;
\gset
select :table_size = diskquota.table_size.size from diskquota.table_size where tableid = 'a'::regclass and segid=-1;


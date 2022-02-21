-- Test tablesize table

create table a(i text) DISTRIBUTED BY (i);

insert into a select * from generate_series(1,10000);

select pg_sleep(2);
create table buffer(oid oid, relname name, size bigint) DISTRIBUTED BY (oid);

insert into buffer select oid, relname, sum(pg_table_size(oid)) from gp_dist_random('pg_class') where oid > 16384 and (relkind='r' or relkind='m') and relname = 'a' group by oid, relname;

select sum(buffer.size) = diskquota.table_size.size from buffer, diskquota.table_size where buffer.oid = diskquota.table_size.tableid group by diskquota.table_size.size;

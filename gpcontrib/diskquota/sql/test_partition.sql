-- Test partition table
create schema s8;
select diskquota.set_schema_quota('s8', '1 MB');
set search_path to s8;
CREATE TABLE measurement (
    city_id         int not null,
    logdate         date not null,
    peaktemp        int,
    unitsales       int
)PARTITION BY RANGE (logdate);
CREATE TABLE measurement_y2006m02 PARTITION OF measurement
    FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');

CREATE TABLE measurement_y2006m03 PARTITION OF measurement
    FOR VALUES FROM ('2006-03-01') TO ('2006-04-01');
insert into measurement select generate_series(1,15000), '2006-02-01' ,1,1;
select pg_sleep(5);
insert into measurement select 1, '2006-02-01' ,1,1;
-- expect insert fail
insert into measurement select generate_series(1,100000000), '2006-03-02' ,1,1;
-- expect insert fail
insert into measurement select 1, '2006-02-01' ,1,1;
-- expect insert fail
insert into measurement select 1, '2006-03-03' ,1,1;
delete from measurement where logdate='2006-03-02';
vacuum full measurement;
select pg_sleep(5);
insert into measurement select 1, '2006-02-01' ,1,1;
insert into measurement select 1, '2006-03-03' ,1,1;

drop table measurement;
reset search_path;
drop schema s8;


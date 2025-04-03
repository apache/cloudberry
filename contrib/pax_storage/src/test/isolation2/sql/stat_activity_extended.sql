-- pg_stat_activity_extended is only used in cloud with warehouse
-- In CBDB, pg_stat_activity_extended warehouse_id should always be NULL

create table test_activity(a int);

0: insert into test_activity select * from generate_series(1,100);

-- in CBDB QD/QE warehouse_id must be NULL

SELECT warehouse_id, query from pg_stat_activity_extended WHERE query LIKE 'insert into test_activity select * from generate_series(1,100);';

SELECT warehouse_id, query from gp_dist_random('pg_stat_activity_extended') WHERE query LIKE 'insert into test_activity select * from generate_series(1,100);' limit 1;

drop table test_activity;
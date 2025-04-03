-- start_matchignore
-- m/LOG:  statement:/
-- m/no filter/
-- m/bloom filter/
-- m/No sparse filter/
-- end_matchignore

set default_table_access_method to pax;
set pax_enable_debug to on;
set pax_enable_sparse_filter to on;

create table t1(same int, v1 int, v2 int, v3 int, v4 int) using pax with (minmax_columns='v1,v2,v3,v4');
create table t2(same int, v1 int, v2 int, v3 int, v4 int) using pax with (minmax_columns='v1,v2,v3,v4');

-- two file with minmax(1,400) and minmax(401,800)
insert into t1 values(1, generate_series(1, 100), generate_series(101, 200), generate_series(201, 300), generate_series(301, 400));
insert into t1 values(1, generate_series(401, 500), generate_series(501, 600), generate_series(601, 700), generate_series(701, 800));

insert into t2 values(1, generate_series(1, 100), generate_series(101, 200), generate_series(201, 300), generate_series(301, 400));
insert into t2 values(1, generate_series(401, 500), generate_series(501, 600), generate_series(601, 700), generate_series(701, 800));

analyze t1;
analyze t2;

-- ORCA plan is poor
set optimizer = off; 

-- query with quals list which size is > 1
set client_min_messages to log;
WITH t1_cte AS (
        SELECT v1, v2, v3
        FROM t1 where v2 > 0
)
SELECT t2.*
FROM t2
join t1_cte on t1_cte.v1 = t2.v1 
WHERE 
	t1_cte.v2 < 100
    and ((t1_cte.v1 = 42 AND t2.v1 = 43) OR (t1_cte.v1 = 44 AND t2.v2 = 45)) 
    and t2.v1 < 90;

reset client_min_messages;
reset optimizer;

drop table t1;
drop table t2;

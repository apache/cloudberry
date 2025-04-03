
-- start_matchignore
-- m/LOG:  statement:/
-- m/no filter/
-- m/bloom filter/
-- m/No sparse filter/
-- end_matchignore

set default_table_access_method to pax;
set pax_enable_debug to on;
set pax_enable_sparse_filter to on;

create  or replace function intrc(iint int)
 returns int as $$ 
 begin return iint; end;
 $$ language plpgsql;

create  or replace function falserc(iint int)
 returns bool as $$ 
 begin return false; end;
 $$ language plpgsql;

create table t1(same int, v1 int, v2 int, v3 int, v4 int) using pax with (minmax_columns='v1,v2,v3,v4');
create table t2(same int, v1 int, v2 int, v3 int, v4 int) using pax with (minmax_columns='v1,v2,v3,v4');
create table t_allnull(v1 int, v2 int, v3 int, v4 int) using pax with (minmax_columns='v1,v2,v3,v4');

-- two file with minmax(1,400) and minmax(401,800)
insert into t1 values(1, generate_series(1, 100), generate_series(101, 200), generate_series(201, 300), generate_series(301, 400));
insert into t1 values(1, generate_series(401, 500), generate_series(501, 600), generate_series(601, 700), generate_series(701, 800));

insert into t2 values(1, generate_series(1, 100), generate_series(101, 200), generate_series(201, 300), generate_series(301, 400));
insert into t2 values(1, generate_series(401, 500), generate_series(501, 600), generate_series(601, 700), generate_series(701, 800));

insert into t_allnull select null from generate_series(1,100);
insert into t_allnull select null from generate_series(1,100);
analyze t1;
analyze t2;
analyze t_allnull;

set client_min_messages to log;

-- no filter
select count(*) from t1;
select count(v1) from t1;
select count(1) from t1;

select count(*) from t_allnull;
select count(v1) from t_allnull;
select count(1) from t_allnull;

-- basic tests
select count(*) from t1 where v1 > 1 and v1 < 10;
select count(*) from t1 where v1 >= 1 and v1 <= 10;
select count(*) from t1 where v1 > 1 and v2 < 110;
select count(*) from t1 where v1 >= 1 and v2 <= 110;
select count(*) from t1 where v1 is not null;
select count(*) from t1 where v1 is null;

select count(*) from t1 where (v1 < 10 and intrc(v2) in (101, 1223, 321));

select count(*) from t1 where v1 > 90 or v1 < 10;
select count(*) from t1 where v1 >= 90 or v1 <= 10;
select count(*) from t1 where v1 > 90 or v2 < 110;
select count(*) from t1 where v1 >= 90 or v2 <= 110;

select count(*) from t1 where v1 = 1 and v2 < 110;
select count(*) from t1 where v1 > 1 and v2 = 110;
select count(*) from t1 where v1 = 1 or v2 < 110;
select count(*) from t1 where v1 > 1 or v2 = 110;

-- invert the var and const(pg optimizer won't invert expr in plan)
select count(*) from t1 where 1 < v1 and 10 > v1;
select count(*) from t1 where 1 <= v1 and 10 >= v1;
select count(*) from t1 where 1 < v1 and 110 > v2;
select count(*) from t1 where 1 <= v1 and 110 >= v2;

select count(*) from t1 where 90 < v1 or 10 > v1;
select count(*) from t1 where 90 <= v1 or 10 >= v1;
select count(*) from t1 where 90 < v1 or 110 > v2;
select count(*) from t1 where 90 <= v1 or 110 >= v2;

select count(*) from t1 where 1 = v1 and 110 > v2;
select count(*) from t1 where 1 < v1 and 110 = v2;

-- nested exprs
select count(*) from t1 where v1 > 90 and (v1 > 10 or v1 < 20);
select count(*) from t1 where v1 > 10 and (v2 > 110 or v2 < 120);

select count(*) from t1 where v1 > 90 or (v1 > 10 and v1 < 20);
select count(*) from t1 where v1 > 10 or (v2 > 110 and v2 < 120);

select count(*) from t1 where v1 > 90 or (v1 > 10 and (v1 < 20 or v2 < 120));
select count(*) from t1 where v1 > 90 and (v1 > 10 or (v1 < 20 and v2 < 120));
select count(*) from t1 where v1 > 90 and (v1 > 10 or (v1 < 20 and intrc(v2) < 120));

select count(*) from t_allnull where v1 is not null;
select count(*) from t_allnull where v1 is null and v2 is not null;
select count(*) from t_allnull where v1 is null or (v2 is null and v3 is not null);

select count(*) from t1 where falserc(v1);

-- varop
select count(*) from t1 where v1 < v2;
select count(*) from t1 where v2 > v1;

select count(*) from t1 where v1 > v2;
select count(*) from t1 where v1 >= v2;

select count(*) from t1 where v2 < v1;
select count(*) from t1 where v2 <= v1;

select count(*) from t1 where v1 = v2;
select count(*) from t1 where v2 = v1;

select count(*) from t1 where v1 < v1; -- stupid case, but still support
select count(*) from t1 where v1 > v1; -- stupid case, but still support

-- var +/-/* const
select count(*) from t1 where v1 - 10 > 100;
select count(*) from t1 where v1 + 10 > 100;

select count(*) from t1 where v1 - 10 > v2;
select count(*) from t1 where v1 + 10 > v2;

select count(*) from t1 where v1 - 10 > (v2 + 100) - 20;
select count(*) from t1 where v1 + 10 > (v2 + 200) - 10;

select count(*) from t1 where v1 + v2 > 1000;
select count(*) from t1 where v1 + v2 < 1000;
select count(*) from t1 where v1 + v2 < v3;

select count(*) from t1 where intrc(v1) + 10 > v2;

-- simply the filter tree
set pax_log_filter_tree to on;
select count(*) from t1 where v1 > 10 or intrc(v2) < 120;
select count(*) from t1 where v1 > 10 and intrc(v2) < 120;
select count(*) from t1 where v1 is not null;
reset pax_log_filter_tree;

reset client_min_messages;

-- IN && bloom filter
create table t_bf(same int, v1 int, v2 int, v3 int, v4 int) using pax with (minmax_columns='v1,v2,v3,v4', bloomfilter_columns='v1,v2,v3,v4');
insert into t_bf values(1, generate_series(1, 100), generate_series(101, 200), generate_series(201, 300), generate_series(301, 400));
insert into t_bf values(1, generate_series(401, 500), generate_series(501, 600), generate_series(601, 700), generate_series(701, 800));

set client_min_messages to log;

select count(*) from t_bf where v1 in (3, 9, -1, '3'::float);
select count(*) from t_bf where v1 in (1000, 10001);
select count(*) from t_bf where v1 in (11, 10001);
select count(*) from t_bf where v1 not in (1000, 10001);
select count(*) from t1 where v1 in (1000, 10001, NULL);
select count(*) from t1 where v1 not in (1000, 10001);
select count(*) from t1 where not (v2 < 1000 and v1 in (1000, 10001));

-- List<> quals
select count(*) from t1 left join t2 on t1.v1 = t2.v1 where t1.v1 > 1 and t2.v1 < 10;

-- coalesce, not support yet 
select count(*) from t1 where coalesce(v1, 2) != 1;

reset client_min_messages;

drop table t1;
drop table t2;
drop table t_allnull;
drop table t_bf;

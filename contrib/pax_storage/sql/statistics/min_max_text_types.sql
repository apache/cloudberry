-- start_matchignore
-- m/LOG:  statement:/
-- m/no filter/
-- m/scan key build success/
-- m/Feature not supported/
-- end_matchignore

set default_table_access_method to pax;
set pax_enable_debug to on;
set pax_enable_sparse_filter to on;
set pax_max_tuples_per_group to 5;

-- 
-- Test the text min/max types support 
-- 
create table t_text(i int, v text) with(minmax_columns='v');
insert into t_text(i, v) values (1, 'pfa'),(1, 'pfb'),(1, 'pfc'),(1, 'pfd'),(1, 'pfe'), 
(1, 'pff'),(1, 'pfg'),(1, 'pfh'),(1, 'pfi'),(1, 'pfj');
insert into t_text(i, v) values (1, 'pfk'),(1, 'pfl'),(1, 'pfm'),(1, 'pfn'),(1, 'pfo'), 
(1, 'pfp'),(1, 'pfq'),(1, 'pfr'),(1, 'pfs'),(1, 'pft');

set client_min_messages to log;
select count(*) from t_text where v > 'pf9'::text;
select count(*) from t_text where v > 'pfe'::text;
select count(*) from t_text where v > 'pfj'::text;
select count(*) from t_text where v > 'pfo'::text;
select count(*) from t_text where v > 'pft'::text;

select count(*) from t_text where v < 'pf9'::text;
select count(*) from t_text where v < 'pff'::text;
select count(*) from t_text where v < 'pfk'::text;
select count(*) from t_text where v < 'pfp'::text;
select count(*) from t_text where v < 'pfz'::text;

select count(*) from t_text where v = 'pf9'::text;
select count(*) from t_text where v = 'pfe'::text;
select count(*) from t_text where v = 'pfj'::text;
select count(*) from t_text where v = 'pfk'::text;
select count(*) from t_text where v = 'pft'::text;
select count(*) from t_text where v = 'pfz'::text;

select count(*) from t_text where v >= 'pf9'::text;
select count(*) from t_text where v >= 'pfa'::text;
select count(*) from t_text where v >= 'pff'::text;
select count(*) from t_text where v >= 'pfk'::text;
select count(*) from t_text where v >= 'pfp'::text;
select count(*) from t_text where v >= 'pfz'::text;

select count(*) from t_text where v <= 'pf9'::text;
select count(*) from t_text where v <= 'pfe'::text;
select count(*) from t_text where v <= 'pfj'::text;
select count(*) from t_text where v <= 'pfo'::text;
select count(*) from t_text where v <= 'pft'::text;

-- oper(text, varchar)
select count(*) from t_text where v > 'pf9'::varchar;
select count(*) from t_text where v > 'pfe'::varchar;
select count(*) from t_text where v > 'pfj'::varchar;
select count(*) from t_text where v > 'pfo'::varchar;
select count(*) from t_text where v > 'pft'::varchar;

select count(*) from t_text where v < 'pf9'::varchar;
select count(*) from t_text where v < 'pff'::varchar;
select count(*) from t_text where v < 'pfk'::varchar;
select count(*) from t_text where v < 'pfp'::varchar;
select count(*) from t_text where v < 'pfz'::varchar;

select count(*) from t_text where v = 'pf9'::varchar;
select count(*) from t_text where v = 'pfe'::varchar;
select count(*) from t_text where v = 'pfj'::varchar;
select count(*) from t_text where v = 'pfk'::varchar;
select count(*) from t_text where v = 'pft'::varchar;
select count(*) from t_text where v = 'pfz'::varchar;

select count(*) from t_text where v >= 'pf9'::varchar;
select count(*) from t_text where v >= 'pfa'::varchar;
select count(*) from t_text where v >= 'pff'::varchar;
select count(*) from t_text where v >= 'pfk'::varchar;
select count(*) from t_text where v >= 'pfp'::varchar;
select count(*) from t_text where v >= 'pfz'::varchar;

select count(*) from t_text where v <= 'pf9'::varchar;
select count(*) from t_text where v <= 'pfe'::varchar;
select count(*) from t_text where v <= 'pfj'::varchar;
select count(*) from t_text where v <= 'pfo'::varchar;
select count(*) from t_text where v <= 'pft'::varchar;

-- oper(text, name)
select count(*) from t_text where v > 'pf9'::name;
select count(*) from t_text where v > 'pfe'::name;
select count(*) from t_text where v > 'pfj'::name;
select count(*) from t_text where v > 'pfo'::name;
select count(*) from t_text where v > 'pft'::name;

select count(*) from t_text where v < 'pf9'::name;
select count(*) from t_text where v < 'pff'::name;
select count(*) from t_text where v < 'pfk'::name;
select count(*) from t_text where v < 'pfp'::name;
select count(*) from t_text where v < 'pfz'::name;

select count(*) from t_text where v = 'pf9'::name;
select count(*) from t_text where v = 'pfe'::name;
select count(*) from t_text where v = 'pfj'::name;
select count(*) from t_text where v = 'pfk'::name;
select count(*) from t_text where v = 'pft'::name;
select count(*) from t_text where v = 'pfz'::name;

select count(*) from t_text where v >= 'pf9'::name;
select count(*) from t_text where v >= 'pfa'::name;
select count(*) from t_text where v >= 'pff'::name;
select count(*) from t_text where v >= 'pfk'::name;
select count(*) from t_text where v >= 'pfp'::name;
select count(*) from t_text where v >= 'pfz'::name;

select count(*) from t_text where v <= 'pf9'::name;
select count(*) from t_text where v <= 'pfe'::name;
select count(*) from t_text where v <= 'pfj'::name;
select count(*) from t_text where v <= 'pfo'::name;
select count(*) from t_text where v <= 'pft'::name;


-- oper(text, name)
select count(*) from t_text where v > 'pf9'::bpchar;
select count(*) from t_text where v > 'pfe'::bpchar;
select count(*) from t_text where v > 'pfj'::bpchar;
select count(*) from t_text where v > 'pfo'::bpchar;
select count(*) from t_text where v > 'pft'::bpchar;

select count(*) from t_text where v < 'pf9'::bpchar;
select count(*) from t_text where v < 'pff'::bpchar;
select count(*) from t_text where v < 'pfk'::bpchar;
select count(*) from t_text where v < 'pfp'::bpchar;
select count(*) from t_text where v < 'pfz'::bpchar;

select count(*) from t_text where v = 'pf9'::bpchar;
select count(*) from t_text where v = 'pfe'::bpchar;
select count(*) from t_text where v = 'pfj'::bpchar;
select count(*) from t_text where v = 'pfk'::bpchar;
select count(*) from t_text where v = 'pft'::bpchar;
select count(*) from t_text where v = 'pfz'::bpchar;

select count(*) from t_text where v >= 'pf9'::bpchar;
select count(*) from t_text where v >= 'pfa'::bpchar;
select count(*) from t_text where v >= 'pff'::bpchar;
select count(*) from t_text where v >= 'pfk'::bpchar;
select count(*) from t_text where v >= 'pfp'::bpchar;
select count(*) from t_text where v >= 'pfz'::bpchar;

select count(*) from t_text where v <= 'pf9'::bpchar;
select count(*) from t_text where v <= 'pfe'::bpchar;
select count(*) from t_text where v <= 'pfj'::bpchar;
select count(*) from t_text where v <= 'pfo'::bpchar;
select count(*) from t_text where v <= 'pft'::bpchar;

reset client_min_messages;
drop table t_text;


-- 
-- Test the varchar min/max types support 
-- 
create table t_varchar(i int, v varchar) with(minmax_columns='v');
insert into t_varchar(i, v) values (1, 'pfa'),(1, 'pfb'),(1, 'pfc'),(1, 'pfd'),(1, 'pfe'),
(1, 'pff'),(1, 'pfg'),(1, 'pfh'),(1, 'pfi'),(1, 'pfj');
insert into t_varchar(i, v) values (1, 'pfk'),(1, 'pfl'),(1, 'pfm'),(1, 'pfn'),(1, 'pfo'),
(1, 'pfp'),(1, 'pfq'),(1, 'pfr'),(1, 'pfs'),(1, 'pft');

set client_min_messages to log;

select count(*) from t_varchar where v > 'pf9'::varchar;
select count(*) from t_varchar where v > 'pfe'::varchar;
select count(*) from t_varchar where v > 'pfj'::varchar;
select count(*) from t_varchar where v > 'pfo'::varchar;
select count(*) from t_varchar where v > 'pft'::varchar;

select count(*) from t_varchar where v < 'pf9'::varchar;
select count(*) from t_varchar where v < 'pff'::varchar;
select count(*) from t_varchar where v < 'pfk'::varchar;
select count(*) from t_varchar where v < 'pfp'::varchar;
select count(*) from t_varchar where v < 'pfz'::varchar;

select count(*) from t_varchar where v = 'pf9'::varchar;
select count(*) from t_varchar where v = 'pfe'::varchar;
select count(*) from t_varchar where v = 'pfj'::varchar;
select count(*) from t_varchar where v = 'pfk'::varchar;
select count(*) from t_varchar where v = 'pft'::varchar;
select count(*) from t_varchar where v = 'pfz'::varchar;

select count(*) from t_varchar where v >= 'pf9'::varchar;
select count(*) from t_varchar where v >= 'pfa'::varchar;
select count(*) from t_varchar where v >= 'pff'::varchar;
select count(*) from t_varchar where v >= 'pfk'::varchar;
select count(*) from t_varchar where v >= 'pfp'::varchar;
select count(*) from t_varchar where v >= 'pfz'::varchar;

select count(*) from t_varchar where v <= 'pf9'::varchar;
select count(*) from t_varchar where v <= 'pfe'::varchar;
select count(*) from t_varchar where v <= 'pfj'::varchar;
select count(*) from t_varchar where v <= 'pfo'::varchar;
select count(*) from t_varchar where v <= 'pft'::varchar;

-- oper(varchar, text)
select count(*) from t_varchar where v > 'pf9'::text;
select count(*) from t_varchar where v > 'pfe'::text;
select count(*) from t_varchar where v > 'pfj'::text;
select count(*) from t_varchar where v > 'pfo'::text;
select count(*) from t_varchar where v > 'pft'::text;

select count(*) from t_varchar where v < 'pf9'::text;
select count(*) from t_varchar where v < 'pff'::text;
select count(*) from t_varchar where v < 'pfk'::text;
select count(*) from t_varchar where v < 'pfp'::text;
select count(*) from t_varchar where v < 'pfz'::text;

select count(*) from t_varchar where v = 'pf9'::text;
select count(*) from t_varchar where v = 'pfe'::text;
select count(*) from t_varchar where v = 'pfj'::text;
select count(*) from t_varchar where v = 'pfk'::text;
select count(*) from t_varchar where v = 'pft'::text;
select count(*) from t_varchar where v = 'pfz'::text;

select count(*) from t_varchar where v >= 'pf9'::text;
select count(*) from t_varchar where v >= 'pfa'::text;
select count(*) from t_varchar where v >= 'pff'::text;
select count(*) from t_varchar where v >= 'pfk'::text;
select count(*) from t_varchar where v >= 'pfp'::text;
select count(*) from t_varchar where v >= 'pfz'::text;

select count(*) from t_varchar where v <= 'pf9'::text;
select count(*) from t_varchar where v <= 'pfe'::text;
select count(*) from t_varchar where v <= 'pfj'::text;
select count(*) from t_varchar where v <= 'pfo'::text;
select count(*) from t_varchar where v <= 'pft'::text;

-- oper(varchar, name)
select count(*) from t_varchar where v > 'pf9'::name;
select count(*) from t_varchar where v > 'pfe'::name;
select count(*) from t_varchar where v > 'pfj'::name;
select count(*) from t_varchar where v > 'pfo'::name;
select count(*) from t_varchar where v > 'pft'::name;

select count(*) from t_varchar where v < 'pf9'::name;
select count(*) from t_varchar where v < 'pff'::name;
select count(*) from t_varchar where v < 'pfk'::name;
select count(*) from t_varchar where v < 'pfp'::name;
select count(*) from t_varchar where v < 'pfz'::name;

select count(*) from t_varchar where v = 'pf9'::name;
select count(*) from t_varchar where v = 'pfe'::name;
select count(*) from t_varchar where v = 'pfj'::name;
select count(*) from t_varchar where v = 'pfk'::name;
select count(*) from t_varchar where v = 'pft'::name;
select count(*) from t_varchar where v = 'pfz'::name;

select count(*) from t_varchar where v >= 'pf9'::name;
select count(*) from t_varchar where v >= 'pfa'::name;
select count(*) from t_varchar where v >= 'pff'::name;
select count(*) from t_varchar where v >= 'pfk'::name;
select count(*) from t_varchar where v >= 'pfp'::name;
select count(*) from t_varchar where v >= 'pfz'::name;

select count(*) from t_varchar where v <= 'pf9'::name;
select count(*) from t_varchar where v <= 'pfe'::name;
select count(*) from t_varchar where v <= 'pfj'::name;
select count(*) from t_varchar where v <= 'pfo'::name;
select count(*) from t_varchar where v <= 'pft'::name;

-- oper(varchar, bpchar)
select count(*) from t_varchar where v > 'pf9'::bpchar;
select count(*) from t_varchar where v > 'pfe'::bpchar;
select count(*) from t_varchar where v > 'pfj'::bpchar;
select count(*) from t_varchar where v > 'pfo'::bpchar;
select count(*) from t_varchar where v > 'pft'::bpchar;

select count(*) from t_varchar where v < 'pf9'::bpchar;
select count(*) from t_varchar where v < 'pff'::bpchar;
select count(*) from t_varchar where v < 'pfk'::bpchar;
select count(*) from t_varchar where v < 'pfp'::bpchar;
select count(*) from t_varchar where v < 'pfz'::bpchar;

select count(*) from t_varchar where v = 'pf9'::bpchar;
select count(*) from t_varchar where v = 'pfe'::bpchar;
select count(*) from t_varchar where v = 'pfj'::bpchar;
select count(*) from t_varchar where v = 'pfk'::bpchar;
select count(*) from t_varchar where v = 'pft'::bpchar;
select count(*) from t_varchar where v = 'pfz'::bpchar;

select count(*) from t_varchar where v >= 'pf9'::bpchar;
select count(*) from t_varchar where v >= 'pfa'::bpchar;
select count(*) from t_varchar where v >= 'pff'::bpchar;
select count(*) from t_varchar where v >= 'pfk'::bpchar;
select count(*) from t_varchar where v >= 'pfp'::bpchar;
select count(*) from t_varchar where v >= 'pfz'::bpchar;

select count(*) from t_varchar where v <= 'pf9'::bpchar;
select count(*) from t_varchar where v <= 'pfe'::bpchar;
select count(*) from t_varchar where v <= 'pfj'::bpchar;
select count(*) from t_varchar where v <= 'pfo'::bpchar;
select count(*) from t_varchar where v <= 'pft'::bpchar;

reset client_min_messages;
drop table t_varchar;


-- 
-- Test the bpchar min/max types support 
-- 
create table t_bpchar(i int, v bpchar) with(minmax_columns='v');
insert into t_bpchar(i, v) values (1, 'pfa'),(1, 'pfb'),(1, 'pfc'),(1, 'pfd'),(1, 'pfe'),
(1, 'pff'),(1, 'pfg'),(1, 'pfh'),(1, 'pfi'),(1, 'pfj');
insert into t_bpchar(i, v) values (1, 'pfk'),(1, 'pfl'),(1, 'pfm'),(1, 'pfn'),(1, 'pfo'),
(1, 'pfp'),(1, 'pfq'),(1, 'pfr'),(1, 'pfs'),(1, 'pft');

set client_min_messages to log;
select count(*) from t_bpchar where v > 'pf9'::bpchar;
select count(*) from t_bpchar where v > 'pfe'::bpchar;
select count(*) from t_bpchar where v > 'pfj'::bpchar;
select count(*) from t_bpchar where v > 'pfo'::bpchar;
select count(*) from t_bpchar where v > 'pft'::bpchar;

select count(*) from t_bpchar where v < 'pf9'::bpchar;
select count(*) from t_bpchar where v < 'pff'::bpchar;
select count(*) from t_bpchar where v < 'pfk'::bpchar;
select count(*) from t_bpchar where v < 'pfp'::bpchar;
select count(*) from t_bpchar where v < 'pfz'::bpchar;

select count(*) from t_bpchar where v = 'pf9'::bpchar;
select count(*) from t_bpchar where v = 'pfe'::bpchar;
select count(*) from t_bpchar where v = 'pfj'::bpchar;
select count(*) from t_bpchar where v = 'pfk'::bpchar;
select count(*) from t_bpchar where v = 'pft'::bpchar;
select count(*) from t_bpchar where v = 'pfz'::bpchar;

select count(*) from t_bpchar where v >= 'pf9'::bpchar;
select count(*) from t_bpchar where v >= 'pfa'::bpchar;
select count(*) from t_bpchar where v >= 'pff'::bpchar;
select count(*) from t_bpchar where v >= 'pfk'::bpchar;
select count(*) from t_bpchar where v >= 'pfp'::bpchar;
select count(*) from t_bpchar where v >= 'pfz'::bpchar;

select count(*) from t_bpchar where v <= 'pf9'::bpchar;
select count(*) from t_bpchar where v <= 'pfe'::bpchar;
select count(*) from t_bpchar where v <= 'pfj'::bpchar;
select count(*) from t_bpchar where v <= 'pfo'::bpchar;
select count(*) from t_bpchar where v <= 'pft'::bpchar;

-- oper(bpchar, text)
select count(*) from t_bpchar where v > 'pf9'::text;
select count(*) from t_bpchar where v > 'pfe'::text;
select count(*) from t_bpchar where v > 'pfj'::text;
select count(*) from t_bpchar where v > 'pfo'::text;
select count(*) from t_bpchar where v > 'pft'::text;

select count(*) from t_bpchar where v < 'pf9'::text;
select count(*) from t_bpchar where v < 'pff'::text;
select count(*) from t_bpchar where v < 'pfk'::text;
select count(*) from t_bpchar where v < 'pfp'::text;
select count(*) from t_bpchar where v < 'pfz'::text;

select count(*) from t_bpchar where v = 'pf9'::text;
select count(*) from t_bpchar where v = 'pfe'::text;
select count(*) from t_bpchar where v = 'pfj'::text;
select count(*) from t_bpchar where v = 'pfk'::text;
select count(*) from t_bpchar where v = 'pft'::text;
select count(*) from t_bpchar where v = 'pfz'::text;

select count(*) from t_bpchar where v >= 'pf9'::text;
select count(*) from t_bpchar where v >= 'pfa'::text;
select count(*) from t_bpchar where v >= 'pff'::text;
select count(*) from t_bpchar where v >= 'pfk'::text;
select count(*) from t_bpchar where v >= 'pfp'::text;
select count(*) from t_bpchar where v >= 'pfz'::text;

select count(*) from t_bpchar where v <= 'pf9'::text;
select count(*) from t_bpchar where v <= 'pfe'::text;
select count(*) from t_bpchar where v <= 'pfj'::text;
select count(*) from t_bpchar where v <= 'pfo'::text;
select count(*) from t_bpchar where v <= 'pft'::text;

-- oper(bpchar, name)
select count(*) from t_bpchar where v > 'pf9'::name;
select count(*) from t_bpchar where v > 'pfe'::name;
select count(*) from t_bpchar where v > 'pfj'::name;
select count(*) from t_bpchar where v > 'pfo'::name;
select count(*) from t_bpchar where v > 'pft'::name;

select count(*) from t_bpchar where v < 'pf9'::name;
select count(*) from t_bpchar where v < 'pff'::name;
select count(*) from t_bpchar where v < 'pfk'::name;
select count(*) from t_bpchar where v < 'pfp'::name;
select count(*) from t_bpchar where v < 'pfz'::name;

select count(*) from t_bpchar where v = 'pf9'::name;
select count(*) from t_bpchar where v = 'pfe'::name;
select count(*) from t_bpchar where v = 'pfj'::name;
select count(*) from t_bpchar where v = 'pfk'::name;
select count(*) from t_bpchar where v = 'pft'::name;
select count(*) from t_bpchar where v = 'pfz'::name;

select count(*) from t_bpchar where v >= 'pf9'::name;
select count(*) from t_bpchar where v >= 'pfa'::name;
select count(*) from t_bpchar where v >= 'pff'::name;
select count(*) from t_bpchar where v >= 'pfk'::name;
select count(*) from t_bpchar where v >= 'pfp'::name;
select count(*) from t_bpchar where v >= 'pfz'::name;

select count(*) from t_bpchar where v <= 'pf9'::name;
select count(*) from t_bpchar where v <= 'pfe'::name;
select count(*) from t_bpchar where v <= 'pfj'::name;
select count(*) from t_bpchar where v <= 'pfo'::name;
select count(*) from t_bpchar where v <= 'pft'::name;

-- oper(bpchar, varchar)
select count(*) from t_bpchar where v > 'pf9'::varchar;
select count(*) from t_bpchar where v > 'pfe'::varchar;
select count(*) from t_bpchar where v > 'pfj'::varchar;
select count(*) from t_bpchar where v > 'pfo'::varchar;
select count(*) from t_bpchar where v > 'pft'::varchar;

select count(*) from t_bpchar where v < 'pf9'::varchar;
select count(*) from t_bpchar where v < 'pff'::varchar;
select count(*) from t_bpchar where v < 'pfk'::varchar;
select count(*) from t_bpchar where v < 'pfp'::varchar;
select count(*) from t_bpchar where v < 'pfz'::varchar;

select count(*) from t_bpchar where v = 'pf9'::varchar;
select count(*) from t_bpchar where v = 'pfe'::varchar;
select count(*) from t_bpchar where v = 'pfj'::varchar;
select count(*) from t_bpchar where v = 'pfk'::varchar;
select count(*) from t_bpchar where v = 'pft'::varchar;
select count(*) from t_bpchar where v = 'pfz'::varchar;

select count(*) from t_bpchar where v >= 'pf9'::varchar;
select count(*) from t_bpchar where v >= 'pfa'::varchar;
select count(*) from t_bpchar where v >= 'pff'::varchar;
select count(*) from t_bpchar where v >= 'pfk'::varchar;
select count(*) from t_bpchar where v >= 'pfp'::varchar;
select count(*) from t_bpchar where v >= 'pfz'::varchar;

select count(*) from t_bpchar where v <= 'pf9'::varchar;
select count(*) from t_bpchar where v <= 'pfe'::varchar;
select count(*) from t_bpchar where v <= 'pfj'::varchar;
select count(*) from t_bpchar where v <= 'pfo'::varchar;
select count(*) from t_bpchar where v <= 'pft'::varchar;

reset client_min_messages;
drop table t_bpchar;

-- 
-- Test the name min/max types support 
-- 
create table t_name(i int, v name) with(minmax_columns='v');
insert into t_name(i, v) values (1, 'pfa'),(1, 'pfb'),(1, 'pfc'),(1, 'pfd'),(1, 'pfe'),
(1, 'pff'),(1, 'pfg'),(1, 'pfh'),(1, 'pfi'),(1, 'pfj');
insert into t_name(i, v) values (1, 'pfk'),(1, 'pfl'),(1, 'pfm'),(1, 'pfn'),(1, 'pfo'),
(1, 'pfp'),(1, 'pfq'),(1, 'pfr'),(1, 'pfs'),(1, 'pft');

set client_min_messages to log;
select count(*) from t_name where v > 'pf9'::name;
select count(*) from t_name where v > 'pfe'::name;
select count(*) from t_name where v > 'pfj'::name;
select count(*) from t_name where v > 'pfo'::name;
select count(*) from t_name where v > 'pft'::name;

select count(*) from t_name where v < 'pf9'::name;
select count(*) from t_name where v < 'pff'::name;
select count(*) from t_name where v < 'pfk'::name;
select count(*) from t_name where v < 'pfp'::name;
select count(*) from t_name where v < 'pfz'::name;

select count(*) from t_name where v = 'pf9'::name;
select count(*) from t_name where v = 'pfe'::name;
select count(*) from t_name where v = 'pfj'::name;
select count(*) from t_name where v = 'pfk'::name;
select count(*) from t_name where v = 'pft'::name;
select count(*) from t_name where v = 'pfz'::name;

select count(*) from t_name where v >= 'pf9'::name;
select count(*) from t_name where v >= 'pfa'::name;
select count(*) from t_name where v >= 'pff'::name;
select count(*) from t_name where v >= 'pfk'::name;
select count(*) from t_name where v >= 'pfp'::name;
select count(*) from t_name where v >= 'pfz'::name;

select count(*) from t_name where v <= 'pf9'::name;
select count(*) from t_name where v <= 'pfe'::name;
select count(*) from t_name where v <= 'pfj'::name;
select count(*) from t_name where v <= 'pfo'::name;
select count(*) from t_name where v <= 'pft'::name;

-- oper(name, text)
select count(*) from t_name where v > 'pf9'::text;
select count(*) from t_name where v > 'pfe'::text;
select count(*) from t_name where v > 'pfj'::text;
select count(*) from t_name where v > 'pfo'::text;
select count(*) from t_name where v > 'pft'::text;

select count(*) from t_name where v < 'pf9'::text;
select count(*) from t_name where v < 'pff'::text;
select count(*) from t_name where v < 'pfk'::text;
select count(*) from t_name where v < 'pfp'::text;
select count(*) from t_name where v < 'pfz'::text;

select count(*) from t_name where v = 'pf9'::text;
select count(*) from t_name where v = 'pfe'::text;
select count(*) from t_name where v = 'pfj'::text;
select count(*) from t_name where v = 'pfk'::text;
select count(*) from t_name where v = 'pft'::text;
select count(*) from t_name where v = 'pfz'::text;

select count(*) from t_name where v >= 'pf9'::text;
select count(*) from t_name where v >= 'pfa'::text;
select count(*) from t_name where v >= 'pff'::text;
select count(*) from t_name where v >= 'pfk'::text;
select count(*) from t_name where v >= 'pfp'::text;
select count(*) from t_name where v >= 'pfz'::text;

select count(*) from t_name where v <= 'pf9'::text;
select count(*) from t_name where v <= 'pfe'::text;
select count(*) from t_name where v <= 'pfj'::text;
select count(*) from t_name where v <= 'pfo'::text;
select count(*) from t_name where v <= 'pft'::text;

-- oper(name, bpchar)
select count(*) from t_name where v > 'pf9'::bpchar;
select count(*) from t_name where v > 'pfe'::bpchar;
select count(*) from t_name where v > 'pfj'::bpchar;
select count(*) from t_name where v > 'pfo'::bpchar;
select count(*) from t_name where v > 'pft'::bpchar;

select count(*) from t_name where v < 'pf9'::bpchar;
select count(*) from t_name where v < 'pff'::bpchar;
select count(*) from t_name where v < 'pfk'::bpchar;
select count(*) from t_name where v < 'pfp'::bpchar;
select count(*) from t_name where v < 'pfz'::bpchar;

select count(*) from t_name where v = 'pf9'::bpchar;
select count(*) from t_name where v = 'pfe'::bpchar;
select count(*) from t_name where v = 'pfj'::bpchar;
select count(*) from t_name where v = 'pfk'::bpchar;
select count(*) from t_name where v = 'pft'::bpchar;
select count(*) from t_name where v = 'pfz'::bpchar;

select count(*) from t_name where v >= 'pf9'::bpchar;
select count(*) from t_name where v >= 'pfa'::bpchar;
select count(*) from t_name where v >= 'pff'::bpchar;
select count(*) from t_name where v >= 'pfk'::bpchar;
select count(*) from t_name where v >= 'pfp'::bpchar;
select count(*) from t_name where v >= 'pfz'::bpchar;

select count(*) from t_name where v <= 'pf9'::bpchar;
select count(*) from t_name where v <= 'pfe'::bpchar;
select count(*) from t_name where v <= 'pfj'::bpchar;
select count(*) from t_name where v <= 'pfo'::bpchar;
select count(*) from t_name where v <= 'pft'::bpchar;

-- oper(name, varchar)
select count(*) from t_name where v > 'pf9'::varchar;
select count(*) from t_name where v > 'pfe'::varchar;
select count(*) from t_name where v > 'pfj'::varchar;
select count(*) from t_name where v > 'pfo'::varchar;
select count(*) from t_name where v > 'pft'::varchar;

select count(*) from t_name where v < 'pf9'::varchar;
select count(*) from t_name where v < 'pff'::varchar;
select count(*) from t_name where v < 'pfk'::varchar;
select count(*) from t_name where v < 'pfp'::varchar;
select count(*) from t_name where v < 'pfz'::varchar;

select count(*) from t_name where v = 'pf9'::varchar;
select count(*) from t_name where v = 'pfe'::varchar;
select count(*) from t_name where v = 'pfj'::varchar;
select count(*) from t_name where v = 'pfk'::varchar;
select count(*) from t_name where v = 'pft'::varchar;
select count(*) from t_name where v = 'pfz'::varchar;

select count(*) from t_name where v >= 'pf9'::varchar;
select count(*) from t_name where v >= 'pfa'::varchar;
select count(*) from t_name where v >= 'pff'::varchar;
select count(*) from t_name where v >= 'pfk'::varchar;
select count(*) from t_name where v >= 'pfp'::varchar;
select count(*) from t_name where v >= 'pfz'::varchar;

select count(*) from t_name where v <= 'pf9'::varchar;
select count(*) from t_name where v <= 'pfe'::varchar;
select count(*) from t_name where v <= 'pfj'::varchar;
select count(*) from t_name where v <= 'pfo'::varchar;
select count(*) from t_name where v <= 'pft'::varchar;

reset client_min_messages;
drop table t_name;

reset pax_enable_debug;
reset pax_enable_sparse_filter;
reset pax_max_tuples_per_group;

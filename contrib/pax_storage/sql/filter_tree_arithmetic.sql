
-- start_matchignore
-- m/LOG:  statement:/
-- m/no filter/
-- m/bloom filter/
-- m/No sparse filter/
-- end_matchignore

set default_table_access_method to pax;
set pax_enable_debug to on;
set pax_enable_sparse_filter to on;

create table t_arithmetic(same int, v1 int, v2 int, v3 int) using pax with (minmax_columns='v1,v2,v3');
create table ta_mul(same int, v1 int, v2 int, v3 int) using pax with (minmax_columns='v1,v2,v3');

-- file1: [1, 100], [101, 200], [201, 300]
-- file2: [401, 500], [501, 600], [601, 700]
insert into t_arithmetic values(1, generate_series(1, 100), generate_series(101, 200), generate_series(201, 300));
insert into t_arithmetic values(1, generate_series(401, 500), generate_series(501, 600), generate_series(601, 700));

-- [1, 100], [-100, -1], [-50, 49]
insert into ta_mul values(1, generate_series(1, 100), generate_series(-100, -1), generate_series(-50, 49));

set client_min_messages to log;

-- ArithmeticOpExpr(var,const), (var,var), (const,var)

-- column v1 file1: [-9, 90] file2:[391, 490]
-- column v2 file1: [101, 200] file2:[501, 600]
select count(*) from t_arithmetic where v1 - 10 > 0;
select count(*) from t_arithmetic where v1 - 10 > 100;
select count(*) from t_arithmetic where v1 - 10 > 90;
select count(*) from t_arithmetic where v1 - 10 > 89;
select count(*) from t_arithmetic where v1 - 10 > 491;
select count(*) from t_arithmetic where v1 - 10 > 490;
select count(*) from t_arithmetic where v1 - 10 > 489;

select count(*) from t_arithmetic where 0 < v1 - 10;
select count(*) from t_arithmetic where 100 < v1 - 10;
select count(*) from t_arithmetic where 90  < v1 - 10;
select count(*) from t_arithmetic where 89  < v1 - 10;
select count(*) from t_arithmetic where 491 < v1 - 10;
select count(*) from t_arithmetic where 490 < v1 - 10;
select count(*) from t_arithmetic where 489 < v1 - 10;

select count(*) from t_arithmetic where v1 - 10 < -10;
select count(*) from t_arithmetic where v1 - 10 < -9;
select count(*) from t_arithmetic where v1 - 10 <= -9;
select count(*) from t_arithmetic where v1 - 10 < 391;
select count(*) from t_arithmetic where v1 - 10 <= 391;
select count(*) from t_arithmetic where v1 - 10 < 392;

-- (1000 - v1) file1: [900, 999] file2:[500, 599]
select count(*) from t_arithmetic where 1000 - v1 > 999;
select count(*) from t_arithmetic where 1000 - v1 >= 999;
select count(*) from t_arithmetic where 1000 - v1 > 599;
select count(*) from t_arithmetic where 1000 - v1 >= 599;

select count(*) from t_arithmetic where 1000 - v1 < 500;
select count(*) from t_arithmetic where 1000 - v1 <= 500;
select count(*) from t_arithmetic where 1000 - v1 < 900;
select count(*) from t_arithmetic where 1000 - v1 <= 900;

select count(*) from t_arithmetic where -10 >  v1 - 10;
select count(*) from t_arithmetic where -9  >  v1 - 10;
select count(*) from t_arithmetic where -9  >= v1 - 10;
select count(*) from t_arithmetic where 391 >  v1 - 10;
select count(*) from t_arithmetic where 391 >= v1 - 10;
select count(*) from t_arithmetic where 392 >  v1 - 10;


select count(*) from t_arithmetic where v1 - 10 > v2;
select count(*) from t_arithmetic where v1 + 10 > v2;
select count(*) from t_arithmetic where v1 - 10 > v2 - 10;
select count(*) from t_arithmetic where v1 + 10 > v2 + 10;
select count(*) from t_arithmetic where v1 - 10 < v2;
select count(*) from t_arithmetic where v1 + 10 < v2;
select count(*) from t_arithmetic where v1 + 1 > v2;
select count(*) from t_arithmetic where v1 + 1 >= v2;

select count(*) from t_arithmetic where v1 > v2 - 1;
select count(*) from t_arithmetic where v1 >= v2 - 1;

-- column v1 file1: [1, 100] file2:[401, 500]
-- column v2 file1: [101, 200] file2:[501, 600]
-- (v1 - v2) file1: [-199, -1] file2:[-199, -1]
-- (v1 + v2) file1: [102, 300] file2:[902, 1100]
-- (v1 + v1) file1: [2, 200] file2:[802, 1000]
-- (v1 - v1) file1: [-99, 99] file2:[-99, 99]
select count(*) from t_arithmetic where v1 - v2 > 0;
select count(*) from t_arithmetic where v1 - v2 > -1;
select count(*) from t_arithmetic where v1 - v2 >= -1;
select count(*) from t_arithmetic where v1 - v2 > -2;

select count(*) from t_arithmetic where 0 < v1 - v2;
select count(*) from t_arithmetic where -1 < v1 - v2;
select count(*) from t_arithmetic where -1 <= v1 - v2;
select count(*) from t_arithmetic where -2 < v1 - v2;

select count(*) from t_arithmetic where v1 + v2 < 102;
select count(*) from t_arithmetic where v1 + v2 <= 102;
select count(*) from t_arithmetic where v1 + v2 < 902;
select count(*) from t_arithmetic where v1 + v2 <= 902;
select count(*) from t_arithmetic where v1 + v2 > 1100;
select count(*) from t_arithmetic where v1 + v2 >= 1100;
select count(*) from t_arithmetic where v1 + v2 > 300;
select count(*) from t_arithmetic where v1 + v2 >= 300;

select count(*) from t_arithmetic where v2 + v1 < 102;
select count(*) from t_arithmetic where v2 + v1 <= 102;
select count(*) from t_arithmetic where v2 + v1 < 902;
select count(*) from t_arithmetic where v2 + v1 <= 902;
select count(*) from t_arithmetic where v2 + v1 > 1100;
select count(*) from t_arithmetic where v2 + v1 >= 1100;
select count(*) from t_arithmetic where v2 + v1 > 300;
select count(*) from t_arithmetic where v2 + v1 >= 300;

select count(*) from t_arithmetic where v1 + v1 < 2;
select count(*) from t_arithmetic where v1 + v1 <= 2;
select count(*) from t_arithmetic where v1 + v1 < 802;
select count(*) from t_arithmetic where v1 + v1 <= 802;

select count(*) from t_arithmetic where v1 + v1 > 1000;
select count(*) from t_arithmetic where v1 + v1 >= 1000;
select count(*) from t_arithmetic where v1 + v1 > 200;
select count(*) from t_arithmetic where v1 + v1 >= 200;

select count(*) from t_arithmetic where v1 - v1 > 99;
select count(*) from t_arithmetic where v1 - v1 >= 99;
select count(*) from t_arithmetic where v1 - v1 < -99;
select count(*) from t_arithmetic where v1 - v1 <= -99;

-- column v1 file1: [-9, 90] file2:[391, 490]
select count(*) from t_arithmetic where v1 - 10 > v2 + 10; -- column v2 file1: [111, 210] file2:[511, 610]
select count(*) from t_arithmetic where v1 - 10 > v2 - 10; -- column v2 file1: [91, 190] file2:[491, 590]
select count(*) from t_arithmetic where v1 - 10 > v2 - 11; -- column v2 file1: [90, 189] file2:[490, 589]
select count(*) from t_arithmetic where v1 - 10 >= v2 - 11; -- column v2 file1: [90, 189] file2:[490, 589]
select count(*) from t_arithmetic where v1 - 10 > v2 - 12;  -- column v2 file1: [89, 188] file2:[489, 588]
select count(*) from t_arithmetic where v1 - 10 > v2 - 1000; -- column v2 min/max less than v1

-- +-* others
select count(*) from t_arithmetic where v1 - 10 > (v2 + 100) - 20;
select count(*) from t_arithmetic where v1 + 10 > (v2 + 200) - 10;
select count(*) from t_arithmetic where v1 + v2 < v3;


-- mul oper
-- 1. left = [pos, pos] right = [pos, pos]
-- min = lmin * rmin, max = lmax * rmax
-- 2. left = [pos, pos], right = [neg, pos]
--   min = lmax * rmin, max = lmax * rmax
-- 3. left = [pos, pos], right = [neg, neg]
--   min = lmax * rmin, max = lmin * rmax
-- 4. left = [neg, pos], right = [pos, pos] -- 
--   min = lmin * rmax, max = lmax * rmax
-- 5. left = [neg, pos], right = [neg, pos]
--   min = min(lmin * rmax, lmax * rmin), max = max(lmax * rmax, lmin * rmin)
-- 6. left = [neg, pos], right = [neg, neg]
--   min = lmax * rmin, max = lmin * rmin
-- 7. left = [neg, neg], right = [pos, pos]
--   min = min(lmin * rmax, lmax * rmin), max = lmax * lmin
-- 8. left = [neg, neg], right = [neg, pos]
--   min = lmin * rmax, max = lmin * rmin
-- 9. left = [neg, neg], right = [neg, neg]
--   min = lmax * rmax, max = lmin * rmin

-- ta_mul [1, 100], [-100, -1], [-50, 49]

-- case1: v1 * v1: [1, 10000] - [pos, pos] * [pos, pos]
select count(*) from ta_mul where v1 * v1 < 1;
select count(*) from ta_mul where v1 * v1 <= 1;
select count(*) from ta_mul where v1 * v1 > 10000;
select count(*) from ta_mul where v1 * v1 >= 10000;

-- case2: v1 * v3: [-5000, 4900] - [pos, pos] * [neg, pos]
select count(*) from ta_mul where v1 * v3 < -5000;
select count(*) from ta_mul where v1 * v3 <= -5000;
select count(*) from ta_mul where v1 * v3 > 4900;
select count(*) from ta_mul where v1 * v3 >= 4900;

-- case3: v1 * v2: [-10000, -1] - [pos, pos] * [neg, neg]
select count(*) from ta_mul where v1 * v2 < -10000;
select count(*) from ta_mul where v1 * v2 <= -10000;
select count(*) from ta_mul where v1 * v2 > -1;
select count(*) from ta_mul where v1 * v2 >= -1;

-- case4: v3 * v1: [-5000, 4900] - [neg, pos] * [pos, pos]
select count(*) from ta_mul where v3 * v1 < -5000;
select count(*) from ta_mul where v3 * v1 <= -5000;
select count(*) from ta_mul where v3 * v1 > 4900;
select count(*) from ta_mul where v3 * v1 >= 4900;

-- case5: v3 * v3: [-2450, 2500] - [neg, pos] * [neg, pos]
select count(*) from ta_mul where v3 * v3 < -2450;
select count(*) from ta_mul where v3 * v3 <= -2450;
select count(*) from ta_mul where v3 * v3 > 2500;
select count(*) from ta_mul where v3 * v3 >= 2500;

-- case6: v3 * v2: [-4900, 5000] - [neg, pos] * [neg, neg]
select count(*) from ta_mul where v3 * v2 < -4900;
select count(*) from ta_mul where v3 * v2 <= -4900;
select count(*) from ta_mul where v3 * v2 > 5000;
select count(*) from ta_mul where v3 * v2 >= 5000;

-- case7: v2 * v1: [-10000, -1] - [neg, neg] * [pos, pos]
select count(*) from ta_mul where v2 * v1 < -10000;
select count(*) from ta_mul where v2 * v1 <= -10000;
select count(*) from ta_mul where v2 * v1 > -1;
select count(*) from ta_mul where v2 * v1 >= -1;

-- case8: v2 * v3: [-4900, 5000] - [neg, neg] * [neg, pos]
select count(*) from ta_mul where v2 * v3 < -4900;
select count(*) from ta_mul where v2 * v3 <= -4900;
select count(*) from ta_mul where v2 * v3 > 5000;
select count(*) from ta_mul where v2 * v3 >= 5000;

-- case9: v2 * v2: [1, 10000] - [neg, neg] * [neg, neg]
select count(*) from ta_mul where v2 * v2 < 1;
select count(*) from ta_mul where v2 * v2 <= 1;
select count(*) from ta_mul where v2 * v2 > 10000;
select count(*) from ta_mul where v2 * v2 >= 10000;

reset client_min_messages;

drop table t_arithmetic;
drop table ta_mul;

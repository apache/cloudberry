-- start_matchignore
-- m/LOG:  statement:/
-- m/no filter/
-- m/scan key build success/
-- end_matchignore

set default_table_access_method to pax;
set pax_enable_debug to on;
set pax_enable_sparse_filter to on;
set pax_max_tuples_per_group to 5;

-- 
-- Test the box min/max types support 
-- 
create table t_box(i int, v box) with(minmax_columns='v');
insert into t_box(i, v) values (1, box'(0,0),(1,1)'), (1, box'(0,0),(1,2)'),(1, box'(0,0),(1,3)'),
    (1, box'(0,0),(1,4)'), (1, box'(0,0),(1,5)'),(1, box'(0,0),(1,6)'),
    (1, box'(0,0),(1,7)'), (1, box'(0,0),(1,8)'),(1, box'(0,0),(1,9)'),
    (1, box'(0,0),(1,10)');
insert into t_box(i, v) values (1, box'(0,0),(1,11)'), (1, box'(0,0),(1,12)'),(1, box'(0,0),(1,13)'),
    (1, box'(0,0),(1,14)'), (1, box'(0,0),(1,15)'),(1, box'(0,0),(1,16)'),
    (1, box'(0,0),(1,17)'), (1, box'(0,0),(1,18)'),(1, box'(0,0),(1,19)'),
    (1, box'(0,0),(1,20)');

set client_min_messages to log;
select count(*) from t_box where v > box'(0,0),(0,0)';
select count(*) from t_box where v > box'(0,0),(1,5)';
select count(*) from t_box where v > box'(0,0),(1,10)';
select count(*) from t_box where v > box'(0,0),(1,15)';
select count(*) from t_box where v > box'(0,0),(1,20)';

select count(*) from t_box where v < box'(0,0),(0,0)';
select count(*) from t_box where v < box'(0,0),(1,6)';
select count(*) from t_box where v < box'(0,0),(1,11)';
select count(*) from t_box where v < box'(0,0),(1,16)';
select count(*) from t_box where v < box'(0,0),(1,21)';

select count(*) from t_box where v = box'(0,0),(0,0)';
select count(*) from t_box where v = box'(0,0),(1,5)';
select count(*) from t_box where v = box'(0,0),(1,10)';
select count(*) from t_box where v = box'(0,0),(1,11)';
select count(*) from t_box where v = box'(0,0),(1,20)';
select count(*) from t_box where v = box'(0,0),(1,21)';

select count(*) from t_box where v >= box'(0,0),(0,0)';
select count(*) from t_box where v >= box'(0,0),(1,6)';
select count(*) from t_box where v >= box'(0,0),(1,11)';
select count(*) from t_box where v >= box'(0,0),(1,16)';
select count(*) from t_box where v >= box'(0,0),(1,21)';

select count(*) from t_box where v <= box'(0,0),(0,0)';
select count(*) from t_box where v <= box'(0,0),(1,5)';
select count(*) from t_box where v <= box'(0,0),(1,10)';
select count(*) from t_box where v <= box'(0,0),(1,15)';
select count(*) from t_box where v <= box'(0,0),(1,20)';
reset client_min_messages;
drop table t_box;

-- 
-- Test the lseg min/max types support 
-- 
create table t_lseg(i int, v lseg) with(minmax_columns='v');
insert into t_lseg(i, v) values (1, lseg'(0,0),(1,1)'), (1, lseg'(0,0),(1,2)'),(1, lseg'(0,0),(1,3)'),
    (1, lseg'(0,0),(1,4)'), (1, lseg'(0,0),(1,5)'),(1, lseg'(0,0),(1,6)'),
    (1, lseg'(0,0),(1,7)'), (1, lseg'(0,0),(1,8)'),(1, lseg'(0,0),(1,9)'),
    (1, lseg'(0,0),(1,10)');
insert into t_lseg(i, v) values (1, lseg'(0,0),(1,11)'), (1, lseg'(0,0),(1,12)'),(1, lseg'(0,0),(1,13)'),
    (1, lseg'(0,0),(1,14)'), (1, lseg'(0,0),(1,15)'),(1, lseg'(0,0),(1,16)'),
    (1, lseg'(0,0),(1,17)'), (1, lseg'(0,0),(1,18)'),(1, lseg'(0,0),(1,19)'),
    (1, lseg'(0,0),(1,20)');

set client_min_messages to log;
select count(*) from t_lseg where v > lseg'(0,0),(0,0)';
select count(*) from t_lseg where v > lseg'(0,0),(1,5)';
select count(*) from t_lseg where v > lseg'(0,0),(1,10)';
select count(*) from t_lseg where v > lseg'(0,0),(1,15)';
select count(*) from t_lseg where v > lseg'(0,0),(1,20)';

select count(*) from t_lseg where v < lseg'(0,0),(0,0)';
select count(*) from t_lseg where v < lseg'(0,0),(1,6)';
select count(*) from t_lseg where v < lseg'(0,0),(1,11)';
select count(*) from t_lseg where v < lseg'(0,0),(1,16)';
select count(*) from t_lseg where v < lseg'(0,0),(1,21)';

select count(*) from t_lseg where v = lseg'(0,0),(0,0)';
select count(*) from t_lseg where v = lseg'(0,0),(1,5)';
select count(*) from t_lseg where v = lseg'(0,0),(1,10)';
select count(*) from t_lseg where v = lseg'(0,0),(1,11)';
select count(*) from t_lseg where v = lseg'(0,0),(1,20)';
select count(*) from t_lseg where v = lseg'(0,0),(1,21)';

select count(*) from t_lseg where v >= lseg'(0,0),(0,0)';
select count(*) from t_lseg where v >= lseg'(0,0),(1,6)';
select count(*) from t_lseg where v >= lseg'(0,0),(1,11)';
select count(*) from t_lseg where v >= lseg'(0,0),(1,16)';
select count(*) from t_lseg where v >= lseg'(0,0),(1,21)';

select count(*) from t_lseg where v <= lseg'(0,0),(0,0)';
select count(*) from t_lseg where v <= lseg'(0,0),(1,5)';
select count(*) from t_lseg where v <= lseg'(0,0),(1,10)';
select count(*) from t_lseg where v <= lseg'(0,0),(1,15)';
select count(*) from t_lseg where v <= lseg'(0,0),(1,20)';
reset client_min_messages;
drop table t_lseg;

create table t_circle(i int, v circle) with(minmax_columns='v');
insert into t_circle(i, v) values (1, circle'<(0,0),1>'), (1, circle'<(0,0),2>'), (1, circle'<(0,0),3>'),
    (1, circle'<(0,0),4>'), (1, circle'<(0,0),5>'), (1, circle'<(0,0),6>'),
    (1, circle'<(0,0),7>'), (1, circle'<(0,0),8>'), (1, circle'<(0,0),9>'),
    (1, circle'<(0,0),10>');

insert into t_circle(i, v) values (1, circle'<(0,0),11>'), (1, circle'<(0,0),12>'), (1, circle'<(0,0),13>'),
    (1, circle'<(0,0),14>'), (1, circle'<(0,0),15>'), (1, circle'<(0,0),16>'),
    (1, circle'<(0,0),17>'), (1, circle'<(0,0),18>'), (1, circle'<(0,0),19>'),
    (1, circle'<(0,0),20>');

set client_min_messages to log;
select count(*) from t_circle where v > circle'<(0,0),0>';
select count(*) from t_circle where v > circle'<(0,0),5>';
select count(*) from t_circle where v > circle'<(0,0),10>';
select count(*) from t_circle where v > circle'<(0,0),15>';
select count(*) from t_circle where v > circle'<(0,0),20>';

select count(*) from t_circle where v < circle'<(0,0),0>';
select count(*) from t_circle where v < circle'<(0,0),6>';
select count(*) from t_circle where v < circle'<(0,0),11>';
select count(*) from t_circle where v < circle'<(0,0),16>';
select count(*) from t_circle where v < circle'<(0,0),21>';

select count(*) from t_circle where v = circle'<(0,0),0>';
select count(*) from t_circle where v = circle'<(0,0),5>';
select count(*) from t_circle where v = circle'<(0,0),10>';
select count(*) from t_circle where v = circle'<(0,0),11>';
select count(*) from t_circle where v = circle'<(0,0),20>';
select count(*) from t_circle where v = circle'<(0,0),21>';

select count(*) from t_circle where v >= circle'<(0,0),0>';
select count(*) from t_circle where v >= circle'<(0,0),6>';
select count(*) from t_circle where v >= circle'<(0,0),11>';
select count(*) from t_circle where v >= circle'<(0,0),16>';
select count(*) from t_circle where v >= circle'<(0,0),21>';

select count(*) from t_circle where v <= circle'<(0,0),0>';
select count(*) from t_circle where v <= circle'<(0,0),5>';
select count(*) from t_circle where v <= circle'<(0,0),10>';
select count(*) from t_circle where v <= circle'<(0,0),15>';
select count(*) from t_circle where v <= circle'<(0,0),20>';
reset client_min_messages;
drop table t_circle;


create table t_path(i int, v path) with(minmax_columns='v');
insert into t_path(i, v) values (1, path'(0,0),(1,1),(2,1)'), (1, path'(0,0),(1,1),(2,2)'), (1, path'(0,0),(1,1),(2,3)'),
(1, path'(0,0),(1,1),(2,4)'), (1, path'(0,0),(1,1),(2,5)'), (1, path'(0,0),(1,1),(2,6)'),
(1, path'(0,0),(1,1),(2,7)'), (1, path'(0,0),(1,1),(2,8)'), (1, path'(0,0),(1,1),(2,9)'),
(1, path'(0,0),(1,1),(2,10)');

insert into t_path(i, v) values (1, path'(0,0),(1,1),(2,11)'), (1, path'(0,0),(1,1),(2,12)'), (1, path'(0,0),(1,1),(2,13)'),
(1, path'(0,0),(1,1),(2,14)'), (1, path'(0,0),(1,1),(2,15)'), (1, path'(0,0),(1,1),(2,16)'),
(1, path'(0,0),(1,1),(2,17)'), (1, path'(0,0),(1,1),(2,18)'), (1, path'(0,0),(1,1),(2,19)'),
(1, path'(0,0),(1,1),(2,20)');

set client_min_messages to log;
select count(*) from t_path where v > path'(0,0),(1,1),(2,0)';
select count(*) from t_path where v > path'(0,0),(1,1),(2,5)';
select count(*) from t_path where v > path'(0,0),(1,1),(2,10)';
select count(*) from t_path where v > path'(0,0),(1,1),(2,15)';
select count(*) from t_path where v > path'(0,0),(1,1),(2,20)';

select count(*) from t_path where v < path'(0,0),(1,1),(2,0)';
select count(*) from t_path where v < path'(0,0),(1,1),(2,6)';
select count(*) from t_path where v < path'(0,0),(1,1),(2,11)';
select count(*) from t_path where v < path'(0,0),(1,1),(2,16)';
select count(*) from t_path where v < path'(0,0),(1,1),(2,21)';

select count(*) from t_path where v = path'(0,0),(1,1),(2,0)';
select count(*) from t_path where v = path'(0,0),(1,1),(2,5)';
select count(*) from t_path where v = path'(0,0),(1,1),(2,10)';
select count(*) from t_path where v = path'(0,0),(1,1),(2,11)';
select count(*) from t_path where v = path'(0,0),(1,1),(2,20)';
select count(*) from t_path where v = path'(0,0),(1,1),(2,21)';

select count(*) from t_path where v >= path'(0,0),(1,1),(2,0)';
select count(*) from t_path where v >= path'(0,0),(1,1),(2,6)';
select count(*) from t_path where v >= path'(0,0),(1,1),(2,11)';
select count(*) from t_path where v >= path'(0,0),(1,1),(2,16)';
select count(*) from t_path where v >= path'(0,0),(1,1),(2,21)';

select count(*) from t_path where v <= path'(0,0),(1,1),(2,0)';
select count(*) from t_path where v <= path'(0,0),(1,1),(2,5)';
select count(*) from t_path where v <= path'(0,0),(1,1),(2,10)';
select count(*) from t_path where v <= path'(0,0),(1,1),(2,15)';
select count(*) from t_path where v <= path'(0,0),(1,1),(2,20)';
reset client_min_messages;
drop table t_path;

reset pax_enable_debug;
reset pax_enable_sparse_filter;
reset pax_max_tuples_per_group;

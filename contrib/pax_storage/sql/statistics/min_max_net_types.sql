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
-- Test the inet min/max types support 
-- 
create table t_inet(i int, v inet) with(minmax_columns='v');
insert into t_inet(i, v) values (1, inet'192.168.31.1/32'), (1, inet'192.168.31.2/32'), (1, inet'192.168.31.3/32'),
    (1, inet'192.168.31.4/32'), (1, inet'192.168.31.5/32'), (1, inet'192.168.31.6/32'),
    (1, inet'192.168.31.7/32'), (1, inet'192.168.31.8/32'), (1, inet'192.168.31.9/32'),
    (1, inet'192.168.31.10/32');

insert into t_inet(i, v) values (1, inet'192.168.31.11/32'), (1, inet'192.168.31.12/32'), (1, inet'192.168.31.13/32'),
    (1, inet'192.168.31.14/32'), (1, inet'192.168.31.15/32'), (1, inet'192.168.31.16/32'),
    (1, inet'192.168.31.17/32'), (1, inet'192.168.31.18/32'), (1, inet'192.168.31.19/32'),
    (1, inet'192.168.31.20/32');

set client_min_messages to log;
select count(*) from t_inet where v > '192.168.30.1'::inet;
select count(*) from t_inet where v > '192.168.31.5'::inet;
select count(*) from t_inet where v > '192.168.31.10'::inet;
select count(*) from t_inet where v > '192.168.31.15'::inet;
select count(*) from t_inet where v > '192.168.31.20'::inet;

select count(*) from t_inet where v < '192.168.30.1'::inet;
select count(*) from t_inet where v < '192.168.31.6'::inet;
select count(*) from t_inet where v < '192.168.31.11'::inet;
select count(*) from t_inet where v < '192.168.31.16'::inet;
select count(*) from t_inet where v < '192.168.31.21'::inet;

select count(*) from t_inet where v = '192.168.0.1'::inet;
select count(*) from t_inet where v = '192.168.31.5'::inet;
select count(*) from t_inet where v = '192.168.31.10'::inet;
select count(*) from t_inet where v = '192.168.31.11'::inet;
select count(*) from t_inet where v = '192.168.31.20'::inet;
select count(*) from t_inet where v = '192.168.31.21'::inet;

select count(*) from t_inet where v >= '192.168.30.1'::inet;
select count(*) from t_inet where v >= '192.168.31.6'::inet;
select count(*) from t_inet where v >= '192.168.31.11'::inet;
select count(*) from t_inet where v >= '192.168.31.16'::inet;
select count(*) from t_inet where v >= '192.168.31.21'::inet;

select count(*) from t_inet where v <= '192.168.0.1'::inet;
select count(*) from t_inet where v <= '192.168.31.5'::inet;
select count(*) from t_inet where v <= '192.168.31.10'::inet;
select count(*) from t_inet where v <= '192.168.31.15'::inet;
select count(*) from t_inet where v <= '192.168.31.20'::inet;
reset client_min_messages;
drop table t_inet;

-- 
-- Test the macaddr min/max types support 
-- 
-- no oper(macaddr, macaddr8) or oper(macaddr8, macaddr) in pg
create table t_mac(i int, v macaddr) with(minmax_columns='v');
insert into t_mac(i, v) values (1, '12:34:56:00:00:01'::macaddr), (1, '12:34:56:00:00:02'::macaddr), (1, '12:34:56:00:00:03'::macaddr),
(1, '12:34:56:00:00:04'::macaddr), (1, '12:34:56:00:00:05'::macaddr), (1, '12:34:56:00:00:06'::macaddr),
(1, '12:34:56:00:00:07'::macaddr), (1, '12:34:56:00:00:08'::macaddr), (1, '12:34:56:00:00:09'::macaddr),
(1, '12:34:56:00:00:10'::macaddr);

insert into t_mac(i, v) values (1, '12:34:56:00:00:11'::macaddr), (1, '12:34:56:00:00:12'::macaddr), (1, '12:34:56:00:00:13'::macaddr),
(1, '12:34:56:00:00:14'::macaddr), (1, '12:34:56:00:00:15'::macaddr), (1, '12:34:56:00:00:16'::macaddr),
(1, '12:34:56:00:00:17'::macaddr), (1, '12:34:56:00:00:18'::macaddr), (1, '12:34:56:00:00:19'::macaddr),
(1, '12:34:56:00:00:20'::macaddr);

set client_min_messages to log;
select count(*) from t_mac where v > '12:34:56:00:00:00'::macaddr;
select count(*) from t_mac where v > '12:34:56:00:00:05'::macaddr;
select count(*) from t_mac where v > '12:34:56:00:00:10'::macaddr;
select count(*) from t_mac where v > '12:34:56:00:00:15'::macaddr;
select count(*) from t_mac where v > '12:34:56:00:00:20'::macaddr;

select count(*) from t_mac where v < '12:34:56:00:00:00'::macaddr;
select count(*) from t_mac where v < '12:34:56:00:00:06'::macaddr;
select count(*) from t_mac where v < '12:34:56:00:00:11'::macaddr;
select count(*) from t_mac where v < '12:34:56:00:00:16'::macaddr;
select count(*) from t_mac where v < '12:34:56:00:00:21'::macaddr;

select count(*) from t_mac where v = '00:00:00:00:00:00'::macaddr;
select count(*) from t_mac where v = '12:34:56:00:00:05'::macaddr;
select count(*) from t_mac where v = '12:34:56:00:00:10'::macaddr;
select count(*) from t_mac where v = '12:34:56:00:00:11'::macaddr;
select count(*) from t_mac where v = '12:34:56:00:00:20'::macaddr;
select count(*) from t_mac where v = '12:34:56:00:00:21'::macaddr;

select count(*) from t_mac where v >= '12:34:56:00:00:00'::macaddr;
select count(*) from t_mac where v >= '12:34:56:00:00:06'::macaddr;
select count(*) from t_mac where v >= '12:34:56:00:00:11'::macaddr;
select count(*) from t_mac where v >= '12:34:56:00:00:16'::macaddr;
select count(*) from t_mac where v >= '12:34:56:00:00:21'::macaddr;

select count(*) from t_mac where v <= '00:00:00:00:00:00'::macaddr;
select count(*) from t_mac where v <= '12:34:56:00:00:05'::macaddr;
select count(*) from t_mac where v <= '12:34:56:00:00:10'::macaddr;
select count(*) from t_mac where v <= '12:34:56:00:00:15'::macaddr;
select count(*) from t_mac where v <= '12:34:56:00:00:20'::macaddr;
reset client_min_messages;
drop table t_mac;

-- 
-- Test the macaddr8 min/max types support 
-- 
create table t_mac8(i int, v macaddr8) with(minmax_columns='v');
insert into t_mac8(i, v) values (1, '12:34:56:00:00:00:00:01'::macaddr8), (1, '12:34:56:00:00:00:00:02'::macaddr8), (1, '12:34:56:00:00:00:00:03'::macaddr8),
(1, '12:34:56:00:00:00:00:04'::macaddr8), (1, '12:34:56:00:00:00:00:05'::macaddr8), (1, '12:34:56:00:00:00:00:06'::macaddr8),
(1, '12:34:56:00:00:00:00:07'::macaddr8), (1, '12:34:56:00:00:00:00:08'::macaddr8), (1, '12:34:56:00:00:00:00:09'::macaddr8),
(1, '12:34:56:00:00:00:00:10'::macaddr8);

insert into t_mac8(i, v) values (1, '12:34:56:00:00:00:00:11'::macaddr8), (1, '12:34:56:00:00:00:00:12'::macaddr8), (1, '12:34:56:00:00:00:00:13'::macaddr8),
(1, '12:34:56:00:00:00:00:14'::macaddr8), (1, '12:34:56:00:00:00:00:15'::macaddr8), (1, '12:34:56:00:00:00:00:16'::macaddr8),
(1, '12:34:56:00:00:00:00:17'::macaddr8), (1, '12:34:56:00:00:00:00:18'::macaddr8), (1, '12:34:56:00:00:00:00:19'::macaddr8),
(1, '12:34:56:00:00:00:00:20'::macaddr8);

set client_min_messages to log;
select count(*) from t_mac8 where v > '12:34:56:00:00:00:00:00'::macaddr8;
select count(*) from t_mac8 where v > '12:34:56:00:00:00:00:05'::macaddr8;
select count(*) from t_mac8 where v > '12:34:56:00:00:00:00:10'::macaddr8;
select count(*) from t_mac8 where v > '12:34:56:00:00:00:00:15'::macaddr8;
select count(*) from t_mac8 where v > '12:34:56:00:00:00:00:20'::macaddr8;

select count(*) from t_mac8 where v < '12:34:56:00:00:00:00:00'::macaddr8;
select count(*) from t_mac8 where v < '12:34:56:00:00:00:00:06'::macaddr8;
select count(*) from t_mac8 where v < '12:34:56:00:00:00:00:11'::macaddr8;
select count(*) from t_mac8 where v < '12:34:56:00:00:00:00:16'::macaddr8;
select count(*) from t_mac8 where v < '12:34:56:00:00:00:00:21'::macaddr8;

select count(*) from t_mac8 where v = '00:00:00:00:00:00:00:00'::macaddr8;
select count(*) from t_mac8 where v = '12:34:56:00:00:00:00:05'::macaddr8;
select count(*) from t_mac8 where v = '12:34:56:00:00:00:00:10'::macaddr8;
select count(*) from t_mac8 where v = '12:34:56:00:00:00:00:11'::macaddr8;
select count(*) from t_mac8 where v = '12:34:56:00:00:00:00:20'::macaddr8;
select count(*) from t_mac8 where v = '12:34:56:00:00:00:00:21'::macaddr8;

select count(*) from t_mac8 where v >= '12:34:56:00:00:00:00:00'::macaddr8;
select count(*) from t_mac8 where v >= '12:34:56:00:00:00:00:06'::macaddr8;
select count(*) from t_mac8 where v >= '12:34:56:00:00:00:00:11'::macaddr8;
select count(*) from t_mac8 where v >= '12:34:56:00:00:00:00:16'::macaddr8;
select count(*) from t_mac8 where v >= '12:34:56:00:00:00:00:21'::macaddr8;

select count(*) from t_mac8 where v <= '00:00:00:00:00:00:00:00'::macaddr8;
select count(*) from t_mac8 where v <= '12:34:56:00:00:00:00:05'::macaddr8;
select count(*) from t_mac8 where v <= '12:34:56:00:00:00:00:10'::macaddr8;
select count(*) from t_mac8 where v <= '12:34:56:00:00:00:00:15'::macaddr8;
select count(*) from t_mac8 where v <= '12:34:56:00:00:00:00:20'::macaddr8;
reset client_min_messages;
drop table t_mac8;

reset pax_enable_debug;
reset pax_enable_sparse_filter;
reset pax_max_tuples_per_group;

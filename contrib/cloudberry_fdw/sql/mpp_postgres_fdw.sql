CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER testserver2 FOREIGN DATA WRAPPER postgres_fdw;
DO $d$
BEGIN
EXECUTE $$CREATE SERVER mpps1 FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname 'fdw1',
                     port '$$||current_setting('port')||$$'
            )$$;
EXECUTE $$CREATE SERVER mpps2 FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname 'fdw2',
                     port '$$||current_setting('port')||$$'
            )$$;
EXECUTE $$CREATE SERVER mpps3 FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (dbname 'fdw3',
                     port '$$||current_setting('port')||$$'
            )$$;
END;
$d$;
 
CREATE USER MAPPING FOR CURRENT_USER SERVER mpps1;
CREATE USER MAPPING FOR CURRENT_USER SERVER mpps2;
CREATE USER MAPPING FOR CURRENT_USER SERVER mpps3;

DROP DATABASE IF EXISTS fdw1;
DROP DATABASE IF EXISTS fdw2;
DROP DATABASE IF EXISTS fdw3;

CREATE DATABASE fdw1;
CREATE DATABASE fdw2;
CREATE DATABASE fdw3;

\c fdw1
create table t1(a int, b text);
create table t2(a int, b text);

insert into t1 values(1, 'fdw1');
insert into t2 values(1, 'fdw1');

\c fdw2
create table t1(a int, b text);
create table t2(a int, b text);
insert into t1 values(1, 'fdw2');
insert into t2 values(1, 'fdw2');

\c fdw3
create table t1(a int, b text);
create table t2(a int, b text);
insert into t1 values(1, 'fdw3');
insert into t2 values(1, 'fdw3');

\c contrib_regression

CREATE FOREIGN TABLE fs1 (
    a int,
    b text
    )
    SERVER mpps1
    OPTIONS (schema_name 'public', table_name 't1', mpp_execute 'all segments');

ADD FOREIGN SEGMENT FROM SERVER mpps1 INTO fs1;

explain (costs off) select * from fs1;
select * from fs1;

ADD FOREIGN SEGMENT FROM SERVER mpps2 INTO fs1;

explain (costs off) select * from fs1;
select * from fs1;

explain (costs off) select count(*) from fs1;
select count(*) from fs1;

select count(*),b from fs1 group by b;

ADD FOREIGN SEGMENT FROM SERVER mpps3 INTO fs1;

explain (costs off) select * from fs1;
select * from fs1;

explain (costs off) select count(*) from fs1;
select count(*) from fs1;

select count(*),b from fs1 group by b;

----------------------
-- Test join push down
----------------------
CREATE FOREIGN TABLE fs2 (
    a int,
    b text
    )
    SERVER mpps1
    OPTIONS (schema_name 'public', table_name 't2', mpp_execute 'all segments');

ADD FOREIGN SEGMENT FROM SERVER mpps1 INTO fs2;
ADD FOREIGN SEGMENT FROM SERVER mpps2 INTO fs2;
ADD FOREIGN SEGMENT FROM SERVER mpps3 INTO fs2;

explain (costs off) select * from fs1, fs2 where fs1.a = fs2.a;
select * from fs1,fs2 where fs1.a = fs2.a;

explain (costs off) select * from fs1, fs2 where fs1.a = fs2.a and fs1.gp_foreign_server = fs2.gp_foreign_server;
select * from fs1,fs2 where fs1.a = fs2.a and fs1.gp_foreign_server = fs2.gp_foreign_server;

explain (costs off) select count(*) from fs1, fs2 where fs1.a = fs2.a;
select count(*) from fs1,fs2 where fs1.a = fs2.a;

explain (costs off) select count(*) from fs1, fs2 where fs1.a = fs2.a and fs1.gp_foreign_server = fs2.gp_foreign_server;
select count(*) from fs1,fs2 where fs1.a = fs2.a and fs1.gp_foreign_server = fs2.gp_foreign_server;

----------------------------
-- Test with enable parallel
----------------------------
set enable_parallel to true;

explain (costs off) select * from fs1;
select * from fs1;

explain (costs off) select count(*) from fs1;
select count(*) from fs1;

explain (costs off) select * from fs1, fs2 where fs1.a = fs2.a;
select * from fs1,fs2 where fs1.a = fs2.a;

select count(*),b from fs1 group by b;

explain (costs off) select * from fs1, fs2 where fs1.a = fs2.a and fs1.gp_foreign_server = fs2.gp_foreign_server;
select * from fs1,fs2 where fs1.a = fs2.a and fs1.gp_foreign_server = fs2.gp_foreign_server;

explain (costs off) select count(*) from fs1, fs2 where fs1.a = fs2.a;
select count(*) from fs1,fs2 where fs1.a = fs2.a;

explain (costs off) select count(*) from fs1, fs2 where fs1.a = fs2.a and fs1.gp_foreign_server = fs2.gp_foreign_server;
select count(*) from fs1,fs2 where fs1.a = fs2.a and fs1.gp_foreign_server = fs2.gp_foreign_server;

reset enable_parallel;

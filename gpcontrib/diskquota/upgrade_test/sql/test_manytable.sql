-- start_ignore
-- \! gpconfig -c diskquota.max_active_tables -v 2 > /dev/null
-- end_ignore
-- \! echo $?

CREATE DATABASE test_manytable01;
CREATE DATABASE test_manytable02;

\c test_manytable01

CREATE TABLE a01(i int) DISTRIBUTED BY (i);
CREATE TABLE a02(i int) DISTRIBUTED BY (i);
CREATE TABLE a03(i int) DISTRIBUTED BY (i);

INSERT INTO a01 values(generate_series(0, 500));
INSERT INTO a02 values(generate_series(0, 500));
INSERT INTO a03 values(generate_series(0, 500));

\c test_manytable02
CREATE TABLE b01(i int) DISTRIBUTED BY (i);
INSERT INTO b01 values(generate_series(0, 500));

\c postgres
DROP DATABASE test_manytable01;
DROP DATABASE test_manytable02;

-- start_ignore
\! gpconfig -r diskquota.max_active_tables
\! gpstop -far
-- end_ignore

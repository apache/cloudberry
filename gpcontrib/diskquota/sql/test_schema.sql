-- Test schema
CREATE SCHEMA s1;
SELECT diskquota.set_schema_quota('s1', '1 MB');
SET search_path TO s1;

CREATE TABLE a(i int);
INSERT INTO a SELECT generate_series(1,100);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100000);
SELECT pg_sleep(5);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);
CREATE TABLE a2(i int);
-- expect insert fail
INSERT INTO a2 SELECT generate_series(1,100);

-- Test alter table set schema
CREATE SCHEMA s2;
ALTER TABLE s1.a SET SCHEMA s2;
SELECT pg_sleep(20);
-- expect insert succeed
INSERT INTO a2 SELECT generate_series(1,200);
-- expect insert succeed
INSERT INTO s2.a SELECT generate_series(1,200);

ALTER TABLE s2.a SET SCHEMA badquota;
-- expect failed
INSERT INTO badquota.a SELECT generate_series(0, 100);

SELECT pg_sleep(10);
SELECT schema_name, quota_in_mb FROM diskquota.show_fast_schema_quota_view WHERE schema_name = 's1';

RESET search_path;
DROP TABLE s1.a2, badquota.a;
DROP SCHEMA s1, s2;


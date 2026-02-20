-- test rename schema
CREATE SCHEMA srs1;
SELECT diskquota.set_schema_quota('srs1', '1 MB');
set search_path to srs1;
CREATE TABLE a(i int) DISTRIBUTED BY (i);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,10);
ALTER SCHEMA srs1 RENAME TO srs2;
SET search_path TO srs2;

-- expect insert fail
INSERT INTO a SELECT generate_series(1,10);
-- test rename table
ALTER TABLE a RENAME TO a2;
-- expect insert fail
INSERT INTO a2 SELECT generate_series(1,10);

DROP TABLE a2;
RESET search_path;
DROP SCHEMA srs2;

-- test rename role
CREATE SCHEMA srr1;
CREATE ROLE srerole NOLOGIN;
SELECT diskquota.set_role_quota('srerole', '1MB');
SET search_path TO srr1;
CREATE TABLE a(i int) DISTRIBUTED BY (i);
ALTER TABLE a OWNER TO srerole;

-- expect insert fail
INSERT INTO a SELECT generate_series(1,100000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,10);
ALTER ROLE srerole RENAME TO srerole2;
-- expect insert fail
INSERT INTO a SELECT generate_series(1,10);
-- test rename table
ALTER TABLE a RENAME TO a2;
-- expect insert fail
INSERT INTO a2 SELECT generate_series(1,10);

DROP TABLE a2;
DROP ROLE srerole2;
RESET search_path;
DROP SCHEMA srr1;


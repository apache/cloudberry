CREATE SCHEMA hardlimit_s;
SET search_path TO hardlimit_s;

SELECT diskquota.enable_hardlimit();
SELECT diskquota.set_schema_quota('hardlimit_s', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();

-- heap table
CREATE TABLE t1 AS SELECT generate_series(1,1000000); -- expect fail

SELECT diskquota.pause();

CREATE TABLE t1 AS SELECT generate_series(1,1000000); -- expect succeed

-- disable hardlimit and do some clean-ups.
SELECT diskquota.disable_hardlimit();
SELECT diskquota.resume();

DROP SCHEMA hardlimit_s CASCADE;

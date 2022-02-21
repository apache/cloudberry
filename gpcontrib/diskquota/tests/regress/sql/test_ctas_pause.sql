CREATE SCHEMA hardlimit_s;
SET search_path TO hardlimit_s;

\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
\! gpstop -u > /dev/null
SELECT diskquota.set_schema_quota('hardlimit_s', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();

-- heap table
CREATE TABLE t1 (i) AS SELECT generate_series(1,1000000) DISTRIBUTED BY (i); -- expect fail

SELECT diskquota.pause();

CREATE TABLE t1 (i) AS SELECT generate_series(1,1000000) DISTRIBUTED BY (i); -- expect succeed

-- disable hardlimit and do some clean-ups.
\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null
SELECT diskquota.resume();

DROP SCHEMA hardlimit_s CASCADE;

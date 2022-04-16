CREATE DATABASE test_clean_blackmap_after_drop;

\c test_clean_blackmap_after_drop
CREATE EXTENSION diskquota;

\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
\! gpstop -u > /dev/null

CREATE ROLE r;
SELECT diskquota.set_role_quota('r', '1MB');
CREATE TABLE b (t TEXT) DISTRIBUTED BY (t);
ALTER TABLE b OWNER TO r;
SELECT diskquota.wait_for_worker_new_epoch();

INSERT INTO b SELECT generate_series(1, 100000000); -- fail

DROP EXTENSION diskquota;

INSERT INTO b SELECT generate_series(1, 100); -- ok

\c contrib_regression
DROP DATABASE test_clean_blackmap_after_drop;
DROP ROLE r;

\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpstop -u > /dev/null

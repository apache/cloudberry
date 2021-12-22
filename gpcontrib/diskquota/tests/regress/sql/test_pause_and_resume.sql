-- Test pause and resume.
CREATE SCHEMA s1;
SET search_path TO s1;

CREATE TABLE a(i int);
CREATE TABLE b(i int);

-- expect insert succeed
INSERT INTO a SELECT generate_series(1,100000);

SELECT diskquota.set_schema_quota('s1', '1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);

-- pause extension
SELECT diskquota.pause();

-- expect insert succeed
INSERT INTO a SELECT generate_series(1,100);
-- expect insert succeed
INSERT INTO b SELECT generate_series(1,100);

-- resume extension
SELECT diskquota.resume();

-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);

RESET search_path;
DROP TABLE s1.a, s1.b;
DROP SCHEMA s1;


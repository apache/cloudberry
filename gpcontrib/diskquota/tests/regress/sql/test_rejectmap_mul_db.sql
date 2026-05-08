-- One db's rejectmap update should not impact on other db's rejectmap
CREATE DATABASE tjmu1;
CREATE DATABASE tjmu2;

-- start_ignore
\! gpconfig -c "diskquota.hard_limit" -v "on" > /dev/null
-- increase the naptime to avoid active table gets cleared by tjmu1's worker
\! gpconfig -c "diskquota.naptime" -v 1 > /dev/null
\! gpstop -u > /dev/null
-- end_ignore

\c tjmu1
CREATE EXTENSION diskquota;
SELECT diskquota.set_schema_quota('public', '1MB');
CREATE TABLE b (t TEXT) DISTRIBUTED BY (t);
SELECT diskquota.wait_for_worker_new_epoch();
-- Trigger hard limit to dispatch rejectmap for tjmu1
INSERT INTO b SELECT generate_series(1, 100000000); -- fail
-- FIXME: Pause to avoid tjmu1's worker clear the active table. Since there are bugs, this might be flaky.
SELECT diskquota.pause();
-- The rejectmap should contain entries with dbnode = 0 and dbnode = tjmu1_oid. count = 1
SELECT COUNT(DISTINCT r.dbnode) FROM (SELECT (diskquota.show_rejectmap()).* FROM gp_dist_random('gp_id')) as r where r.dbnode != 0;

\c tjmu2
CREATE EXTENSION diskquota;
SELECT diskquota.set_schema_quota('public', '1MB');
CREATE TABLE b (t TEXT) DISTRIBUTED BY (t);
SELECT diskquota.wait_for_worker_new_epoch();
-- Trigger hard limit to dispatch rejectmap for tjmu2
INSERT INTO b SELECT generate_series(1, 100000000); -- fail
SELECT diskquota.wait_for_worker_new_epoch();
SELECT diskquota.pause();

--\c tjmu1
-- The rejectmap should contain entris with dbnode = 0 and dbnode = tjmu1_oid and tjmu2_oid. count = 2
-- The entries for tjmu1 should not be cleared
SELECT COUNT(DISTINCT r.dbnode) FROM (SELECT (diskquota.show_rejectmap()).* FROM gp_dist_random('gp_id')) as r where r.dbnode != 0;

-- start_ignore
\! gpconfig -c "diskquota.hard_limit" -v "off" > /dev/null
\! gpconfig -c "diskquota.naptime" -v 0 > /dev/null
\! gpstop -u > /dev/null
-- end_ignore

\c tjmu1
DROP EXTENSION diskquota;
\c tjmu2
DROP EXTENSION diskquota;

\c contrib_regression
DROP DATABASE tjmu1;
DROP DATABASE tjmu2;


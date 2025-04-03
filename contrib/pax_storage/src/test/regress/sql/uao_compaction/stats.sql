-- @Description Tests that the pg_class statistics are updated on
-- lazy vacuum.

CREATE TABLE uao_stats (a INT, b INT, c CHAR(128)) WITH (appendonly=true) DISTRIBUTED BY (a);
CREATE INDEX uao_stats_index ON uao_stats(b);
INSERT INTO uao_stats SELECT i as a, i as b, 'hello world' as c FROM generate_series(1, 50) AS i;
INSERT INTO uao_stats SELECT i as a, i as b, 'hello world' as c FROM generate_series(51, 100) AS i;
ANALYZE uao_stats;

-- ensure that the scan go through the index
SET enable_seqscan=false;
SELECT relname, reltuples FROM pg_class WHERE relname = 'uao_stats';
SELECT relname, reltuples FROM pg_class WHERE relname = 'uao_stats_index';
DELETE FROM uao_stats WHERE a < 16;
VACUUM uao_stats;
SELECT relname, reltuples FROM pg_class WHERE relname = 'uao_stats';

-- New strategy of VACUUM AO/CO was introduced by PR #13255 for performance enhancement.
-- Index dead tuples will not always be cleaned up completely after VACUUM, resulting
-- index stats pg_class->reltuples will not always be accurate. So ignore the stats check
-- for reltuples to coordinate with the new behavior.
-- start_ignore
SELECT relname, reltuples FROM pg_class WHERE relname = 'uao_stats_index';
-- end_ignore

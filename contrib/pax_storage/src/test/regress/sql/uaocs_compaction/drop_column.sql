-- @Description Tests dropping a column after a compaction
CREATE TABLE uaocs_drop (a INT, b INT, c CHAR(128)) WITH (appendonly=true, orientation=column) DISTRIBUTED BY (a);
CREATE INDEX uaocs_drop_index ON uaocs_drop(b);
INSERT INTO uaocs_drop SELECT i as a, 1 as b, 'hello world' as c FROM generate_series(1, 10) AS i;
ANALYZE uaocs_drop;

DELETE FROM uaocs_drop WHERE a < 4;
SELECT COUNT(*) FROM uaocs_drop;
SELECT relname, reltuples FROM pg_class WHERE relname = 'uaocs_drop';
SELECT relname, reltuples FROM pg_class WHERE relname = 'uaocs_drop_index';
VACUUM uaocs_drop;
SELECT relname, reltuples FROM pg_class WHERE relname = 'uaocs_drop';
-- New strategy of VACUUM AO/CO was introduced by PR #13255 for performance enhancement.
-- Index dead tuples will not always be cleaned up completely after VACUUM, resulting
-- index stats pg_class->reltuples will not always be accurate. So ignore the stats check
-- for reltuples to coordinate with the new behavior.
-- start_ignore
SELECT relname, reltuples FROM pg_class WHERE relname = 'uaocs_drop_index';
-- end_ignore
ALTER TABLE uaocs_drop DROP COLUMN c;
SELECT * FROM uaocs_drop;
INSERT INTO uaocs_drop VALUES (42, 42);
SELECT * FROM uaocs_drop;

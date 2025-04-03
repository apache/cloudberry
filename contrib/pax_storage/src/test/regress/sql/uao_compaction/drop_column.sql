-- @Description Tests dropping a column after a compaction

CREATE TABLE uao_drop_col (a INT, b INT, c CHAR(128)) WITH (appendonly=true) DISTRIBUTED BY (a);
CREATE INDEX uao_drop_col_index ON uao_drop_col(b);
INSERT INTO uao_drop_col SELECT i as a, 1 as b, 'hello world' as c FROM generate_series(1, 10) AS i;
ANALYZE uao_drop_col;

DELETE FROM uao_drop_col WHERE a < 4;
SELECT COUNT(*) FROM uao_drop_col;
SELECT relname, reltuples FROM pg_class WHERE relname = 'uao_drop_col';
SELECT relname, reltuples FROM pg_class WHERE relname = 'uao_drop_col_index';
VACUUM uao_drop_col;
SELECT relname, reltuples FROM pg_class WHERE relname = 'uao_drop_col';
-- New strategy of VACUUM AO/CO was introduced by PR #13255 for performance enhancement.
-- Index dead tuples will not always be cleaned up completely after VACUUM, resulting
-- index stats pg_class->reltuples will not always be accurate. So ignore the stats check
-- for reltuples to coordinate with the new behavior.
-- start_ignore
SELECT relname, reltuples FROM pg_class WHERE relname = 'uao_drop_col_index';
-- end_ignore
ALTER TABLE uao_drop_col DROP COLUMN c;
SELECT * FROM uao_drop_col;
INSERT INTO uao_drop_col VALUES (42, 42);
SELECT * FROM uao_drop_col;

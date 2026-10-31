-- Values read from a table come with a short (1-byte) varlena header, and a
-- 3-byte string must not be mistaken for an empty one.
CREATE SCHEMA abc;
CREATE TABLE gp_dbms_assert_t (s text) DISTRIBUTED REPLICATED;
INSERT INTO gp_dbms_assert_t VALUES ('abc'), ('xyz'), ('abcd');
SELECT s, dbms_assert.simple_sql_name(s) FROM gp_dbms_assert_t ORDER BY s;
SELECT s, dbms_assert.schema_name(s) FROM gp_dbms_assert_t WHERE s = 'abc';
DROP TABLE gp_dbms_assert_t;
DROP SCHEMA abc;

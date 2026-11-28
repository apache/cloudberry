-- regexp_not_like() is a Cloudberry addition, the negation of regexp_like()
SELECT oracle.regexp_not_like('abc', 'b');
SELECT oracle.regexp_not_like('abc', 'x');
SELECT oracle.regexp_not_like('ABC', 'b', 'i');
SELECT oracle.regexp_not_like('ABC', 'b', 'c');
SELECT oracle.regexp_not_like('ABC', 'b', NULL);
SELECT oracle.regexp_not_like(NULL, 'b') IS NULL AS null_source;
SELECT oracle.regexp_not_like('abc', NULL, 'i') IS NULL AS null_pattern;

-- newline handling follows regexp_like(): '^' only matches at line start with 'm'
SELECT oracle.regexp_not_like('foo' || chr(10) || 'bar', '^bar');
SELECT oracle.regexp_not_like('foo' || chr(10) || 'bar', '^bar', 'm');

-- evaluated on the segments, it must agree with NOT regexp_like()
CREATE TABLE gp_regexp_t (id int, s text) DISTRIBUTED BY (id);
INSERT INTO gp_regexp_t
  SELECT i, CASE WHEN i % 3 = 0 THEN 'Apple' || i ELSE 'pear' || i END
  FROM generate_series(1, 30) i;
SELECT count(*) FROM gp_regexp_t WHERE oracle.regexp_not_like(s, '^apple', 'i');
SELECT count(*) FROM gp_regexp_t
  WHERE oracle.regexp_not_like(s, '^apple', 'i')
        IS DISTINCT FROM NOT oracle.regexp_like(s, '^apple', 'i');
DROP TABLE gp_regexp_t;

-- a match whose first group does not take part is still a match
SELECT oracle.regexp_like('b', '(a)?b') AS like2,
       oracle.regexp_like('b', '(a)?b', 'i') AS like3,
       oracle.regexp_not_like('b', '(a)?b') AS not_like2,
       oracle.regexp_not_like('b', '(a)?b', 'i') AS not_like3;

-- like regexp_like(), it can be used in an index
SELECT p.oid::regprocedure, p.provolatile, p.proparallel
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
 WHERE n.nspname = 'oracle' AND p.proname IN ('regexp_like', 'regexp_not_like')
 ORDER BY 1::text;
CREATE TABLE gp_regexp_idx (id int, s text) DISTRIBUTED BY (id);
CREATE INDEX ON gp_regexp_idx ((oracle.regexp_not_like(s, '^a')));
DROP TABLE gp_regexp_idx;

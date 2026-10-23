--
-- targetlist set returning function tests
--

-- SRF is not under any other expression --
explain verbose select generate_series(1,4) as x;
select generate_series(1,4) as x;

-- SRF is present under a FUNCEXPR which is not a SRF
explain verbose select abs(generate_series(-5,-1)) as absolute;
select abs(generate_series(-5,-1)) as absolute;

-- SRF is present under a OPEXPR(+)
explain verbose select generate_series(1,4)+1 as output;
select generate_series(1,4)+1 as output;

-- SRF is present under an SRF expression
explain verbose select generate_series(generate_series(1,3),4);
select generate_series(generate_series(1,3),4) as output;

-- The inner SRF is present under an OPEXPR which in turn is under an SRF
explain verbose select generate_series(generate_series(1,2)+1,4) as output;
select generate_series(generate_series(1,2)+1,4) as output;

-- The outer SRF is present under an OPEXPR
explain verbose select generate_series(generate_series(1,2),4)+1 as output;
select generate_series(generate_series(1,2),4)+1 as output;

-- Both inner and outer SRF are present under OPEXPR
explain verbose select generate_series(generate_series(1,2)+1,4)+1 as output;
select generate_series(generate_series(1,2)+1,4)+1 as output;
explain verbose select generate_series(1,3)+1 as x from (select generate_series(1, 3)) as y;
select generate_series(1,3)+1 as x from (select generate_series(1, 3)) as y;

create table test_srf(a int,b int,c int) distributed by (a);
insert into test_srf values(2,2,2);
insert into test_srf values(3,2,2);
explain verbose select generate_series(1,a) as output,b,c from test_srf;
select generate_series(1,a) as output,b,c from test_srf;
explain verbose select generate_series(1,a+1),b+generate_series(1,4),c from test_srf;
select generate_series(1,a+1),b+generate_series(1,4),c from test_srf;
drop table test_srf;

-- Test that the preprocessor step where
-- IN subquery is converted to EXIST subquery with a predicate,
-- is not happening if inner sub query is SRF
-- Fixed as part of github issue #15644

explain verbose SELECT a IN (SELECT generate_series(1,a)) AS x FROM (SELECT generate_series(1, 3) AS a) AS s;
SELECT a IN (SELECT generate_series(1,a)) AS x FROM (SELECT generate_series(1, 3) AS a) AS s;

SELECT a FROM (values(1),(2),(3)) as t(a) where a IN (SELECT generate_series(1,a));
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT a FROM (values(1),(2),(3)) as t(a) where a IN (SELECT generate_series(1,a));

CREATE TABLE t_outer (a int, b int) DISTRIBUTED BY (a);
INSERT INTO t_outer SELECT i, i+1 FROM generate_series(1,3) as i;  
CREATE TABLE t_inner (a int, b int) DISTRIBUTED BY (a);
INSERT INTO t_inner SELECT i, i+1 FROM generate_series(1,3) as i;

SELECT * FROM t_outer WHERE t_outer.b IN (SELECT generate_series(1, t_outer.b) FROM t_inner);
EXPLAIN (VERBOSE, COSTS OFF)
  SELECT * FROM t_outer WHERE t_outer.b IN (SELECT generate_series(1, t_outer.b)  FROM t_inner);
DROP TABLE t_outer, t_inner;

-- Correlated SRFs must be rescanned when their PARAM_EXEC inputs change.
-- Keep the rows on one segment so successive invocations share executor state.
BEGIN;
CREATE TEMP TABLE projectset_outer(bucket int, id int, n int)
    ON COMMIT DROP DISTRIBUTED BY (bucket);
CREATE TEMP TABLE projectset_inner(bucket int, id int, vals bigint[])
    ON COMMIT DROP DISTRIBUTED BY (bucket);
INSERT INTO projectset_outer
    SELECT 1, i, i % 4 FROM generate_series(1, 12) i;
INSERT INTO projectset_inner VALUES
    (1,1,ARRAY[401,578]), (1,3,ARRAY[999]), (1,4,ARRAY[]::bigint[]),
    (1,5,ARRAY[578]), (1,6,NULL), (1,7,ARRAY[401,401,NULL]),
    (1,8,ARRAY[NULL]::bigint[]), (1,9,ARRAY[578,401]),
    (1,10,ARRAY[999,578]), (1,12,ARRAY[401]);
ANALYZE projectset_outer;
ANALYZE projectset_inner;

SELECT a.id,
       (SELECT string_agg(s.val::text, '-' ORDER BY s.val)
        FROM (SELECT unnest(ARRAY[401,578]::bigint[]) AS val
              INTERSECT SELECT unnest(b.vals)) s) AS facets
FROM projectset_outer a
LEFT JOIN projectset_inner b ON b.bucket = a.bucket AND b.id = a.id
ORDER BY a.id;

-- The upper ProjectSet references an OUTER_VAR; the parameter is below it.
SELECT a.id,
       (SELECT string_agg(v::text, ',' ORDER BY v)
        FROM (SELECT generate_series(1, generate_series(1, a.n)) v) s) AS nested
FROM projectset_outer a
ORDER BY a.id;
-- A Result's one-time filter must not hide parameters used by its child.
SELECT a.id,
       (SELECT sum(s.v + 1)
        FROM (SELECT generate_series(1, a.n) AS v ORDER BY v LIMIT 2) s
        WHERE a.id > 0) AS gated
FROM projectset_outer a
ORDER BY a.id;
ROLLBACK;

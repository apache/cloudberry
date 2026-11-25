-- parallel: the fragment split.  Each QE picks its own share from the list the
-- QD published, so the two things worth asserting are that the shares are
-- complete (every row arrives) and disjoint (no row arrives twice) - the second
-- is what count(*) = count(DISTINCT id) says, since every fixture id is unique.
--
-- Which segment gets which fragment deliberately rotates with the session id
-- and the command counter, so the assertions below are the sorted shape of the
-- split rather than a fixed assignment.
\pset format unaligned
DO $$
BEGIN
  EXECUTE format('CREATE SERVER par_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
END
$$;
CREATE FOREIGN TABLE lance_feature.par_1 (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_1.lance');
CREATE FOREIGN TABLE lance_feature.par_2 (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_2.lance');
CREATE FOREIGN TABLE lance_feature.par_3 (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_3.lance');
CREATE FOREIGN TABLE lance_feature.par_7 (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_7.lance');
CREATE FOREIGN TABLE lance_feature.par_100 (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_100.lance');
-- Fewer fragments than segments, as many, and many more.
SELECT count(*) AS rows, count(DISTINCT id) AS ids, sum(n) AS sum_n FROM lance_feature.par_1;
SELECT count(*) AS rows, count(DISTINCT id) AS ids, sum(n) AS sum_n FROM lance_feature.par_2;
SELECT count(*) AS rows, count(DISTINCT id) AS ids, sum(n) AS sum_n FROM lance_feature.par_3;
SELECT count(*) AS rows, count(DISTINCT id) AS ids, sum(n) AS sum_n FROM lance_feature.par_7;
SELECT count(*) AS rows, count(DISTINCT id) AS ids, sum(n) AS sum_n FROM lance_feature.par_100;
-- The rotation moves with the command counter, so repeating a statement takes
-- a different split; the answer may not move with it.
SELECT count(*) AS again FROM lance_feature.par_7;
SELECT count(*) AS again FROM lance_feature.par_7;
SELECT count(*) AS again FROM lance_feature.par_7;
-- How the rows landed, as a sorted shape: one non-empty segment for a single
-- fragment, two for two, and 7 or 100 fragments spread as evenly as a modulo
-- can spread them.
SELECT array_agg(c ORDER BY c) AS per_segment
  FROM (SELECT gp_execution_segment() AS seg, count(*) AS c FROM lance_feature.par_1 GROUP BY 1) s;
SELECT array_agg(c ORDER BY c) AS per_segment
  FROM (SELECT gp_execution_segment() AS seg, count(*) AS c FROM lance_feature.par_2 GROUP BY 1) s;
SELECT array_agg(c ORDER BY c) AS per_segment
  FROM (SELECT gp_execution_segment() AS seg, count(*) AS c FROM lance_feature.par_3 GROUP BY 1) s;
SELECT array_agg(c ORDER BY c) AS per_segment
  FROM (SELECT gp_execution_segment() AS seg, count(*) AS c FROM lance_feature.par_7 GROUP BY 1) s;
SELECT array_agg(c ORDER BY c) AS per_segment
  FROM (SELECT gp_execution_segment() AS seg, count(*) AS c FROM lance_feature.par_100 GROUP BY 1) s;
-- The debugging execution modes read every fragment in one process and have to
-- return exactly the same rows.
CREATE FOREIGN TABLE lance_feature.par_3_coord (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_3.lance', mpp_execute 'coordinator');
CREATE FOREIGN TABLE lance_feature.par_3_any (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_3.lance', mpp_execute 'any');
SELECT count(*) AS rows, sum(n) AS sum_n FROM lance_feature.par_3_coord;
SELECT count(*) AS rows, sum(n) AS sum_n FROM lance_feature.par_3_any;
SELECT count(*) AS coordinator_only FROM (
  SELECT * FROM lance_feature.par_3_coord EXCEPT SELECT * FROM lance_feature.par_3) d;
SELECT count(*) AS all_segments_only FROM (
  SELECT * FROM lance_feature.par_3 EXCEPT SELECT * FROM lance_feature.par_3_coord) d;
SELECT count(*) AS any_only FROM (
  SELECT * FROM lance_feature.par_3_any EXCEPT SELECT * FROM lance_feature.par_3) d;
-- The same override, set on the server instead of the table.
DO $$
BEGIN
  EXECUTE format('CREATE SERVER par_coord FOREIGN DATA WRAPPER lance_fdw '
                 'OPTIONS (base_uri %L, mpp_execute %L)',
                 current_setting('regress.fixture_dir'), 'coordinator');
END
$$;
CREATE FOREIGN TABLE lance_feature.par_3_server_coord (id integer, v text, n bigint)
  SERVER par_coord OPTIONS (uri 'frag_3.lance');
SELECT count(*) AS rows, sum(n) AS sum_n FROM lance_feature.par_3_server_coord;
-- A foreign table or its server may narrow the execution width with
-- num_segments, and Cloudberry then runs the scan on contents 0..N-1 only.
-- The split has to use that same N: dividing the fragments among every content
-- of the cluster would leave the ones belonging to a segment that never runs
-- unread.  Row counts are the whole dataset either way; what changes is
-- the shape, and with three fragments over two segments it is 1 + 2.
CREATE FOREIGN TABLE lance_feature.par_3_two (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_3.lance', num_segments '2');
CREATE FOREIGN TABLE lance_feature.par_3_one (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_3.lance', num_segments '1');
CREATE FOREIGN TABLE lance_feature.par_7_two (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_7.lance', num_segments '2');
SELECT count(*) AS rows, count(DISTINCT id) AS ids, sum(n) AS sum_n FROM lance_feature.par_3_two;
SELECT count(*) AS rows, count(DISTINCT id) AS ids, sum(n) AS sum_n FROM lance_feature.par_3_one;
SELECT count(*) AS rows, count(DISTINCT id) AS ids, sum(n) AS sum_n FROM lance_feature.par_7_two;
-- Repeated, because the rotation moves the shares between the two segments
-- that do run.
SELECT count(*) AS again FROM lance_feature.par_3_two;
SELECT count(*) AS again FROM lance_feature.par_3_two;
SELECT array_agg(c ORDER BY c) AS per_segment
  FROM (SELECT gp_execution_segment() AS seg, count(*) AS c FROM lance_feature.par_3_two GROUP BY 1) s;
SELECT array_agg(c ORDER BY c) AS per_segment
  FROM (SELECT gp_execution_segment() AS seg, count(*) AS c FROM lance_feature.par_3_one GROUP BY 1) s;
SELECT array_agg(c ORDER BY c) AS per_segment
  FROM (SELECT gp_execution_segment() AS seg, count(*) AS c FROM lance_feature.par_7_two GROUP BY 1) s;
-- And the rows themselves are the same rows.
SELECT count(*) AS narrow_only FROM (
  SELECT * FROM lance_feature.par_3_two EXCEPT SELECT * FROM lance_feature.par_3) d;
SELECT count(*) AS wide_only FROM (
  SELECT * FROM lance_feature.par_3 EXCEPT SELECT * FROM lance_feature.par_3_two) d;
-- A width wider than the cluster is refused: Cloudberry would build that gang
-- by repeating a content, two QEs would carry the same segment index and read
-- the same fragments, and the planned indexes above the cluster size would have
-- no process at all.  The test cluster has three segments.
CREATE FOREIGN TABLE lance_feature.par_3_wide (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_3.lance', num_segments '4');
SELECT lance_feature.message($$SELECT count(*) FROM lance_feature.par_3_wide$$) AS wider_than_the_cluster;
-- The narrow widths above still work, and so does the default.
SELECT count(*) AS rows FROM lance_feature.par_3_two;
SELECT count(*) AS rows FROM lance_feature.par_3;
-- The refusal belongs to the all-segments path alone.  Under coordinator and
-- any, one process reads every fragment, so there is no split for the width to
-- narrow and nothing to refuse: the same above-cluster width is simply ignored
-- and all 15 rows come back.  A regression that moved the check out of the
-- all-segments branch on the QD would turn these two into errors.
CREATE FOREIGN TABLE lance_feature.par_3_wide_coord (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_3.lance', mpp_execute 'coordinator', num_segments '4');
CREATE FOREIGN TABLE lance_feature.par_3_wide_any (id integer, v text, n bigint)
  SERVER par_files OPTIONS (uri 'frag_3.lance', mpp_execute 'any', num_segments '4');
SELECT count(*) AS rows, sum(n) AS sum_n FROM lance_feature.par_3_wide_coord;
SELECT count(*) AS rows, sum(n) AS sum_n FROM lance_feature.par_3_wide_any;

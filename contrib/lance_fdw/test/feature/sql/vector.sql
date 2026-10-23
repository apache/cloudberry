-- vector: the three distance operators on real[], and ORDER BY distance LIMIT k
-- sent to Lance's nearest-neighbour search.
--
-- vectors.lance has no index, so Lance searches it exhaustively and every
-- pushed-down result must equal the exact one; vectors_idx.lance holds the same
-- rows with an IVF_PQ index, whose results are approximate and are only ever
-- checked by bounds.  Which segment searches depends on the session id, so it
-- is compared against that, never printed.  vectors_seg.lance has the same rows
-- again, indexed the distributed way: one segment for each of fragments 0-2 and
-- none for fragment 3, which is what a search split by fragment has to handle.
\pset format unaligned
DO $$
BEGIN
  EXECUTE format('CREATE SERVER vec_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
END
$$;
CREATE FOREIGN TABLE lance_feature.vec (id integer, cat integer, emb real[])
  SERVER vec_files OPTIONS (uri 'vectors.lance');
CREATE FOREIGN TABLE lance_feature.vec_idx (id integer, cat integer, emb real[])
  SERVER vec_files OPTIONS (uri 'vectors_idx.lance');
CREATE FOREIGN TABLE lance_feature.vec_coord (id integer, cat integer, emb real[])
  SERVER vec_files OPTIONS (uri 'vectors.lance', mpp_execute 'coordinator');
CREATE FOREIGN TABLE lance_feature.vec_any (id integer, cat integer, emb real[])
  SERVER vec_files OPTIONS (uri 'vectors.lance', mpp_execute 'any');
CREATE FOREIGN TABLE lance_feature.vec_nulls (id integer, cat integer, emb real[])
  SERVER vec_files OPTIONS (uri 'vectors_nulls.lance');
CREATE FOREIGN TABLE lance_feature.vec_f64 (id integer, c_fsl_f64_4 double precision[])
  SERVER vec_files OPTIONS (uri 'types_all.lance');
CREATE FOREIGN TABLE lance_feature.vec_seg (id integer, cat integer, emb real[])
  SERVER vec_files OPTIONS (uri 'vectors_seg.lance');

-- The ids a statement returns, in order, from a top-level query: wrapping the
-- statement in a subquery would itself turn the pushdown off.
CREATE FUNCTION lance_feature.ids(stmt text) RETURNS int[]
LANGUAGE plpgsql AS $$
DECLARE
  r record;
  out int[] := '{}';
BEGIN
  FOR r IN EXECUTE stmt LOOP
    out := out || r.id;
  END LOOP;
  RETURN out;
END;
$$;
-- The same statement with the pushdown on and off: equal, or what differs.
CREATE FUNCTION lance_feature.same_as_exact(stmt text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  pushed int[];
  exact int[];
BEGIN
  pushed := lance_feature.ids(stmt);
  PERFORM set_config('lance_fdw.enable_vector_pushdown', 'off', true);
  exact := lance_feature.ids(stmt);
  PERFORM set_config('lance_fdw.enable_vector_pushdown', 'on', true);
  IF pushed = exact THEN
    RETURN 'same as exact';
  END IF;
  RETURN format('pushed %s, exact %s', pushed, exact);
END;
$$;
CREATE FUNCTION lance_feature.pushed(stmt text) RETURNS boolean
LANGUAGE sql AS $$
  SELECT lance_feature.plan_mentions(stmt, 'COSTS OFF', 'Lance Vector Search:');
$$;
-- How many distinct segments the rows of a statement selecting seg came from.
CREATE FUNCTION lance_feature.segments(stmt text) RETURNS int
LANGUAGE plpgsql AS $$
DECLARE
  r record;
  segs int[] := '{}';
BEGIN
  FOR r IN EXECUTE stmt LOOP
    IF NOT r.seg = ANY (segs) THEN
      segs := segs || r.seg;
    END IF;
  END LOOP;
  RETURN cardinality(segs);
END;
$$;
-- Whether EXPLAIN ANALYZE names the segment this session's id selects.
CREATE FUNCTION lance_feature.search_segment(stmt text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  line text;
  expected int := (current_setting('gp_session_id')::int % 3 + 3) % 3;
BEGIN
  FOR line IN EXECUTE format('EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) %s', stmt)
  LOOP
    line := btrim(line);
    IF line LIKE 'Lance Search Segment: %' THEN
      IF line = 'Lance Search Segment: ' || expected THEN
        RETURN 'the session''s segment';
      END IF;
      RETURN line;
    END IF;
  END LOOP;
  RETURN 'no search';
END;
$$;
-- Which way EXPLAIN ANALYZE says the search ran, and why.
CREATE FUNCTION lance_feature.search_mode(stmt text) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
  line text;
BEGIN
  FOR line IN EXECUTE format('EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) %s', stmt)
  LOOP
    line := btrim(line);
    IF line LIKE 'Lance Search Mode: %' THEN
      RETURN substr(line, length('Lance Search Mode: ') + 1);
    END IF;
  END LOOP;
  RETURN 'no search mode';
END;
$$;
-- recall@10 of an indexed table against the exact vec, over forty queries taken
-- from the rows themselves, half of them with a cat filter.
CREATE FUNCTION lance_feature.recall(tbl text DEFAULT 'vec_idx') RETURNS numeric
LANGUAGE plpgsql AS $$
DECLARE
  q real[];
  i int;
  stmt text;
  approx int[];
  exact int[];
  hits int := 0;
BEGIN
  FOR i IN 0..39 LOOP
    SELECT emb INTO q FROM lance_feature.vec WHERE id = i * 37 % 960;
    stmt := CASE WHEN i % 2 = 0
      THEN format('SELECT id FROM lance_feature.%%I ORDER BY emb <-> %L::real[] LIMIT 10', q)
      ELSE format('SELECT id FROM lance_feature.%%I WHERE cat = %s ORDER BY emb <-> %L::real[] LIMIT 10',
                  i % 5, q)
    END;
    approx := lance_feature.ids(format(stmt, tbl));
    exact := lance_feature.ids(format(stmt, 'vec'));
    hits := hits + (SELECT count(*) FROM unnest(approx) a WHERE a = ANY (exact));
  END LOOP;
  RETURN hits / 400.0;
END;
$$;

-- The operators, on plain arrays: Euclidean, 1 - cos (NaN for a zero
-- vector, as in pgvector), and the negative inner product.  STRICT: NULL in,
-- NULL out.
SELECT '{1,2,3}'::real[] <-> '{4,6,3}'::real[] AS l2,
       '{1,0}'::real[] <=> '{0,1}'::real[] AS cos_orthogonal,
       '{1,0}'::real[] <=> '{2,0}'::real[] AS cos_parallel,
       '{0,0}'::real[] <=> '{1,0}'::real[] AS cos_zero,
       '{1,2,3}'::real[] <#> '{4,5,6}'::real[] AS neg_ip,
       NULL::real[] <-> '{1}'::real[] AS l2_null,
       '{}'::real[] <-> '{}'::real[] AS l2_empty;
-- What has no distance is an error, never a guess.
SELECT * FROM lance_feature.report($$SELECT '{1,2,3}'::real[] <-> '{1,2}'::real[]$$);
SELECT * FROM lance_feature.report($$SELECT '{{1,2},{3,4}}'::real[] <=> '{1,2,3,4}'::real[]$$);
SELECT * FROM lance_feature.report($$SELECT '{1,NULL}'::real[] <#> '{1,2}'::real[]$$);

-- Each operator goes to Lance with its metric and k; the filter goes
-- with it, and the plan above the scan is the plain scan's.
SELECT * FROM lance_feature.explain_lance(
  $$SELECT id FROM lance_feature.vec WHERE cat = 2 ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 5$$);
SELECT * FROM lance_feature.explain_lance(
  $$SELECT id FROM lance_feature.vec ORDER BY '{1,2,3,4,5,6,7,8}' <=> emb LIMIT 7$$);
SELECT * FROM lance_feature.explain_lance(
  $$SELECT id FROM lance_feature.vec WHERE cat IN (1, 3) ORDER BY emb <#> '{1,2,3,4,5,6,7,8}' LIMIT 3$$,
  'COSTS OFF, VERBOSE');
SET lance_fdw.nprobes = 4;
SET lance_fdw.refine_factor = 2;
SELECT * FROM lance_feature.explain_lance(
  $$SELECT id FROM lance_feature.vec ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$,
  'COSTS OFF, VERBOSE');
RESET lance_fdw.nprobes;
RESET lance_fdw.refine_factor;

-- Without an index Lance searches every row: the same rows as the
-- exact path, in PostgreSQL's order, for each operator, filtered or not.
SELECT id, round((emb <-> '{1,2,3,4,5,6,7,8}')::numeric, 4) AS dist
  FROM lance_feature.vec WHERE cat = 2 ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 5;
SELECT lance_feature.same_as_exact(
  $$SELECT id FROM lance_feature.vec ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 20$$) AS l2;
SELECT lance_feature.same_as_exact(
  $$SELECT id FROM lance_feature.vec WHERE cat = 4 ORDER BY emb <=> '{-1,2,-3,4,-5,6,-7,8}' LIMIT 20$$) AS cosine;
SELECT lance_feature.same_as_exact(
  $$SELECT id FROM lance_feature.vec WHERE id >= 500 ORDER BY emb <#> '{3,1,4,1,5,9,2,6}' LIMIT 20$$) AS inner_product;
SELECT lance_feature.same_as_exact(
  $$SELECT id FROM lance_feature.vec WHERE cat = 0 ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 1000$$) AS k_beyond_rows;

-- The filter is applied before the search, not after it: the query is
-- row 0, in cluster 0, and the filter keeps only cluster 7.  Searching first and
-- filtering afterwards would leave nothing; the search must still find k rows,
-- and with nprobes set as low as it goes too.
SELECT cardinality(lance_feature.ids(
  $$SELECT id FROM lance_feature.vec_idx
     WHERE id IN (7, 19, 31, 43, 55, 67, 79, 91, 103, 115, 127, 139, 151, 163)
     ORDER BY emb <-> '{-2.5468276,-3.586937,-5.857939,-2.8779135,5.3313246,-9.145959,-4.7810936,-5.7633853}'
     LIMIT 10$$)) AS rows_found;
SET lance_fdw.nprobes = 1;
SELECT cardinality(lance_feature.ids(
  $$SELECT id FROM lance_feature.vec_idx
     WHERE id IN (7, 19, 31, 43, 55, 67, 79, 91, 103, 115, 127, 139, 151, 163)
     ORDER BY emb <-> '{-2.5468276,-3.586937,-5.857939,-2.8779135,5.3313246,-9.145959,-4.7810936,-5.7633853}'
     LIMIT 10$$)) AS rows_found_nprobes_1;
SELECT cardinality(lance_feature.ids(
  $$SELECT id FROM lance_feature.vec_idx WHERE id = 7
     ORDER BY emb <-> '{-2.5468276,-3.586937,-5.857939,-2.8779135,5.3313246,-9.145959,-4.7810936,-5.7633853}'
     LIMIT 10$$)) AS one_far_row_nprobes_1;
RESET lance_fdw.nprobes;

-- Searched by one segment - the one the session id selects - every row
-- comes from it, and the result is still the exact one.
SET lance_fdw.vector_search_mode = single;
SELECT lance_feature.segments(
  $$SELECT gp_execution_segment() AS seg, id FROM lance_feature.vec
     ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 30$$) AS segments_with_rows;
SELECT lance_feature.search_segment(
  $$SELECT id FROM lance_feature.vec ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$);
SELECT lance_feature.search_mode(
  $$SELECT id FROM lance_feature.vec ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$);
SELECT lance_feature.same_as_exact(
  $$SELECT id FROM lance_feature.vec WHERE cat = 4 ORDER BY emb <=> '{-1,2,-3,4,-5,6,-7,8}' LIMIT 20$$) AS single_cosine;
RESET lance_fdw.vector_search_mode;

-- Distributed Top-K.  With no index every segment searches a range of the
-- fragments - every row is read either way, and this way the work divides -
-- and the rows come from all of them.  The exact cases above already ran
-- this way.
SELECT lance_feature.search_mode(
  $$SELECT id FROM lance_feature.vec ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS no_index;
SELECT lance_feature.search_segment(
  $$SELECT id FROM lance_feature.vec ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$);
SELECT lance_feature.segments(
  $$SELECT gp_execution_segment() AS seg, id FROM lance_feature.vec
     ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 30$$) AS segments_with_rows;
-- An index of one segment would be probed in full by every segment, so it
-- stays with one.
SELECT lance_feature.search_mode(
  $$SELECT id FROM lance_feature.vec_idx ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS one_segment;
SELECT lance_feature.search_segment(
  $$SELECT id FROM lance_feature.vec_idx ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$);
-- An index of several segments splits with the fragments.
SELECT lance_feature.search_mode(
  $$SELECT id FROM lance_feature.vec_seg ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS three_segments;
-- Fragment 3 is in no segment: whoever has it searches it exhaustively, so a
-- query equal to one of its rows finds that row first.  Ids 720-959 are
-- fragment 3.
SELECT (lance_feature.ids(format(
  'SELECT id FROM lance_feature.vec_seg ORDER BY emb <-> %L::real[] LIMIT 5',
  (SELECT emb FROM lance_feature.vec WHERE id = 800))))[1] AS unindexed_row_first;
SELECT (lance_feature.ids(format(
  'SELECT id FROM lance_feature.vec_seg WHERE cat = 0 ORDER BY emb <-> %L::real[] LIMIT 5',
  (SELECT emb FROM lance_feature.vec WHERE id = 900))))[1] AS unindexed_row_first_filtered;
-- Each segment returns its own k: no row twice, and k of them.
SELECT count(*) AS k, count(DISTINCT id) AS distinct_ids
  FROM unnest(lance_feature.ids(
    $$SELECT id FROM lance_feature.vec_seg ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 50$$)) AS id;
SELECT lance_feature.recall('vec_seg') >= 0.8 AS recall_at_10_segments;
SET lance_fdw.refine_factor = 5;
SELECT lance_feature.recall('vec_seg') >= 0.95 AS recall_at_10_segments_refine_5;
RESET lance_fdw.refine_factor;
-- A query metric the segments were not trained with is searched exhaustively
-- fragment by fragment, so the result is exact.
SELECT lance_feature.same_as_exact(
  $$SELECT id FROM lance_feature.vec_seg WHERE cat = 3 ORDER BY emb <=> '{1,2,3,4,5,6,7,8}' LIMIT 10$$) AS cosine_on_l2_segments;
-- Fewer fragments than segments: vectors_nulls has two, so one segment has
-- nothing to search and must not search the whole dataset instead.
SELECT lance_feature.same_as_exact(
  $$SELECT id FROM lance_feature.vec_nulls ORDER BY emb <-> '{1,0,0,0}' LIMIT 3$$) AS two_fragments;
SELECT count(*) AS k, count(DISTINCT id) AS distinct_ids
  FROM unnest(lance_feature.ids(
    $$SELECT id FROM lance_feature.vec_nulls ORDER BY emb <-> '{1,0,0,0}' LIMIT 7$$)) AS id;
-- The setting overrides the choice either way, and a cached plan follows it
-- without being replanned: the choice is made when the statement starts.
-- DISTRIBUTED is a keyword in Cloudberry, so that value needs its quotes.
SELECT lance_feature.message($$SET lance_fdw.vector_search_mode = distributed$$) AS unquoted;
SET lance_fdw.vector_search_mode = 'distributed';
SELECT lance_feature.search_mode(
  $$SELECT id FROM lance_feature.vec_idx ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS forced_distributed;
SELECT count(*) AS k, count(DISTINCT id) AS distinct_ids
  FROM unnest(lance_feature.ids(
    $$SELECT id FROM lance_feature.vec_idx ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 20$$)) AS id;
SET lance_fdw.vector_search_mode = single;
SELECT lance_feature.search_mode(
  $$SELECT id FROM lance_feature.vec_seg ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS forced_single;
SELECT (lance_feature.ids(format(
  'SELECT id FROM lance_feature.vec_seg ORDER BY emb <-> %L::real[] LIMIT 5',
  (SELECT emb FROM lance_feature.vec WHERE id = 800))))[1] AS unindexed_row_first_single;
RESET lance_fdw.vector_search_mode;
SET plan_cache_mode = force_generic_plan;
PREPARE vq_mode(real[]) AS SELECT id FROM lance_feature.vec ORDER BY emb <-> $1 LIMIT 3;
SELECT lance_feature.search_mode($$EXECUTE vq_mode('{1,2,3,4,5,6,7,8}')$$) AS generic_auto;
SET lance_fdw.vector_search_mode = single;
SELECT lance_feature.search_mode($$EXECUTE vq_mode('{1,2,3,4,5,6,7,8}')$$) AS generic_single;
RESET lance_fdw.vector_search_mode;
DEALLOCATE vq_mode;
RESET plan_cache_mode;
-- The coordinator searches on its own whatever the setting says.
SET lance_fdw.vector_search_mode = 'distributed';
SELECT lance_feature.search_segment(
  $$SELECT id FROM lance_feature.vec_coord ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS coordinator_distributed;
SELECT lance_feature.search_mode(
  $$SELECT id FROM lance_feature.vec_coord ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS coordinator_mode;
RESET lance_fdw.vector_search_mode;
SELECT lance_feature.message($$SET lance_fdw.vector_search_mode = 'everywhere'$$);

-- The index is approximate; its recall over the fixture is bounded, not
-- exact, because training it is not reproducible.  Re-ranking enough candidates
-- by their exact distance recovers the exact answer.
SELECT lance_feature.recall() >= 0.8 AS recall_at_10_default;
SET lance_fdw.refine_factor = 5;
SELECT lance_feature.recall() >= 0.95 AS recall_at_10_refine_5;
RESET lance_fdw.refine_factor;
-- A metric the index was not trained with: Lance searches exhaustively and the
-- result is exact again.
SELECT lance_feature.same_as_exact(
  $$SELECT id FROM lance_feature.vec_idx WHERE cat = 3 ORDER BY emb <=> '{1,2,3,4,5,6,7,8}' LIMIT 10$$) AS cosine_on_l2_index;

-- A prepared statement keeps pushing down under a generic plan, where
-- the query vector is a parameter that only the QD's evaluation resolves.
SET plan_cache_mode = force_generic_plan;
PREPARE vq(real[]) AS
  SELECT id FROM lance_feature.vec WHERE cat = 1 ORDER BY emb <-> $1 LIMIT 4;
SELECT lance_feature.pushed($$EXECUTE vq('{1,2,3,4,5,6,7,8}')$$) AS generic_plan_pushed;
EXECUTE vq('{1,2,3,4,5,6,7,8}');
EXECUTE vq('{-8,-7,-6,-5,-4,-3,-2,-1}');
SELECT lance_feature.same_as_exact($$EXECUTE vq('{-8,-7,-6,-5,-4,-3,-2,-1}')$$) AS generic_plan;

-- At execution the QD looks at the value: NULL, NaN, a zero vector under
-- cosine and a vector of the wrong size are not searched for, and the
-- statement runs the plain way - with the exact path's result, or its error.
SELECT * FROM lance_feature.explain_lance($$EXECUTE vq(NULL)$$,
  'ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF');
-- Every distance is NULL, so which four rows come back is a tie; only the count
-- is the exact path's.
SELECT cardinality(lance_feature.ids($$EXECUTE vq(NULL)$$)) AS rows_null_vector;
SELECT * FROM lance_feature.explain_lance($$EXECUTE vq('{1,2,3,4,5,6,7,NaN}')$$,
  'ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF');
SELECT lance_feature.message($$EXECUTE vq('{1,2,3}')$$);
PREPARE vq_empty(real[]) AS
  SELECT id FROM lance_feature.vec WHERE cat = 9 ORDER BY emb <-> $1 LIMIT 4;
EXECUTE vq_empty('{1,2,3}');
PREPARE vq_cos(real[]) AS
  SELECT id FROM lance_feature.vec ORDER BY emb <=> $1 LIMIT 4;
SELECT * FROM lance_feature.explain_lance($$EXECUTE vq_cos('{0,0,0,0,0,0,0,0}')$$,
  'ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF');
-- Every distance is NaN: a tie again, so the count is what is compared.
SELECT cardinality(lance_feature.ids($$EXECUTE vq_cos('{0,0,0,0,0,0,0,0}')$$)) AS rows_zero_vector;

-- Turning the pushdown off reaches cached plans at once, and so does a
-- SET LOCAL and the restore at the end of its transaction.
SET lance_fdw.enable_vector_pushdown = off;
SELECT lance_feature.pushed($$EXECUTE vq('{1,2,3,4,5,6,7,8}')$$) AS after_set_off;
RESET lance_fdw.enable_vector_pushdown;
SELECT lance_feature.pushed($$EXECUTE vq('{1,2,3,4,5,6,7,8}')$$) AS after_reset;
BEGIN;
SET LOCAL lance_fdw.enable_vector_pushdown = off;
SELECT lance_feature.pushed($$EXECUTE vq('{1,2,3,4,5,6,7,8}')$$) AS inside_set_local;
ROLLBACK;
SELECT lance_feature.pushed($$EXECUTE vq('{1,2,3,4,5,6,7,8}')$$) AS after_rollback;
SET lance_fdw.vector_pushdown_max_k = 3;
SELECT lance_feature.pushed($$EXECUTE vq('{1,2,3,4,5,6,7,8}')$$) AS k_above_max;
RESET lance_fdw.vector_pushdown_max_k;
RESET plan_cache_mode;
DEALLOCATE vq;
DEALLOCATE vq_empty;
DEALLOCATE vq_cos;

-- 'coordinator' searches in the QD itself; 'any' does not push down.
SELECT * FROM lance_feature.explain_lance(
  $$SELECT id FROM lance_feature.vec_coord WHERE cat = 2 ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$,
  'ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF');
SELECT lance_feature.same_as_exact(
  $$SELECT id FROM lance_feature.vec_coord WHERE cat = 2 ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 10$$) AS coordinator;
SELECT lance_feature.pushed(
  $$SELECT id FROM lance_feature.vec_any ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS any_pushed;
SELECT lance_feature.ids(
  $$SELECT id FROM lance_feature.vec_any WHERE cat = 2 ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS any_ids;

-- Every shape where cutting to k first would not be the same thing
-- stays with PostgreSQL.
SELECT lance_feature.pushed($$SELECT v.id FROM lance_feature.vec v JOIN lance_feature.vec w USING (id)
  ORDER BY v.emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS join_;
SELECT lance_feature.pushed($$SELECT * FROM (SELECT id FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3) s$$) AS subquery;
SELECT lance_feature.pushed($$WITH c AS MATERIALIZED (SELECT id FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3) SELECT * FROM c$$) AS cte;
SELECT lance_feature.pushed($$SELECT cat, min(emb <-> '{1,2,3,4,5,6,7,8}') FROM lance_feature.vec
  GROUP BY cat ORDER BY 2 LIMIT 3$$) AS group_by;
SELECT lance_feature.pushed($$SELECT DISTINCT cat, emb <-> '{1,2,3,4,5,6,7,8}' FROM lance_feature.vec
  ORDER BY 2 LIMIT 3$$) AS distinct_;
SELECT lance_feature.pushed($$SELECT id, count(*) OVER () FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS window_;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3 OFFSET 1$$) AS offset_;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' FETCH FIRST 3 ROWS WITH TIES$$) AS with_ties;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' DESC LIMIT 3$$) AS desc_;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' NULLS FIRST LIMIT 3$$) AS nulls_first;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}', id LIMIT 3$$) AS two_sort_keys;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec WHERE id + 1 > 3
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS local_qual;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> NULL::real[] LIMIT 3$$) AS null_constant;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> emb LIMIT 3$$) AS column_as_query;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> array_fill(random()::real, ARRAY[8]) LIMIT 3$$) AS volatile_function_as_query;
-- An immutable function of constants is folded to a constant before the scan is
-- planned, so that one is a constant query vector like any other.
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> array_fill(1::real, ARRAY[8]) LIMIT 3$$) AS immutable_function_as_query;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec_f64
  ORDER BY c_fsl_f64_4::real[] <-> '{1,2,3,4}' LIMIT 3$$) AS double_precision_column;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3 FOR UPDATE$$) AS for_update;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 10001$$) AS k_above_default_max;
SET lance_fdw.enable_filter_pushdown = off;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec WHERE cat = 2
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS filter_pushdown_off;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS filter_pushdown_off_no_where;
RESET lance_fdw.enable_filter_pushdown;
-- ORCA hands GetForeignPaths a PlannerInfo of its own making, so it never
-- pushes down; pushdown.sql switches the optimizer on the same way.
SET optimizer = on;
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS orca;
RESET optimizer;
-- Same rows either way, for the shapes whose answer does not depend on ties.
SELECT lance_feature.same_as_exact($$SELECT id FROM lance_feature.vec WHERE id + 1 > 3
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS local_qual;
SELECT lance_feature.same_as_exact($$SELECT id FROM lance_feature.vec
  ORDER BY emb <-> '{1,2,3,4,5,6,7,8}' LIMIT 3 OFFSET 1$$) AS offset_;

-- An operator of the same name and signature in another schema is not ours.
CREATE SCHEMA vec_other;
CREATE FUNCTION vec_other.not_l2(real[], real[]) RETURNS double precision
  LANGUAGE sql IMMUTABLE AS 'SELECT 0::double precision';
CREATE OPERATOR vec_other.<-> (LEFTARG = real[], RIGHTARG = real[], FUNCTION = vec_other.not_l2);
SELECT lance_feature.pushed($$SELECT id FROM lance_feature.vec
  ORDER BY emb OPERATOR(vec_other.<->) '{1,2,3,4,5,6,7,8}' LIMIT 3$$) AS other_schema_operator;
DROP SCHEMA vec_other CASCADE;

-- A row whose vector is NULL - or, under cosine, zero - has no distance
-- to search by.  The exact path sorts it last and returns it only when fewer
-- than k rows have a distance; the search never returns it.
SELECT lance_feature.ids($$SELECT id FROM lance_feature.vec_nulls ORDER BY emb <-> '{1,0,0,0}' LIMIT 3$$) AS pushed_3,
       lance_feature.same_as_exact($$SELECT id FROM lance_feature.vec_nulls ORDER BY emb <-> '{1,0,0,0}' LIMIT 3$$) AS exact_3;
SELECT lance_feature.ids($$SELECT id FROM lance_feature.vec_nulls ORDER BY emb <-> '{1,0,0,0}' LIMIT 10$$) AS pushed_10;
SET lance_fdw.enable_vector_pushdown = off;
SELECT lance_feature.ids($$SELECT id FROM lance_feature.vec_nulls ORDER BY emb <-> '{1,0,0,0}' LIMIT 10$$) AS exact_10;
SELECT lance_feature.ids($$SELECT id FROM lance_feature.vec_nulls ORDER BY emb <=> '{1,0,0,0}' LIMIT 10$$) AS exact_10_cosine;
RESET lance_fdw.enable_vector_pushdown;
SELECT lance_feature.ids($$SELECT id FROM lance_feature.vec_nulls ORDER BY emb <=> '{1,0,0,0}' LIMIT 10$$) AS pushed_10_cosine;

DROP SERVER vec_files CASCADE;

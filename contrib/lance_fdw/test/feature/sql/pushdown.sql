-- pushdown: qualifiers that go to Lance, and the columns that stop travelling
-- because they did.  This block carries the types with no side condition at
-- all - bool and the three signed integer widths - so anything that fails here
-- is the machinery, not a precondition.
--
-- Every assertion is phrased from outside: what EXPLAIN says, and whether the
-- rows change when the escape hatch is closed.
\pset format unaligned
DO $$
BEGIN
  EXECUTE format('CREATE SERVER pd_files FOREIGN DATA WRAPPER lance_fdw OPTIONS (base_uri %L)',
                 current_setting('regress.fixture_dir'));
END
$$;
CREATE FOREIGN TABLE lance_feature.pd_types (
  id integer, c_bool boolean, c_int16 smallint, c_int32 integer, c_int64 bigint,
  c_float64 double precision, c_utf8 text)
  SERVER pd_files OPTIONS (uri 'types_all.lance');

-- A qualifier that goes down leaves the local Filter: line and takes
-- its column out of the projection; one that stays does neither.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_types WHERE c_int32 = 0');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_types WHERE c_int64 = 0::bigint');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_types WHERE c_bool');
-- Two qualifiers: both go down and are joined with AND.  PostgreSQL has
-- already split the top-level AND into separate RestrictInfos, so this is the
-- rejoining, not a BoolExpr.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_types WHERE c_int32 = 0 AND c_bool');
-- count(*) with a pushed qualifier: no column is projected at all, but the
-- filter still names one.
SELECT * FROM lance_feature.explain_lance(
  'SELECT count(*) FROM lance_feature.pd_types WHERE c_int32 = 0');

-- Off the whitelist, and each for its own reason: a cross-type operator
-- (bigint vs an untyped 4 resolves to int84gt), a type whose equivalence this
-- block has not argued (float8, text), and a column-to-column comparison.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_types WHERE c_int64 > 4');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_types WHERE c_float64 > 0');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_types WHERE c_utf8 = ''ascii''');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_types WHERE c_int32 = c_int16');

-- Same query, pushdown on and off, compared as a multiset.  Counting and
-- summing rather than differencing is what keeps a lost duplicate visible,
-- which EXCEPT would hide (DECISIONS #9).

-- The same comparison, written directly so the output is readable: each query
-- must return the same multiset with the hatch open and closed.
SET lance_fdw.enable_filter_pushdown = on;
SELECT count(*) AS on_eq0 FROM lance_feature.pd_types WHERE c_int32 = 0;
SELECT count(*) AS on_bool FROM lance_feature.pd_types WHERE c_bool;
SELECT count(*) AS on_ne FROM lance_feature.pd_types WHERE c_int32 <> 0;
SELECT count(*) AS on_lt FROM lance_feature.pd_types WHERE c_int32 < 0;
SELECT count(*) AS on_and FROM lance_feature.pd_types WHERE c_int32 = 0 AND c_bool;
SELECT sum(id) AS on_sum, count(*) AS on_rows FROM lance_feature.pd_types WHERE c_int64 <> 0::bigint;
SET lance_fdw.enable_filter_pushdown = off;
SELECT count(*) AS off_eq0 FROM lance_feature.pd_types WHERE c_int32 = 0;
SELECT count(*) AS off_bool FROM lance_feature.pd_types WHERE c_bool;
SELECT count(*) AS off_ne FROM lance_feature.pd_types WHERE c_int32 <> 0;
SELECT count(*) AS off_lt FROM lance_feature.pd_types WHERE c_int32 < 0;
SELECT count(*) AS off_and FROM lance_feature.pd_types WHERE c_int32 = 0 AND c_bool;
SELECT sum(id) AS off_sum, count(*) AS off_rows FROM lance_feature.pd_types WHERE c_int64 <> 0::bigint;
RESET lance_fdw.enable_filter_pushdown;

-- The rows themselves, not just their count: NULL must stay excluded from both
-- sides of a comparison, exactly as PostgreSQL has it.
SELECT id FROM lance_feature.pd_types WHERE c_int32 = 0 ORDER BY id;
SELECT id FROM lance_feature.pd_types WHERE c_int32 IS NULL ORDER BY id;

-- All three execution modes see the same multiset.
ALTER FOREIGN TABLE lance_feature.pd_types OPTIONS (ADD mpp_execute 'coordinator');
SELECT count(*) AS coordinator_eq0 FROM lance_feature.pd_types WHERE c_int32 = 0;
ALTER FOREIGN TABLE lance_feature.pd_types OPTIONS (SET mpp_execute 'any');
SELECT count(*) AS any_eq0 FROM lance_feature.pd_types WHERE c_int32 = 0;
ALTER FOREIGN TABLE lance_feature.pd_types OPTIONS (DROP mpp_execute);
SELECT count(*) AS all_segments_eq0 FROM lance_feature.pd_types WHERE c_int32 = 0;

-- The hatch has to bite even on a plan that was cached before it closed.
-- A prepared statement with no parameters is planned once and reused, and that
-- cached plan has already dropped the local qualifier and frozen the filter
-- string; nothing in the relcache changes when the GUC does, so without the
-- assign hook's ResetPlanCache() it would go on pushing down (DECISIONS #7).
--
-- The count cannot show this - it is the same either way, which is the whole
-- point - so the observable is the plan itself.  No parameter appears here on
-- purpose: a Param is off the whitelist, and a qualifier that never pushed down
-- could not demonstrate anything.
SET plan_cache_mode = force_generic_plan;
PREPARE pd_p AS SELECT count(*) FROM lance_feature.pd_types WHERE c_int32 = 0;
EXECUTE pd_p;
EXECUTE pd_p;
EXECUTE pd_p;
EXECUTE pd_p;
EXECUTE pd_p;
EXECUTE pd_p;
-- Cached and pushing down.
SELECT * FROM lance_feature.explain_lance('EXECUTE pd_p');
SET lance_fdw.enable_filter_pushdown = off;
-- Same prepared statement, same session: the filter has to be gone.
SELECT * FROM lance_feature.explain_lance('EXECUTE pd_p');
EXECUTE pd_p;
DEALLOCATE pd_p;
RESET plan_cache_mode;
RESET lance_fdw.enable_filter_pushdown;

-- ---------------------------------------------------------------------------
-- The entries that carry a side condition, each with its condition.
-- ---------------------------------------------------------------------------
CREATE FOREIGN TABLE lance_feature.pd_t3 (
  id integer, c_bool boolean, c_int16 smallint, c_int32 integer, c_int64 bigint,
  c_float32 real, c_float64 double precision, c_utf8 text,
  c_date32 date, c_ts_us timestamp, c_ts_us_tz timestamptz)
  SERVER pd_files OPTIONS (uri 'types_all.lance');

-- Cross-type comparison (Design revision 3).  The constant is narrower than
-- the column, so it is rendered in the column's type - which is what int84gt
-- does internally.  The wide-constant direction has no such argument.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_int64 > 4');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_int16 = 1');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_float64 > 0');

-- float: we measured that Lance orders NaN the way PostgreSQL does, so
-- these need no rewrite of any kind.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_float64 > 0.5');
SELECT count(*) AS f64_gt FROM lance_feature.pd_t3 WHERE c_float64 > 0.5;
SELECT count(*) AS f32_ne FROM lance_feature.pd_t3 WHERE c_float32 <> 0::real;

-- text equality needs a deterministic collation; ordering needs C/POSIX in a
-- UTF-8 database.  Both hold here, so both go down.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_utf8 = ''ascii''');
-- Plain ordering under this database's collation.  lc_collate_is_c() only
-- recognises the literal C and POSIX locales, so a C.UTF-8 database does not
-- qualify even though it does sort by code point: the rule is deliberately
-- narrower than the truth, and being narrow only costs a pushdown.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_utf8 > ''z''');
-- Spelt out, the precondition holds and the comparison goes down.  This is the
-- case we measured: Lance orders UTF-8 by bytes, and so does C.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_utf8 > ''z'' COLLATE "C"');
SELECT count(*) AS c_gt_z FROM lance_feature.pd_t3 WHERE c_utf8 > 'z' COLLATE "C";
SELECT count(*) AS c_le_ascii FROM lance_feature.pd_t3 WHERE c_utf8 <= 'ascii' COLLATE "C";
-- A non-C collation: PostgreSQL then orders by ICU rules while Lance orders by
-- UTF-8 bytes, so the ordering comparison must refuse.  Equality under the same
-- collation still goes down, because it is deterministic and deterministic
-- equality is byte equality on both sides.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_utf8 > ''z'' COLLATE "unicode"');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_utf8 = ''ascii'' COLLATE "unicode"');
-- A string with a quote in it: the only escape the dialect has is doubling.
SELECT count(*) AS quoted FROM lance_feature.pd_t3 WHERE c_utf8 = 'it''s';
SELECT count(*) AS backslash FROM lance_feature.pd_t3 WHERE c_utf8 = 'quote"back\slash';

-- date and timestamp without a timezone, with the DATE/TIMESTAMP prefixes the
-- dialect requires.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_date32 = DATE ''2000-02-29''');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_ts_us > TIMESTAMP ''1970-01-01 00:00:00''');
SELECT count(*) AS date_eq FROM lance_feature.pd_t3 WHERE c_date32 = DATE '2000-02-29';
SELECT count(*) AS ts_gt FROM lance_feature.pd_t3 WHERE c_ts_us > TIMESTAMP '1970-01-01 00:00:00';
-- infinity has no counterpart in Lance.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_date32 < DATE ''infinity''');

-- timestamptz is refused outright: it agrees only when the Arrow timezone is
-- UTC, and planning cannot see the timezone.  This one is the
-- silent-wrong-rows case, so the assertion is that no filter is pushed.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_ts_us_tz > TIMESTAMPTZ ''1970-01-01 00:00:00+00''');
SELECT count(*) AS tz_gt FROM lance_feature.pd_t3
  WHERE c_ts_us_tz > TIMESTAMPTZ '1970-01-01 00:00:00+00';

-- OR, NOT, IS NULL, IN - all measured equivalent.
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_int32 = 0 OR c_int32 = 424242');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE NOT (c_int32 = 0)');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_int32 IS NULL');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_int32 IN (0, 424242)');
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_int32 IN (0, NULL)');
-- x <> ANY(...) has the same node shape as IN but the opposite meaning; it
-- must not be rendered as IN (DECISIONS #4).
SELECT * FROM lance_feature.explain_lance(
  'SELECT id FROM lance_feature.pd_t3 WHERE c_int32 <> ANY (ARRAY[0, 424242])');

-- The whole set with side conditions, on and off, as multisets.
SET lance_fdw.enable_filter_pushdown = on;
SELECT count(*) AS a1 FROM lance_feature.pd_t3 WHERE c_int32 = 0 OR c_int32 = 424242;
SELECT count(*) AS a2 FROM lance_feature.pd_t3 WHERE NOT (c_int32 = 0);
SELECT count(*) AS a3 FROM lance_feature.pd_t3 WHERE c_int32 IS NULL;
SELECT count(*) AS a4 FROM lance_feature.pd_t3 WHERE c_int32 IS NOT NULL;
SELECT count(*) AS a5 FROM lance_feature.pd_t3 WHERE c_int32 IN (0, 424242);
SELECT count(*) AS a6 FROM lance_feature.pd_t3 WHERE c_int32 IN (0, NULL);
SELECT count(*) AS a7 FROM lance_feature.pd_t3 WHERE c_int32 NOT IN (0, NULL);
SELECT count(*) AS a8 FROM lance_feature.pd_t3 WHERE c_utf8 > 'z';
SELECT count(*) AS a9 FROM lance_feature.pd_t3 WHERE c_utf8 <= 'ascii';
SELECT count(*) AS a10 FROM lance_feature.pd_t3 WHERE c_int64 > 4;
SET lance_fdw.enable_filter_pushdown = off;
SELECT count(*) AS b1 FROM lance_feature.pd_t3 WHERE c_int32 = 0 OR c_int32 = 424242;
SELECT count(*) AS b2 FROM lance_feature.pd_t3 WHERE NOT (c_int32 = 0);
SELECT count(*) AS b3 FROM lance_feature.pd_t3 WHERE c_int32 IS NULL;
SELECT count(*) AS b4 FROM lance_feature.pd_t3 WHERE c_int32 IS NOT NULL;
SELECT count(*) AS b5 FROM lance_feature.pd_t3 WHERE c_int32 IN (0, 424242);
SELECT count(*) AS b6 FROM lance_feature.pd_t3 WHERE c_int32 IN (0, NULL);
SELECT count(*) AS b7 FROM lance_feature.pd_t3 WHERE c_int32 NOT IN (0, NULL);
SELECT count(*) AS b8 FROM lance_feature.pd_t3 WHERE c_utf8 > 'z';
SELECT count(*) AS b9 FROM lance_feature.pd_t3 WHERE c_utf8 <= 'ascii';
SELECT count(*) AS b10 FROM lance_feature.pd_t3 WHERE c_int64 > 4;
RESET lance_fdw.enable_filter_pushdown;

-- An empty IN list never reaches the wrapper: PostgreSQL folds `= ANY('{}')`
-- into a One-Time Filter before planning the scan, so the foreign scan does not
-- run at all.  The deparser's own empty-array branch is unreachable from SQL and
-- is kept only as a backstop.
SELECT count(*) AS empty_in FROM lance_feature.pd_t3 WHERE c_int32 = ANY('{}'::int[]);

-- ORCA is compiled into this server even though the answer files are the
-- planner's.  Pushdown must not change the rows under it either.
SET optimizer = on;
SET lance_fdw.enable_filter_pushdown = on;
SELECT count(*) AS orca_on_eq, sum(id) AS orca_on_sum FROM lance_feature.pd_t3 WHERE c_int32 = 0;
SELECT count(*) AS orca_on_x, sum(id) AS orca_on_xsum FROM lance_feature.pd_t3 WHERE c_int64 > 4;
SET lance_fdw.enable_filter_pushdown = off;
SELECT count(*) AS orca_off_eq, sum(id) AS orca_off_sum FROM lance_feature.pd_t3 WHERE c_int32 = 0;
SELECT count(*) AS orca_off_x, sum(id) AS orca_off_xsum FROM lance_feature.pd_t3 WHERE c_int64 > 4;
RESET optimizer;
RESET lance_fdw.enable_filter_pushdown;

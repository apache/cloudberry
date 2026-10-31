-- Behaviour of AOCO tables under ALTER TABLE that nothing else covers.
--
-- Indexes over a column a fast ADD COLUMN left missing.
--
-- The index build reads the column through the ordinary scan, so the entries
-- for the rows in the missing region have to carry the value from
-- attmissingval, and every way of reading the index back has to agree with a
-- sequential scan.  Nothing covered this before: the existing index tests all
-- build over columns present in every row.
--
-- The missing region is deliberately small so the planner has a reason to
-- pick an index at all.  Answers are compared as booleans rather than
-- printed, so they do not depend on which plan each optimizer settles on.
CREATE TABLE aoco_missing_idx (a int, b text) USING ao_column DISTRIBUTED BY (a);
INSERT INTO aoco_missing_idx SELECT i, 'x' || i FROM generate_series(1, 300) i;
ALTER TABLE aoco_missing_idx ADD COLUMN c int DEFAULT 7;
INSERT INTO aoco_missing_idx SELECT i, 'y' || i, i FROM generate_series(301, 30000) i;

CREATE INDEX aoco_missing_idx_btree ON aoco_missing_idx(c);
CREATE INDEX aoco_missing_idx_brin ON aoco_missing_idx USING brin (c) WITH (pages_per_range=8);
ANALYZE aoco_missing_idx;

-- the answers, taken with every index path shut off
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
CREATE TEMP TABLE aoco_missing_idx_seq AS
SELECT (SELECT count(*) FROM aoco_missing_idx WHERE c = 7) AS eq_n,
       (SELECT sum(a) FROM aoco_missing_idx WHERE c = 7) AS eq_s,
       (SELECT count(*) FROM aoco_missing_idx WHERE c < 400) AS lt_n,
       (SELECT sum(a) FROM aoco_missing_idx WHERE c < 400) AS lt_s,
       (SELECT count(c) FROM aoco_missing_idx WHERE c = 7) AS only_n;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET enable_indexonlyscan;

SET enable_seqscan = off;
SELECT (SELECT count(*) FROM aoco_missing_idx WHERE c = 7) = eq_n AS eq_n_ok,
       (SELECT sum(a) FROM aoco_missing_idx WHERE c = 7) = eq_s AS eq_s_ok,
       (SELECT count(*) FROM aoco_missing_idx WHERE c < 400) = lt_n AS lt_n_ok,
       (SELECT sum(a) FROM aoco_missing_idx WHERE c < 400) = lt_s AS lt_s_ok,
       (SELECT count(c) FROM aoco_missing_idx WHERE c = 7) = only_n AS only_n_ok
  FROM aoco_missing_idx_seq;
RESET enable_seqscan;

-- and again once the missing region has been deleted from and updated
DELETE FROM aoco_missing_idx WHERE c = 7 AND a % 3 = 0;
UPDATE aoco_missing_idx SET c = 999 WHERE c = 7 AND a % 5 = 0;
VACUUM aoco_missing_idx;
ANALYZE aoco_missing_idx;

SET enable_indexscan = off;
SET enable_bitmapscan = off;
SET enable_indexonlyscan = off;
CREATE TEMP TABLE aoco_missing_idx_seq2 AS
SELECT (SELECT count(*) FROM aoco_missing_idx WHERE c = 7) AS eq_n,
       (SELECT sum(a) FROM aoco_missing_idx WHERE c = 7) AS eq_s,
       (SELECT count(*) FROM aoco_missing_idx WHERE c = 999) AS up_n,
       (SELECT sum(a) FROM aoco_missing_idx WHERE c = 999) AS up_s;
RESET enable_indexscan;
RESET enable_bitmapscan;
RESET enable_indexonlyscan;

SET enable_seqscan = off;
SELECT (SELECT count(*) FROM aoco_missing_idx WHERE c = 7) = eq_n AS eq_n_ok,
       (SELECT sum(a) FROM aoco_missing_idx WHERE c = 7) = eq_s AS eq_s_ok,
       (SELECT count(*) FROM aoco_missing_idx WHERE c = 999) = up_n AS up_n_ok,
       (SELECT sum(a) FROM aoco_missing_idx WHERE c = 999) = up_s AS up_s_ok
  FROM aoco_missing_idx_seq2;
RESET enable_seqscan;

DROP TABLE aoco_missing_idx;

-- ANALYZE over a column a fast ADD COLUMN left missing.
--
-- Half the rows take their value from attmissingval and never appear in the
-- column's data file at all, so the sample has to pick them up through the
-- same missing-value path the scan uses or the statistics come out skewed
-- towards whatever was written after the ALTER.
--
-- The heap table alongside carries the same values and is the reference:
-- its ANALYZE is untouched upstream code.  A statistics target of 1000 makes
-- the sample cover the whole table, so both sides are exact and this does not
-- depend on sampling luck.
CREATE TABLE aoco_missing_stats (a int, b text) USING ao_column DISTRIBUTED BY (a);
INSERT INTO aoco_missing_stats SELECT i, 'x' || i FROM generate_series(1, 30000) i;
ALTER TABLE aoco_missing_stats ADD COLUMN c int DEFAULT 7;
INSERT INTO aoco_missing_stats SELECT i, 'y' || i, i FROM generate_series(30001, 60000) i;

CREATE TABLE heap_missing_stats (a int, b text, c int) DISTRIBUTED BY (a);
INSERT INTO heap_missing_stats SELECT i, 'x' || i, 7 FROM generate_series(1, 30000) i;
INSERT INTO heap_missing_stats SELECT i, 'y' || i, i FROM generate_series(30001, 60000) i;

ALTER TABLE aoco_missing_stats ALTER COLUMN c SET STATISTICS 1000;
ALTER TABLE heap_missing_stats ALTER COLUMN c SET STATISTICS 1000;
ANALYZE aoco_missing_stats;
ANALYZE heap_missing_stats;

-- same statistics as the heap table, and the right ones: half the rows hold
-- the missing value, nothing is null, and 30001 of 60000 values are distinct
SELECT ao.null_frac = hp.null_frac AS null_frac_same,
       ao.n_distinct = hp.n_distinct AS n_distinct_same,
       (ao.most_common_vals::text::int[])[1] = (hp.most_common_vals::text::int[])[1] AS mcv_same,
       ao.most_common_freqs[1] = hp.most_common_freqs[1] AS mcf_same,
       ao.null_frac = 0 AS no_nulls,
       (ao.most_common_vals::text::int[])[1] = 7 AS mcv_is_missing_value,
       ao.most_common_freqs[1] = 0.5 AS mcv_is_half,
       ao.n_distinct = -0.5000167::real AS n_distinct_right
  FROM pg_stats ao, pg_stats hp
 WHERE ao.tablename = 'aoco_missing_stats' AND ao.attname = 'c'
   AND hp.tablename = 'heap_missing_stats' AND hp.attname = 'c';

-- the row count itself has to be exact, not just the distribution
SELECT (SELECT reltuples::bigint FROM pg_class WHERE oid = 'aoco_missing_stats'::regclass) = 60000 AS reltuples_ok;

DROP TABLE aoco_missing_stats;
DROP TABLE heap_missing_stats;

-- ALTER TABLE <aoco> SET (compresstype=..., compresslevel=...) rewrites the
-- table, so it has to actually re-encode the columns that were following the
-- table-level default rather than just relabel pg_class.reloptions.  A column
-- carrying its own explicit ENCODING keeps it.

PREPARE col_encoding AS
SELECT c.relname, a.attname, e.attoptions
  FROM pg_attribute_encoding e, pg_class c, pg_attribute a
 WHERE e.attrelid = c.oid AND e.attnum = a.attnum AND e.attrelid = a.attrelid
   AND c.relname LIKE $1
 ORDER BY c.relname, a.attnum;

-- 1. a column with no ENCODING of its own tracks a later change to the
--    table-level options; one with an explicit ENCODING does not
CREATE TABLE atsr_basic(a int, b int, c int ENCODING (compresstype=zstd, compresslevel=2))
  USING ao_column DISTRIBUTED BY (a);
EXECUTE col_encoding('atsr_basic');

ALTER TABLE atsr_basic SET (compresstype=zstd, compresslevel=5);
EXECUTE col_encoding('atsr_basic');
SELECT reloptions FROM pg_class WHERE oid = 'atsr_basic'::regclass;

-- 2. RESET clears pg_class.reloptions but does not rewrite an AO relation in
--    cbdb -- ATExecSetRelOptions() does not report aoopt_changed for it -- so
--    the columns stay as they are.  That is the consistent answer while no
--    rewrite happens: moving the catalog without re-encoding the data would
--    leave the two disagreeing.  Spelling the new default out explicitly does
--    rewrite, and does take the columns with it.
ALTER TABLE atsr_basic RESET (compresstype, compresslevel);
EXECUTE col_encoding('atsr_basic');
SELECT reloptions FROM pg_class WHERE oid = 'atsr_basic'::regclass;

ALTER TABLE atsr_basic SET (compresstype=none, compresslevel=0);
EXECUTE col_encoding('atsr_basic');

DROP TABLE atsr_basic;

-- 3. an explicit ALTER COLUMN ... SET ENCODING in the same command as a
--    table-level SET.  ATExecSetColumnEncoding snapshots every column's
--    current encoding into tab->new_crsds as a side effect, so the other
--    columns must still be recognised as following the old default.
CREATE TABLE atsr_mixed(a int, b int, c int) USING ao_column DISTRIBUTED BY (a);
ALTER TABLE atsr_mixed
      ALTER COLUMN c SET ENCODING (compresstype=zstd, compresslevel=1),
      SET (compresstype=zstd, compresslevel=7);
EXECUTE col_encoding('atsr_mixed');
DROP TABLE atsr_mixed;

-- 4. the rewrite has to keep the data readable, and actually compress it
CREATE TABLE atsr_data(a int, b text) USING ao_column DISTRIBUTED BY (a);
INSERT INTO atsr_data SELECT i, repeat('compressible', 50) FROM generate_series(1, 20000) i;
CREATE TEMP TABLE atsr_data_before AS
SELECT pg_relation_size('atsr_data') AS sz;

ALTER TABLE atsr_data SET (compresstype=zstd, compresslevel=9);
EXECUTE col_encoding('atsr_data');

SELECT count(*) AS n,
       sum(a) AS sum_a,
       count(*) FILTER (WHERE b = repeat('compressible', 50)) AS b_intact
  FROM atsr_data;
SELECT pg_relation_size('atsr_data') < sz / 2 AS got_smaller FROM atsr_data_before;

DROP TABLE atsr_data;

-- 5. cbdb rejects AO storage options on a partitioned root outright, so the
--    root never gets a new default to hand down.  Pinned here so that whoever
--    lifts that restriction sees this change and ports the root handling that
--    goes with it; a leaf partition takes the new default like any other
--    table.
CREATE TABLE atsr_part(a int, b int) USING ao_column DISTRIBUTED BY (a)
  PARTITION BY RANGE(a);
CREATE TABLE atsr_part_1 PARTITION OF atsr_part FOR VALUES FROM (1) TO (100);
INSERT INTO atsr_part SELECT i, i FROM generate_series(1, 99) i;

ALTER TABLE atsr_part SET (compresstype=zstd, compresslevel=6);  -- fail

ALTER TABLE atsr_part_1 SET (compresstype=zstd, compresslevel=6);
EXECUTE col_encoding('atsr_part%');
SELECT count(*) FROM atsr_part;

DROP TABLE atsr_part;
DEALLOCATE col_encoding;

--------------------------------------------------------------------------------
-- ALTER COLUMN TYPE must rebuild every index that depends on the column
--
-- The AOCO column-only rewrite returns before reindex_relation(), so
-- aocs_writecol_reindex() stands in for it.  It used to decide which indexes
-- to rebuild by testing pg_index.indkey alone, which misses an expression
-- index (indkey holds InvalidAttrNumber, the reference lives in indexprs) and
-- a partial index that names the column only in its predicate.  Those indexes
-- were left with a freshly assigned but empty relfilenode -- not even a btree
-- metapage -- so every later plan failed in _bt_getrootheight() with
-- "could not read block 0".
--------------------------------------------------------------------------------
CREATE TABLE at_reidx_plain(k int, a int) USING ao_column DISTRIBUTED BY (k);
CREATE TABLE at_reidx_expr(k int, a int) USING ao_column DISTRIBUTED BY (k);
CREATE TABLE at_reidx_pred(k int, a int) USING ao_column DISTRIBUTED BY (k);
CREATE TABLE at_reidx_other(k int, a int, b int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_reidx_plain SELECT i, i FROM generate_series(1, 20) i;
INSERT INTO at_reidx_expr  SELECT i, i FROM generate_series(1, 20) i;
INSERT INTO at_reidx_pred  SELECT i, i FROM generate_series(1, 20) i;
INSERT INTO at_reidx_other SELECT i, i, i FROM generate_series(1, 20) i;

CREATE INDEX at_reidx_plain_i ON at_reidx_plain (a);
CREATE INDEX at_reidx_expr_i  ON at_reidx_expr ((a + 1));
CREATE INDEX at_reidx_pred_i  ON at_reidx_pred (k) WHERE a > 10;
-- an expression index that does NOT reference the altered column: must keep
-- working, and must not be needlessly rebuilt into something broken either
CREATE INDEX at_reidx_other_i ON at_reidx_other ((b + 1));

ALTER TABLE at_reidx_plain ALTER COLUMN a TYPE bigint;
ALTER TABLE at_reidx_expr  ALTER COLUMN a TYPE bigint;
ALTER TABLE at_reidx_pred  ALTER COLUMN a TYPE bigint;
ALTER TABLE at_reidx_other ALTER COLUMN a TYPE bigint;

-- A seqscan is enough to trip the unbuilt-index bug: the planner reads every
-- btree's metapage through _bt_getrootheight() regardless of the plan it picks.
SET enable_indexscan = off;
SET enable_bitmapscan = off;
SELECT count(*) FROM at_reidx_plain WHERE a > 10;
SELECT count(*) FROM at_reidx_expr  WHERE a + 1 > 11;
SELECT count(*) FROM at_reidx_pred  WHERE a > 10;
SELECT count(*) FROM at_reidx_other WHERE b + 1 > 11;
RESET enable_indexscan;
RESET enable_bitmapscan;

-- and the index path must agree with the seqscan path
SET enable_seqscan = off;
SELECT count(*) FROM at_reidx_plain WHERE a > 10;
SELECT count(*) FROM at_reidx_expr  WHERE a + 1 > 11;
SELECT count(*) FROM at_reidx_pred  WHERE k > 10 AND a > 10;
SELECT count(*) FROM at_reidx_other WHERE b + 1 > 11;
RESET enable_seqscan;

DROP TABLE at_reidx_plain, at_reidx_expr, at_reidx_pred, at_reidx_other;

--------------------------------------------------------------------------------
-- ALTER COLUMN TYPE must still enforce NOT NULL and CHECK constraints
--
-- The column-only rewrite returns from ATRewriteTables() before the scan loop
-- that would have validated them, so aocs_writecol_rewritesegfiles() checks
-- them per row instead.  Letting a violation through is worse than it looks:
-- the catalog still claims the constraint holds, so the planner refutes
-- predicates with it ("One-Time Filter: false") and queries quietly return
-- too few rows.
--------------------------------------------------------------------------------
CREATE TABLE at_con_notnull(k int, a int NOT NULL) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_con_notnull VALUES (1, 1);
ALTER TABLE at_con_notnull ALTER COLUMN a TYPE bigint USING NULL::bigint;  -- fail
SELECT count(*) FROM at_con_notnull WHERE a IS NULL;

CREATE TABLE at_con_check(k int, a int CHECK (a > 0)) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_con_check VALUES (1, 1);
ALTER TABLE at_con_check ALTER COLUMN a TYPE bigint USING -a::bigint;  -- fail
SELECT count(*) FROM at_con_check WHERE a <= 0;

-- A CHECK that references a column other than the rewritten one evaluates
-- correctly too: the rewrite scan is opened with a NULL projection, so every
-- column is read and the slot handed to ExecCheck() is a complete tuple.
CREATE TABLE at_con_multicol(k int, a int, b int, CHECK (a < b))
  USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_con_multicol VALUES (1, 1, 10);
ALTER TABLE at_con_multicol ALTER COLUMN a TYPE bigint USING 100::bigint;  -- fail
ALTER TABLE at_con_multicol ALTER COLUMN a TYPE bigint USING 5::bigint;
SELECT * FROM at_con_multicol;

-- A valid ALTER on a constrained column must still skip the table rewrite:
-- validating is not a reason to give up the optimization.
CREATE TABLE at_con_norewrite(k int, a int NOT NULL CHECK (a > 0))
  USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_con_norewrite SELECT i, i FROM generate_series(1, 100) i;
CREATE TEMP TABLE at_con_before AS
SELECT pg_relation_filenode('at_con_norewrite') AS fn;

ALTER TABLE at_con_norewrite ALTER COLUMN a TYPE bigint;

SELECT pg_relation_filenode('at_con_norewrite') = fn AS not_rewritten
  FROM at_con_before;
SELECT count(*), sum(a) FROM at_con_norewrite;

DROP TABLE at_con_notnull, at_con_check, at_con_multicol, at_con_norewrite;

--------------------------------------------------------------------------------
-- ADD COLUMN must enforce CHECK constraints against the existing rows
--
-- The column-only ADD COLUMN path never reads the existing columns -- that is
-- the whole optimization -- so the access method cannot evaluate a CHECK that
-- mentions them: the slot it works from is all-NULL apart from the new
-- columns.  Evaluating constraints there was wrong in both directions, so
-- ATRewriteTables() runs the ordinary validate-only scan afterwards instead,
-- which sees the tuples as they stand on disk.  NOT NULL on a newly added
-- column is still checked by the access method: that value is in the slot.
--------------------------------------------------------------------------------
CREATE TABLE at_add_fn(k int, a int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_add_fn VALUES (1, 5);
-- 100 < 5 is false; used to be accepted because "a" read as NULL
ALTER TABLE at_add_fn ADD COLUMN c int DEFAULT 100 CHECK (c < a);  -- fail
SELECT count(*) FROM at_add_fn;

CREATE TABLE at_add_fp(k int, a int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_add_fp VALUES (1, 5);
-- every row satisfies this; used to be rejected because "a" read as NULL
ALTER TABLE at_add_fp ADD COLUMN c int DEFAULT 1 CHECK (a IS NOT NULL);
SELECT * FROM at_add_fp;

CREATE TABLE at_add_ok(k int, a int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_add_ok VALUES (1, 5);
ALTER TABLE at_add_ok ADD COLUMN c int DEFAULT 1 CHECK (c < a);
SELECT * FROM at_add_ok;

-- a CHECK naming only pre-existing columns is validated too
CREATE TABLE at_add_old(k int, a int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_add_old VALUES (1, -3);
ALTER TABLE at_add_old ADD COLUMN c int DEFAULT 1 CHECK (a > 0);  -- fail

-- NOT NULL on the added column is still the access method's job
CREATE TABLE at_add_nn(k int, a int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_add_nn VALUES (1, 5);
ALTER TABLE at_add_nn ADD COLUMN c int NOT NULL DEFAULT NULL;  -- fail

-- and a plain ADD COLUMN must still skip the table rewrite
CREATE TABLE at_add_norw(k int, a int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_add_norw SELECT i, i FROM generate_series(1, 100) i;
CREATE TEMP TABLE at_add_before AS
SELECT pg_relation_filenode('at_add_norw') AS fn DISTRIBUTED BY (fn);

ALTER TABLE at_add_norw ADD COLUMN c int DEFAULT 7;

SELECT pg_relation_filenode('at_add_norw') = fn AS not_rewritten FROM at_add_before;
SELECT count(*), sum(a), sum(c) FROM at_add_norw;

DROP TABLE at_add_fn, at_add_fp, at_add_ok, at_add_old, at_add_nn, at_add_norw;

--------------------------------------------------------------------------------
-- Constraint validation must ignore deleted rows
--
-- The column-only rewrite scans under SnapshotAny on purpose: every row has to
-- be rewritten, visible or not, so that the new segfile's block structure --
-- and therefore every row number -- still lines up with the columns that are
-- not being rewritten.  Those dead rows are not part of the table's contents,
-- though, so the per-row validation has to skip them.  Checking them rejected
-- statements that are perfectly valid for the live data, which heap and ao_row
-- accept: they rewrite under an MVCC snapshot and never see the dead rows.
--------------------------------------------------------------------------------
CREATE TABLE at_dead_nn(k int, a int NOT NULL) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_dead_nn VALUES (1, 1), (2, 2);
DELETE FROM at_dead_nn WHERE k = 1;
-- only the deleted row would become NULL
ALTER TABLE at_dead_nn ALTER COLUMN a TYPE bigint USING nullif(a, 1)::bigint;
SELECT * FROM at_dead_nn;

CREATE TABLE at_dead_ck(k int, a int CHECK (a > 0)) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_dead_ck VALUES (1, 1), (2, 2);
DELETE FROM at_dead_ck WHERE k = 1;
-- only the deleted row would violate the CHECK
ALTER TABLE at_dead_ck ALTER COLUMN a TYPE bigint USING (a - 1)::bigint;
SELECT * FROM at_dead_ck;

-- a live violating row is still caught when a dead one is dragged along
CREATE TABLE at_dead_live(k int, a int NOT NULL) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_dead_live VALUES (1, 1), (2, 2);
DELETE FROM at_dead_live WHERE k = 2;
ALTER TABLE at_dead_live ALTER COLUMN a TYPE bigint USING nullif(a, 1)::bigint;  -- fail

-- a rolled back DELETE leaves the row live, so it must still be checked
CREATE TABLE at_dead_abort(k int, a int NOT NULL) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_dead_abort VALUES (1, 1), (2, 2);
BEGIN;
DELETE FROM at_dead_abort WHERE k = 1;
ROLLBACK;
ALTER TABLE at_dead_abort ALTER COLUMN a TYPE bigint USING nullif(a, 1)::bigint;  -- fail

-- the DELETE may be in the same transaction as the ALTER
CREATE TABLE at_dead_samexact(k int, a int NOT NULL) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_dead_samexact VALUES (1, 1), (2, 2);
BEGIN;
DELETE FROM at_dead_samexact WHERE k = 1;
ALTER TABLE at_dead_samexact ALTER COLUMN a TYPE bigint USING nullif(a, 1)::bigint;
COMMIT;
SELECT * FROM at_dead_samexact;

-- the rewritten column must still read back correctly around the dead rows
CREATE TABLE at_dead_rows(k int, a int NOT NULL, c int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_dead_rows SELECT i, i, i * 10 FROM generate_series(1, 1000) i;
DELETE FROM at_dead_rows WHERE a % 7 = 0;
CREATE INDEX at_dead_rows_a ON at_dead_rows(a);
ALTER TABLE at_dead_rows ALTER COLUMN a TYPE bigint USING nullif(a, 7)::bigint;
SELECT count(*), sum(a), sum(c) FROM at_dead_rows;
SELECT count(*) FROM at_dead_rows WHERE a BETWEEN 100 AND 200;
VACUUM at_dead_rows;
SELECT count(*), sum(a), sum(c) FROM at_dead_rows;

-- ADD COLUMN validates through ATRewriteTables(), whose scan is an ordinary
-- MVCC one, so the dead rows never reach it
CREATE TABLE at_dead_add(k int, a int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_dead_add VALUES (1, -1), (2, 5);
DELETE FROM at_dead_add WHERE k = 1;
ALTER TABLE at_dead_add ADD COLUMN b int DEFAULT 10, ADD CONSTRAINT positive_a CHECK (a > 0);
SELECT * FROM at_dead_add;

DROP TABLE at_dead_nn, at_dead_ck, at_dead_live, at_dead_abort, at_dead_samexact,
           at_dead_rows, at_dead_add;

--------------------------------------------------------------------------------
-- ADD COLUMN combined with ALTER COLUMN TYPE in one command
--
-- Both column-only paths run, the rewrite first having nothing to say about
-- the columns ADD COLUMN appends: it builds its tuples from the descriptor
-- taken before the command, so those columns sit at the zero the slot was
-- allocated with, marked not-null.  Checking a constraint over one of them
-- there read a datum of 0 -- for a text column, a NULL pointer into the
-- detoaster, which took the segment down.  The constraints are checked by the
-- validate-only pass that follows instead, which reads whole tuples back off
-- disk.
--------------------------------------------------------------------------------
CREATE TABLE at_both(k int, a int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_both VALUES (1, 20);
ALTER TABLE at_both ALTER COLUMN a TYPE bigint,
  ADD COLUMN b text DEFAULT 'ok' CHECK (length(b) > 0);
SELECT * FROM at_both;

-- the new column's CHECK must still be enforced, not merely survived
CREATE TABLE at_both_newcol(k int, a int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_both_newcol VALUES (1, 20);
ALTER TABLE at_both_newcol ALTER COLUMN a TYPE bigint,
  ADD COLUMN b text DEFAULT '' CHECK (length(b) > 0);  -- fail

-- and so must a CHECK on the column being rewritten, whose checking the
-- rewrite defers to the same pass
CREATE TABLE at_both_oldcol(k int, a int CHECK (a > 0)) USING ao_column
  DISTRIBUTED BY (k);
INSERT INTO at_both_oldcol VALUES (1, 20);
ALTER TABLE at_both_oldcol ALTER COLUMN a TYPE bigint USING -a::bigint,
  ADD COLUMN b text DEFAULT 'ok';  -- fail

-- NOT NULL on the added column, alongside a rewrite
CREATE TABLE at_both_nn(k int, a int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_both_nn VALUES (1, 20);
ALTER TABLE at_both_nn ALTER COLUMN a TYPE bigint,
  ADD COLUMN b int NOT NULL DEFAULT NULL;  -- fail

DROP TABLE at_both, at_both_newcol, at_both_oldcol, at_both_nn;

--------------------------------------------------------------------------------
-- Deleted rows must not be fed to the rewrite's expression either
--
-- The rewrite scans under SnapshotAny, so it sees rows a DELETE has already
-- taken out of the table.  Exempting those from the constraint checks is not
-- enough: the expression is evaluated before any check runs, and a cast fails
-- on a value that is no longer part of the table.  The visibility map also has
-- to be opened for that case, which the old condition -- something to check --
-- did not cover, a plain ALTER COLUMN TYPE ... USING having no constraints at
-- all.  heap and ao_row rewrite under an MVCC snapshot and never see the row.
--------------------------------------------------------------------------------
CREATE TABLE at_dead_cast(k int, a text) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_dead_cast VALUES (1, 'bad'), (1, '2');
DELETE FROM at_dead_cast WHERE a = 'bad';
ALTER TABLE at_dead_cast ALTER COLUMN a TYPE int USING a::int;
SELECT * FROM at_dead_cast;

-- a live row that cannot be cast is still an error
CREATE TABLE at_live_cast(k int, a text) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_live_cast VALUES (1, 'bad'), (1, '2');
ALTER TABLE at_live_cast ALTER COLUMN a TYPE int USING a::int;  -- fail

-- the dead row is exempt from the checks too, but a live one is not
CREATE TABLE at_dead_cast_nn(k int, a text NOT NULL) USING ao_column
  DISTRIBUTED BY (k);
INSERT INTO at_dead_cast_nn VALUES (1, 'bad'), (1, '2');
DELETE FROM at_dead_cast_nn WHERE a = 'bad';
ALTER TABLE at_dead_cast_nn ALTER COLUMN a TYPE int USING NULLIF(a, '2')::int;  -- fail

-- SET ENCODING only restates the column, so it keeps skipping the per-row
-- visibility lookup; deleted rows must still come through untouched
CREATE TABLE at_dead_enc(k int, a int) USING ao_column DISTRIBUTED BY (k);
INSERT INTO at_dead_enc SELECT 1, i FROM generate_series(1, 50) i;
DELETE FROM at_dead_enc WHERE a <= 10;
ALTER TABLE at_dead_enc ALTER COLUMN a SET ENCODING (compresstype=zstd, compresslevel=1);
SELECT count(*), sum(a) FROM at_dead_enc;

DROP TABLE at_dead_cast, at_live_cast, at_dead_cast_nn, at_dead_enc;

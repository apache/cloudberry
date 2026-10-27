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

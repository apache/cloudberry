--
-- An UPDATE that does not move the row between segments runs as an in-place
-- update on append-optimized tables too: the storage engine turns it into a
-- delete plus an append by itself, exactly as the Postgres planner has always
-- assumed.  ORCA used to be unable to emit that plan -- the executor fetched
-- the old tuple by tid, which AO cannot do -- so the DXL-to-PlStmt translation
-- forced a split update, failed to find the DMLAction column, and silently
-- fell back to the planner.
--
CREATE SCHEMA orca_split_update;
SET search_path TO orca_split_update;

-- make a fallback visible instead of silent
SET optimizer_trace_fallback = on;

CREATE TABLE ao_su (id bigint, v bigint) USING ao_row DISTRIBUTED BY (id);
CREATE TABLE aocs_su (id bigint, v bigint) USING ao_column DISTRIBUTED BY (id);
CREATE TABLE heap_su (id bigint, v bigint) USING heap DISTRIBUTED BY (id);

INSERT INTO ao_su SELECT g, g FROM generate_series(1, 10) g;
INSERT INTO aocs_su SELECT g, g FROM generate_series(1, 10) g;
INSERT INTO heap_su SELECT g, g FROM generate_series(1, 10) g;

-- Updating a non-distribution column: the row stays on its segment, so the
-- plan is a plain ModifyTable with neither a Split Update nor a Motion.
EXPLAIN (COSTS OFF) UPDATE ao_su SET v = 999 WHERE id = 1;
EXPLAIN (COSTS OFF) UPDATE aocs_su SET v = 999 WHERE id = 1;

-- Updating the distribution column moves the row, which still needs the split
-- and a Motion.
EXPLAIN (COSTS OFF) UPDATE ao_su SET id = 999 WHERE id = 1;

-- Heap has always taken this path; it must be unaffected.
EXPLAIN (COSTS OFF) UPDATE heap_su SET v = 999 WHERE id = 1;

-- The update must not leave the old tuple version behind.
UPDATE ao_su SET v = 999 WHERE id = 1;
UPDATE aocs_su SET v = 999 WHERE id = 1;
UPDATE heap_su SET v = 999 WHERE id = 1;

SELECT count(*) AS ao_rows FROM ao_su;
SELECT count(*) AS aocs_rows FROM aocs_su;
SELECT id, v FROM ao_su WHERE id <= 2 ORDER BY id;
SELECT id, v FROM aocs_su WHERE id <= 2 ORDER BY id;
SELECT id, v FROM heap_su WHERE id <= 2 ORDER BY id;

DROP SCHEMA orca_split_update CASCADE;

-- Memory reserved for a query over a table with many partitions.
--
-- The Append runs one partition at a time, so the branches never hold their
-- memory together.  Reserving memory for all of them at once used to make the
-- statement fail outright.
CREATE TABLE memquota_parts (id int, d date) DISTRIBUTED BY (id)
  PARTITION BY RANGE (d)
  (START ('2020-01-01'::date) END ('2020-07-19'::date) EVERY ('1 day'::interval));

-- ORCA scans the partitions with a single Dynamic Seq Scan, so the Append this
-- test is about is only built by the Postgres planner.
SET optimizer = off;
SET statement_mem = '2MB';

SELECT count(*) FROM memquota_parts;

-- the same through a subquery, so the Append is not the top node
SELECT count(*) FROM (SELECT * FROM memquota_parts WHERE id > 0) x;

RESET statement_mem;
RESET optimizer;
DROP TABLE memquota_parts;

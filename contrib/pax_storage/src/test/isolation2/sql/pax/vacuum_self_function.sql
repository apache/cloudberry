-- @Description Test scenario where a backend accesses PAX table with a snapshot
-- that was acquired before vacuum.
--
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT, b INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1, 100) AS i;

DELETE FROM pax_tbl WHERE a <= 30;

create or replace function myfunc() returns bigint as $$
begin  /* inside a function */
  perform pg_sleep(10); /* inside a function */
  return (select count(*) from pax_tbl);  /* inside a function */
end;  /* inside a function */
$$ stable language plpgsql;

-- Launch function into the background.
1&: select myfunc();

-- Meanwhile, DELETE some rows and VACUUM. VACUUM should not recycle the
-- old tuple versions that are still needed by the function later. It will
-- compact the segfile, but keep the old segfile in AWAITING_DROP state.
2: DELETE FROM pax_tbl WHERE a <= 50;
2: SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');
2: VACUUM pax_tbl;
2: SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');

-- A second VACUUM shouldn't recycle them either.
2: VACUUM pax_tbl;
2: SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');
1<:

-- Now that the first transaction has finished, VACUUM can recycle.
2: VACUUM pax_tbl;
2: SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('pax_tbl');

-- start_ignore
CREATE EXTENSION gp_inject_fault;
-- end_ignore

-- start_matchsubs
-- m/WARNING:.*Any temporary tables for this session have been dropped because the gang was disconnected/
-- s/session id \=\s*\d+/session id \= DUMMY/gm
-- end_matchsubs

CREATE TEMP TABLE t1();

-- Emulate an OOM error and throw FATAL on any palloc() call.
SELECT gp_inject_fault('cdb_freelist_append_oom', 'skip', dbid),
       gp_inject_fault('fatal_on_palloc_oom', 'skip', dbid)
  FROM gp_segment_configuration
  WHERE role = 'p' AND content = -1;

-- We should gracefully recover instead of entering recursion, getting SIGSEGV,
-- or trying to allocate any more memory.
DO $$
  DECLARE
    rec1 RECORD;
  BEGIN
    FOR rec1 IN SELECT * FROM t1
    LOOP
      NULL;
    END LOOP;
  END;
$$;

SELECT gp_inject_fault('all', 'reset', dbid)
  FROM gp_segment_configuration
  WHERE role = 'p' AND content = -1;

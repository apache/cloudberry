-- pg_query_state signal API (extension v1.2): catalog contract + negative paths.
--
-- Deterministic coverage only: SQL-visible function/type registration and the
-- input-validation error branches. The asynchronous happy path (poll a live
-- query and observe per-node stats) is exercised separately under isolation2,
-- since it depends on a second running backend and timing.
-- start_ignore
CREATE EXTENSION IF NOT EXISTS gp_stats_collector;
-- end_ignore

--
-- Catalog contract: the three SQL-visible functions are registered in the gpsc
-- schema with the expected return type and dispatch (exec) location.
-- proexeclocation: c = coordinator, a = any (QE-local), s = all segments.
--
SELECT proname,
       pronargs,
       prorettype::regtype AS returns,
       proexeclocation
FROM pg_proc
WHERE pronamespace = 'gpsc'::regnamespace
  AND proname IN ('pg_query_state', 'pg_query_state_backends', 'cbdb_mpp_query_state')
ORDER BY proname;

-- Composite identifier type used by the signal layer is present.
SELECT typname
FROM pg_type
WHERE typnamespace = 'gpsc'::regnamespace
  AND typname = 'gp_segment_pid';

--
-- Negative: a backend cannot poll its own state.
--
SELECT gpsc.pg_query_state(pg_backend_pid(), '\x00112233445566778899aabbccddeeff'::bytea);
SELECT * FROM gpsc.pg_query_state_backends(pg_backend_pid());

--
-- Negative: a pid that maps to no live backend is rejected.
--
SELECT gpsc.pg_query_state(-1, '\x00112233445566778899aabbccddeeff'::bytea);
SELECT * FROM gpsc.pg_query_state_backends(-1);

-- Cleanup
-- start_ignore
DROP EXTENSION gp_stats_collector;
-- end_ignore

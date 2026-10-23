-- Test that gp_log_backend_memory_contexts dispatches and
-- handles responses correctly

-- show expected number of successful responses to logging a
-- known-good session with no target contentID
WITH sessionCTE AS (
    SELECT sess_id
    FROM pg_stat_activity
    WHERE application_name = 'pg_regress/gp_log_mem_dispatch'
)
SELECT gp_log_backend_memory_contexts(sess_id) FROM sessionCTE;

-- show warnings and 0 successful responses to logging a
-- known-bad session
WITH noSessionCTE AS (
    SELECT MIN(sess_id) + 1 as no_sess_id
    FROM pg_stat_activity
    WHERE sess_id + 1 NOT IN (SELECT sess_id FROM pg_stat_activity)
)
SELECT gp_log_backend_memory_contexts(no_sess_id) FROM noSessionCTE;

-- show expected number of successful responses to logging a
-- known-good session with a target contentID
WITH sessionCTE AS (
    SELECT sess_id
    FROM pg_stat_activity
    WHERE application_name = 'pg_regress/gp_log_mem_dispatch'
)
SELECT gp_log_backend_memory_contexts(sess_id, 0) FROM sessionCTE;

-- show warnings and 0 successful responses to logging a
-- known-bad contentID
WITH sessionCTE AS (
    SELECT sess_id
    FROM pg_stat_activity
    WHERE application_name = 'pg_regress/gp_log_mem_dispatch'
)
SELECT gp_log_backend_memory_contexts(sess_id, -3) FROM sessionCTE;

-- gp_log_backend_memory_contexts() follows the GRANT model of the
-- pg_log_backend_memory_contexts() it calls on the segments: a regular role
-- is denied (EXECUTE is revoked from PUBLIC); a role granted EXECUTE on the
-- wrapper alone is still refused by the function itself, which is the state
-- of a cluster upgraded without initdb, where proacl stays NULL; a role
-- granted EXECUTE on both functions may log its own session.
CREATE ROLE regress_log_mem_user;
SET SESSION AUTHORIZATION regress_log_mem_user;
SELECT gp_log_backend_memory_contexts(0);
SELECT gp_log_backend_memory_contexts(0, 0);
RESET SESSION AUTHORIZATION;

GRANT EXECUTE ON FUNCTION gp_log_backend_memory_contexts(bigint) TO regress_log_mem_user;
GRANT EXECUTE ON FUNCTION gp_log_backend_memory_contexts(bigint, bigint) TO regress_log_mem_user;
SET SESSION AUTHORIZATION regress_log_mem_user;
SELECT gp_log_backend_memory_contexts(0);
SELECT gp_log_backend_memory_contexts(0, 0);
-- the check also runs on the segments, where no dispatch happens first; the
-- segment of the error is not deterministic, so only report its SQLSTATE
DO $$
BEGIN
    PERFORM gp_log_backend_memory_contexts(0) FROM gp_dist_random('gp_id');
    RAISE NOTICE 'gp_log_backend_memory_contexts() unexpectedly succeeded';
EXCEPTION WHEN insufficient_privilege THEN
    RAISE NOTICE 'segment refused with SQLSTATE %', SQLSTATE;
END;
$$;
RESET SESSION AUTHORIZATION;

GRANT EXECUTE ON FUNCTION pg_log_backend_memory_contexts(integer) TO regress_log_mem_user;
SET SESSION AUTHORIZATION regress_log_mem_user;
SELECT gp_log_backend_memory_contexts(current_setting('gp_session_id')::bigint);
SELECT gp_log_backend_memory_contexts(current_setting('gp_session_id')::bigint, 0);
RESET SESSION AUTHORIZATION;

REVOKE EXECUTE ON FUNCTION pg_log_backend_memory_contexts(integer) FROM regress_log_mem_user;
REVOKE EXECUTE ON FUNCTION gp_log_backend_memory_contexts(bigint) FROM regress_log_mem_user;
REVOKE EXECUTE ON FUNCTION gp_log_backend_memory_contexts(bigint, bigint) FROM regress_log_mem_user;
DROP ROLE regress_log_mem_user;

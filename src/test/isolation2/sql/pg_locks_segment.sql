-- Verify that pg_locks properly decodes segment lock rows (in particular
-- timestamptz waitstart and NULL columns) and cleans up libpq results on
-- early termination.

CREATE TABLE pg_locks_segment_test(a int) DISTRIBUTED BY (a);
INSERT INTO pg_locks_segment_test VALUES (1);

-- 1. Segment row decoding with a non-NULL waitstart.
-- Hold AccessExclusiveLock on segment 0 in utility mode so session 1 blocks
-- waiting for AccessShareLock on segment 0.
0U: BEGIN;
0U: LOCK TABLE pg_locks_segment_test IN ACCESS EXCLUSIVE MODE;

1&: SELECT * FROM pg_locks_segment_test;

2: SELECT wait_until_waiting_for_required_lock('pg_locks_segment_test', 'AccessShareLock', 0);

-- Confirm waitstart is decoded as a current timestamptz (previously atoll()
-- parsed the text timestamp as microseconds near 2000-01-01) and that other
-- segment columns and NULLs are preserved.
2: SELECT locktype,
          mode,
          granted,
          gp_segment_id,
          database = (SELECT oid FROM pg_database WHERE datname = current_database()) AS db_match,
          page IS NULL AND tuple IS NULL AS nulls_ok,
          waitstart IS NOT NULL
            AND waitstart >= now() - interval '1 hour'
            AND waitstart <= now() + interval '1 minute' AS waitstart_ok
   FROM pg_locks
   WHERE gp_segment_id = 0
     AND relation = 'pg_locks_segment_test'::regclass
     AND NOT granted;

-- 2. Early termination of pg_locks should release segment PGresults cleanly
-- via the memory context reset callback without error.
2: SELECT count(*) FROM (SELECT * FROM pg_locks WHERE gp_segment_id >= 0 LIMIT 1) s;
2: SELECT count(*) FROM (SELECT * FROM pg_locks LIMIT 0) s;

-- Release the lock and let session 1 complete.
0U: ROLLBACK;
1<:

DROP TABLE pg_locks_segment_test;

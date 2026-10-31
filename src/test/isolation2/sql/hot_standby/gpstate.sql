-- A hot standby accepts connections without having been promoted.
!\retcode python3 sql/hot_standby/gpstate.py streaming;

-- Losing the WAL connection does not promote a hot standby either.
-- The helper uses SELECT gp_inject_fault_infinite(...) to skip FTS probes,
-- and restores primary_conninfo and FTS even when the assertion fails.
!\retcode python3 sql/hot_standby/gpstate.py disconnected;

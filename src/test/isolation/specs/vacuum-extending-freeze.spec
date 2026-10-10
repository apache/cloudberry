# Check VM counts after freezing and cumulative counts of cleared VM bits.
# A repeatable-read transaction keeps deleted tuples visible to its snapshot.
# It also caches the table statistics before the other session deletes rows:
# the VM bits clear during DELETE, while the cached counters stay unchanged
# until the reader commits. Force the writer's counters to flush so this
# distinction does not depend on the statistics flush interval.

setup
{
    CREATE TABLE vestat (x int, y int)
        WITH (autovacuum_enabled = off, fillfactor = 70);

    INSERT INTO vestat
        SELECT i, i FROM generate_series(1, 5000) AS g(i);

    CREATE INDEX vestat_idx ON vestat (x);

    CREATE TABLE stats_state (frozen_flag_count int, all_visibile_flag_count int,
                        cleared_frozen_flag_count int, cleared_all_visibile_flag_count int);
    INSERT INTO stats_state VALUES (0,0,0,0);
    ANALYZE vestat;

    -- Ensure stats are flushed before starting the scenario
    SELECT pg_stat_force_next_flush();
}

teardown
{
    DROP TABLE IF EXISTS vestat;
    DROP TABLE IF EXISTS stats_state;
    RESET vacuum_freeze_min_age;
    RESET vacuum_freeze_table_age;

}

session s1
setup
{
    SET stats_fetch_consistency = 'cache';
}

step s1_get_set_vm_flags_stats
{
    SELECT pg_stat_force_next_flush();

    SELECT c.relallfrozen > frozen_flag_count as relallfrozen, c.relallvisible > all_visibile_flag_count as relallvisible
        FROM pg_class c, stats_state
        WHERE c.oid = 'vestat'::regclass;

    UPDATE stats_state
        SET frozen_flag_count = c.relallfrozen,
            all_visibile_flag_count = c.relallvisible
        FROM pg_class c
        WHERE c.oid = 'vestat'::regclass;
}

step s1_get_cleared_vm_flags_stats
{
    SELECT pg_stat_force_next_flush();

    SELECT v.visible_page_marks_cleared > cleared_all_visibile_flag_count as visible_page_marks_cleared,
           v.frozen_page_marks_cleared > cleared_frozen_flag_count as frozen_page_marks_cleared
        FROM pg_stat_all_tables v, stats_state
        WHERE v.relname = 'vestat';

    UPDATE stats_state
        SET cleared_all_visibile_flag_count = v.visible_page_marks_cleared,
            cleared_frozen_flag_count = v.frozen_page_marks_cleared
        FROM pg_stat_all_tables v
        WHERE v.relname = 'vestat';
}

step s1_select_from_index
{
    BEGIN ISOLATION LEVEL REPEATABLE READ;
    SELECT count(x) FROM vestat WHERE x > 2000;

    UPDATE stats_state
        SET cleared_all_visibile_flag_count = v.visible_page_marks_cleared,
            cleared_frozen_flag_count = v.frozen_page_marks_cleared
        FROM pg_stat_all_tables v
        WHERE v.relname = 'vestat';
}

step s1_commit
{
    COMMIT;
}

session s2
setup
{
    -- Configure aggressive freezing vacuum behavior
    SET vacuum_freeze_min_age = 0;
    SET vacuum_freeze_table_age = 0;
}
step s2_delete_from_table
{
    DELETE FROM vestat WHERE x > 4930;
    SELECT pg_stat_force_next_flush();
}
step s2_vacuum_freeze
{
    VACUUM FREEZE vestat;
}

step s1_update_table
{
    UPDATE vestat SET x = x + 1001 where x >= 2500;
    SELECT pg_stat_force_next_flush();
}

permutation
    s2_vacuum_freeze
    s1_get_set_vm_flags_stats
    s1_update_table
    s1_get_cleared_vm_flags_stats
    s2_vacuum_freeze
    s1_get_set_vm_flags_stats
    s2_vacuum_freeze
    s1_select_from_index
    s2_delete_from_table
    s1_get_cleared_vm_flags_stats
    s2_vacuum_freeze
    s1_get_set_vm_flags_stats
    s1_commit
    s1_get_cleared_vm_flags_stats

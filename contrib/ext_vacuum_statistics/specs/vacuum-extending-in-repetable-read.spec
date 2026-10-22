# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Test for checking recently_dead_tuples, tuples_deleted and frozen tuples in ext_vacuum_statistics.pg_stats_vacuum_tables.
# recently_dead_tuples values are counted when vacuum hasn't cleared tuples because they were deleted recently.
# recently_dead_tuples aren't increased after releasing lock compared with tuples_deleted, which increased
# by the value of the cleared tuples that the vacuum managed to clear.

setup
{
    CREATE TABLE test_vacuum_stat_isolation(id int, ival int) WITH (autovacuum_enabled = off);
    CREATE EXTENSION ext_vacuum_statistics;
    SET track_io_timing = on;
}

teardown
{
    DROP EXTENSION ext_vacuum_statistics CASCADE;
    DROP TABLE test_vacuum_stat_isolation CASCADE;
    RESET track_io_timing;
}

session s1
setup {
    SET track_io_timing = on;
}
step s1_begin_repeatable_read {
    BEGIN transaction ISOLATION LEVEL REPEATABLE READ;
    select count(ival) from test_vacuum_stat_isolation where id>900;
}
step s1_commit { COMMIT; }

session s2
setup {
    SET track_io_timing = on;
}
step s2_insert                  { INSERT INTO test_vacuum_stat_isolation(id, ival) SELECT ival, ival%10 FROM generate_series(1,1000) As ival; }
step s2_update                  { UPDATE test_vacuum_stat_isolation SET ival = ival + 2 where id > 900; }
step s2_delete                  { DELETE FROM test_vacuum_stat_isolation where id > 900; }
step s2_insert_interrupt        { INSERT INTO test_vacuum_stat_isolation values (1,1); }
step s2_vacuum                  { VACUUM test_vacuum_stat_isolation; }
step s2_checkpoint              { CHECKPOINT; }
step s2_print_vacuum_stats_table
{
    SELECT
        vt.relname, vt.tuples_deleted, vt.recently_dead_tuples, vt.missed_dead_tuples, vt.missed_dead_pages, vt.tuples_frozen
    FROM ext_vacuum_statistics.pg_stats_vacuum_tables vt, pg_class c
    WHERE vt.relname = 'test_vacuum_stat_isolation' AND vt.relid = c.oid;
}

permutation
    s2_insert
    s2_print_vacuum_stats_table
    s1_begin_repeatable_read
    s2_update
    s2_insert_interrupt
    s2_vacuum
    s2_print_vacuum_stats_table
    s1_commit
    s2_checkpoint
    s2_vacuum
    s2_print_vacuum_stats_table

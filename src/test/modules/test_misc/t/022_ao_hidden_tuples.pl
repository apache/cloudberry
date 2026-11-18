# Copyright (c) 2026, PostgreSQL Global Development Group

# AO hidden rows have both a cumulative observation count and a latest sample.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacstat_ao_hidden');
$node->init;
$node->append_conf('postgresql.conf', q{
autovacuum = off
});
$node->start;
$node->safe_psql('postgres', q{
CREATE TABLE hidden_heap (id int);
INSERT INTO hidden_heap VALUES (1);
VACUUM hidden_heap;
});
is($node->safe_psql('postgres', q{
SELECT dead_tuples FROM pg_stat_vacuum_tables
 WHERE relname = 'hidden_heap'}), '0', 'AO hidden-row snapshot stays zero for heap');

for my $orientation ('row', 'column')
{
    my $table = "hidden_$orientation";
    my $count_sql = qq{
SELECT recently_dead_tuples, dead_tuples FROM pg_stat_vacuum_tables
 WHERE relname = '$table'};
    $node->safe_psql('postgres', qq{
CREATE TABLE $table (id int) WITH (appendonly = true, orientation = $orientation);
INSERT INTO $table SELECT generate_series(1, 1000);
DELETE FROM $table WHERE id <= 100;
SET gp_appendonly_compaction_threshold = 100;
SET vacuum_freeze_table_age = 0;
VACUUM $table;
});
    is($node->safe_psql('postgres', qq{
SELECT recently_dead_tuples = 100 AND tuples_deleted = 0 AND compacted_segments = 0
       AND dead_pages = 0 AND dead_tuples = 100 AND freeze_age_vacuum_count = 0
  FROM pg_stat_vacuum_tables WHERE relname = '$table'}), 't',
       "$orientation AO counts hidden rows left below the compaction threshold");

    $node->safe_psql('postgres', "SET gp_appendonly_compaction = off; VACUUM $table");
    is($node->safe_psql('postgres', $count_sql), '200|100',
       "$orientation AO accumulates remaining rows but replaces the latest snapshot");

    $node->safe_psql('postgres', qq{
DELETE FROM $table WHERE id = 101;
SET gp_appendonly_compaction = off;
SET track_counts = off;
VACUUM $table;
});
    is($node->safe_psql('postgres', $count_sql), '200|100',
       "$orientation AO disabled collection preserves both counts despite another hidden row");

    $node->safe_psql('postgres', "SET gp_appendonly_compaction_threshold = 1; VACUUM $table");
    is($node->safe_psql('postgres', qq{
SELECT recently_dead_tuples = 200 AND dead_tuples = 0
       AND tuples_deleted = 101 AND compacted_segments > 0
  FROM pg_stat_vacuum_tables WHERE relname = '$table'}), 't',
       "$orientation AO samples after compaction and clears the latest hidden-row count");
    $node->safe_psql('postgres', "VACUUM $table");
    is($node->safe_psql('postgres', $count_sql), '200|0',
       "$orientation AO empty hidden-row sample does not reuse an earlier count");

    $node->safe_psql('postgres', qq{
DELETE FROM $table WHERE id = 102;
SET gp_appendonly_compaction_threshold = 100;
VACUUM $table;
});
    is($node->safe_psql('postgres', $count_sql), '201|1',
       "$orientation AO adds a fresh remaining-row sample after compaction");
}

$node->restart;
for my $orientation ('row', 'column')
{
    my $table = "hidden_$orientation";
    my $count_sql = qq{
SELECT recently_dead_tuples, dead_tuples FROM pg_stat_vacuum_tables
 WHERE relname = '$table'};
    is($node->safe_psql('postgres', $count_sql), '201|1',
       "$orientation AO remaining-row total and snapshot survive a clean restart");
    $node->safe_psql('postgres', qq{
SELECT pg_stat_reset_single_table_counters('$table'::regclass)});
    is($node->safe_psql('postgres', $count_sql), '0|0',
       "$orientation AO relation reset clears the remaining-row total and snapshot");
    $node->safe_psql('postgres', "SET gp_appendonly_compaction_threshold = 100; VACUUM $table");
    is($node->safe_psql('postgres', $count_sql), '1|1',
       "$orientation AO resumes accumulating remaining rows after reset");

    # Unlike heap's rewrite, AO VACUUM FULL still runs compaction phases.
    $node->safe_psql('postgres', "VACUUM FULL $table");
    is($node->safe_psql('postgres', qq{
SELECT tuples_deleted = 1 AND compacted_segments > 0 AND dead_tuples = 0
       AND freeze_age_vacuum_count = 0
  FROM pg_stat_vacuum_tables WHERE relname = '$table'}), 't',
       "$orientation AO VACUUM FULL reports compaction work without heap freezing");
}

$node->stop;
done_testing();

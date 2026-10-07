# Copyright (c) 2026, PostgreSQL Global Development Group

# AO dead rows left by each completed vacuum are cumulative observations.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacstat_ao_hidden');
$node->init;
$node->append_conf('postgresql.conf', q{
shared_preload_libraries = 'ext_vacuum_statistics'
autovacuum = off
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION ext_vacuum_statistics');

for my $orientation ('row', 'column')
{
    my $table = "hidden_$orientation";
    my $count_sql = qq{
SELECT recently_dead_tuples FROM ext_vacuum_statistics.pg_stats_vacuum_tables
 WHERE relname = '$table'};
    $node->safe_psql('postgres', qq{
CREATE TABLE $table (id int) WITH (appendonly = true, orientation = $orientation);
INSERT INTO $table SELECT generate_series(1, 1000);
DELETE FROM $table WHERE id <= 100;
SET gp_appendonly_compaction_threshold = 100;
VACUUM $table;
});
    is($node->safe_psql('postgres', qq{
SELECT recently_dead_tuples = 100 AND tuples_deleted = 0 AND compacted_segments = 0
  FROM ext_vacuum_statistics.pg_stats_vacuum_tables WHERE relname = '$table'}), 't',
       "$orientation AO counts hidden rows left below the compaction threshold");

    $node->safe_psql('postgres', "SET gp_appendonly_compaction = off; VACUUM $table");
    is($node->safe_psql('postgres', $count_sql), '200',
       "$orientation AO adds the same remaining rows again when compaction is disabled");

    $node->safe_psql('postgres', qq{
SET gp_appendonly_compaction = off;
SET vacuum_statistics.enabled = off;
VACUUM $table;
});
    is($node->safe_psql('postgres', $count_sql), '200',
       "$orientation AO disabled statistics collection does not add remaining rows");

    $node->safe_psql('postgres', "SET gp_appendonly_compaction_threshold = 1; VACUUM $table");
    is($node->safe_psql('postgres', qq{
SELECT recently_dead_tuples = 200 AND tuples_deleted = 100 AND compacted_segments > 0
  FROM ext_vacuum_statistics.pg_stats_vacuum_tables WHERE relname = '$table'}), 't',
       "$orientation AO samples after compaction and does not add rows just removed");
    $node->safe_psql('postgres', "VACUUM $table");
    is($node->safe_psql('postgres', $count_sql), '200',
       "$orientation AO empty hidden-row sample does not reuse an earlier count");

    $node->safe_psql('postgres', qq{
DELETE FROM $table WHERE id = 101;
SET gp_appendonly_compaction_threshold = 100;
VACUUM $table;
});
    is($node->safe_psql('postgres', $count_sql), '201',
       "$orientation AO adds a fresh remaining-row sample after compaction");
}

$node->restart;
for my $orientation ('row', 'column')
{
    my $table = "hidden_$orientation";
    my $count_sql = qq{
SELECT recently_dead_tuples FROM ext_vacuum_statistics.pg_stats_vacuum_tables
 WHERE relname = '$table'};
    is($node->safe_psql('postgres', $count_sql), '201',
       "$orientation AO remaining-row total survives a clean restart");
    $node->safe_psql('postgres', qq{
SELECT ext_vacuum_statistics.extvac_reset_entry(
  (SELECT oid FROM pg_database WHERE datname = current_database()), '$table'::regclass)});
    is($node->safe_psql('postgres', $count_sql), '0',
       "$orientation AO relation reset clears the remaining-row total");
    $node->safe_psql('postgres', "SET gp_appendonly_compaction_threshold = 100; VACUUM $table");
    is($node->safe_psql('postgres', $count_sql), '1',
       "$orientation AO resumes accumulating remaining rows after reset");
}

$node->stop;
done_testing();

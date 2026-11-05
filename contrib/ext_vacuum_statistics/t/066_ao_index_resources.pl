# Copyright (c) 2026, PostgreSQL Global Development Group

# AO compaction index insertions belong to the database resource aggregate,
# while per-index entries describe vacuum passes only.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('ao_index_resources');
$node->init;
$node->append_conf('postgresql.conf', q{
shared_preload_libraries = 'ext_vacuum_statistics'
autovacuum = off
gp_appendonly_compaction_threshold = 1
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION ext_vacuum_statistics');
for my $storage ('row', 'column')
{
    my $table = "resources_$storage";
    $node->safe_psql('postgres', qq{
CREATE TABLE $table (id int, padding text)
  WITH (appendonly = true, orientation = $storage);
CREATE INDEX ${table}_idx ON $table (id);
CREATE INDEX ${table}_expr ON $table ((id + 1));
INSERT INTO $table SELECT g, repeat('x', 100) FROM generate_series(1, 4000) g;
DELETE FROM $table WHERE id <= 2000;
SELECT ext_vacuum_statistics.vacuum_statistics_reset();
});
    my ($stdout, $stderr);
    is($node->psql('postgres', "VACUUM (VERBOSE) $table",
        stdout => \$stdout, stderr => \$stderr), 0, "$storage vacuum succeeds");
    my ($inserts) = $stderr =~
      /AO compaction index inserts \(all indexes\)\):\n(.*?)(?=vacuum resource usage for|\z)/s;
    ok(defined $inserts, "$storage reports aggregate index insertions");
    $inserts //= '';
    my ($records) = $inserts =~ /WAL usage: (\d+) records,/;
    cmp_ok($records // 0, '>=', 4000, "$storage accounts for both indexes");
    is($node->safe_psql('postgres', q{
SELECT d.db_wal_records - (
  SELECT coalesce(sum(wal_records), 0) FROM (
    SELECT wal_records FROM ext_vacuum_statistics.pg_stats_vacuum_tables
    UNION ALL
    SELECT wal_records FROM ext_vacuum_statistics.pg_stats_vacuum_indexes
  ) r)
FROM ext_vacuum_statistics.pg_stats_vacuum_database d
WHERE dbname = current_database()
}), $records, "$storage database includes the insertion aggregate exactly once");
    is($node->safe_psql('postgres', qq{
SELECT count(*) = 2 AND bool_and(total_blks_hit + total_blks_read > 0)
FROM ext_vacuum_statistics.pg_stats_vacuum_indexes
WHERE indexrelname IN ('${table}_idx', '${table}_expr')
}), 't', "$storage retains per-index vacuum resources");
    is($node->safe_psql('postgres', qq{
SELECT tuples_deleted = 2000 AND tuples_moved = 2000
FROM pg_stat_vacuum_tables WHERE relname = '$table'
}), 't', "$storage reports native work once");
    $node->safe_psql('postgres', 'SELECT pg_stat_force_next_flush()');
    is($node->safe_psql('postgres', q{
SELECT d.tuples_deleted = (
  SELECT coalesce(sum(v.tuples_deleted), 0)
  FROM pg_class c CROSS JOIN LATERAL pg_stat_get_vacuum_stats(c.oid) v
  WHERE NOT c.relisshared AND c.relkind <> 'i')
FROM pg_stat_vacuum d WHERE datname = current_database()
}), 't', "$storage database accumulates native table work exactly once");

}
$node->stop;
done_testing();

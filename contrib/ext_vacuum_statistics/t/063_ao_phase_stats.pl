# Copyright (c) 2026, PostgreSQL Global Development Group

# Completed AO vacuums publish a segment-state snapshot and resource totals
# split by phase. Exercise retained segments, recycling, resets and persistence.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacstat_ao_phases');
$node->init;
$node->append_conf('postgresql.conf', q{
shared_preload_libraries = 'ext_vacuum_statistics'
autovacuum = off
gp_appendonly_compaction_threshold = 1
track_io_timing = on
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION ext_vacuum_statistics');

my @phases = qw(ao_pre_cleanup ao_compaction ao_post_cleanup);
my @resources = qw(blks_read blks_hit blks_dirtied blks_written
                   wal_records wal_fpi wal_bytes blk_read_time blk_write_time);
my @fields = ('awaiting_drop_segments',
              map { my $p = $_; map { "${p}_$_" } @resources } @phases);
my $zero = join(' AND ', map { "$_ = 0" } @fields);
my $nonnegative = join(' AND ', map { "$_ >= 0" } @fields);
my $phase_sums = join(' AND ', map {
    my $resource = $_;
    my $total = $resource =~ /^blks_/ ? "total_$resource" : $resource;
    my $sum = join(' + ', map { "${_}_$resource" } @phases);
    $resource =~ /_time$/ ? "abs($total - ($sum)) < 0.000001" : "$total = ($sum)"
} @resources);
my $view = 'ext_vacuum_statistics.pg_stats_vacuum_ao_tables';
my %saved;

for my $orientation ('row', 'column')
{
    my $table = "phases_$orientation";
    my $where = "FROM $view WHERE relname = '$table'";
    $node->safe_psql('postgres', qq{
CREATE TABLE $table (id int, padding text)
  WITH (appendonly = true, orientation = $orientation);
CREATE INDEX ${table}_idx ON $table (id);
INSERT INTO $table SELECT g, repeat('x', 100) FROM generate_series(1, 4000) g;
DELETE FROM $table WHERE id <= 2000;
});
    # The delete is visible, so compaction can discard its rows. This reader
    # still needs the old segfile and prevents post-cleanup from recycling it.
    my $reader = $node->background_psql('postgres');
    $reader->query_safe("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM $table");
    # VERBOSE and the extension consume the same measurements, once each.
    $node->safe_psql('postgres', "VACUUM (VERBOSE) $table");
    my $awaiting = $node->safe_psql('postgres', "SELECT awaiting_drop_segments $where");
    cmp_ok($awaiting, '>', 0, "$orientation retains compacted segments for the old snapshot");
    my $segrel = $node->safe_psql('postgres', qq{
SELECT segrelid::regclass FROM pg_appendonly WHERE relid = '$table'::regclass});
    is($awaiting, $node->safe_psql('postgres', "SELECT count(*) FROM $segrel WHERE state = 2"),
       "$orientation awaiting-drop snapshot agrees with AO metadata");
    is($node->safe_psql('postgres', "SELECT $nonnegative AND $phase_sums $where"), 't',
       "$orientation all phase resources are nonnegative and sum to table totals");
    is($node->safe_psql('postgres', qq{
SELECT ao_pre_cleanup_blks_hit + ao_pre_cleanup_blks_read > 0
   AND ao_compaction_blks_hit + ao_compaction_blks_read > 0
   AND ao_post_cleanup_blks_hit + ao_post_cleanup_blks_read > 0
   AND ao_compaction_wal_records > 0 AND tuples_deleted = 2000 $where}), 't',
       "$orientation records resources in all phases and WAL during compaction");
    my $compacted = $node->safe_psql('postgres', "SELECT compacted_segments $where");
    $node->safe_psql('postgres', "VACUUM $table");
    is($node->safe_psql('postgres', "SELECT awaiting_drop_segments $where"), $awaiting,
       "$orientation replaces the awaiting-drop snapshot instead of adding it again");
    # Even a phase with no compaction can perform catalog work and emit WAL.
    is($node->safe_psql('postgres', "SELECT compacted_segments $where"), $compacted,
       "$orientation retained segments are not compacted again");
    $reader->query_safe('COMMIT');
    $reader->quit;
    my $before_disabled = $node->safe_psql('postgres', 'SELECT ' . join(', ', @fields) . " $where");
    $node->safe_psql('postgres', "SET vacuum_statistics.enabled = off; VACUUM $table");
    is($node->safe_psql('postgres', 'SELECT ' . join(', ', @fields) . " $where"), $before_disabled,
       "$orientation disabled collection preserves phase counters and segment snapshot");
    $node->safe_psql('postgres', "VACUUM $table");
    is($node->safe_psql('postgres', "SELECT awaiting_drop_segments $where"), '0',
       "$orientation refreshes the snapshot to zero after recycling");
    is($node->safe_psql('postgres', "SELECT $nonnegative AND $phase_sums $where"), 't',
       "$orientation phase accounting remains consistent across repeated vacuums");
    $saved{$orientation} = $node->safe_psql('postgres', 'SELECT ' . join(', ', @fields) . " $where");
}

$node->safe_psql('postgres', q{
CREATE TABLE phase_heap (id int);
INSERT INTO phase_heap SELECT generate_series(1, 1000);
DELETE FROM phase_heap;
VACUUM phase_heap;
});
is($node->safe_psql('postgres', "SELECT string_agg(relname, ',' ORDER BY relname) FROM $view"),
   'phases_column,phases_row', 'AO view contains only parent AO/AOCS tables, excluding heap and auxiliary relations');
is($node->safe_psql('postgres', q{
SELECT count(*) FROM pg_attribute
WHERE attrelid = 'ext_vacuum_statistics.pg_stats_vacuum_tables'::regclass
  AND attname ~ '^ao_(pre_cleanup|compaction|post_cleanup)_'
}), '0', 'general table view does not expose AO phase columns');

$node->restart;
for my $orientation ('row', 'column')
{
    my $table = "phases_$orientation";
    my $where = "FROM $view WHERE relname = '$table'";
    is($node->safe_psql('postgres', 'SELECT ' . join(', ', @fields) . " $where"), $saved{$orientation},
       "$orientation phase counters survive a clean restart");
    $node->safe_psql('postgres', qq{
SELECT ext_vacuum_statistics.extvac_reset_entry(
 (SELECT oid FROM pg_database WHERE datname = current_database()), '$table'::regclass)});
    is($node->safe_psql('postgres', "SELECT $zero $where"), 't',
       "$orientation relation reset clears all new fields");
    $node->safe_psql('postgres', "DELETE FROM $table WHERE id <= 3000; VACUUM $table");
    is($node->safe_psql('postgres', "SELECT ao_compaction_wal_records > 0 AND $phase_sums $where"), 't',
       "$orientation phase reporting resumes after reset");
}
$node->safe_psql('postgres', q{
SELECT ext_vacuum_statistics.extvac_reset_db_entry(
 (SELECT oid FROM pg_database WHERE datname = current_database()))});
is($node->safe_psql('postgres', "SELECT bool_and($zero) FROM $view"), 't',
   'database reset clears the new fields');
$node->safe_psql('postgres', 'VACUUM phases_row, phases_column');
$node->safe_psql('postgres', 'SELECT ext_vacuum_statistics.vacuum_statistics_reset()');
is($node->safe_psql('postgres', "SELECT bool_and($zero) FROM $view"), 't',
   'global reset clears the new fields');

$node->stop;
done_testing();

# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacstat_dead_pages');
$node->init;
$node->append_conf('postgresql.conf', q{
autovacuum = off
max_parallel_maintenance_workers = 2
min_parallel_index_scan_size = 0
});
$node->start;
$node->safe_psql('postgres', q{
CREATE TABLE retained_heap (id int, padding text);
INSERT INTO retained_heap SELECT g, repeat('x', 100) FROM generate_series(1, 2000) g;
});
my $pages = $node->safe_psql('postgres', q{
SELECT count(DISTINCT split_part(ctid::text, ',', 1)) FROM retained_heap WHERE id <= 1000});
my $heap_sql = q{
SELECT dead_pages FROM pg_stat_vacuum_tables WHERE relname = 'retained_heap'};
my $snapshot = $node->background_psql('postgres');
$snapshot->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM retained_heap');
$node->safe_psql('postgres', 'DELETE FROM retained_heap WHERE id <= 1000; VACUUM retained_heap');
is($node->safe_psql('postgres', $heap_sql), $pages,
   'heap counts pages containing tuples retained for an older snapshot');
is($node->safe_psql('postgres', q{
SELECT recently_dead_tuples = 1000 AND missed_dead_pages = 0
  FROM pg_stat_vacuum_tables WHERE relname = 'retained_heap'}), 't',
   'retained dead pages are independent of cleanup-lock misses');
$node->safe_psql('postgres', 'VACUUM retained_heap');
is($node->safe_psql('postgres', $heap_sql), 2 * $pages,
   'another vacuum accumulates the same retained heap pages again');
is($node->safe_psql('postgres', q{
SELECT tuples_deleted, recently_dead_tuples FROM pg_stat_vacuum_tables
 WHERE relname = 'retained_heap'}), '0|2000',
   'both vacuums accumulate retained tuples without reporting removals');
$snapshot->query_safe('COMMIT');
$snapshot->quit;
$node->safe_psql('postgres', 'VACUUM retained_heap');
is($node->safe_psql('postgres', $heap_sql), 2 * $pages,
   'removing the retained tuples does not add dead pages');
is($node->safe_psql('postgres', q{
SELECT tuples_deleted, recently_dead_tuples FROM pg_stat_vacuum_tables
 WHERE relname = 'retained_heap'}), '1000|2000',
   'releasing the snapshot permits removal without adding retained tuples');

# A cursor holds a buffer pin, forcing the scan without a cleanup lock.
for my $recent (0, 1)
{
    my $table = "pinned_$recent";
    $node->safe_psql('postgres', qq{
CREATE TABLE $table (id int);
INSERT INTO $table SELECT generate_series(1, 20);
});
    $node->safe_psql('postgres', "DELETE FROM $table WHERE id = 20") unless $recent;
    my $pin = $node->background_psql('postgres');
    $pin->query_safe("BEGIN ISOLATION LEVEL REPEATABLE READ; DECLARE c CURSOR FOR SELECT * FROM $table; FETCH 1 FROM c");
    $node->safe_psql('postgres', "DELETE FROM $table WHERE id = 20") if $recent;
    $node->safe_psql('postgres', "VACUUM (TRUNCATE OFF) $table");
    is($node->safe_psql('postgres', qq{
SELECT dead_pages, missed_dead_pages FROM pg_stat_vacuum_tables
 WHERE relname = '$table'}), $recent ? '1|0' : '0|1',
       "pinned page distinguishes recently-dead tuples from missed removable tuples ($recent)");
    $pin->query_safe('COMMIT');
    $pin->quit;
}

# A snapshot taken after DELETE permits removal of heap tuples, but prevents
# recycling index pages deleted by the following VACUUM. Two indexes exercise
# serial reporting with PARALLEL 0 and Cloudberry's PARALLEL 2 fallback.
my $nrows = 20000 * ($node->safe_psql('postgres', 'SHOW block_size') / 8192);
my %index_totals;
for my $parallel (0, 2)
{
    my $table = "index_pages_$parallel";
    $node->safe_psql('postgres', qq{
CREATE TABLE $table (id int);
CREATE INDEX ${table}_a ON $table (id);
CREATE INDEX ${table}_b ON $table ((id + 1));
INSERT INTO $table SELECT generate_series(1, $nrows);
DELETE FROM $table WHERE id <= $nrows / 2;
});
    my $hold = $node->background_psql('postgres');
    $hold->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT txid_current_snapshot()');
    my ($stdout, $stderr);
    my $result = $node->psql('postgres', "VACUUM (VERBOSE, INDEX_CLEANUP ON, PARALLEL $parallel) $table",
                            stdout => \$stdout, stderr => \$stderr);
    is($result, 0, "vacuum with PARALLEL $parallel succeeds");
    like($stderr, qr/disabling parallel option of vacuum.*cannot vacuum tables in parallel/,
         'Cloudberry reports the serial fallback for PARALLEL') if $parallel;
    for my $suffix ('a', 'b')
    {
        my $index = "${table}_$suffix";
        my $deleted = $node->safe_psql('postgres', qq{
SELECT pages_deleted FROM pg_stat_vacuum_indexes WHERE indexrelname = '$index'});
        cmp_ok($deleted, '>', 0, "$index has newly deleted pages");
        is($node->safe_psql('postgres', qq{
SELECT dead_pages FROM pg_stat_vacuum_indexes WHERE indexrelname = '$index'}),
           $deleted, "$index counts its non-reusable pages once after cleanup");
        $index_totals{$index} = 2 * $deleted;
    }
    $node->safe_psql('postgres', "VACUUM (INDEX_CLEANUP ON, PARALLEL $parallel) $table");
    for my $suffix ('a', 'b')
    {
        my $index = "${table}_$suffix";
        is($node->safe_psql('postgres', qq{
SELECT dead_pages FROM pg_stat_vacuum_indexes WHERE indexrelname = '$index'}),
           $index_totals{$index}, "$index accumulates pages still waiting for reuse");
    }
    $hold->query_safe('COMMIT');
    $hold->quit;
    $node->safe_psql('postgres', 'SELECT txid_current()');
    $node->safe_psql('postgres', "VACUUM (INDEX_CLEANUP ON, PARALLEL $parallel) $table");
    for my $suffix ('a', 'b')
    {
        my $index = "${table}_$suffix";
        is($node->safe_psql('postgres', qq{
SELECT dead_pages FROM pg_stat_vacuum_indexes WHERE indexrelname = '$index'}),
           $index_totals{$index}, "$index does not count pages that became reusable");
    }
}

$node->restart;
is($node->safe_psql('postgres', $heap_sql), 2 * $pages,
   'heap dead-page counter survives a clean restart');
for my $index (sort keys %index_totals)
{
    is($node->safe_psql('postgres', qq{
SELECT dead_pages FROM pg_stat_vacuum_indexes WHERE indexrelname = '$index'}),
       $index_totals{$index}, "$index dead-page counter survives a clean restart");
}
$node->safe_psql('postgres', 'SELECT pg_stat_reset()');
is($node->safe_psql('postgres', qq{
SELECT dead_pages FROM pg_stat_vacuum_tables WHERE relname = 'retained_heap'}),
   '0', 'reset clears heap dead pages');
is($node->safe_psql('postgres', q{
SELECT bool_and(dead_pages = 0) FROM pg_stat_vacuum_indexes
 WHERE indexrelname LIKE 'index_pages_%'}), 't', 'reset clears index dead pages');

$node->stop;
done_testing();

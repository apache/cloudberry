# Copyright (c) 2026, PostgreSQL Global Development Group

# Distinguish freeze-age scans, unremovable tuples, and newly deleted pages.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacuum_measurements');
$node->init;
$node->append_conf('postgresql.conf', q{
shared_preload_libraries = 'ext_vacuum_statistics'
autovacuum = off
});
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION ext_vacuum_statistics;
CREATE TABLE freeze_age (id int);
INSERT INTO freeze_age SELECT generate_series(1, 1000);
});
my $freeze_sql = q{
SELECT freeze_age_vacuum_count FROM ext_vacuum_statistics.pg_stats_vacuum_tables
 WHERE relname = 'freeze_age'};
$node->safe_psql('postgres', 'VACUUM (DISABLE_PAGE_SKIPPING) freeze_age');
is($node->safe_psql('postgres', $freeze_sql), '0',
   'DISABLE_PAGE_SKIPPING alone does not count as a freeze-age vacuum');
$node->safe_psql('postgres', 'VACUUM (FREEZE) freeze_age');
is($node->safe_psql('postgres', $freeze_sql), '1', 'VACUUM FREEZE counts once');
$node->safe_psql('postgres', q{
SET vacuum_freeze_table_age = 0;
VACUUM freeze_age;
});
is($node->safe_psql('postgres', $freeze_sql), '2',
   'a zero freeze table age also makes the scan aggressive');
is($node->safe_psql('postgres', q{
SELECT vacuum_failsafe_count FROM pg_stat_all_tables WHERE relname = 'freeze_age'}),
   '0', 'freeze-age scans are distinct from failsafe runs');

# Hold a snapshot that still needs every deleted tuple. These are recently
# dead tuples, not removable tuples skipped because a cleanup lock was busy.
$node->safe_psql('postgres', q{
CREATE TABLE held_rows (id int, padding text);
INSERT INTO held_rows SELECT g, repeat('x', 100) FROM generate_series(1, 1000) g;
});
my $pages = $node->safe_psql('postgres', q{
SELECT count(DISTINCT split_part(ctid::text, ',', 1)) FROM held_rows});
my $reader = $node->background_psql('postgres');
$reader->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM held_rows');
$node->safe_psql('postgres', 'DELETE FROM held_rows');
$node->safe_psql('postgres', 'VACUUM held_rows');
my $dead_sql = q{
SELECT dead_pages, missed_dead_pages FROM ext_vacuum_statistics.pg_stats_vacuum_tables
 WHERE relname = 'held_rows'};
is($node->safe_psql('postgres', $dead_sql), "$pages|0",
   'dead_pages counts pages with unremovable tuples, independently of missed_dead_pages');
$node->safe_psql('postgres', 'VACUUM held_rows');
is($node->safe_psql('postgres', $dead_sql), (2 * $pages) . '|0',
   'each vacuum accumulates its own observation of remaining dead pages');
$reader->query_safe('COMMIT');
$reader->quit;
$node->safe_psql('postgres', 'VACUUM held_rows');
is($node->safe_psql('postgres', $dead_sql), (2 * $pages) . '|0',
   'removing the old tuples adds no unremovable pages');

# Simulate the zero page that can remain after a crash during index extension.
# It is reusable before vacuum starts, so discovering it is not deletion work.
$node->safe_psql('postgres', q{
CREATE TABLE spg_pages (p point);
CREATE INDEX spg_pages_idx ON spg_pages USING spgist (p);
INSERT INTO spg_pages SELECT point(g, g) FROM generate_series(1, 10) g;
});
my $index_path = $node->safe_psql('postgres', q{
SELECT pg_relation_filepath('spg_pages_idx')});
my $block_size = $node->safe_psql('postgres', 'SHOW block_size');
$node->stop;
append_to_file($node->data_dir . '/' . $index_path, "\0" x $block_size);
$node->start;
$node->safe_psql('postgres', 'VACUUM (INDEX_CLEANUP ON) spg_pages');
my $spg_sql = q{
SELECT pages_deleted FROM ext_vacuum_statistics.pg_stats_vacuum_indexes
 WHERE indexrelname = 'spg_pages_idx'};
my $deleted = $node->safe_psql('postgres', $spg_sql);
is($deleted, '0', 'SP-GiST does not count a pre-existing zero page as newly deleted');
$node->safe_psql('postgres', 'VACUUM (INDEX_CLEANUP ON, DISABLE_PAGE_SKIPPING) spg_pages');
is($node->safe_psql('postgres', $spg_sql), $deleted,
   'SP-GiST rescan does not count previously emptied pages again');
is($node->safe_psql('postgres', q{
SELECT dead_pages FROM ext_vacuum_statistics.pg_stats_vacuum_indexes
 WHERE indexrelname = 'spg_pages_idx'}), '0',
   'reusable SP-GiST pages are not counted as dead pages');

$node->restart;
is($node->safe_psql('postgres', $freeze_sql), '2', 'freeze-age counter survives restart');
is($node->safe_psql('postgres', $dead_sql), (2 * $pages) . '|0',
   'dead-page counter survives restart');
$node->safe_psql('postgres', q{
SELECT pg_stat_reset_vacuum_stats(relid)
 FROM ext_vacuum_statistics.pg_stats_vacuum_tables
 WHERE relname IN ('freeze_age', 'held_rows');
});
is($node->safe_psql('postgres', $freeze_sql), '0', 'reset clears freeze-age counter');
is($node->safe_psql('postgres', $dead_sql), '0|0', 'reset clears dead-page counter');
$node->stop;
done_testing();

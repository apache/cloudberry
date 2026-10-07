# Copyright (c) 2026, PostgreSQL Global Development Group

# Count work on heap pages, rather than the current visibility-map state.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacstat_heap_pages');
$node->init;
$node->append_conf('postgresql.conf', q{
shared_preload_libraries = 'ext_vacuum_statistics'
autovacuum = off
});
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION ext_vacuum_statistics;
CREATE TABLE heap_pages (id int PRIMARY KEY, padding text);
INSERT INTO heap_pages SELECT g, repeat('x', 100) FROM generate_series(1, 10000) g;
});
my $pages = $node->safe_psql('postgres', q{
SELECT count(DISTINCT split_part(ctid::text, ',', 1)) FROM heap_pages});
my $counts_sql = q{
SELECT pages_frozen, pages_all_visible
  FROM ext_vacuum_statistics.pg_stats_vacuum_tables WHERE relname = 'heap_pages'};

# No pruning or freezing is needed, but all populated pages become visible.
$node->safe_psql('postgres', 'VACUUM heap_pages');
is($node->safe_psql('postgres', $counts_sql), "0|$pages",
   'ordinary vacuum marks all populated pages visible without freezing');

# Upgrading all-visible pages to all-frozen must not count them visible again.
$node->safe_psql('postgres', 'VACUUM (FREEZE) heap_pages');
is($node->safe_psql('postgres', $counts_sql), "$pages|$pages",
   'freeze counts each affected page without recounting its all-visible bit');
is($node->safe_psql('postgres', q{
SELECT tuples_frozen FROM ext_vacuum_statistics.pg_stats_vacuum_tables
 WHERE relname = 'heap_pages'}), '10000',
   'tuple and page freezing counters have distinct units');
$node->safe_psql('postgres', 'VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) heap_pages');
is($node->safe_psql('postgres', $counts_sql), "$pages|$pages",
   'rescanning already frozen and visible pages adds no work');

# Changing the indexed key requires index cleanup and a second heap pass.
# The old and new tuple can occupy either one or two distinct heap pages.
$node->safe_psql('postgres', q{
CREATE TABLE changed_pages AS
  SELECT split_part(ctid::text, ',', 1) AS blk FROM heap_pages WHERE id = 1;
UPDATE heap_pages SET id = 10001 WHERE id = 1;
INSERT INTO changed_pages
  SELECT split_part(ctid::text, ',', 1) FROM heap_pages WHERE id = 10001;
});
my $changed = $node->safe_psql('postgres', 'SELECT count(DISTINCT blk) FROM changed_pages');
$node->safe_psql('postgres', 'VACUUM (FREEZE, INDEX_CLEANUP ON) heap_pages');
my $expected = ($pages + 1) . '|' . ($pages + $changed);
is($node->safe_psql('postgres', $counts_sql), $expected,
   'index cleanup restores changed visibility bits and freezes the new tuple');
$node->safe_psql('postgres', 'VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) heap_pages');
is($node->safe_psql('postgres', $counts_sql), $expected,
   'another full scan does not count second-pass visibility work again');

# Empty pages can become all-visible without any tuples being frozen.
$node->safe_psql('postgres', q{
CREATE TABLE empty_pages (id int);
INSERT INTO empty_pages SELECT generate_series(1, 10000);
});
my $empty_pages = $node->safe_psql('postgres', q{
SELECT count(DISTINCT split_part(ctid::text, ',', 1)) FROM empty_pages});
$node->safe_psql('postgres', 'DELETE FROM empty_pages');
$node->safe_psql('postgres', 'VACUUM (FREEZE, TRUNCATE OFF) empty_pages');
my $empty_sql = q{
SELECT pages_frozen, pages_all_visible
  FROM ext_vacuum_statistics.pg_stats_vacuum_tables WHERE relname = 'empty_pages'};
is($node->safe_psql('postgres', $empty_sql), "0|$empty_pages",
   'empty pages become visible without adding frozen pages');
$node->safe_psql('postgres', 'VACUUM (FREEZE, TRUNCATE OFF, DISABLE_PAGE_SKIPPING) empty_pages');
is($node->safe_psql('postgres', $empty_sql), "0|$empty_pages",
   'rescanning empty pages does not count existing visibility marks');

$node->restart;
is($node->safe_psql('postgres', $counts_sql), $expected,
   'heap page counters survive a clean restart');

$node->safe_psql('postgres', q{
SELECT ext_vacuum_statistics.extvac_reset_entry(
  (SELECT oid FROM pg_database WHERE datname = current_database()), 'heap_pages'::regclass)});
is($node->safe_psql('postgres', $counts_sql), '0|0',
   'relation reset clears both heap page counters');
$node->safe_psql('postgres', 'VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) heap_pages');
is($node->safe_psql('postgres', $counts_sql), '0|0',
   'post-reset vacuum does not report existing frozen or visible pages as work');

$node->stop;
done_testing();

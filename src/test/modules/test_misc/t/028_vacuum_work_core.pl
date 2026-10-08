# Copyright (c) 2026, PostgreSQL Global Development Group

# Ordinary statistics storage must collect VACUUM work without an extension.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacuum_work_core');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;
is($node->safe_psql('postgres', 'SHOW shared_preload_libraries'), '',
   'no statistics extension is preloaded');
$node->safe_psql('postgres', q{
CREATE TABLE core_work (id int PRIMARY KEY);
INSERT INTO core_work SELECT generate_series(1, 2000);
DELETE FROM core_work WHERE id <= 1000;
SELECT pg_stat_force_next_flush();
});
my $db_sql = q{SELECT tuples_deleted FROM pg_stat_vacuum WHERE datname = current_database()};
my $before = $node->safe_psql('postgres', $db_sql);
$node->safe_psql('postgres', q{
VACUUM (INDEX_CLEANUP ON) core_work;
SELECT pg_stat_force_next_flush();
});
is($node->safe_psql('postgres', q{
SELECT tuples_deleted FROM pg_stat_vacuum_tables WHERE relname = 'core_work'
}), '1000', 'core reports heap tuples removed');
is($node->safe_psql('postgres', q{
SELECT tuples_deleted FROM pg_stat_vacuum_indexes WHERE indexrelname = 'core_work_pkey'
}), '1000', 'core separately reports index entries removed');
$node->poll_query_until('postgres', "SELECT tuples_deleted = " . ($before + 1000) . " FROM pg_stat_vacuum WHERE datname = current_database()")
  or die 'database vacuum work did not flush';
is($node->safe_psql('postgres', $db_sql), $before + 1000,
   'database counts table work once, excluding index entries');
my $saved = $node->safe_psql('postgres', q{
SELECT pg_stat_get_vacuum_stats('core_work'::regclass)::text
});
$node->restart;
is($node->safe_psql('postgres', q{
SELECT pg_stat_get_vacuum_stats('core_work'::regclass)::text
}), $saved, 'ordinary statistics persistence preserves vacuum work');
$node->safe_psql('postgres', q{
SELECT pg_stat_reset_single_table_counters('core_work'::regclass);
});
is($node->safe_psql('postgres', q{
SELECT tuples_deleted = 0 AND pages_scanned = 0 AND pages_all_visible = 0
FROM pg_stat_vacuum_tables WHERE relname = 'core_work'
}), 't', 'ordinary relation reset clears core vacuum counters');
is($node->safe_psql('postgres', $db_sql), $before + 1000,
   'relation reset leaves database totals intact');
$node->safe_psql('postgres', 'SELECT pg_stat_reset()');
is($node->safe_psql('postgres', $db_sql), '0',
   'ordinary database reset clears database vacuum work');

# Dropping a relation discards its native work counters transactionally.
my $relid = $node->safe_psql('postgres', "SELECT 'core_work'::regclass::oid");
$node->safe_psql('postgres', 'VACUUM (FREEZE) core_work');
$node->safe_psql('postgres', 'BEGIN; DROP TABLE core_work; ROLLBACK;');
is($node->safe_psql('postgres', q{
SELECT freeze_age_vacuum_count FROM pg_stat_vacuum_tables WHERE relname = 'core_work'}),
   '1', 'rolled-back DROP preserves core counters');
$node->safe_psql('postgres', 'DROP TABLE core_work');
is($node->safe_psql('postgres', "SELECT tuples_deleted = 0 AND freeze_age_vacuum_count = 0 FROM pg_stat_get_vacuum_stats($relid)"),
   't', 'committed DROP removes core vacuum statistics');

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
SELECT pages_deleted FROM pg_stat_vacuum_indexes
 WHERE indexrelname = 'spg_pages_idx'};
my $deleted = $node->safe_psql('postgres', $spg_sql);
is($deleted, '0', 'SP-GiST does not count a pre-existing zero page as newly deleted');
$node->safe_psql('postgres', 'VACUUM (INDEX_CLEANUP ON, DISABLE_PAGE_SKIPPING) spg_pages');
is($node->safe_psql('postgres', $spg_sql), $deleted,
   'SP-GiST rescan does not count previously emptied pages again');
is($node->safe_psql('postgres', q{
SELECT dead_pages FROM pg_stat_vacuum_indexes
 WHERE indexrelname = 'spg_pages_idx'}), '0',
   'reusable SP-GiST pages are not counted as dead pages');

$node->stop;
done_testing();

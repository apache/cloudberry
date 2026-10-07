# Copyright (c) 2026, PostgreSQL Global Development Group

# Frozen tuple work accumulates once and survives clean restart.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacstat_heap_tuples');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;
$node->safe_psql('postgres', q{
CREATE TABLE heap_tuples (id int PRIMARY KEY);
INSERT INTO heap_tuples SELECT generate_series(1, 10000);
});
my $counts_sql = q{
SELECT tuples_frozen FROM pg_stat_vacuum_tables WHERE relname = 'heap_tuples'};
$node->safe_psql('postgres', 'VACUUM heap_tuples');
is($node->safe_psql('postgres', $counts_sql), '0',
   'ordinary vacuum does not freeze young tuples');
$node->safe_psql('postgres', 'VACUUM (FREEZE) heap_tuples');
is($node->safe_psql('postgres', $counts_sql), '10000',
   'freeze counts each newly frozen tuple');
$node->safe_psql('postgres', 'VACUUM (FREEZE, DISABLE_PAGE_SKIPPING) heap_tuples');
is($node->safe_psql('postgres', $counts_sql), '10000',
   'rescanning frozen tuples adds no work');
$node->safe_psql('postgres', 'UPDATE heap_tuples SET id = 10001 WHERE id = 1');
$node->safe_psql('postgres', 'VACUUM (FREEZE, INDEX_CLEANUP ON) heap_tuples');
is($node->safe_psql('postgres', $counts_sql), '10001',
   'subsequent vacuum counts the new tuple');
$node->restart;
is($node->safe_psql('postgres', $counts_sql), '10001',
   'frozen tuple work survives clean restart');
$node->safe_psql('postgres', q{
SELECT pg_stat_reset_single_table_counters('heap_tuples'::regclass)});
is($node->safe_psql('postgres', $counts_sql), '0',
   'relation reset clears frozen tuple work');
$node->stop;
done_testing();

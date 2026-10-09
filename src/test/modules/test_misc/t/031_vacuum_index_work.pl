# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacuum_index_work');
$node->init;
$node->append_conf('postgresql.conf', 'autovacuum = off');
$node->start;

# Insert-only GIN cleanup deletes pending-list pages, not posting-tree pages.
$node->safe_psql('postgres', q{
CREATE TABLE gin_pending (a int[]);
CREATE INDEX gin_pending_idx ON gin_pending USING gin (a)
  WITH (gin_pending_list_limit = 65536);
INSERT INTO gin_pending SELECT ARRAY[g, g + 1] FROM generate_series(1, 4000) g;
VACUUM gin_pending;
});
my $pending_sql = q{SELECT pages_deleted FROM pg_stat_vacuum_indexes
                   WHERE indexrelname = 'gin_pending_idx'};
my $pending = $node->safe_psql('postgres', $pending_sql);
cmp_ok($pending, '>', 0, 'GIN pending-list cleanup counts page deletion work');
$node->safe_psql('postgres', 'VACUUM gin_pending');
is($node->safe_psql('postgres', $pending_sql), $pending,
   'GIN does not count the same pending-list pages again');

# A snapshot after DELETE allows tuple removal but retains index tombstones.
my $nrows = 100000 * ($node->safe_psql('postgres', 'SHOW block_size') / 8192);
$node->safe_psql('postgres', qq{
CREATE TABLE gin_retained (id int, a int[]);
CREATE INDEX gin_retained_idx ON gin_retained USING gin (a) WITH (fastupdate = off);
INSERT INTO gin_retained SELECT g, ARRAY[1] FROM generate_series(1, $nrows) g;
DELETE FROM gin_retained WHERE id <= $nrows / 2;
});
my $hold = $node->background_psql('postgres');
$hold->query_safe('BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT txid_current_snapshot()');
$node->safe_psql('postgres', 'VACUUM (INDEX_CLEANUP ON) gin_retained');
my $dead_sql = q{SELECT dead_pages FROM pg_stat_vacuum_indexes
                WHERE indexrelname = 'gin_retained_idx'};
my $dead = $node->safe_psql('postgres', $dead_sql);
cmp_ok($dead, '>', 0, 'GIN reports deleted pages retained by a snapshot');
$node->safe_psql('postgres', 'VACUUM (INDEX_CLEANUP ON) gin_retained');
is($node->safe_psql('postgres', $dead_sql), 2 * $dead,
   'GIN counts pre-existing retained pages on the next vacuum');
$hold->query_safe('COMMIT');
$hold->quit;
$node->safe_psql('postgres', 'SELECT txid_current(); VACUUM (INDEX_CLEANUP ON) gin_retained');
is($node->safe_psql('postgres', $dead_sql), 2 * $dead,
   'recyclable GIN pages do not contribute retained-page work');

# Identical hash keys force an overflow chain even as the hash table grows.
$node->safe_psql('postgres', qq{
CREATE TABLE hash_overflow (a int);
CREATE INDEX hash_overflow_idx ON hash_overflow USING hash (a);
INSERT INTO hash_overflow SELECT 1 FROM generate_series(1, $nrows);
DELETE FROM hash_overflow;
VACUUM (INDEX_CLEANUP ON) hash_overflow;
});
my $hash_sql = q{SELECT pages_deleted FROM pg_stat_vacuum_indexes
                WHERE indexrelname = 'hash_overflow_idx'};
my $freed = $node->safe_psql('postgres', $hash_sql);
cmp_ok($freed, '>', 0, 'hash vacuum counts released overflow pages');
$node->safe_psql('postgres', 'VACUUM (INDEX_CLEANUP ON) hash_overflow');
is($node->safe_psql('postgres', $hash_sql), $freed,
   'hash cleanup does not count already released overflow pages again');
is($node->safe_psql('postgres', q{SELECT dead_pages FROM pg_stat_vacuum_indexes
                                 WHERE indexrelname = 'hash_overflow_idx'}), '0',
   'hash overflow pages are reusable immediately');

for my $orientation ('row', 'column')
{
    my $table = "aux_$orientation";
    $node->safe_psql('postgres', qq{
CREATE TABLE $table (a int) WITH (appendonly = true, orientation = $orientation);
CREATE INDEX ${table}_idx ON $table (a);
INSERT INTO $table SELECT generate_series(1, 1000);
VACUUM $table;
});
    is($node->safe_psql('postgres', qq{
SELECT count(v.relid) = 3 AND sum(v.pages_scanned) > 0
FROM pg_appendonly a JOIN pg_class c
 ON c.oid IN (a.segrelid, a.blkdirrelid, a.visimaprelid)
LEFT JOIN pg_stat_vacuum_tables v ON v.relid = c.oid
WHERE a.relid = '$table'::regclass AND c.relkind IN ('o', 'b', 'M')
}), 't', "$orientation AO auxiliary heap vacuum work is visible");
}

$node->stop;
done_testing();

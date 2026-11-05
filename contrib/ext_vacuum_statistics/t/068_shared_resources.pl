# Copyright (c) 2026, PostgreSQL Global Development Group

# Shared catalogs have their own database aggregate, keyed by InvalidOid.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('shared_resources');
$node->init;
$node->append_conf('postgresql.conf', q{
shared_preload_libraries = 'ext_vacuum_statistics'
autovacuum = off
});
$node->start;
$node->safe_psql('postgres', q{
CREATE EXTENSION ext_vacuum_statistics;
CREATE TABLE local_resources (id int);
});
$node->safe_psql('postgres', 'VACUUM local_resources');
my $local = q[SELECT db_blks_read + db_blks_hit
FROM ext_vacuum_statistics.pg_stats_vacuum_database
WHERE dbname = current_database()];
my $local_before = $node->safe_psql('postgres', $local);
cmp_ok($local_before, '>', 0, 'ordinary database resources collected');

$node->safe_psql('postgres', 'VACUUM pg_shdepend');
my $shared = q[SELECT db_blks_read + db_blks_hit
FROM ext_vacuum_statistics.pg_stats_vacuum_database WHERE dboid = 0];
my $shared_before = $node->safe_psql('postgres', $shared);
cmp_ok($shared_before || 0, '>', 0, 'shared resources visible in database view');
is($node->safe_psql('postgres', q[SELECT count(*)
FROM ext_vacuum_statistics.pg_stats_vacuum_database
WHERE dboid = 0 AND dbname IS NULL]), '1', 'one shared row with a NULL database name');
is($node->safe_psql('postgres', q[SELECT total_blks_read + total_blks_hit
FROM ext_vacuum_statistics.pg_stats_get_vacuum_database(0)]),
	$shared_before, 'getter exposes the shared aggregate');
is($node->safe_psql('postgres', $local), $local_before,
	'shared vacuum is not attributed to the connected database');

$node->restart;
is($node->safe_psql('postgres', $shared), $shared_before,
	'shared aggregate survives clean restart');
$node->safe_psql('postgres', q[SELECT ext_vacuum_statistics.extvac_reset_db_entry(0)]);
is($node->safe_psql('postgres', $shared), '0', 'shared aggregate reset');
is($node->safe_psql('postgres', q[SELECT total_blks_read + total_blks_hit
FROM ext_vacuum_statistics.pg_stats_vacuum_tables WHERE relname = 'pg_shdepend']),
	'0', 'shared relation resources reset with the aggregate');
is($node->safe_psql('postgres', $local), $local_before,
	'shared reset preserves ordinary database resources');
$node->safe_psql('postgres', 'VACUUM pg_shdepend');
cmp_ok($node->safe_psql('postgres', $shared) || 0, '>', 0,
	'shared resources accumulate after reset');
$node->stop;
done_testing();

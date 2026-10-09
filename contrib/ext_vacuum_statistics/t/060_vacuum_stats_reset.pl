# Copyright (c) 2026, PostgreSQL Global Development Group

# Reset privileges, scope, and preservation of the entry's type, including
# after a restart and through the built-in pg_stat_reset() path.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacstat_reset');
$node->init;
$node->append_conf('postgresql.conf', q{
shared_preload_libraries = 'ext_vacuum_statistics'
autovacuum = off
});
$node->start;
$node->safe_psql('postgres', 'CREATE DATABASE other_reset');
for my $db ('postgres', 'other_reset')
{
	$node->safe_psql($db, q{
CREATE EXTENSION ext_vacuum_statistics;
CREATE TABLE reset_t (id int PRIMARY KEY) WITH (autovacuum_enabled = off);
INSERT INTO reset_t SELECT generate_series(1, 1000);
DELETE FROM reset_t WHERE id < 500;
});
	$node->safe_psql($db, 'VACUUM reset_t');
}
my $dboid = $node->safe_psql('postgres',
	q[SELECT oid FROM pg_database WHERE datname = current_database()]);
my $relid = $node->safe_psql('postgres', q[SELECT 'reset_t'::regclass::oid]);
my $idxid = $node->safe_psql('postgres', q[SELECT 'reset_t_pkey'::regclass::oid]);
my $table = q[SELECT total_blks_read + total_blks_hit FROM ext_vacuum_statistics.pg_stats_vacuum_tables WHERE relname = 'reset_t'];
my $index = q[SELECT total_blks_read + total_blks_hit FROM ext_vacuum_statistics.pg_stats_vacuum_indexes WHERE indexrelname = 'reset_t_pkey'];
my $database = q[SELECT db_blks_read + db_blks_hit FROM ext_vacuum_statistics.pg_stats_vacuum_database WHERE dbname = current_database()];
my $table_before = $node->safe_psql('postgres', $table);
my $index_before = $node->safe_psql('postgres', $index);
my $db_before = $node->safe_psql('postgres', $database);
cmp_ok($table_before, '>', 0, 'table statistics populated');
cmp_ok($index_before, '>', 0, 'index statistics populated');
cmp_ok($db_before, '>', 0, 'database statistics populated');

$node->safe_psql('postgres', q{
CREATE ROLE reset_reader;
GRANT USAGE ON SCHEMA ext_vacuum_statistics TO reset_reader;
});
for my $call ('extvac_reset_entry(0, 0)', 'extvac_reset_db_entry(0)',
	'vacuum_statistics_reset()')
{
	my ($stdout, $stderr);
	my $ret = $node->psql('postgres',
		"SET ROLE reset_reader; SELECT ext_vacuum_statistics.$call",
		stdout => \$stdout, stderr => \$stderr);
	isnt($ret, 0, "$call denied to a schema reader");
	like($stderr, qr/permission denied for function/, 'function ACL rejects reset');
}
is($node->safe_psql('postgres', $table), $table_before,
	'denied resets leave statistics unchanged');

$node->safe_psql('postgres', qq{
GRANT EXECUTE ON FUNCTION ext_vacuum_statistics.extvac_reset_entry(oid, oid) TO reset_reader;
SET ROLE reset_reader;
SELECT ext_vacuum_statistics.extvac_reset_entry($dboid, $relid);
});
is($node->safe_psql('postgres', $table), '0',
	'delegated relation reset preserves a visible zero row');
is($node->safe_psql('postgres', $index), $index_before,
	'table reset leaves index counters unchanged');
is($node->safe_psql('postgres', $database), $db_before,
	'table reset leaves database aggregate unchanged');
$node->safe_psql('postgres',
	"SELECT ext_vacuum_statistics.extvac_reset_entry($dboid, $idxid)");
is($node->safe_psql('postgres', $index), '0',
	'index reset preserves a visible zero row');

# SQL return types do not claim to count reset entries. Missing entries are
# harmless no-ops, consistent with built-in reset functions.
is($node->safe_psql('postgres', q{
SELECT bool_and(p.prorettype = 'void'::regtype AND p.proparallel = 'u' AND p.provolatile = 'v')
FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
WHERE n.nspname = 'ext_vacuum_statistics' AND p.proname LIKE '%reset%'
}), 't', 'all resets return void and are volatile and parallel unsafe');
$node->safe_psql('postgres', q{
SELECT ext_vacuum_statistics.extvac_reset_entry(4294967294, 4294967294);
SELECT ext_vacuum_statistics.extvac_reset_db_entry(4294967294);
});

$node->safe_psql('postgres', 'VACUUM reset_t');
$node->restart;
$node->safe_psql('postgres',
	"SELECT ext_vacuum_statistics.extvac_reset_db_entry($dboid)");
is($node->safe_psql('postgres', $table), '0', 'database reset retains table type after restart');
is($node->safe_psql('postgres', $index), '0', 'database reset retains index type after restart');
is($node->safe_psql('postgres', $database), '0', 'database reset retains aggregate type after restart');
cmp_ok($node->safe_psql('other_reset', $table), '>', 0,
	'database reset leaves another database untouched');

$node->safe_psql('postgres', 'VACUUM reset_t');
$node->safe_psql('postgres', 'SELECT pg_stat_reset()');
is($node->safe_psql('postgres', $table), '0', 'core reset retains table row');
is($node->safe_psql('postgres', $index), '0', 'core reset retains index row');
is($node->safe_psql('postgres', $database), '0', 'core reset retains database row');
$node->safe_psql('postgres', 'VACUUM reset_t');
cmp_ok($node->safe_psql('postgres', $table), '>', 0,
	'vacuum resumes accumulation after reset');
$node->safe_psql('postgres', 'SELECT ext_vacuum_statistics.vacuum_statistics_reset()');
for my $db ('postgres', 'other_reset')
{
	is($node->safe_psql($db, $table), '0', "global reset clears table in $db");
	is($node->safe_psql($db, $index), '0', "global reset clears index in $db");
	is($node->safe_psql($db, $database), '0', "global reset clears aggregate in $db");
}
$node->stop;
done_testing();

# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('custom_stats');
$node->init;
$node->append_conf('postgresql.conf', q{
shared_preload_libraries = 'test_custom_stats'
shared_buffers = '16MB'
autovacuum = off
});
$node->start;
is($node->safe_psql('postgres', 'SELECT 1'), '1',
	'custom fixed statistics fit in the reserved shared memory');
$node->safe_psql('postgres', q{
CREATE FUNCTION test_custom_stats_create() RETURNS void
AS 'test_custom_stats', 'test_custom_stats_create' LANGUAGE C;
CREATE FUNCTION test_custom_stats_get(int) RETURNS bigint
AS 'test_custom_stats', 'test_custom_stats_get' LANGUAGE C STRICT;
SELECT test_custom_stats_create();
CREATE TABLE stats_survivor (id int);
INSERT INTO stats_survivor VALUES (1), (2), (3);
SELECT pg_stat_force_next_flush();
});
my $custom = 'SELECT test_custom_stats_get(0), test_custom_stats_get(1), test_custom_stats_get(2)';
is($node->safe_psql('postgres', $custom), '789|123|456',
	'fixed, hash and named custom entries have nonzero values');
my $relation = q{SELECT n_tup_ins FROM pg_stat_all_tables WHERE relname = 'stats_survivor'};
$node->poll_query_until('postgres', "$relation", '3')
  or die 'table statistics not flushed';
my $database = q{SELECT tup_inserted, stats_reset FROM pg_stat_database WHERE datname = current_database()};
my $db_before = $node->safe_psql('postgres', $database);
my $wal = 'SELECT stats_reset FROM pg_stat_wal';
my $wal_before = $node->safe_psql('postgres', $wal);

$node->restart;
is($node->safe_psql('postgres', $custom), '789|123|456',
	'all three custom payload formats survive a clean restart');
is($node->safe_psql('postgres', $relation), '3',
	'relation counters survive with provider loaded');
$node->stop;
my $statfile = $node->data_dir . '/pg_stat/pgstat.stat';
my $saved = slurp_file($statfile);

$node->append_conf('postgresql.conf', "shared_preload_libraries = ''");
$node->start;
is($node->safe_psql('postgres', $relation), '3',
	'relation counters survive removal of the custom provider');
is($node->safe_psql('postgres', $database), $db_before,
	'database counters and reset timestamp survive provider removal');
is($node->safe_psql('postgres', $wal), $wal_before,
	'built-in fixed statistics are not reset on provider removal');
unlike(slurp_file($node->logfile), qr/corrupted statistics file/,
	'unavailable custom kinds do not make the file corrupt');
$node->stop;

# All supported hosts use a four-byte kind and length in this native format.
# Locate the known custom fixed payload rather than assuming an entry order.
my $record = 'F' . pack('L', 8) . pack('L', 25) . pack('Q', 789);
my $offset = index($saved, $record);
ok($offset >= 0, 'found the persisted custom fixed entry');
BAIL_OUT('custom fixed entry not found') if $offset < 0;
sub write_stats
{
	my ($data) = @_;
	open(my $fh, '>:raw', $statfile) or die "open $statfile: $!";
	print {$fh} $data;
	close($fh) or die "close $statfile: $!";
}

# Skipping an unavailable kind must still detect a truncated payload.
write_stats(substr($saved, 0, $offset + length($record) - 1));
$node->rotate_logfile;
$node->start;
like(slurp_file($node->logfile), qr/corrupted statistics file/,
	'truncated unavailable-kind payload is rejected');
$node->stop;

# A loaded provider must validate the saved length before reading its payload.
my $bad_length = $saved;
substr($bad_length, $offset + 1, 4, pack('L', 7));
write_stats($bad_length);
$node->append_conf('postgresql.conf', "shared_preload_libraries = 'test_custom_stats'");
$node->rotate_logfile;
$node->start;
like(slurp_file($node->logfile), qr/corrupted statistics file/,
	'known-kind payload with an incompatible length is rejected');
$node->stop;

for my $name ('relation', 'ReLaTiOn', 'test_custom_fixed', 'TEST_CUSTOM_FIXED')
{
	$node->append_conf('postgresql.conf', "test_custom_stats.name = '$name'");
	$node->rotate_logfile;
	ok(!$node->start(fail_ok => 1), "reject colliding statistics name $name");
	like(slurp_file($node->logfile), qr/Existing cumulative statistics with ID \d+ has the same name/,
		'failure identifies the name collision');
}
done_testing();

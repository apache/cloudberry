# Copyright (c) 2026, PostgreSQL Global Development Group

use strict;
use warnings;
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

# Startup allocates a large fixed-numbered custom statistics area.
$node->start;
is($node->safe_psql('postgres', 'SELECT 1'), '1',
	'custom fixed statistics fit in the reserved shared memory');

# Persist a variable-numbered entry without a serialized-name callback.
$node->safe_psql('postgres', q{
CREATE FUNCTION test_custom_stats_create() RETURNS void
AS 'test_custom_stats', 'test_custom_stats_create' LANGUAGE C;
SELECT test_custom_stats_create();
});
$node->stop;

# The fixed kind does not write to disk, so the unknown kind is encountered
# in the hash-entry reader, not in the fixed-entry reader.
$node->append_conf('postgresql.conf', "shared_preload_libraries = ''");
$node->start;
is($node->safe_psql('postgres', 'SELECT 1'), '1',
	'server starts after removing the custom statistics module');
like(slurp_file($node->logfile),
	qr/could not find information of kind 24 for entry of type S/,
	'unknown custom hash entry is rejected without crashing');
$node->stop;

done_testing();

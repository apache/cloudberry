# Copyright (c) 2026, PostgreSQL Global Development Group

# Test the vacuum_interrupt_count counter of pg_stat_vacuum.
#
# vacuum_interrupt_count records how many times a vacuum in the database was
# interrupted by an error.  We provoke that by starting a vacuum that sleeps
# at its cost-based delay points and canceling it, with pg_cancel_backend(),
# while it is still running.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->append_conf(
	'postgresql.conf', qq[
autovacuum = off
]);
$node->start;

# fillfactor = 10 spreads the rows over many pages so the vacuum hits enough
# cost-delay points to stay running until we cancel it.
$node->safe_psql(
	'postgres', qq[
CREATE TABLE vacstat_int (id int) WITH (autovacuum_enabled = off, fillfactor = 10);
INSERT INTO vacstat_int SELECT generate_series(1, 1000);
DELETE FROM vacstat_int;
]);

# Start a vacuum that sleeps at every cost-delay point, in the background.  The
# \echo lets query_until() return as soon as the VACUUM has been launched.
my $appname = 'vacuum_interrupt_test';
my $vac = $node->background_psql('postgres', on_error_stop => 0);
$vac->query_until(
	qr/start/, qq[
SET application_name = '$appname';
SET vacuum_cost_delay = '100ms';
SET vacuum_cost_limit = 1;
\\echo start
VACUUM vacstat_int;
]);

# Wait until the vacuum is actually running, then cancel it.
$node->poll_query_until(
	'postgres', qq[
SELECT count(*) = 1 FROM pg_stat_activity
 WHERE application_name = '$appname' AND query LIKE 'VACUUM%' AND state = 'active'])
  or die "timed out waiting for the vacuum to start";

my $cancelled = $node->safe_psql(
	'postgres', qq[
SELECT pg_cancel_backend(pid) FROM pg_stat_activity
 WHERE application_name = '$appname' AND query LIKE 'VACUUM%']);
is($cancelled, 't', 'canceled the running vacuum');

$vac->quit;
like($vac->{stderr}, qr/canceling statement due to user request/,
	'vacuum canceled by user request');

is( $node->safe_psql(
		'postgres', qq[
SELECT vacuum_interrupt_count > 0 FROM pg_stat_vacuum WHERE datname = current_database()]),
	't',
	'vacuum_interrupt_count advanced in pg_stat_vacuum');

$node->stop;

done_testing();

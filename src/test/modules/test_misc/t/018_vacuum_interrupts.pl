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

# Wait for a cost-delay point inside the vacuum, not merely an active query.
# The query becomes active before the table vacuum error handler is installed.
$node->poll_query_until(
	'postgres', qq[
SELECT count(*) = 1 FROM pg_stat_activity
 WHERE application_name = '$appname' AND query LIKE 'VACUUM%' AND state = 'active'
   AND wait_event = 'VacuumDelay'])
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

# AO compaction evaluates expression indexes.  Exceptions caught inside an
# index function must not count as interrupted vacuums, while escaping ones
# must count exactly once.
$node->safe_psql('postgres', q{
CREATE FUNCTION vacstat_index_value(i int) RETURNS int
LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN
    IF current_setting('application_name') = 'vacuum_unhandled_error' THEN
        RAISE EXCEPTION 'unhandled index error';
    END IF;
    BEGIN
        RAISE EXCEPTION 'handled index error';
    EXCEPTION WHEN raise_exception THEN
        IF current_setting('application_name') = 'vacuum_handled_error' THEN
            RAISE NOTICE 'caught index error during compaction';
        END IF;
        RETURN i;
    END;
END;
$$;
});

for my $orientation ('row', 'column')
{
    my $table = "vacstat_caught_$orientation";
    $node->safe_psql('postgres', qq{
CREATE TABLE $table (id int) WITH (appendonly=true, orientation=$orientation);
CREATE INDEX ${table}_idx ON $table (vacstat_index_value(id));
INSERT INTO $table SELECT generate_series(1, 100);
DELETE FROM $table WHERE id % 2 = 0;
});
    my $before = $node->safe_psql('postgres', q{
SELECT vacuum_interrupt_count FROM pg_stat_vacuum WHERE datname = current_database()
});
    my ($stdout, $stderr);
    is($node->psql('postgres', qq{
SET application_name = 'vacuum_handled_error';
SET gp_appendonly_compaction_threshold = 1;
VACUUM $table;
}, stdout => \$stdout, stderr => \$stderr), 0,
       "$orientation vacuum succeeds with caught index exceptions");
    like($stderr, qr/caught index error during compaction/,
         "$orientation compaction evaluated the exception-handling index function");
    is($node->safe_psql('postgres', q{
SELECT vacuum_interrupt_count FROM pg_stat_vacuum WHERE datname = current_database()
}), $before, "$orientation caught exceptions do not increment interruptions");
    is($node->safe_psql('postgres', "SELECT count(*) FROM $table"), '50',
       "$orientation vacuum preserves surviving rows");

    $node->safe_psql('postgres', "DELETE FROM $table WHERE id <= 25");
    isnt($node->psql('postgres', qq{
SET application_name = 'vacuum_unhandled_error';
SET gp_appendonly_compaction_threshold = 1;
VACUUM $table;
}, stdout => \$stdout, stderr => \$stderr), 0,
         "$orientation vacuum fails on an escaping index exception");
    like($stderr, qr/unhandled index error/,
         "$orientation reports the escaping index exception");
    is($node->safe_psql('postgres', q{
SELECT vacuum_interrupt_count FROM pg_stat_vacuum WHERE datname = current_database()
}), $before + 1, "$orientation escaping exception counts exactly once");
    $node->safe_psql('postgres', "DROP TABLE $table");
}
$node->safe_psql('postgres', 'DROP FUNCTION vacstat_index_value(int)');

$node->stop;

done_testing();

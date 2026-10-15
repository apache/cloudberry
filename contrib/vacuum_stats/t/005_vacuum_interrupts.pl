# Copyright (c) 2026, PostgreSQL Global Development Group

# Adapt v44-0004 to the PG14 collector: wait for heap processing before
# canceling VACUUM, then wait for its deferred statistics report.
use strict;
use warnings FATAL => 'all';
use PostgresNode;
use TestLib;
use Test::More;

my $node = get_new_node('vacuum_interrupts');
$node->init;
$node->append_conf('postgresql.conf', "autovacuum = off\n");
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION vacuum_stats');

my $db_count = q{SELECT vacuum_interrupt_count FROM pg_stat_vacuum_database
                WHERE datname = current_database()};
my $shared_count = q{SELECT vacuum_interrupt_count FROM pg_stat_vacuum_database
                    WHERE datid = 0};
my $nrows = 1000 * ($node->safe_psql('postgres', 'SHOW block_size') / 8192);
$node->safe_psql('postgres', qq{
CREATE TABLE vacstat_int (id int PRIMARY KEY)
  WITH (autovacuum_enabled = off, fillfactor = 10);
INSERT INTO vacstat_int SELECT generate_series(1, $nrows);
DELETE FROM vacstat_int WHERE id % 2 = 0;
});

# VERBOSE emits lower-severity messages while the vacuum error callback is
# installed. They must not be mistaken for an interrupted vacuum.
$node->safe_psql('postgres', 'VACUUM (VERBOSE, INDEX_CLEANUP ON) vacstat_int');
$node->poll_query_until('postgres', q{
SELECT vacuum_count = 1 FROM pg_stat_all_tables_internal WHERE relname = 'vacstat_int'
}) or BAIL_OUT('successful vacuum report did not reach the collector');
is($node->safe_psql('postgres', $db_count), '0',
   'successful vacuum and its VERBOSE messages do not count as errors');

my ($stdout, $stderr) = ('', '');
is($node->psql('postgres', 'SELECT 1 / 0',
               stdout => \$stdout, stderr => \$stderr), 3,
   'unrelated statement raises an error');
is($node->safe_psql('postgres', $db_count), '0',
   'an error outside VACUUM does not increment the counter');

sub cancel_vacuum
{
	my ($relation, $track_counts) = @_;
	my ($in, $out) = ('', '');
	my $timer = IPC::Run::timeout($TestLib::timeout_default);
	my $vac = $node->background_psql('postgres', \$in, \$out, $timer,
	                               on_error_stop => 0);
	$out = '';
	$in = qq{
SET application_name = 'vacuum_interrupt_test';
SET track_counts = $track_counts;
SET vacuum_cost_delay = '100ms';
SET vacuum_cost_limit = 1;
\\echo vacuum_started
VACUUM (DISABLE_PAGE_SKIPPING) $relation;
\\echo vacuum_done :ERROR :SQLSTATE
};
	pump_until($vac, $timer, \$out, qr/^vacuum_started\r?$/m)
	  or BAIL_OUT('background psql did not launch VACUUM');

	# An active VACUUM query alone is insufficient: it might still be waiting
	# for a relation lock, before the heap error callback has been installed.
	$node->poll_query_until('postgres', qq{
SELECT count(*) = 1
FROM pg_stat_activity a JOIN pg_stat_progress_vacuum v USING (pid)
WHERE a.application_name = 'vacuum_interrupt_test'
  AND v.relid = '$relation'::regclass AND v.phase = 'scanning heap'
  AND a.wait_event = 'VacuumDelay'
}) or BAIL_OUT("VACUUM of $relation did not enter heap processing");
	is($node->safe_psql('postgres', q{
SELECT pg_cancel_backend(pid) FROM pg_stat_activity
WHERE application_name = 'vacuum_interrupt_test'
}), 't', "sent cancellation to VACUUM of $relation");
	pump_until($vac, $timer, \$out, qr/^vacuum_done\b/m)
	  or BAIL_OUT('canceled VACUUM did not return');
	like($out, qr/^vacuum_done true 57014\r?$/m,
	     "VACUUM of $relation reports query_canceled");
	$in = "\\q\n";
	$vac->finish;
}

for my $expected (1..2)
{
	cancel_vacuum('vacstat_int', 'on');
	ok($node->poll_query_until('postgres',
	   "SELECT ($db_count) = $expected"),
	   "canceled vacuum increments database total to exactly $expected");
}
is($node->safe_psql('postgres', $shared_count), '0',
   'local relation errors leave shared-object statistics alone');

cancel_vacuum('vacstat_int', 'off');
# Drain reports, including the canceled backend's final message, before
# asserting that a disabled counter did not change.
$node->restart;
is($node->safe_psql('postgres', $db_count), '2',
   'track_counts off suppresses counting; earlier errors survive restart');

# Make pg_authid large enough to observe cost-delay waits even with 32kB
# blocks. Shared relations report errors to datid zero, not the session's DB.
$node->safe_psql('postgres', qq{
DO \$\$ BEGIN
  FOR i IN 1..$nrows LOOP
    EXECUTE format('CREATE ROLE vacstat_role_%s', i);
  END LOOP;
END \$\$;
});
cancel_vacuum('pg_authid', 'on');
ok($node->poll_query_until('postgres', "SELECT ($shared_count) = 1"),
   'shared relation error reaches the datid zero entry');
is($node->safe_psql('postgres', $db_count), '2',
   'shared relation error leaves the current database total unchanged');

$node->safe_psql('postgres', 'SELECT pg_stat_reset()');
ok($node->poll_query_until('postgres', "SELECT ($db_count) = 0"),
   'ordinary database reset clears vacuum interruptions');
$node->restart;
is($node->safe_psql('postgres', $db_count), '0',
   'reset value survives restart');
is($node->safe_psql('postgres', $shared_count), '1',
   'database reset preserves shared errors, which survive restart');
is($node->safe_psql('postgres', q{
SELECT count(*) FROM pg_attribute
WHERE attrelid = 'pg_stat_vacuum_database'::regclass
  AND attname = 'vacuum_interrupt_count' AND NOT attisdropped
}), '1', 'extension view exposes vacuum_interrupt_count');

$node->stop;
done_testing();

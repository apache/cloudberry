# Test for pausing and resuming recovery at specific restore points,
# both at initial startup and in a continuous fashion by advancing
# gp_pause_on_restore_point_replay.

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 12;
use File::Copy;

# Initialize and start primary node
my $node_primary = get_new_node('primary');
$node_primary->init(has_archiving => 1, allows_streaming => 1);
$node_primary->start;

my $node_standby = get_new_node("standby");

sub test_pause_in_recovery
{
	my ($restore_point, $test_lsn, $num_rows) = @_;

	# Wait until standby has replayed enough data
	my $caughtup_query = "SELECT pg_last_wal_replay_lsn() = '$test_lsn'::pg_lsn";
	$node_standby->poll_query_until('postgres', $caughtup_query)
		or die "Timed out while waiting for standby to catch up";

	# Check data has been replayed
	my $result = $node_standby->safe_psql('postgres', "SELECT count(*) FROM table_foo;");
	is($result, $num_rows, "check standby content for $restore_point");
	ok($node_standby->safe_psql('postgres', 'SELECT pg_is_wal_replay_paused();') eq 't',
		"standby is paused in recovery on $restore_point");
}

# Create data before taking the backup
$node_primary->safe_psql('postgres', "CREATE TABLE table_foo AS SELECT generate_series(1,1000);");
# Take backup from which all operations will be run
$node_primary->backup('my_backup');
my $lsn0 = $node_primary->safe_psql('postgres', "SELECT pg_create_restore_point('rp0');");
# Switching WAL guarantees that the restore point is available to the standby
$node_primary->safe_psql('postgres', "SELECT pg_switch_wal();");

# Add more data, create restore points and switch wal to guarantee
# that the restore point is available to the standby

# rp1
$node_primary->safe_psql('postgres', "INSERT INTO table_foo VALUES (generate_series(1001,2000))");
my $lsn1 = $node_primary->safe_psql('postgres', "SELECT pg_create_restore_point('rp1');");
$node_primary->safe_psql('postgres', "SELECT pg_switch_wal();");

# rp2
$node_primary->safe_psql('postgres', "INSERT INTO table_foo VALUES (generate_series(2001, 3000))");
my $lsn2 = $node_primary->safe_psql('postgres', "SELECT pg_create_restore_point('rp2');");
$node_primary->safe_psql('postgres', "SELECT pg_switch_wal();");

# rp3
$node_primary->safe_psql('postgres', "INSERT INTO table_foo VALUES (generate_series(3001, 4000))");
$node_primary->safe_psql('postgres', "SELECT pg_create_restore_point('rp3');");
$node_primary->safe_psql('postgres', "SELECT pg_switch_wal();");

# rp4
$node_primary->safe_psql('postgres', "INSERT INTO table_foo VALUES (generate_series(4001, 5000))");
my $lsn4 = $node_primary->safe_psql('postgres', "SELECT pg_create_restore_point('rp4');");
$node_primary->safe_psql('postgres', "SELECT pg_switch_wal();");

# rp5
$node_primary->safe_psql('postgres', "INSERT INTO table_foo VALUES (generate_series(5001, 6000))");
$node_primary->safe_psql('postgres', "SELECT pg_create_restore_point('rp5');");
$node_primary->safe_psql('postgres', "SELECT pg_switch_wal();");

# Restore the backup
$node_standby->init_from_backup($node_primary, 'my_backup', has_restoring => 1);
# Enable `hot_standby`
$node_standby->append_conf('postgresql.conf', qq(hot_standby = 'on'));

# Set rp0 as a restore point to pause on start up
$node_standby->append_conf('postgresql.conf', qq(gp_pause_on_restore_point_replay = 'rp0'));
# Start the standby
$node_standby->start;
test_pause_in_recovery('rp0', $lsn0, 1000);

# Advance to rp1
$node_standby->adjust_conf('postgresql.conf', 'gp_pause_on_restore_point_replay', "rp1");
$node_standby->reload;
$node_standby->safe_psql('postgres', "SELECT pg_wal_replay_resume();");
test_pause_in_recovery('rp1', $lsn1, 2000);

# Advance to rp2
$node_standby->adjust_conf('postgresql.conf', 'gp_pause_on_restore_point_replay', "rp2");
$node_standby->reload;
$node_standby->safe_psql('postgres', "SELECT pg_wal_replay_resume();");
test_pause_in_recovery('rp2', $lsn2, 3000);

# Verify that a restart will bring us back to rp2
$node_standby->restart;
test_pause_in_recovery('rp2', $lsn2, 3000);

# Skip rp3 and advance to rp4
$node_standby->adjust_conf('postgresql.conf', 'gp_pause_on_restore_point_replay', "rp4");
$node_standby->reload;
$node_standby->safe_psql('postgres', "SELECT pg_wal_replay_resume();");
test_pause_in_recovery('rp4', $lsn4, 5000);

# Do not advance to rp5; signal promote and then resume recovery
$node_standby->safe_psql('postgres', "SELECT pg_promote(false);");
$node_standby->safe_psql('postgres', "SELECT pg_wal_replay_resume();");

# Wait for standby to promote
$node_standby->poll_query_until('postgres', "SELECT NOT pg_is_in_recovery();")
	or die "Timed out while waiting for standby to exit recovery";

# Check that we promoted with rp4's table count and not rp5's
my $result = $node_standby->safe_psql('postgres', "SELECT count(*) FROM table_foo;");
is($result, 5000, "check standby content after promotion");

# Make sure the former standby is now writable
$node_standby->safe_psql('postgres', "INSERT INTO table_foo VALUES (generate_series(6001, 7000));");
$result = $node_standby->safe_psql('postgres', "SELECT count(*) FROM table_foo;");
is($result, 6000, "check standby is writable after promotion");

$node_primary->teardown_node;
$node_standby->teardown_node;

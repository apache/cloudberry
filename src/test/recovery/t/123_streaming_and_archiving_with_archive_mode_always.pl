use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 8;
use File::Copy;

# Initialize the master node with WAL archiving configured
my $node_master = get_new_node('master');
$node_master->init(has_archiving => 1, allows_streaming => 1);
# Set wal_sender_archiving_status_interval to a small value so that the master sends the report quickly
$node_master->append_conf('postgresql.conf', 'wal_sender_archiving_status_interval = 50ms');
$node_master->start;
my $master_archive = $node_master->archive_dir;
my $master_data = $node_master->data_dir;
# Set the archive_command to an incorrect value, causing archiving to fail
$node_master->safe_psql('postgres', "ALTER SYSTEM SET archive_command = 'exit 1'; SELECT pg_reload_conf();");
# Make a backup copy
$node_master->backup('my_backup');
# Initialize the standby node. It will inherit archive_command and wal_sender_archiving_status_interval from the master node
my $node_standby = get_new_node('standby');
$node_standby->init_from_backup($node_master, 'my_backup', has_streaming => 1);
# Set archive_mode to always
$node_standby->append_conf('postgresql.conf', 'archive_mode = always');
my $standby_archive = $node_standby->archive_dir;
my $standby_data = $node_standby->data_dir;
$node_standby->start;
# Generate some data
$node_master->safe_psql('postgres', "CREATE TABLE test AS SELECT generate_series(1,10);");
# Remember current WAL segment
my $walfile = $node_master->safe_psql('postgres', "SELECT pg_xlogfile_name(pg_current_xlog_location());");
# Switch WAL segment and make checkpoint
$node_master->safe_psql('postgres', "SELECT pg_switch_xlog(); CHECKPOINT;");
# Remember another WAL segment
my $walfile2 = $node_master->safe_psql('postgres', "SELECT pg_xlogfile_name(pg_current_xlog_location());");
# After the WAL switch, the current WAL file will be marked as ready to be archived on master.
# But this WAL file will not be archived due to the incorrect archive_command
my $walfile_ready = "pg_xlog/archive_status/$walfile.ready";
my $walfile_done = "pg_xlog/archive_status/$walfile.done";
# Wait for the standby to catch up
$node_master->wait_for_catchup($node_standby, 'write', $node_master->lsn('insert'));
# Wait for archive failure
$node_master->poll_query_until('postgres', "SELECT failed_count > 0 FROM pg_stat_archiver", 't')
    or die "Timed out while waiting for archiving to fail";
# .ready files exist on master and standby for the remembered WAL segment
ok( -f "$master_data/$walfile_ready", ".ready file exists on master for WAL segment $walfile");
ok( -f "$standby_data/$walfile_ready", ".ready file exists on standby for WAL segment $walfile");
# .done files do not exist on master and standby for remembered WAL segment
ok( !-f "$master_data/$walfile_done", ".done file does not exist on master for WAL segment $walfile");
ok( !-f "$standby_data/$walfile_done", ".done file does not exist on standby for WAL segment $walfile");
# Make WAL archiving work again for master by resetting archive_command
$node_master->safe_psql('postgres', "ALTER SYSTEM RESET archive_command; SELECT pg_reload_conf();");
# Force the archiver process to wake up and start archiving
$node_master->safe_psql('postgres', "SELECT pg_switch_xlog();");
# Wait until master creates a .done file for the archived segment
wait_until_file_exists("$master_data/$walfile_done", ".done file to exist on master for WAL segment $walfile");
# The .ready file should no longer be on master
ok( !-f "$master_data/$walfile_ready", ".ready file does not exist for WAL segment $walfile");
# Wait for master to load the WAL segment into the archive.
wait_until_file_exists("$master_archive/$walfile", "$walfile to be archived by the master");
# Wait until master sends an archiving notification to standby.
run_cmd_until(["grep", "sending archival report: $walfile2", $node_master->logfile], "sending archival report: $walfile2")
    or die "Timed out while waiting for sending archival report: $walfile2";
# The .ready file should still be on standby.
ok( -f "$standby_data/$walfile_ready", ".ready file exists on standby for WAL segment $walfile");
# The .done file should still not be on standby.
ok( !-f "$standby_data/$walfile_done", ".done file does not exist on standby for WAL segment $walfile");
# Standby has not loaded the WAL segment into the archive yet.
ok( !-f "$standby_archive/$walfile", "$walfile does not exist in standby's archive");

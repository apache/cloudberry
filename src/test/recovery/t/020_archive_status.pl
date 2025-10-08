#
# Tests related to WAL archiving and recovery.
#
use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More tests => 7;
use Config;

my $primary = get_new_node('master');
$primary->init(
	has_archiving    => 1,
	allows_streaming => 1);
$primary->append_conf('postgresql.conf', 'autovacuum = off');
$primary->start;
my $primary_data = $primary->data_dir;

# Temporarily use an archive_command value to make the archiver fail,
# knowing that archiving is enabled.  Note that we cannot use a command
# that does not exist as in this case the archiver process would just exit
# without reporting the failure to pg_stat_archiver.  This also cannot
# use a plain "false" as that's unportable on Windows.  So, instead, as
# a portable solution, use an archive command based on a command known to
# work but will fail: copy with an incorrect original path.
my $incorrect_command =
  $TestLib::windows_os
  ? qq{copy "%p_does_not_exist" "%f_does_not_exist"}
  : qq{cp "%p_does_not_exist" "%f_does_not_exist"};
$primary->safe_psql(
	'postgres', qq{
    ALTER SYSTEM SET archive_command TO '$incorrect_command';
    SELECT pg_reload_conf();
});

# Save the WAL segment currently in use and switch to a new segment.
# This will be used to track the activity of the archiver.
my $segment_name_1 = $primary->safe_psql('postgres',
	q{SELECT pg_xlogfile_name(pg_current_xlog_location())});
my $segment_path_1       = "pg_xlog/archive_status/$segment_name_1";
my $segment_path_1_ready = "$segment_path_1.ready";
my $segment_path_1_done  = "$segment_path_1.done";
$primary->safe_psql(
	'postgres', q{
	CREATE TABLE mine AS SELECT generate_series(1,10) AS x;
	SELECT pg_switch_xlog();
	CHECKPOINT;
});

# Wait for an archive failure.
$primary->poll_query_until('postgres',
	q{SELECT failed_count > 0 FROM pg_stat_archiver}, 't')
  or die "Timed out while waiting for archiving to fail";
ok( -f "$primary_data/$segment_path_1_ready",
	".ready file exists for WAL segment $segment_name_1 waiting to be archived"
);
ok( !-f "$primary_data/$segment_path_1_done",
	".done file does not exist for WAL segment $segment_name_1 waiting to be archived"
);

is( $primary->safe_psql(
		'postgres', q{
		SELECT archived_count, last_failed_wal
		FROM pg_stat_archiver
	}),
	"0|$segment_name_1",
	"pg_stat_archiver failed to archive $segment_name_1");

# Crash the cluster for the next test in charge of checking that non-archived
# WAL segments are not removed.
$primary->stop('immediate');

# Recovery tests for the archiving with a standby partially check
# the recovery behavior when restoring a backup taken using a
# snapshot with no pg_start/stop_backup.  In this situation,
# the recovered standby should enter first crash recovery then
# switch to regular archive recovery.  Note that the base backup
# is taken here so as archive_command will fail.  This is necessary
# for the assumptions of the tests done with the standbys below.
$primary->backup_fs_cold('backup');

$primary->start;
ok( -f "$primary_data/$segment_path_1_ready",
	".ready file for WAL segment $segment_name_1 still exists after crash recovery on primary"
);

# Allow WAL archiving again and wait for a success.
$primary->safe_psql(
	'postgres', q{
	ALTER SYSTEM RESET archive_command;
	SELECT pg_reload_conf();
});

$primary->poll_query_until('postgres',
	q{SELECT archived_count = 1 FROM pg_stat_archiver})
  or die "Timed out while waiting for archiving to finish";

ok(!-f "$primary_data/$segment_path_1_ready",
	".ready file for archived WAL segment $segment_name_1 removed");

ok(-f "$primary_data/$segment_path_1_done",
	".done file for archived WAL segment $segment_name_1 exists");

is( $primary->safe_psql(
		'postgres', q{ SELECT last_archived_wal FROM pg_stat_archiver }),
	$segment_name_1,
	"archive success reported in pg_stat_archiver for WAL segment $segment_name_1"
);

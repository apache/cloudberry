
<<<<<<< HEAD
# Copyright (c) 2021-2023, PostgreSQL Global Development Group
=======
# Copyright (c) 2021-2022, PostgreSQL Global Development Group
>>>>>>> main

# Test replay of tablespace/database creation/drop

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
<<<<<<< HEAD
use Time::HiRes qw(usleep);

sub test_tablespace
{
	my ($strategy) = @_;

	my $node_primary = PostgreSQL::Test::Cluster->new("primary1_$strategy");
=======

sub test_tablespace
{
	my $node_primary = PostgreSQL::Test::Cluster->new("primary1");
>>>>>>> main
	$node_primary->init(allows_streaming => 1);
	$node_primary->start;
	$node_primary->psql(
		'postgres',
		qq[
			SET allow_in_place_tablespaces=on;
			CREATE TABLESPACE dropme_ts1 LOCATION '';
			CREATE TABLESPACE dropme_ts2 LOCATION '';
			CREATE TABLESPACE source_ts  LOCATION '';
			CREATE TABLESPACE target_ts  LOCATION '';
			CREATE DATABASE template_db IS_TEMPLATE = true;
			SELECT pg_create_physical_replication_slot('slot', true);
		]);
	my $backup_name = 'my_backup';
	$node_primary->backup($backup_name);

<<<<<<< HEAD
	my $node_standby = PostgreSQL::Test::Cluster->new("standby2_$strategy");
=======
	my $node_standby = PostgreSQL::Test::Cluster->new("standby2");
>>>>>>> main
	$node_standby->init_from_backup($node_primary, $backup_name,
		has_streaming => 1);
	$node_standby->append_conf('postgresql.conf',
		"allow_in_place_tablespaces = on");
<<<<<<< HEAD
	$node_standby->append_conf('postgresql.conf', "primary_slot_name = slot");
	$node_standby->start;

	# Make sure the connection is made
	$node_primary->wait_for_catchup($node_standby, 'write');
=======
	$node_standby->start;

	# Make sure connection is made
	$node_primary->poll_query_until('postgres',
		'SELECT count(*) = 1 FROM pg_stat_replication');
	$node_primary->safe_psql('postgres', "SELECT pg_drop_replication_slot('slot')");

	$node_standby->safe_psql('postgres', 'CHECKPOINT');
>>>>>>> main

	# Do immediate shutdown just after a sequence of CREATE DATABASE / DROP
	# DATABASE / DROP TABLESPACE. This causes CREATE DATABASE WAL records
	# to be applied to already-removed directories.
	my $query = q[
<<<<<<< HEAD
		CREATE DATABASE dropme_db1 WITH TABLESPACE dropme_ts1 STRATEGY=<STRATEGY>;
		CREATE TABLE t (a int) TABLESPACE dropme_ts2;
		CREATE DATABASE dropme_db2 WITH TABLESPACE dropme_ts2 STRATEGY=<STRATEGY>;
		CREATE DATABASE moveme_db TABLESPACE source_ts STRATEGY=<STRATEGY>;
		ALTER DATABASE moveme_db SET TABLESPACE target_ts;
		CREATE DATABASE newdb TEMPLATE template_db STRATEGY=<STRATEGY>;
=======
		CREATE DATABASE dropme_db1 WITH TABLESPACE dropme_ts1;
		CREATE TABLE t (a int) TABLESPACE dropme_ts2;
		CREATE DATABASE dropme_db2 WITH TABLESPACE dropme_ts2;
		CREATE DATABASE moveme_db TABLESPACE source_ts;
		ALTER DATABASE moveme_db SET TABLESPACE target_ts;
		CREATE DATABASE newdb TEMPLATE template_db;
>>>>>>> main
		ALTER DATABASE template_db IS_TEMPLATE = false;
		DROP DATABASE dropme_db1;
		DROP TABLE t;
		DROP DATABASE dropme_db2; DROP TABLESPACE dropme_ts2;
		DROP TABLESPACE source_ts;
		DROP DATABASE template_db;
	];
<<<<<<< HEAD
	$query =~ s/<STRATEGY>/$strategy/g;

	$node_primary->safe_psql('postgres', $query);
	$node_primary->wait_for_catchup($node_standby, 'write');
=======

	$node_primary->safe_psql('postgres', $query);
	$node_primary->wait_for_catchup($node_standby, 'replay',
		$node_primary->lsn('write'));
>>>>>>> main

	# show "create missing directory" log message
	$node_standby->safe_psql('postgres',
		"ALTER SYSTEM SET log_min_messages TO debug1;");
	$node_standby->stop('immediate');
	# Should restart ignoring directory creation error.
<<<<<<< HEAD
	is($node_standby->start(fail_ok => 1),
		1, "standby node started for $strategy");
	$node_standby->stop('immediate');
}

test_tablespace("FILE_COPY");
test_tablespace("WAL_LOG");

# Ensure that a missing tablespace directory during create database
# replay immediately causes panic if the standby has already reached
# consistent state (archive recovery is in progress).  This is
# effective only for CREATE DATABASE WITH STRATEGY=FILE_COPY.
=======
	is($node_standby->start(fail_ok => 1), 1, "standby node started");
	$node_standby->stop('immediate');
}

test_tablespace();

# Ensure that a missing tablespace directory during create database
# replay immediately causes panic if the standby has already reached
# consistent state (archive recovery is in progress).
>>>>>>> main

my $node_primary = PostgreSQL::Test::Cluster->new('primary2');
$node_primary->init(allows_streaming => 1);
$node_primary->start;

# Create tablespace
$node_primary->safe_psql(
	'postgres', q[
		SET allow_in_place_tablespaces=on;
		CREATE TABLESPACE ts1 LOCATION ''
<<<<<<< HEAD
			]);
$node_primary->safe_psql('postgres',
	"CREATE DATABASE db1 WITH TABLESPACE ts1 STRATEGY=FILE_COPY");
=======
	]);
$node_primary->safe_psql('postgres',
	"CREATE DATABASE db1 WITH TABLESPACE ts1");
>>>>>>> main

# Take backup
my $backup_name = 'my_backup';
$node_primary->backup($backup_name);
my $node_standby = PostgreSQL::Test::Cluster->new('standby3');
$node_standby->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby->append_conf('postgresql.conf',
	"allow_in_place_tablespaces = on");
$node_standby->start;

# Make sure standby reached consistency and starts accepting connections
$node_standby->poll_query_until('postgres', 'SELECT 1', '1');

# Remove standby tablespace directory so it will be missing when
# replay resumes.
my $tspoid = $node_standby->safe_psql('postgres',
	"SELECT oid FROM pg_tablespace WHERE spcname = 'ts1';");
my $tspdir = $node_standby->data_dir . "/pg_tblspc/$tspoid";
File::Path::rmtree($tspdir);

<<<<<<< HEAD
my $logstart = -s $node_standby->logfile;
=======
my $logstart = get_log_size($node_standby);
>>>>>>> main

# Create a database in the tablespace and a table in default tablespace
$node_primary->safe_psql(
	'postgres',
	q[
		CREATE TABLE should_not_replay_insertion(a int);
<<<<<<< HEAD
		CREATE DATABASE db2 WITH TABLESPACE ts1 STRATEGY=FILE_COPY;
=======
		CREATE DATABASE db2 WITH TABLESPACE ts1;
>>>>>>> main
		INSERT INTO should_not_replay_insertion VALUES (1);
	]);

# Standby should fail and should not silently skip replaying the wal
# In this test, PANIC turns into WARNING by allow_in_place_tablespaces.
# Check the log messages instead of confirming standby failure.
<<<<<<< HEAD
my $max_attempts = $PostgreSQL::Test::Utils::timeout_default * 10;
=======
my $max_attempts = $PostgreSQL::Test::Utils::timeout_default;
>>>>>>> main
while ($max_attempts-- >= 0)
{
	last
	  if (
<<<<<<< HEAD
		$node_standby->log_contains(
			qr!WARNING: ( [A-Z0-9]+:)? creating missing directory: pg_tblspc/!,
			$logstart));
	usleep(100_000);
=======
		find_in_log(
			$node_standby, "WARNING:  creating missing directory: pg_tblspc/",
			$logstart));
	sleep 1;
>>>>>>> main
}
ok($max_attempts > 0, "invalid directory creation is detected");

done_testing();
<<<<<<< HEAD
=======


# return the size of logfile of $node in bytes
sub get_log_size
{
	my ($node) = @_;

	return (stat $node->logfile)[7];
}

# find $pat in logfile of $node after $off-th byte
sub find_in_log
{
	my ($node, $pat, $off) = @_;

	$off = 0 unless defined $off;
	my $log = PostgreSQL::Test::Utils::slurp_file($node->logfile);
	return 0 if (length($log) <= $off);

	$log = substr($log, $off);

	return $log =~ m/$pat/;
}
>>>>>>> main

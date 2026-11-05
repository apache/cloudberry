# Copyright (c) 2026, PostgreSQL Global Development Group

# Test that vacuum statistics entries do not outlive the relations they
# describe: DROP TABLE, a rolled back DROP, a restart, DROP DATABASE, and a
# new relation that gets the OID of an old one.
#
# Entries are looked up with pg_stats_get_vacuum_tables() and
# pg_stats_get_vacuum_indexes(), which find an entry by OID even when the
# relation is no longer in pg_class.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('vacstat_gc');
$node->init;
$node->append_conf(
	'postgresql.conf', q{
shared_preload_libraries = 'ext_vacuum_statistics'
autovacuum = off
});
$node->start;

$node->safe_psql('postgres', 'CREATE EXTENSION ext_vacuum_statistics');

# SQL that counts the statistics entries of the given tables and indexes.
sub entries_sql
{
	my ($dboid, $tables, $indexes) = @_;
	my $t = join(',', @$tables);
	my $i = join(',', @$indexes);

	return qq{
SELECT (SELECT count(*)
          FROM unnest('{$t}'::oid[]) AS r,
               ext_vacuum_statistics.pg_stats_get_vacuum_tables($dboid, r)) +
       (SELECT count(*)
          FROM unnest('{$i}'::oid[]) AS r,
               ext_vacuum_statistics.pg_stats_get_vacuum_indexes($dboid, r))};
}

sub count_entries
{
	return $node->safe_psql('postgres', entries_sql(@_));
}

# Wait until the entries are gone.
sub wait_for_no_entries
{
	my ($what, @args) = @_;

	$node->poll_query_until('postgres',
		'SELECT (' . entries_sql(@args) . ') = 0')
	  or die "timed out waiting for $what";
	return;
}

# OIDs of the relations of the given kind whose names match a pattern.
sub relids
{
	my ($dbname, $relkind, $pattern) = @_;

	return split /\n/,
	  $node->safe_psql($dbname,
		"SELECT oid FROM pg_class WHERE relkind = '$relkind' AND relname LIKE '$pattern' ORDER BY oid"
	  );
}

my $dboid = $node->safe_psql('postgres',
	"SELECT oid FROM pg_database WHERE datname = 'postgres'");

# Vacuum a handful of tables, each with an index.
my @tables = map { "gc_tab$_" } (1 .. 20);

$node->safe_psql('postgres',
	join('', map { qq[
CREATE TABLE $_ (id int);
CREATE INDEX ${_}_idx ON $_ (id);
INSERT INTO $_ SELECT generate_series(1, 100);
DELETE FROM $_;] } @tables));
$node->safe_psql('postgres', 'VACUUM ' . join(', ', @tables));

my @tabids = relids('postgres', 'r', 'gc_tab%');
my @idxids = relids('postgres', 'i', 'gc_tab%');
is(scalar(@tabids) + scalar(@idxids), 40, 'found the tables and indexes');
is(count_entries($dboid, \@tabids, \@idxids),
	40, 'vacuum created an entry for each table and index');

# A rolled back DROP must keep the statistics.
$node->safe_psql('postgres', 'BEGIN; DROP TABLE gc_tab1; ROLLBACK;');
is(count_entries($dboid, \@tabids, \@idxids),
	40, 'rolled back DROP TABLE kept the statistics');

# Dropping the tables drops their entries and those of their indexes.
$node->safe_psql('postgres', 'DROP TABLE ' . join(', ', @tables));
wait_for_no_entries('DROP TABLE to drop the statistics',
	$dboid, \@tabids, \@idxids);
is(count_entries($dboid, \@tabids, \@idxids),
	0, 'DROP TABLE dropped the statistics entries');

# Nothing may come back from the statistics file either.
$node->restart;
is(count_entries($dboid, \@tabids, \@idxids),
	0, 'no dropped entries came back after a restart');

# The same for a whole database.
$node->safe_psql('postgres', 'CREATE DATABASE vacstat_gc_db');
$node->safe_psql(
	'vacstat_gc_db', q{
CREATE TABLE gc_dbtab (id int);
CREATE INDEX gc_dbtab_idx ON gc_dbtab (id);
INSERT INTO gc_dbtab SELECT generate_series(1, 100);
DELETE FROM gc_dbtab;
VACUUM gc_dbtab;
});
my $otherdb = $node->safe_psql('postgres',
	"SELECT oid FROM pg_database WHERE datname = 'vacstat_gc_db'");
my @dbtabids = relids('vacstat_gc_db', 'r', 'gc_dbtab');
my @dbidxids = relids('vacstat_gc_db', 'i', 'gc_dbtab_idx');
is(count_entries($otherdb, \@dbtabids, \@dbidxids),
	2, 'vacuum in another database created statistics entries');

$node->safe_psql('postgres', 'DROP DATABASE vacstat_gc_db');
wait_for_no_entries('DROP DATABASE to drop the statistics',
	$otherdb, \@dbtabids, \@dbidxids);
is(count_entries($otherdb, \@dbtabids, \@dbidxids),
	0, 'DROP DATABASE dropped the statistics entries');

# A relation created on the OID of an earlier one must not inherit its
# statistics.  DROP never leaves an entry behind, and OIDs are only handed
# out again after a wraparound, so fake the situation: take a vacuumed table
# out of the catalogs behind the back of DROP, which leaves its entry
# orphaned, then create a new table with the very same OIDs in binary upgrade
# mode.
$node->safe_psql(
	'postgres', q{
CREATE TABLE gc_reuse (id int);
INSERT INTO gc_reuse SELECT generate_series(1, 100);
DELETE FROM gc_reuse;
VACUUM gc_reuse;
});
my ($relid, $reltype, $typarray) = split /\|/,
  $node->safe_psql(
	'postgres', q{
SELECT c.oid, c.reltype, t.typarray
  FROM pg_class c JOIN pg_type t ON t.oid = c.reltype
 WHERE c.relname = 'gc_reuse'});
is( $node->safe_psql(
		'postgres',
		"SELECT tuples_deleted FROM ext_vacuum_statistics.pg_stats_vacuum_tables WHERE relid = $relid"
	),
	'100',
	'the table whose OID gets recycled has statistics');

$node->safe_psql(
	'postgres', qq{
SET allow_system_table_mods = on;
DELETE FROM pg_depend
 WHERE (classid = 'pg_class'::regclass AND objid = $relid)
    OR (refclassid = 'pg_class'::regclass AND refobjid = $relid)
    OR (classid = 'pg_type'::regclass AND objid IN ($reltype, $typarray))
    OR (refclassid = 'pg_type'::regclass AND refobjid IN ($reltype, $typarray));
DELETE FROM pg_attribute WHERE attrelid = $relid;
DELETE FROM pg_type WHERE oid IN ($reltype, $typarray);
DELETE FROM pg_class WHERE oid = $relid;
});

# Binary upgrade mode is a postmaster switch, so start the node by hand.
# Cloudberry: a standalone node also needs the options that
# PostgreSQL::Test::Cluster::start() passes to it.
$node->stop;
command_ok(
	[
		'pg_ctl',
		'--pgdata' => $node->data_dir,
		'--log' => $node->logfile,
		'--options' => "-b -c gp_role=utility --gp_dbid=$node->{_dbid} "
		  . '--gp_contentid=0 -c maintenance_mode=on',
		'--wait', 'start'
	],
	'started in binary upgrade mode');

# The relfilenumber only has to be unused; the old file is still on disk.
# Cloudberry: the OIDs are preassigned for a given namespace and name.
my ($ret, $stdout, $stderr) = $node->psql(
	'postgres', qq{
SELECT binary_upgrade_set_next_heap_pg_class_oid('$relid'::oid,
         'public'::regnamespace, 'gc_reuse_new');
SELECT binary_upgrade_set_next_heap_relfilenode('3000000000'::oid);
SELECT binary_upgrade_set_next_pg_type_oid('$reltype'::oid,
         'public'::regnamespace, 'gc_reuse_new');
SELECT binary_upgrade_set_next_array_pg_type_oid('$typarray'::oid,
         'public'::regnamespace, '_gc_reuse_new');
CREATE TABLE gc_reuse_new (id int);
});
is($ret, 0, 'created a table on the recycled OID');
like(
	$stderr,
	qr/resetting existing statistics for kind ext_vacuum_statistics_relation/,
	'creating the table reset the orphaned statistics entry');

command_ok(
	[
		'pg_ctl',
		'--pgdata' => $node->data_dir,
		'--mode' => 'fast',
		'--wait', 'stop'
	],
	'stopped binary upgrade mode');
$node->start;

is( $node->safe_psql(
		'postgres', "SELECT oid FROM pg_class WHERE relname = 'gc_reuse_new'"),
	$relid,
	'the new table got the recycled OID');
is( $node->safe_psql(
		'postgres', q{
SELECT coalesce((SELECT tuples_deleted
                   FROM ext_vacuum_statistics.pg_stats_vacuum_tables
                  WHERE relname = 'gc_reuse_new'), 0)}),
	'0',
	'the new table did not inherit the statistics of the old one');

$node->stop;

done_testing();

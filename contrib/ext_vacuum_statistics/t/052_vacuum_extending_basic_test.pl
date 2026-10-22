# Copyright (c) 2025 PostgreSQL Global Development Group
# Test cumulative vacuum stats system using TAP
#
# This test validates the accuracy and behavior of cumulative vacuum statistics
# across heap tables, indexes, and databases using:
#
#   • ext_vacuum_statistics.pg_stats_vacuum_tables
#   • ext_vacuum_statistics.pg_stats_vacuum_indexes
#   • ext_vacuum_statistics.pg_stats_vacuum_database
#
# A polling helper function repeatedly checks the stats views until expected
# deltas appear or a configurable timeout expires. This guarantees that
# stats-collector propagation delays do not lead to flaky test behavior.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

#------------------------------------------------------------------------------
# Test harness setup
#------------------------------------------------------------------------------

my $node = PostgreSQL::Test::Cluster->new('stat_vacuum');
$node->init;

# Configure the server: preload extension and logging level
$node->append_conf('postgresql.conf', q{
    shared_preload_libraries = 'ext_vacuum_statistics'
    log_min_messages = notice
});

my $stderr;
my $base_stats;
my $wals;
my $ibase_stats;
my $iwals;

$node->start(
    '>' => \$base_stats,
	'2>' => \$stderr
);

#------------------------------------------------------------------------------
# Database creation and initialization
#------------------------------------------------------------------------------

$node->safe_psql('postgres', q{
    CREATE DATABASE statistic_vacuum_database_regression;
    CREATE EXTENSION ext_vacuum_statistics;
});
# Main test database name and number of rows to insert
my $dbname   = 'statistic_vacuum_database_regression';
# The scenarios below expect the table and its index to span several pages
# of the default 8kB size; scale the row count with the block size, so that
# builds with larger blocks (32kB in Cloudberry) keep the same page counts.
my $size_tab = 1000 *
  ($node->safe_psql('postgres', 'SHOW block_size') / 8192);

# Enable required session settings and force the stats collector to flush next
$node->safe_psql($dbname, q{
    SET track_functions = 'all';
    SELECT pg_stat_force_next_flush();
});

#------------------------------------------------------------------------------
# Create test table and populate it
#------------------------------------------------------------------------------

$node->safe_psql(
    $dbname,
    "CREATE EXTENSION ext_vacuum_statistics;
     CREATE TABLE vestat (x int PRIMARY KEY)
         WITH (autovacuum_enabled = off, fillfactor = 10);
     INSERT INTO vestat SELECT x FROM generate_series(1, $size_tab) AS g(x);
     ANALYZE vestat;"
);

#------------------------------------------------------------------------------
# Timing parameters for polling loops
#------------------------------------------------------------------------------

my $timeout    = 30;     # overall wait timeout in seconds
my $interval   = 0.015;  # poll interval in seconds (15 ms)
my $start_time = time();
my $updated    = 0;

#------------------------------------------------------------------------------
# wait_for_vacuum_stats
#
# Polls ext_vacuum_statistics.pg_stats_vacuum_tables and ext_vacuum_statistics.pg_stats_vacuum_indexes until both the
# table-level and index-level counters exceed the provided baselines, or until
# the configured timeout elapses.
#
# Expected named args (baseline values):
#   tab_tuples_deleted
#   tab_wal_records
#   idx_tuples_deleted
#   idx_wal_records
#
# Returns: 1 if the condition is met before timeout, 0 otherwise.
#------------------------------------------------------------------------------

sub wait_for_vacuum_stats {
    my (%args) = @_;
    my $tab_tuples_deleted = ($args{tab_tuples_deleted} or 0);
    my $tab_wal_records    = ($args{tab_wal_records} or 0);
    my $idx_tuples_deleted = ($args{idx_tuples_deleted} or 0);
    my $idx_wal_records    = ($args{idx_wal_records} or 0);

    my $start = time();
    while ((time() - $start) < $timeout) {

        my $result_query = $node->safe_psql(
            $dbname,
            "VACUUM vestat;
             SELECT
                (SELECT (tuples_deleted > $tab_tuples_deleted AND wal_records > $tab_wal_records)
                  FROM ext_vacuum_statistics.pg_stats_vacuum_tables
                  WHERE relname = 'vestat')
                AND
                (SELECT (tuples_deleted > $idx_tuples_deleted AND wal_records > $idx_wal_records)
                  FROM ext_vacuum_statistics.pg_stats_vacuum_indexes
                  WHERE indexrelname = 'vestat_pkey');"
        );

        return 1 if ($result_query eq 't');

        sleep($interval);
    }

    return 0;
}

#------------------------------------------------------------------------------
# Variables to hold vacuum-stat snapshots for later comparisons
#------------------------------------------------------------------------------

my $tuples_deleted = 0;
my $pages_scanned = 0;
my $pages_removed = 0;
my $wal_records = 0;
my $wal_bytes = 0;
my $wal_fpi = 0;

my $index_tuples_deleted = 0;
my $index_pages_deleted = 0;
my $index_wal_records = 0;
my $index_wal_bytes = 0;
my $index_wal_fpi = 0;

my $tuples_deleted_prev = 0;
my $pages_scanned_prev = 0;
my $pages_removed_prev = 0;
my $wal_records_prev = 0;
my $wal_bytes_prev = 0;
my $wal_fpi_prev = 0;

my $index_tuples_deleted_prev = 0;
my $index_pages_deleted_prev = 0;
my $index_wal_records_prev = 0;
my $index_wal_bytes_prev = 0;
my $index_wal_fpi_prev = 0;

#------------------------------------------------------------------------------
# fetch_vacuum_stats
#
# Reads current values of relevant vacuum counters for the test table and its
# primary index, storing them in package variables for subsequent comparisons.
#------------------------------------------------------------------------------

sub fetch_vacuum_stats {
    # fetch actual base vacuum statistics
    my $base_statistics = $node->safe_psql(
        $dbname,
        "SELECT tuples_deleted, pages_scanned, pages_removed, wal_records, wal_bytes, wal_fpi
           FROM ext_vacuum_statistics.pg_stats_vacuum_tables
          WHERE relname = 'vestat';"
    );

    $base_statistics =~ s/\s*\|\s*/ /g;   # transform " | " into space
    ($tuples_deleted, $pages_scanned, $pages_removed, $wal_records, $wal_bytes, $wal_fpi)
        = split /\s+/, $base_statistics;

    # --- index stats ---
    my $index_base_statistics = $node->safe_psql(
        $dbname,
        "SELECT tuples_deleted, pages_deleted, wal_records, wal_bytes, wal_fpi
           FROM ext_vacuum_statistics.pg_stats_vacuum_indexes
          WHERE indexrelname = 'vestat_pkey';"
    );

    $index_base_statistics =~ s/\s*\|\s*/ /g;   # transform " | " into space
    ($index_tuples_deleted, $index_pages_deleted, $index_wal_records, $index_wal_bytes, $index_wal_fpi)
        = split /\s+/, $index_base_statistics;
}

#------------------------------------------------------------------------------
# save_vacuum_stats
#
# Save current values (previously fetched by fetch_vacuum_stats) so that we
# later fetch new values and compare them.
#------------------------------------------------------------------------------
sub save_vacuum_stats {
    $tuples_deleted_prev = $tuples_deleted;
    $pages_scanned_prev = $pages_scanned;
    $pages_removed_prev = $pages_removed;
    $wal_records_prev = $wal_records;
    $wal_bytes_prev = $wal_bytes;
    $wal_fpi_prev = $wal_fpi;

    $index_tuples_deleted_prev = $index_tuples_deleted;
    $index_pages_deleted_prev = $index_pages_deleted;
    $index_wal_records_prev = $index_wal_records;
    $index_wal_bytes_prev = $index_wal_bytes;
    $index_wal_fpi_prev = $index_wal_fpi;
}

#------------------------------------------------------------------------------
# print_vacuum_stats_on_error
#
# Print values in case of an error
#------------------------------------------------------------------------------
sub print_vacuum_stats_on_error {
    diag(
            "Statistics in the failed test\n" .
            "Table statistics:\n" .
            "  Before test:\n" .
            "    tuples_deleted    = $tuples_deleted_prev\n" .
            "    pages_scanned     = $pages_scanned_prev\n" .
            "    pages_removed     = $pages_removed_prev\n" .
            "    wal_records       = $wal_records_prev\n" .
            "    wal_bytes         = $wal_bytes_prev\n" .
            "    wal_fpi           = $wal_fpi_prev\n" .
            "  After test:\n" .
            "    tuples_deleted    = $tuples_deleted\n" .
            "    pages_scanned     = $pages_scanned\n" .
            "    pages_removed     = $pages_removed\n" .
            "    wal_records       = $wal_records\n" .
            "    wal_bytes         = $wal_bytes\n" .
            "    wal_fpi           = $wal_fpi\n" .
            "Index statistics:\n" .
            "   Before test:\n" .
            "    tuples_deleted    = $index_tuples_deleted_prev\n" .
            "    pages_deleted     = $index_pages_deleted_prev\n" .
            "    wal_records       = $index_wal_records_prev\n" .
            "    wal_bytes         = $index_wal_bytes_prev\n" .
            "    wal_fpi           = $index_wal_fpi_prev\n" .
            "  After test:\n" .
            "    tuples_deleted    = $index_tuples_deleted\n" .
            "    pages_deleted     = $index_pages_deleted\n" .
            "    wal_records       = $index_wal_records\n" .
            "    wal_bytes         = $index_wal_bytes\n" .
            "    wal_fpi           = $index_wal_fpi\n"
    );
};



#------------------------------------------------------------------------------
# Test 1: Delete half the rows, run VACUUM, and wait for stats to advance
#------------------------------------------------------------------------------
subtest 'Test 1: Delete half the rows, run VACUUM' => sub
{

$node->safe_psql($dbname, "DELETE FROM vestat WHERE x % 2 = 0;");
$node->safe_psql($dbname, "VACUUM vestat;");

# Poll the stats view until expected deltas appear or timeout
$updated = wait_for_vacuum_stats(
    tab_tuples_deleted => 0,
    tab_wal_records => 0,
    idx_tuples_deleted => 0,
    idx_wal_records => 0,
);
ok($updated, 'vacuum stats updated after vacuuming half-deleted table (tuples_deleted and wal_fpi advanced)')
  or diag "Timeout waiting for ext_vacuum_statistics update after $timeout seconds after vacuuming half-deleted table";

fetch_vacuum_stats();

ok($tuples_deleted > $tuples_deleted_prev, 'table tuples_deleted has increased');
ok($pages_scanned > $pages_scanned_prev, 'table pages_scanned has increased');
ok($pages_removed == $pages_removed_prev, 'table pages_removed stay the same');
ok($wal_records > $wal_records_prev, 'table wal_records has increased');
ok($wal_bytes > $wal_bytes_prev, 'table wal_bytes has increased');
ok($wal_fpi > $wal_fpi_prev, 'table wal_fpi has increased');

ok($index_pages_deleted == $index_pages_deleted_prev, 'index pages_deleted stay the same');
ok($index_tuples_deleted > $index_tuples_deleted_prev, 'index tuples_deleted has increased');
ok($index_wal_records > $index_wal_records_prev, 'index wal_records has increased');
ok($index_wal_bytes > $index_wal_bytes_prev, 'index wal_bytes has increased');
ok($index_wal_fpi == $index_wal_fpi_prev, 'index wal_fpi stay the same');

} or print_vacuum_stats_on_error();

#------------------------------------------------------------------------------
# Test 2: Delete all rows, run VACUUM, and wait for stats to advance
#------------------------------------------------------------------------------
subtest 'Test 2: Delete all rows, run VACUUM' => sub
{
save_vacuum_stats();

$node->safe_psql($dbname, "DELETE FROM vestat;");
$node->safe_psql($dbname, "VACUUM vestat;");

$updated = wait_for_vacuum_stats(
    tab_tuples_deleted => $tuples_deleted_prev,
    tab_wal_records => $wal_records_prev,
    idx_tuples_deleted => $index_tuples_deleted_prev,
    idx_wal_records => $index_wal_records_prev,
);

ok($updated, 'vacuum stats updated after vacuuming all-deleted table (tuples_deleted and wal_records advanced)')
  or diag "Timeout waiting for ext_vacuum_statistics update after $timeout seconds after vacuuming all-deleted table";

fetch_vacuum_stats();

ok($tuples_deleted > $tuples_deleted_prev, 'table tuples_deleted has increased');
ok($pages_scanned > $pages_scanned_prev, 'table pages_scanned has increased');
ok($pages_removed > $pages_removed_prev, 'table pages_removed has increased');
ok($wal_records > $wal_records_prev, 'table wal_records has increased');
ok($wal_bytes > $wal_bytes_prev, 'table wal_bytes has increased');
ok($wal_fpi > 0, 'table wal_fpi has increased');

ok($index_pages_deleted > $index_pages_deleted_prev, 'index pages_deleted has increased');
ok($index_tuples_deleted > $index_tuples_deleted_prev, 'index tuples_deleted has increased');
ok($index_wal_records > $index_wal_records_prev, 'index wal_records has increased');
ok($index_wal_bytes > $index_wal_bytes_prev, 'index wal_bytes has increased');
ok($index_wal_fpi == $index_wal_fpi_prev, 'index wal_fpi stay the same');

} or print_vacuum_stats_on_error();

#------------------------------------------------------------------------------
# Test 3: Test VACUUM FULL — it should not report to the stats collector
#------------------------------------------------------------------------------
subtest 'Test 3: Test VACUUM FULL — it should not report to the stats collector' => sub
{
save_vacuum_stats();

$node->safe_psql(
    $dbname,
    "INSERT INTO vestat SELECT x FROM generate_series(1, $size_tab) AS g(x);
     CHECKPOINT;
     DELETE FROM vestat;
     VACUUM FULL vestat;"
);

fetch_vacuum_stats();

ok($tuples_deleted == $tuples_deleted_prev, 'table tuples_deleted stay the same');
ok($pages_scanned == $pages_scanned_prev, 'table pages_scanned stay the same');
ok($pages_removed == $pages_removed_prev, 'table pages_removed stay the same');
ok($wal_records == $wal_records_prev, 'table wal_records stay the same');
ok($wal_bytes == $wal_bytes_prev, 'table wal_bytes stay the same');
ok($wal_fpi == $wal_fpi_prev, 'table wal_fpi stay the same');

ok($index_pages_deleted == $index_pages_deleted_prev, 'index pages_deleted stay the same');
ok($index_tuples_deleted == $index_tuples_deleted_prev, 'index tuples_deleted stay the same');
ok($index_wal_records == $index_wal_records_prev, 'index wal_records stay the same');
ok($index_wal_bytes == $index_wal_bytes_prev, 'index wal_bytes stay the same');
ok($index_wal_fpi == $index_wal_fpi_prev, 'index wal_fpi stay the same');

} or print_vacuum_stats_on_error();

$node->stop;

done_testing();

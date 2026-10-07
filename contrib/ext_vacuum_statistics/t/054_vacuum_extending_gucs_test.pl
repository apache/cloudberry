# Copyright (c) 2025 PostgreSQL Global Development Group
#
# Test GUC parameters for ext_vacuum_statistics extension:
#   vacuum_statistics.enabled
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
 
use Test::More;

#------------------------------------------------------------------------------
# Test cluster setup
#------------------------------------------------------------------------------

my $node = PostgreSQL::Test::Cluster->new('ext_stat_vacuum_gucs');
$node->init;

$node->append_conf('postgresql.conf', q{
    shared_preload_libraries = 'ext_vacuum_statistics'
    log_min_messages = notice
});

$node->start;

#------------------------------------------------------------------------------
# Database creation and initialization
#------------------------------------------------------------------------------

$node->safe_psql('postgres', q{
    CREATE DATABASE statistic_vacuum_gucs;
});

my $dbname = 'statistic_vacuum_gucs';

$node->safe_psql($dbname, q{
    CREATE EXTENSION ext_vacuum_statistics;
    CREATE TABLE guc_test (x int PRIMARY KEY)
        WITH (autovacuum_enabled = off);
    INSERT INTO guc_test SELECT x FROM generate_series(1, 100) AS g(x);
    ANALYZE guc_test;
});

#------------------------------------------------------------------------------
# Reset stats and run vacuum (all in one session so GUCs persist)
#------------------------------------------------------------------------------

sub reset_and_vacuum {
    my ($db, $table, $opts) = @_;
    $table ||= 'guc_test';
    my $gucs = $opts && $opts->{gucs} ? $opts->{gucs} : [];
    my $modify = $opts && $opts->{modify};
    my $extra = $opts && $opts->{extra_vacuum} ? $opts->{extra_vacuum} : [];
    $extra = [$extra] unless ref $extra eq 'ARRAY';
    my $sql = join("\n", (map { "SET $_;" } @$gucs),
        "SELECT ext_vacuum_statistics.vacuum_statistics_reset();",
        $modify ? (
            "TRUNCATE $table;",
            "INSERT INTO $table SELECT x FROM generate_series(1, 100) AS g(x);",
            "DELETE FROM $table;",
        ) : (),
        "VACUUM $table;",
        (map { "VACUUM $_;" } @$extra),
        # Make pending stats visible to subsequent sessions without sleeping.
        "SELECT pg_stat_force_next_flush();");
    $node->safe_psql($db, $sql);
}

#------------------------------------------------------------------------------
# Test 1: vacuum_statistics.enabled
#------------------------------------------------------------------------------
subtest 'vacuum_statistics.enabled' => sub {
    reset_and_vacuum($dbname);

    # Default: enabled - should have stats
    my $count = $node->safe_psql($dbname,
        "SELECT COUNT(*) FROM ext_vacuum_statistics.pg_stats_vacuum_tables WHERE relname = 'guc_test'");
    ok($count > 0, 'stats collected when enabled');

    # Reset preserves existing rows. With collection disabled, their counters
    # must stay zero even after another vacuum.
    reset_and_vacuum($dbname, 'guc_test', { gucs => ['vacuum_statistics.enabled = off'] });

    $count = $node->safe_psql($dbname,
        "SELECT COUNT(*) FROM ext_vacuum_statistics.pg_stats_vacuum_tables WHERE relname = 'guc_test'");
    is($count, 1, 'reset preserves the row when collection is disabled');

    my $sums = $node->safe_psql($dbname, q{
        SELECT COALESCE(SUM(tuples_deleted), 0)
             + COALESCE(SUM(pages_scanned), 0)
          FROM ext_vacuum_statistics.pg_stats_vacuum_tables
         WHERE relname = 'guc_test'
    });
    is($sums, '0', 'no counters accumulated when disabled');
};

$node->stop;

done_testing();

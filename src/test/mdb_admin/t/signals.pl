
# Copyright (c) 2024-2024, MDB, Mother Russia

# Minimal test testing streaming replication
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init();
$node_primary->start;

# Create some content on primary and check its presence in standby nodes
$node_primary->safe_psql('postgres',
	"
    CREATE DATABASE regress;
    CREATE ROLE mdb_admin;
    CREATE ROLE mdb_reg_lh_1;
    CREATE ROLE mdb_reg_lh_2;
    GRANT pg_signal_backend TO mdb_admin;
    GRANT pg_signal_backend TO mdb_reg_lh_1;
    GRANT mdb_admin TO mdb_reg_lh_2;
");

# Create some content on primary and check its presence in standby nodes
$node_primary->safe_psql('regress',
	"
    CREATE TABLE tab_int(i int);
    INSERT INTO tab_int SELECT * FROm generate_series(1, 1000000);
    ALTER SYSTEM SET autovacuum_vacuum_cost_limit TO 1;
    ALTER SYSTEM SET autovacuum_vacuum_cost_delay TO 100;
    ALTER SYSTEM SET autovacuum_naptime TO 1;    
");

$node_primary->restart;

sleep 1;

my $res_pid = $node_primary->safe_psql('regress',
	"
    SELECT pid FROM pg_stat_activity WHERE backend_type = 'autovacuum worker' and datname = 'regress';;
");


print "pid is $res_pid\n";

ok(1);


my ($res_reg_lh_1, $stdout_reg_lh_1, $stderr_reg_lh_1)  = $node_primary->psql('regress',
	"
    SET ROLE mdb_reg_lh_1;
    SELECT pg_terminate_backend($res_pid);
");

# print ($res_reg_lh_1, $stdout_reg_lh_1, $stderr_reg_lh_1, "\n");

ok($res_reg_lh_1 != 0, "should fail for non-mdb_admin");
like($stderr_reg_lh_1, qr/ERROR:  must be a superuser to terminate superuser process/, "matches");

my ($res_reg_lh_2, $stdout_reg_lh_2, $stderr_reg_lh_2) = $node_primary->psql('regress',
	"
    SET ROLE mdb_reg_lh_2;
    SELECT pg_terminate_backend($res_pid);
");

ok($res_reg_lh_2 == 0, "should success for mdb_admin");

# print ($res_reg_lh_2, $stdout_reg_lh_2, $stderr_reg_lh_2, "\n");

done_testing();
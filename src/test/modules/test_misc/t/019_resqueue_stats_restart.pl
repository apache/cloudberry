# Copyright (c) 2026, PostgreSQL Global Development Group

# Resource queue statistics must survive a clean restart. Queue admission
# needs a dispatcher, so use a coordinator and one primary segment rather
# than the standalone utility-mode instances normally used by TAP.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

plan skip_all => 'temporary cluster requires internal FTS'
  unless check_pg_config('#define USE_INTERNAL_FTS 1');

my $node = PostgreSQL::Test::Cluster->new('resqueue_coordinator');
my $segment = PostgreSQL::Test::Cluster->new('resqueue_segment');
for my $instance ($node, $segment)
{
    $instance->init;
    $instance->append_conf('postgresql.conf', q{
autovacuum = off
gp_resource_manager = 'queue'
listen_addresses = '127.0.0.1'
});
}

sub start_instance
{
    my ($instance, $role, $content) = @_;
    command_ok(
        ['pg_ctl', '-D', $instance->data_dir, '-l', $instance->logfile,
         '-o', "-c gp_role=$role --gp_dbid=$instance->{_dbid} --gp_contentid=$content",
         '-w', 'start'],
        "started content $content in $role mode");
    $instance->_update_pid(1);
}

# Populate the cluster configuration in utility mode before starting the
# dispatcher, as gpinitsystem does.
start_instance($node, 'utility', -1);
for my $entry ([$node, -1], [$segment, 0])
{
    my ($instance, $content) = @$entry;
    my $port = $instance->port;
    my $datadir = $instance->data_dir;
    $node->safe_psql('postgres', qq{
SELECT gp_add_segment($instance->{_dbid}::int2, ${content}::int2,
                      'p', 'p', 's', 'u', $port,
                      'localhost', '127.0.0.1', '$datadir');
});
}
$node->stop;
start_instance($segment, 'execute', 0);
start_instance($node, 'dispatch', -1);

# Explicit connection options override the utility-mode default of psql().
my $connstr = $node->connstr('postgres') . " options=''";
$node->safe_psql('postgres', q{
CREATE RESOURCE QUEUE restart_queue WITH (active_statements = 1);
CREATE ROLE restart_user RESOURCE QUEUE restart_queue;
SET ROLE restart_user;
SELECT 1;
RESET ROLE;
}, connstr => $connstr);
my $stats_sql = q{
SELECT queries_submitted, queries_admitted, queries_completed
  FROM pg_stat_resqueues WHERE queuename = 'restart_queue'};
my $before = $node->safe_psql('postgres', $stats_sql, connstr => $connstr);
like($before, qr/^[1-9][0-9]*[|][1-9][0-9]*[|][1-9][0-9]*$/,
     'query populated the resource queue statistics');
$node->restart;
is($node->safe_psql('postgres', $stats_sql, connstr => $connstr), $before,
   'resource queue statistics survive a clean restart');
$node->stop;
$segment->stop;
done_testing();

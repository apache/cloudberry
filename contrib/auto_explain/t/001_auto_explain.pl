
# Copyright (c) 2021-2023, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Runs the specified query and returns the emitted server log.
# params is an optional hash mapping GUC names to values;
# any such settings are transmitted to the backend via PGOPTIONS.
sub query_log
{
	my ($node, $sql, $params) = @_;
	$params ||= {};

	local $ENV{PGOPTIONS} = join " ",
	  map { "-c $_=$params->{$_}" } keys %$params;

	my $log = $node->logfile();
	my $offset = -s $log;

	$node->safe_psql("postgres", $sql);

	return slurp_file($log, $offset);
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init('auth_extra' => [ '--create-role', 'regress_user1' ]);
$node->append_conf('postgresql.conf',
	"session_preload_libraries = 'auto_explain'");
$node->append_conf('postgresql.conf', "auto_explain.log_min_duration = 0");
$node->append_conf('postgresql.conf', "auto_explain.log_analyze = on");
$node->start;

# Simple query.
my $log_contents = query_log($node, "SELECT * FROM pg_class;");

unlike(
	$log_contents,
	qr/Query Parameters:/,
	"no query parameters logged when none, text mode");


done_testing();

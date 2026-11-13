# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# Portions Copyright (c) 2021-2023, PostgreSQL Global Development Group

use strict;
use warnings;

use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Data::Dumper;

# Do nothing unless Makefile has told us that the build is --with-readline.
if (!defined($ENV{with_readline}) || $ENV{with_readline} ne 'yes')
{
	plan skip_all => 'readline is not supported by this build';
}

# Also, skip if user has set environment variable to command that.
# This is mainly intended to allow working around some of the more broken
# versions of libedit --- some users might find them acceptable even if
# they won't pass these tests.
if (defined($ENV{SKIP_READLINE_TESTS}))
{
	plan skip_all => 'SKIP_READLINE_TESTS is set';
}

# If we don't have IO::Pty, forget it, because IPC::Run depends on that
# to support pty connections
eval { require IO::Pty; };
if ($@)
{
	plan skip_all => 'IO::Pty is needed to run this test';
}

my $node = PostgreSQL::Test::Cluster->new('main');
$node->init;
$node->start;
$node->safe_psql('postgres', q{
CREATE STORAGE SERVER completion_storage;
CREATE STORAGE SERVER "Completion Storage";
CREATE TAG completion_tag ALLOWED_VALUES 'initial';
CREATE PROFILE completion_profile;
CREATE FOREIGN DATA WRAPPER completion_fdw;
CREATE SERVER completion_foreign FOREIGN DATA WRAPPER completion_fdw;
});
my $historyfile = "${PostgreSQL::Test::Utils::log_path}/011_psql_history.txt";
my $h = $node->interactive_psql('postgres', history_file => $historyfile);

# Simple test case: type something and see if psql responds as expected
sub check_completion
{
	my ($send, $pattern, $annotation) = @_;

	# report test failures from caller location
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	# restart per-command timer
	$h->{timeout}->start($PostgreSQL::Test::Utils::timeout_default);

	# send the data to be sent and wait for its result
	my $out = $h->query_until($pattern, $send);
	my $okay = ($out =~ $pattern && !$h->{timeout}->is_expired);
	ok($okay, $annotation);
	# for debugging, log actual output if it didn't match
	local $Data::Dumper::Terse = 1;
	local $Data::Dumper::Useqq = 1;
	diag 'Actual output was ' . Dumper($out) . "Did not match \"$pattern\"\n"
	  if !$okay;
	return $out;
}

# Clear query buffer to start over
# (won't work if we are inside a string literal!)
sub clear_query
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	check_completion("\\r\n", qr/Query buffer reset.*postgres=# /s,
		"\\r works");
	return;
}

# Clear current line to start over
# (this will work in an incomplete string literal, but it's less desirable
# than clear_query because we lose evidence in the history file)
sub clear_line
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	check_completion("\025\n", qr/postgres=# /, "control-U works");
	return;
}

# Cloudberry object-name queries must follow the current completion query
# contract.  Check storage-server rules independently of foreign servers,
# including optional clauses in DROP and storage user mapping commands.
my $completion_log_start = length(slurp_file($node->logfile));
for my $command (
	'ALTER STORAGE SERVER',
	'DROP STORAGE SERVER',
	'DROP STORAGE SERVER IF EXISTS',
	'COMMENT ON STORAGE SERVER',
	'CREATE STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER',
	'CREATE STORAGE USER MAPPING IF NOT EXISTS FOR CURRENT_USER STORAGE SERVER',
	'ALTER STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER',
	'DROP STORAGE USER MAPPING FOR CURRENT_USER STORAGE SERVER',
	'DROP STORAGE USER MAPPING IF EXISTS FOR CURRENT_USER STORAGE SERVER')
{
	check_completion("$command completion_s\t", qr/completion_storage /,
		"complete storage server name after $command");
	clear_query();
}

check_completion("ALTER STORAGE SERVER \t\t", qr/completion_storage/,
	'list storage server names without a prefix');
clear_query();

check_completion('ALTER STORAGE SERVER "Complet' . "\t",
	qr/"Completion Storage\\?" /,
	'complete a quoted storage server name');
clear_line();

my $storage_options = check_completion(
	"ALTER STORAGE SERVER completion_storage \t",
	qr/OPTIONS /, 'offer supported ALTER STORAGE SERVER actions');
unlike($storage_options, qr/RENAME TO/,
	'unsupported storage server rename is not offered');
clear_query();

check_completion("COMMENT ON STORAGE SERVER completion_storage \t",
	qr/IS /, 'complete IS after the storage server name');
clear_query();

for my $command ('ALTER TAG', 'ALTER TAG IF EXISTS', 'DROP TAG', 'DROP TAG IF EXISTS')
{
	check_completion("$command completion_t\t", qr/completion_tag /,
		"complete tag name after $command");
	clear_query();
}
for my $command ('ALTER PROFILE', 'DROP PROFILE', 'DROP PROFILE IF EXISTS')
{
	check_completion("$command completion_p\t", qr/completion_profile /,
		"complete profile name after $command");
	clear_query();
}
for my $command ('ALTER RESOURCE GROUP', 'DROP RESOURCE GROUP')
{
	check_completion("$command admin_\t", qr/admin_group /,
		"complete resource group name after $command");
	clear_query();
}

for my $command ('ALTER TAG', 'DROP TAG', 'DROP PROFILE')
{
	check_completion("$command I\t", qr/IF EXISTS /,
		"offer IF EXISTS after $command");
	clear_query();
	check_completion("$command IF \t", qr/EXISTS /,
		"complete EXISTS after $command IF");
	clear_query();
}

my $conditional_tag_actions = check_completion(
	"ALTER TAG IF EXISTS completion_tag \t\t",
	qr/ADD ALLOWED_VALUES.*DROP ALLOWED_VALUES.*RENAME TO/s,
	'offer supported conditional ALTER TAG actions');
unlike($conditional_tag_actions, qr/OWNER TO|UNSET ALLOWED_VALUES|ALLOWED_VLAUES/,
	'conditional ALTER TAG does not offer unsupported or misspelled actions');
clear_query();

# Execute the statements produced by completion to check that the advertised
# actions are accepted by the parser, rather than only matching their text.
for my $case (
	[ 'ALTER TAG completion_tag', 'A', qr/ADD ALLOWED_VALUES /,
	  "'ordinary';\n" ],
	[ 'ALTER TAG IF EXISTS completion_tag', 'A', qr/ADD ALLOWED_VALUES /,
	  "'conditional';\n" ],
	[ 'ALTER TAG completion_tag', 'D', qr/DROP ALLOWED_VALUES /,
	  "'ordinary';\n" ],
	[ 'ALTER TAG IF EXISTS completion_tag', 'D', qr/DROP ALLOWED_VALUES /,
	  "'conditional';\n" ],
	[ 'ALTER TAG completion_tag', 'O', qr/OWNER TO /, "CURRENT_USER;\n" ],
	[ 'ALTER TAG completion_tag', 'U', qr/UNSET ALLOWED_VALUES /, ";\n" ])
{
	my ($command, $prefix, $pattern, $suffix) = @{$case};
	check_completion("$command $prefix\t", $pattern,
		"complete $command $prefix");
	check_completion($suffix, qr/ALTER TAG\r?\n.*postgres=# /s,
		"execute completed $command $prefix");
}
for my $command ('ALTER TAG completion_tag', 'ALTER TAG IF EXISTS completion_tag')
{
	check_completion("$command R\t", qr/RENAME TO /,
		"complete rename after $command");
	check_completion("completion_tag_renamed;\n", qr/ALTER TAG\r?\n.*postgres=# /s,
		"execute completed rename after $command");
	$node->safe_psql('postgres',
		'ALTER TAG completion_tag_renamed RENAME TO completion_tag');
}

my $completion_log = substr(slurp_file($node->logfile), $completion_log_start);
unlike($completion_log, qr/ERROR:/,
	'Cloudberry object completion does not cause server errors');

$h->quit or die "psql returned $?";
$node->stop;
done_testing();

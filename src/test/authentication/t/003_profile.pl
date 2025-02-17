#--------------------------------------------------------------------------
#
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
# Test password profile feature.
#
# This test can only run with Unix-domain sockets.
#
#--------------------------------------------------------------------------

use strict;
use warnings;
use PostgresNode;
use TestLib;
use Test::More;
if (!$use_unix_sockets)
{
	plan skip_all =>
	  "authentication tests cannot run without Unix-domain sockets";
}
else
{
	plan tests => 96;
}

# Delete pg_hba.conf from the given node, add a new entry to it
# and then execute a reload to refresh it.
sub reset_pg_hba
{
	my $node       = shift;
	my $hba_method = shift;

	unlink($node->data_dir . '/pg_hba.conf');
	$node->append_conf('pg_hba.conf', "local all all $hba_method");
	$node->reload;
	return;
}

# Test access for a single role, useful to wrap all tests into one.
sub test_login
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;

	my $node          = shift;
	my $role          = shift;
	my $password      = shift;
	my $expected_res  = shift;
	my $status_string = 'failed';

	$status_string = 'success' if ($expected_res eq 0);

	my $connstr = "user=$role";
	my $testname =
	  "authentication $status_string for role $role with password $password";

	$ENV{"PGPASSWORD"} = $password;
	if ($expected_res eq 0)
	{
		$node->connect_ok($connstr, $testname);
	}
	else
	{
		# No checks of the error message, only the status code.
		$node->connect_fails($connstr, $testname);
	}
}

# Initialize primary node. Force UTF-8 encoding, so that we can use non-ASCII
# characters in the passwords below.
my $node = get_new_node('primary');
my ($ret, $stdout, $stderr);
$node->init(extra => [ '--locale=C', '--encoding=UTF8' ]);
$node->append_conf('postgresql.conf', "log_connections = on\n");
$node->start;

# Create test profiles.
$node->safe_psql(
    'postgres',
    "CREATE PROFILE myprofile1 LIMIT FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 1;
CREATE PROFILE myprofile2 LIMIT FAILED_LOGIN_ATTEMPTS 2 PASSWORD_REUSE_MAX 2;
CREATE PROFILE myprofile3 LIMIT FAILED_LOGIN_ATTEMPTS -1 PASSWORD_REUSE_MAX 0;
CREATE PROFILE myprofile4 LIMIT FAILED_LOGIN_ATTEMPTS 1 PASSWORD_LOCK_TIME 2 PASSWORD_REUSE_MAX 1;",
"");

# Create test roles.
$node->safe_psql(
	'postgres',
	"SET password_encryption='scram-sha-256';
SET client_encoding='utf8';
CREATE USER profile_user1 LOGIN PASSWORD 'IX' ENABLE PROFILE;
CREATE USER profile_user2 LOGIN PASSWORD 'a' DISABLE PROFILE;
CREATE USER profile_user3 LOGIN PASSWORD E'\\xc2\\xaa' ENABLE PROFILE;
CREATE USER profile_user4 LOGIN PASSWORD E'foo\\x07bar' DISABLE PROFILE;
");


# Test only super users have privileges of manipulating profile
$node->safe_psql(
	'postgres',
	"SET password_encryption='scram-sha-256';
SET client_encoding='utf8';
CREATE USER super_user SUPERUSER;
CREATE USER normal_user;"
);

# Test CREATE PROFILE
($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "CREATE PROFILE test_profile1 LIMIT FAILED_LOGIN_ATTEMPTS 2;"
);
is($ret, 0, 'create profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "CREATE PROFILE test_profile2 LIMIT FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 2;"
);
is($ret, 0, 'create profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "CREATE PROFILE test_profile3 LIMIT FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 2;"
);
is($ret, 3, 'non-super user can not create profile');
like(
    $stderr,
    qr/permission denied to create profile, must be superuser/,
    'expected error from non-super user can not create profile'
);

# Test ALTER PROFILE
$node->safe_psql(
    'postgres',
    "ALTER PROFILE pg_default LIMIT FAILED_LOGIN_ATTEMPTS 4;"
);

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "ALTER PROFILE test_profile1 LIMIT FAILED_LOGIN_ATTEMPTS 3;"
);
is($ret, 0, 'alter profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "ALTER PROFILE test_profile2 LIMIT FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 1 PASSWORD_REUSE_MAX 2;"
);
is($ret, 0, 'alter profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "ALTER PROFILE test_profile1 LIMIT FAILED_LOGIN_ATTEMPTS 3;"
);
is($ret, 3, 'non-super user can not alter profile');
like(
    $stderr,
    qr/permission denied to alter profile, must be superuser/,
    'expected error from non-super user can not alter profile'
);

# Test DROP PROFILE
($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "DROP PROFILE test_profile1;"
);
is($ret, 0, 'drop profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "DROP PROFILE test_profile2;"
);
is($ret, 3, 'non-super user can not drop profile');
like(
    $stderr,
    qr/permission denied to drop profile, must be superuser/,
    'expected error from non-super user can not drop profile'
);

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "DROP PROFILE pg_default;"
);
is($ret, 3, 'can not drop default profile pg_default');
like(
    $stderr,
    qr/Disallow to drop default profile/,
    'expected error from can not drop default profile pg_default'
);

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "DROP PROFILE test_profile2;"
);
is($ret, 0, 'drop profile succeed');

# Test CREATE USER ... PROFILE
($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "CREATE USER test_user1 PROFILE myprofile1;"
);
is($ret, 0, 'create user ... profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "CREATE USER test_user2 PROFILE myprofile2;"
);
is($ret, 0, 'create user ... profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "CREATE USER test_user3 PROFILE myprofile3;"
);
is($ret, 3, 'non-super user can not create user ... profile');
like(
    $stderr,
    qr/must be superuser to create role attached to profile/,
    'expected error from non-super user can not create user ... profile'
);

# Test CREATE USER ... ACCOUNT LOCK/UNLOCK
($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "CREATE USER test_user4 ACCOUNT LOCK;"
);
is($ret, 0, 'create user ... account lock succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "CREATE USER test_user5 ACCOUNT UNLOCK;"
);
is($ret, 0, 'create user ... account unlock succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "CREATE USER test_user6 ACCOUNT LOCK;"
);
is($ret, 3, 'non-super user can not create user ... account lock');
like(
    $stderr,
    qr/must be superuser to create role account lock\/unlock/,
    'expected error from non-super user can not create user ... account lock'
);

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "CREATE USER test_user7 ACCOUNT UNLOCK;"
);
is($ret, 3, 'non-super user can not create user ... account unlock');
like(
    $stderr,
    qr/must be superuser to create role account lock\/unlock/,
    'expected error from non-super user can not create user ... account unlock'
);

# Test CREATE USER ... ENABLE/DISABLE PROFILE
($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "CREATE USER test_user8 ENABLE PROFILE PROFILE myprofile2;"
);
is($ret, 0, 'create user ... enable profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "CREATE USER test_user9 DISABLE PROFILE;"
);
is($ret, 0, 'create user ... disable profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "CREATE USER test_user10 ENABLE PROFILE;"
);
is($ret, 3, 'non-super user can not create user ... enable profile');
like(
    $stderr,
    qr/must be superuser to create role enable\/disable profile/,
    'expected error from non-super user can not create user ... enable profile'
);

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "CREATE USER test_user11 DISABLE PROFILE;"
);
is($ret, 3, 'non-super user can not create user ... disable profile');
like(
    $stderr,
    qr/must be superuser to create role enable\/disable profile/,
    'expected error from non-super user can not create user ... disable profile'
);

# Test ALTER USER ... PROFILE
($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "ALTER USER test_user1 PROFILE myprofile1;"
);
is($ret, 0, 'alter user ... profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "ALTER USER test_user2 PROFILE myprofile2;"
);
is($ret, 0, 'alter user ... profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "ALTER USER test_user4 PROFILE myprofile4;"
);
is($ret, 3, 'non-super user can not alter user ... profile');
like(
    $stderr,
    qr/must be superuser to alter role attached to profile/,
    'expected error from non-super user can not alter user ... profile'
);

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "ALTER USER test_user4 PROFILE myprofile4;"
);
is($ret, 3, 'non-super user can not alter user ... profile');
like(
    $stderr,
    qr/must be superuser to alter role attached to profile/,
    'expected error from non-super user can not alter user ... profile'
);

# Test ALTER USER ... ENABLE/DISABLE PROFILE
($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "ALTER USER test_user1 ENABLE PROFILE;"
);
is($ret, 0, 'alter user ... enable profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "ALTER USER test_user2 DISABLE PROFILE;"
);
is($ret, 0, 'alter user ... disable profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "ALTER USER test_user4 ENABLE PROFILE;"
);
is($ret, 3, 'non-super user can not alter user ... enable profile');
like(
    $stderr,
    qr/must be superuser to alter role enable\/disable profile/,
    'expected error from non-super user can not alter user ... enable profile'
);

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "ALTER USER test_user5 DISABLE PROFILE;"
);
is($ret, 3, 'non-super user can not alter user ... disable profile');
like(
    $stderr,
    qr/must be superuser to alter role enable\/disable profile/,
    'expected error from non-super user can not alter user ... disable profile'
);

# Test ALTER USER ... ACCOUNT
($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "ALTER USER test_user1 ACCOUNT LOCK;"
);
is($ret, 0, 'alter user ... account lock succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "ALTER USER test_user2 ACCOUNT UNLOCK;"
);
is($ret, 0, 'alter user ... account unlock succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "ALTER USER test_user1 ACCOUNT LOCK;"
);
is($ret, 3, 'non-super user can not alter user ... account lock');
like(
    $stderr,
    qr/must be superuser to alter role account lock\/unlock/,
    'expected error from non-super user can not alter user ... account lock'
);

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "ALTER USER test_user1 ACCOUNT UNLOCK;"
);
is($ret, 3, 'non-super user can not alter user ... account unlock');
like(
    $stderr,
    qr/must be superuser to alter role account lock\/unlock/,
    'expected error from non-super user can not alter user ... account unlock'
);

# Test DROP USER
($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "DROP USER test_user1;"
);
is($ret, 0, 'drop user succeed');

# Test privileges of SELECT pg_authid, pg_profile, pg_password_history
($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "SELECT rolname, rolaccountstatus, rolfailedlogins, rolenableprofile
     FROM pg_authid
     WHERE rolname like '%test_user%';"
);
is($ret, 0, 'select pg_authid succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "SELECT rolname, rolaccountstatus, rolfailedlogins, rolenableprofile
     FROM pg_authid
     WHERE rolname like '%test_user%';"
);
is($ret, 3, 'non-super user can not select pg_authid');
like(
    $stderr,
    qr/permission denied for table pg_authid/,
    'expected error from non-super user can not select pg_authid'
);

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
     FROM pg_profile;"
);
is($ret, 0, 'select pg_profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
     FROM pg_profile;"
);
is($ret, 0, 'select pg_profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'super_user',
    'postgres',
    "SELECT COUNT(*)
     FROM pg_password_history;"
);
is($ret, 0, 'select pg_profile succeed');

($ret, $stdout, $stderr) =
$node->role_psql(
    'normal_user',
    'postgres',
    "SELECT COUNT(*)
     FROM pg_password_history;"
);
is($ret, 3, 'non-super user can not select pg_password_history');
like(
    $stderr,
    qr/permission denied for table pg_password_history/,
    'expected error from non-super user can not select pg_password_history'
);

# Test Login Successful
$node->safe_psql(
    'postgres',
    "ALTER USER profile_user1 PROFILE myprofile1 ENABLE PROFILE;
    ALTER USER profile_user2 PROFILE myprofile2 ENABLE PROFILE;
    ALTER USER profile_user3 PROFILE myprofile3 ENABLE PROFILE;
    ALTER USER profile_user4 PROFILE myprofile4 ENABLE PROFILE;
    ALTER USER super_user PROFILE pg_default ENABLE PROFILE PASSWORD 'a_nice_word';"
);

# Require password from now on.
reset_pg_hba($node, 'scram-sha-256');

# Test passwords work OK
test_login($node, 'profile_user1', "I\xc2\xadX",   0);
test_login($node, 'profile_user1', "\xe2\x85\xa8", 0);

test_login($node, 'profile_user2', "a",        0);
test_login($node, 'profile_user2', "\xc2\xaa", 0);

test_login($node, 'profile_user3', "a",        0);
test_login($node, 'profile_user3', "\xc2\xaa", 0);

test_login($node, 'profile_user4', "foo\x07bar", 0);

test_login($node, 'profile_user1', 'IX', 0);

# Test Login Successful and Failed
# Test Account Unlocked
test_login($node, 'profile_user1', "random", 2);
test_login($node, 'profile_user1', "random", 2);

reset_pg_hba($node, 'trust');
($ret, $stdout, $stderr) =
$node->psql(
    'postgres',
    "SELECT rolname, rolaccountstatus, rolfailedlogins, rolenableprofile
    FROM pg_authid
    WHERE rolname = 'profile_user1';"
);
like(
    $stdout,
    qr/profile_user1|0|2|t/,
    'expected results from select pg_authid'
);

reset_pg_hba($node, 'scram-sha-256');
test_login($node, 'profile_user1', "IX", 0);

reset_pg_hba($node, 'trust');
($ret, $stdout, $stderr) =
$node->psql(
    'postgres',
    "SELECT rolname, rolaccountstatus, rolfailedlogins, rolenableprofile
    FROM pg_authid
    WHERE rolname = 'profile_user1';"
);
like(
    $stdout,
    qr/profile_user1|0|0|t/,
    'expected results from select pg_authid'
);

# Test Account Locked
reset_pg_hba($node, 'scram-sha-256');
test_login($node, 'profile_user2', "random", 2);
test_login($node, 'profile_user2', "random", 2);

reset_pg_hba($node, 'trust');
($ret, $stdout, $stderr) =
$node->psql(
    'postgres',
    "SELECT rolname, rolaccountstatus, rolfailedlogins, rolenableprofile
    FROM pg_authid
    WHERE rolname = 'profile_user2';"
);
like(
    $stdout,
    qr/profile_user2|2|2|t/,
    'expected results from select pg_authid'
);

$node->safe_psql(
    'postgres',
    "ALTER USER profile_user2 ACCOUNT UNLOCK"
);
($ret, $stdout, $stderr) =
$node->psql(
    'postgres',
    "SELECT rolname, rolaccountstatus, rolfailedlogins, rolenableprofile
    FROM pg_authid
    WHERE rolname = 'profile_user2';"
);
like(
    $stdout,
    qr/profile_user2|0|2|t/,
    'expected results from select pg_authid'
);

reset_pg_hba($node, 'scram-sha-256');
test_login($node, 'profile_user2', "a", 0);

reset_pg_hba($node, 'trust');
($ret, $stdout, $stderr) =
$node->psql(
    'postgres',
    "SELECT rolname, rolaccountstatus, rolfailedlogins, rolenableprofile
    FROM pg_authid
    WHERE rolname = 'profile_user2';"
);
like(
    $stdout,
    qr/profile_user2|0|0|t/,
    'expected results from select pg_authid'
);

# Test Default Profile Value
reset_pg_hba($node, 'scram-sha-256');

test_login($node, 'profile_user3', "random", 2);
test_login($node, 'profile_user3', "random", 2);
test_login($node, 'profile_user3', "random", 2);

reset_pg_hba($node, 'trust');
($ret, $stdout, $stderr) =
$node->psql(
    'postgres',
    "SELECT rolname, rolaccountstatus, rolfailedlogins, rolenableprofile
    FROM pg_authid
    WHERE rolname = 'profile_user3';"
);
like(
    $stdout,
    qr/profile_user3|0|3|t/,
    'expected results from select pg_authid'
);

($ret, $stdout, $stderr) =
$node->psql(
    'postgres',
    "SELECT prfname, prffailedloginattempts
    FROM pg_profile
    WHERE prfname = 'myprofile3';"
);
like(
    $stdout,
    qr/myprofile3|-1/,
    'expected results from select pg_profile'
);

($ret, $stdout, $stderr) =
$node->psql(
    'postgres',
    "SELECT prfname, prffailedloginattempts
    FROM pg_profile
    WHERE prfname = 'pg_default';"
);
like(
    $stdout,
    qr/pg_default|4/,
    'expected results from select pg_profile'
);

$node->safe_psql(
    'postgres',
    "ALTER PROFILE pg_default LIMIT FAILED_LOGIN_ATTEMPTS 2;"
);

reset_pg_hba($node, 'scram-sha-256');
test_login($node, 'profile_user3', "a", 2);

reset_pg_hba($node, 'trust');
($ret, $stdout, $stderr) =
$node->psql(
    'postgres',
    "SELECT rolname, rolaccountstatus, rolfailedlogins, rolenableprofile
    FROM pg_authid
    WHERE rolname = 'profile_user3';"
);
like(
    $stdout,
    qr/profile_user3|2|3|t/,
    'expected results from select pg_authid'
);

# Test Alter User ... Enable/Disable Profile
reset_pg_hba($node, 'scram-sha-256');
test_login($node, 'profile_user4', "random", 2);
test_login($node, 'profile_user4', "random", 2);

reset_pg_hba($node, 'trust');
($ret, $stdout, $stderr) =
$node->psql(
    'postgres',
    "SELECT rolname, rolaccountstatus, rolfailedlogins, rolenableprofile
    FROM pg_authid
    WHERE rolname = 'profile_user4';"
);
like(
    $stdout,
    qr/profile_user4|2|2|t/,
    'expected results from select pg_authid'
);

reset_pg_hba($node, 'scram-sha-256');
test_login($node, 'profile_user4', "foo\x07bar", 2);

reset_pg_hba($node, 'trust');
$node->safe_psql(
    'postgres',
    "ALTER USER profile_user4 DISABLE PROFILE;"
);

($ret, $stdout, $stderr) =
$node->psql(
    'postgres',
    "SELECT rolname, rolaccountstatus, rolfailedlogins, rolenableprofile
    FROM pg_authid
    WHERE rolname = 'profile_user4';"
);
like(
    $stdout,
    qr/profile_user4|2|2|f/,
    'expected results from select pg_authid'
);

reset_pg_hba($node, 'scram-sha-256');
test_login($node, 'profile_user4', "foo\x07bar", 0);

reset_pg_hba($node, 'trust');
$node->safe_psql(
    'postgres',
    "ALTER USER profile_user4 ENABLE PROFILE;"
);

reset_pg_hba($node, 'scram-sha-256');
test_login($node, 'profile_user4', "foo\x07bar", 2);

# Test Superuser Will Never Locked
test_login($node, 'super_user', "random", 2);
test_login($node, 'super_user', "random", 2);
test_login($node, 'super_user', "random", 2);

reset_pg_hba($node, 'trust');
($ret, $stdout, $stderr) =
$node->psql(
    'postgres',
    "SELECT rolname, rolaccountstatus, rolfailedlogins, rolenableprofile
    FROM pg_authid
    WHERE rolname = 'super_user';"
);
like(
    $stdout,
    qr/super_user|0|0|t/,
    'expected results from select pg_authid'
);

reset_pg_hba($node, 'scram-sha-256');
test_login($node, 'super_user', "a_nice_word", 0);

# cleanup
reset_pg_hba($node, 'trust');
$node->safe_psql(
    'postgres',
    "DROP USER test_user2;
    DROP USER test_user4;
    DROP USER test_user5;
    DROP USER test_user8;
    DROP USER test_user9;
    DROP USER super_user;
    DROP USER normal_user;
    DROP USER profile_user1;
    DROP USER profile_user2;
    DROP USER profile_user3;
    DROP USER profile_user4;
    DROP PROFILE myprofile1;
    DROP PROFILE myprofile2;
    DROP PROFILE myprofile3;
    DROP PROFILE myprofile4;
    ALTER PROFILE pg_default LIMIT FAILED_LOGIN_ATTEMPTS -2 PASSWORD_LOCK_TIME -2 PASSWORD_REUSE_MAX -2;"
);
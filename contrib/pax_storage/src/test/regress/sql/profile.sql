--
-- Test for PROFILE
--

-- Display pg_stas_activity to check the login monitor process
SELECT COUNT(*) FROM pg_stat_activity;

-- Display pg_authid, pg_roles, pg_profile and pg_password_history catalog
\d+ pg_authid;
\d+ pg_roles;
\d+ pg_profile;
\d+ pg_password_history;

SELECT rolname, prfname, rolaccountstatus, rolfailedlogins, rolenableprofile
FROM pg_authid, pg_profile
WHERE pg_authid.rolprofile = pg_profile.oid
AND rolname like '%profile_user%';
SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
FROM pg_profile;

-- Test CREATE PROFILE
CREATE PROFILE myprofile1;
CREATE PROFILE myprofile2 LIMIT FAILED_LOGIN_ATTEMPTS -1 PASSWORD_LOCK_TIME -2;
CREATE PROFILE myprofile3 LIMIT FAILED_LOGIN_ATTEMPTS 4 PASSWORD_LOCK_TIME 1;
CREATE PROFILE myprofile4 LIMIT FAILED_LOGIN_ATTEMPTS 5 PASSWORD_LOCK_TIME 9999 PASSWORD_REUSE_MAX 3;
CREATE PROFILE myprofile4;  -- Failed for myprofile4 already exists

SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
FROM pg_profile;

-- Failed for invalid parameters
CREATE PROFILE myprofile5 LIMIT FAILED_LOGIN_ATTEMPTS -3;
CREATE PROFILE myprofile6 LIMIT FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME -5;
CREATE PROFILE myprofile7 LIMIT FAILED_LOGIN_ATTEMPTS -2 PASSWORD_LOCK_TIME -1 PASSWORD_REUSE_MAX -9999;

CREATE PROFILE myprofile8 LIMIT FAILED_LOGIN_ATTEMPTS 10000;
CREATE PROFILE myprofile9 LIMIT FAILED_LOGIN_ATTEMPTS 9999 PASSWORD_LOCK_TIME 10000;
CREATE PROFILE myprofile10 LIMIT FAILED_LOGIN_ATTEMPTS 9999 PASSWORD_LOCK_TIME -1 PASSWORD_REUSE_MAX 99999;

CREATE PROFILE myprofile11 LIMIT FAILED_LOGIN_ATTEMPTS 9999 FAILED_LOGIN_ATTEMPTS 2;
CREATE PROFILE myprofile12 LIMIT FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 4 PASSWORD_LOCK_TIME 3;
CREATE PROFILE myprofile13 LIMIT FAILED_LOGIN_ATTEMPTS 4 PASSWORD_LOCK_TIME 3 PASSWORD_REUSE_MAX 2 PASSWORD_REUSE_MAX 2;

-- Failed for syntax error
CREATE PROFILE myprofile14 FAILED_LOGIN_ATTEMPTS 1;
CREATE PROFILE myprofile15 PASSWORD_LOCK_TIME -2;
CREATE PROFILE myprofile16 PASSWORD_RESUE_MAX -1;
CREATE PROFILE myprofile17 FAILED_LOGIN_ATTEMPTS 0;

SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
FROM pg_profile;

-- Test CREATE USER ... PROFILE
CREATE USER profile_user1 PROFILE test; -- failed
CREATE USER profile_user1 PROFILE pg_default;
CREATE USER profile_user2 PASSWORD 'a_nice_long_password_123';
CREATE USER profile_user3 PASSWORD 'a_nice_long_password_456' PROFILE myprofile3;
CREATE USER profile_user4 ACCOUNT LOCK PROFILE myprofile4;

SELECT rolname, prfname, rolaccountstatus, rolfailedlogins, rolenableprofile
FROM pg_authid, pg_profile
WHERE pg_authid.rolprofile = pg_profile.oid
AND rolname like '%profile_user%';

-- Test CREATE USER ... ENABLE/DISABLE PROFILE
CREATE USER profile_user5 ENABLE PROFILE PROFILE pg_default;
CREATE USER profile_user6 ENABLE PROFILE PROFILE; -- failed
CREATE USER profile_user7 DISABLE PROFILE PROFILE pg_default;
CREATE USER profile_user8 DISABLE PROFILE PROFILE; -- failed
CREATE USER profile_user9 SUPERUSER;

SELECT rolname, prfname, rolaccountstatus, rolfailedlogins, rolenableprofile
FROM pg_authid, pg_profile
WHERE pg_authid.rolprofile = pg_profile.oid
AND rolname like '%profile_user%';

-- Test ALTER PROFILE
ALTER USER profile_user1 PROFILE myprofile1;
ALTER USER profile_user2 PROFILE myprofile2;

SELECT rolname, prfname, rolaccountstatus, rolfailedlogins, rolenableprofile
FROM pg_authid, pg_profile
WHERE pg_authid.rolprofile = pg_profile.oid
AND rolname like '%profile_user%';

ALTER USER profile_user10 PROFILE myprofile2;  -- failed
SELECT rolname, prfname, rolaccountstatus, rolfailedlogins, rolenableprofile
FROM pg_authid, pg_profile
WHERE pg_authid.rolprofile = pg_profile.oid
AND rolname = 'profile_user9';

ALTER USER profile_user9 PROFILE pg_default;
SELECT rolname, prfname, rolaccountstatus, rolfailedlogins, rolenableprofile
FROM pg_authid, pg_profile
WHERE pg_authid.rolprofile = pg_profile.oid
AND rolname = 'profile_user9';

ALTER PROFILE myprofile1 LIMIT; --  OK
ALTER PROFILE myprofile1 LIMIT PASSWORD_LOCK_TIME 1;
ALTER PROFILE myprofile2 PASSWORD_LOCK_TIME 3;  --  syntax error
ALTER PROFILE myprofile2 LIMIT PASSWORD_LOCK_TIME 3;    --  OK
ALTER PROFILE myprofile3 LIMIT FAILED_LOGIN_ATTEMPTS 3 PASSWORD_REUSE_MAX 2;
ALTER PROFILE myprofile3 LIMIT FAILED_LOGIN_ATTEMPTS 3 PASSWORD_REUSE_MAX 2; -- ALTER PROFILE the same values
ALTER PROFILE myprofile4 LIMIT PASSWORD_LOCK_TIME 10 PASSWORD_REUSE_MAX -1;
ALTER PROFILE myprofile4 LIMIT FAILED_LOGIN_ATTEMPTS 9999 PASSWORD_LOCK_TIME 9999 PASSWORD_REUSE_MAX 9999;
ALTER PROFILE myprofile4 LIMIT FAILED_LOGIN_ATTEMPTS 4 PASSWORD_LOCK_TIME 0 PASSWORD_REUSE_MAX 0;
ALTER PROFILE myprofile5 LIMIT FAILED_LOGIN_ATTEMPTS 3;
ALTER PROFILE pg_default LIMIT FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 1 PASSWORD_REUSE_MAX 3;

ALTER PROFILE myprofile1 LIMIT FAILED_LOGIN_ATTEMPTS 1 FAILED_LOGIN_ATTEMPTS 2;
ALTER PROFILE myprofile2 LIMIT PASSWORD_LOCK_TIME 2 PASSWORD_LOCK_TIME 3;
ALTER PROFILE myprofile3 LIMIT PASSWORD_REUSE_MAX -1 PASSWORD_REUSE_MAX -2;
ALTER PROFILE myprofile1 LIMIT FAILED_LOGIN_ATTEMPTS 1 FAILED_LOGIN_ATTEMPTS 2;
ALTER PROFILE myprofile2 LIMIT FAILED_LOGIN_ATTEMPTS -2 PASSWORD_LOCK_TIME 2 PASSWORD_LOCK_TIME -2;
ALTER PROFILE myprofile3 LIMIT FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME -1 PASSWORD_REUSE_MAX 2 PASSWORD_REUSE_MAX 2;

-- Failed for pg_default value can not be -1
ALTER PROFILE pg_default LIMIT FAILED_LOGIN_ATTEMPTS -1;
ALTER PROFILE pg_default LIMIT PASSWORD_LOCK_TIME -1;
ALTER PROFILE pg_default LIMIT PASSWORD_REUSE_MAX -1;
ALTER PROFILE pg_default LIMIT FAILED_LOGIN_ATTEMPTS 2 PASSWORD_LOCK_TIME 2 PASSWORD_REUSE_MAX -1;

SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
FROM pg_profile;

-- Test ALTER PROFILE ... RENAME TO
ALTER PROFILE pg_default RENAME TO anyname;     -- failed for pg_default profile can't be renamed
ALTER PROFILE myprofile1 RENAME TO myprofile2;  -- failed for myprofile2 already exists
ALTER PROFILE myprofile1 RENAME TO pg_default;  -- failed for pg_default already exists
ALTER PROFILE myprofile1 RENAME TO tempname;    -- OK
ALTER PROFILE myprofile2 RENAME TO myprofile1;  -- OK
ALTER PROFILE myprofile5 RENAME TO tempname2;   -- failed for myprofile5 doesn't exists

SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
FROM pg_profile;

ALTER PROFILE tempname RENAME TO myprofile2;    -- OK

SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
FROM pg_profile;

-- Failed for invalid parameters
ALTER PROFILE myprofile1 LIMIT FAILED_LOGIN_ATTEMPTS 10000;
ALTER PROFILE myprofile2 LIMIT FAILED_LOGIN_ATTEMPTS 9999 PASSWORD_LOCK_TIME 10000;
ALTER PROFILE myprofile3 LIMIT FAILED_LOGIN_ATTEMPTS 9999 PASSWORD_LOCK_TIME 9999 PASSWORD_REUSE_MAX 10000;
ALTER PROFILE myprofile4 LIMIT FAILED_LOGIN_ATTEMPTS 0;
ALTER PROFILE myprofile4 LIMIT FAILED_LOGIN_ATTEMPTS 0 PASSWORD_LOCK_TIME 0 PASSWORD_REUSE_MAX 3;

ALTER PROFILE myprofile4 LIMIT FAILED_LOGIN_ATTEMPTS 9999 FAILED_LOGIN_ATTEMPTS 3;
ALTER PROFILE myprofile4 LIMIT FAILED_LOGIN_ATTEMPTS 9999 PASSWORD_LOCK_TIME 1 PASSWORD_LOCK_TIME 2;
ALTER PROFILE myprofile4 LIMIT FAILED_LOGIN_ATTEMPTS 3 PASSWORD_LOCK_TIME 3 PASSWORD_REUSE_MAX 4 PASSWORD_REUSE_MAX 3;

SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
FROM pg_profile;

-- Failed for syntax error
ALTER PROFILE myprofile1 FAILED_LOGIN_ATTEMPTS 5;
ALTER PROFILE myprofile2 PASSWORD_LOCK_TIME -2;
ALTER PROFILE myprofile3 PASSWORD_RESUE_MAX -1;

SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
FROM pg_profile;

DELETE FROM pg_profile;    -- failed for catalog can't be deleted

SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
FROM pg_profile;

-- Test ALTER USER ... PROFILE
ALTER USER profile_user2 PROFILE myprofile3;
ALTER USER profile_user3 PROFILE myprofile2;
ALTER USER profile_user1 PROFILE myprofile1;
ALTER USER profile_user4 PROFILE myprofile4;
ALTER USER profile_user9 PROFILE myprofile3;

SELECT rolname, prfname, rolaccountstatus, rolfailedlogins, rolenableprofile
FROM pg_authid, pg_profile
WHERE pg_authid.rolprofile = pg_profile.oid
AND rolname like '%profile_user%';

-- Test ALTER USER ... ENABLE/DISABLE PROFILE
ALTER USER profile_user5 DISABLE PROFILE PROFILE myprofile3;
ALTER USER profile_user5 ENABLE PROFILE PROFILE;
ALTER USER profile_user7 ENABLE PROFILE PROFILE myprofile4;
ALTER USER profile_user7 DISABLE PROFILE PROFILE;

SELECT rolname, prfname, rolaccountstatus, rolfailedlogins, rolenableprofile
FROM pg_authid, pg_profile
WHERE pg_authid.rolprofile = pg_profile.oid
AND rolname like '%profile_user%';

-- Test ALTER USER ... PASSWORD
ALTER USER profile_user1 PASSWORD 'test';
ALTER USER profile_user1 PASSWORD 'a_nice_long_password_123';
ALTER USER profile_user1 PASSWORD 'a_new_password';
ALTER USER profile_user1 PASSWORD 'test';
ALTER USER profile_user1 PASSWORD 'a_nice_long_password_123';
ALTER USER profile_user1 PASSWORD 'a_new_password';
ALTER USER profile_user1 PASSWORD 'ABCD';
ALTER USER profile_user1 PASSWORD 'test';
ALTER PROFILE pg_default LIMIT PASSWORD_REUSE_MAX 4;
ALTER USER profile_user1 PASSWORD 'a_nice_long_password_123';

ALTER USER profile_user2 PASSWORD 'test2';
ALTER USER profile_user2 PASSWORD 'a_bad_password';
ALTER USER profile_user2 PASSWORD 'test2' ENABLE PROFILE;
ALTER USER profile_user2 PASSWORD 'a_bad_password';
ALTER USER profile_user2 PASSWORD 'a_nice_password';
ALTER USER profile_user2 PASSWORD 'a_bad_password';
ALTER USER profile_user2 PASSWORD 'test2';
ALTER PROFILE myprofile3 LIMIT PASSWORD_REUSE_MAX 1;
ALTER USER profile_user2 PASSWORD 'a_bad_password'; -- OK
ALTER USER profile_user2 PASSWORD 'test2';  -- OK

ALTER USER profile_user4 PASSWORD 'test3'; -- OK

DELETE FROM pg_password_history;    -- failed for catalog can't be deleted

-- Test ALTER USER ... ACCOUNT LOCK/UNLOCK
ALTER USER profile_user1 ACCOUNT LOCK;
ALTER USER profile_user2 ACCOUNT UNLOCK;
ALTER USER profile_user3 ACCOUNT LOCK;
ALTER USER profile_user4 ACCOUNT UNLOCK;

SELECT rolname, prfname, rolaccountstatus, rolfailedlogins, rolenableprofile
FROM pg_authid, pg_profile
WHERE pg_authid.rolprofile = pg_profile.oid
AND rolname like '%profile_user%';

-- Test for get_role_status()
SELECT get_role_status('profile_user1');
SELECT get_role_status('profile_user2');
SELECT get_role_status('profile_user3');
SELECT get_role_status('profile_user4');
SELECT get_role_status('profile_user5'); -- failed for user does not exist

SELECT rolname, prfname, rolaccountstatus, rolfailedlogins, rolenableprofile
FROM pg_authid, pg_profile
WHERE pg_authid.rolprofile = pg_profile.oid
AND rolname like '%profile_user%';

-- Test update pg_password_history
UPDATE pg_password_history SET passhistpassword = 'random'; -- permission denied

-- Test DROP PROFILE
-- Failed for profile is using by user
DROP PROFILE myprofile1;
DROP PROFILE myprofile2;
DROP PROFILE myprofile3;
DROP PROFILE myprofile4;
DROP PROFILE pg_default;    --  failed, can't drop pg_default profile

SELECT rolname, prfname, rolaccountstatus, rolfailedlogins, rolenableprofile
FROM pg_authid, pg_profile
WHERE pg_authid.rolprofile = pg_profile.oid
AND rolname like '%profile_user%';

-- cleanup
DROP USER profile_user1;
DROP USER profile_user2;
DROP USER profile_user3;
DROP USER profile_user4;
DROP USER profile_user5;
DROP USER profile_user7;

SELECT rolname, prfname, rolaccountstatus, rolfailedlogins, rolenableprofile
FROM pg_authid, pg_profile
WHERE pg_authid.rolprofile = pg_profile.oid
AND rolname like '%profile_user%';

-- Successful
DROP PROFILE myprofile1, myprofile2;
DROP PROFILE myprofile1; -- failed
DROP PROFILE IF EXISTS myprofile2;  -- OK
DROP PROFILE myprofile3; -- failed
DROP PROFILE myprofile4, pg_default; -- failed
DROP PROFILE IF EXISTS myprofile4;  -- OK

SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
FROM pg_profile;

DROP USER profile_user9;
DROP PROFILE myprofile3; -- OK

SELECT prfname, prffailedloginattempts, prfpasswordlocktime, prfpasswordreusemax
FROM pg_profile;

-- Reset pg_default
ALTER PROFILE pg_default LIMIT FAILED_LOGIN_ATTEMPTS -2 PASSWORD_LOCK_TIME -2 PASSWORD_REUSE_MAX 0;

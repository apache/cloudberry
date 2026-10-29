Storage abort isolation tests
============================

Run against an existing dedicated Cloudberry test database with datalake_fdw
preloaded, datalake_fdw_test installed, and a superuser connection. Set normal
PGHOST/PGPORT/PGUSER variables. Python needs PyGreSQL (import pg).
Both cases require a FAULT_INJECTOR build and gp_inject_fault.

  make -C contrib/datalake_fdw installcheck-storage-abort CONTRIB_TESTDB=testdb

Or run either case directly:

  python3 contrib/datalake_fdw/test/isolation2/run.py local --dbname testdb
  python3 contrib/datalake_fdw/test/isolation2/run.py s3 --dbname testdb

ISOLATION2_PYTHON selects Python for the make target. ISOLATION2_EXECUTOR can
override the path to src/test/isolation2/sql_isolation_testcase.py, for example
when testing a PGXS installation from outside the source tree.

These use the real isolation2 SQL executor, with an assertion runner instead
of golden transcripts: temporary pathnames, S3 keys and fault status vary.
Run serially; schema/server names and advisory lock keys are fixed.

local_abort_ownership.sql
-------------------------
Run on the coordinator host, as its OS user, so its backend can access the
runner's private temporary directory. The case checks three interleavings:

1. A stages a file and blocks on row 2. B publishes its own file before A
   aborts. A's abort must preserve B's contents.
2. A is suspended inside Abort(), immediately before staging cleanup. B
   publishes a replacement during that pause. Resuming A must preserve B.
   Against the previous identity-check implementation, place the same fault
   hook after the inode check and before unlink to reproduce the old race.
3. A stages a file and blocks on row 2 without an injected writer error. B
   publishes first. A's Close must fail with AlreadyExists and preserve B.

The case also checks that the destination is absent before close, staging
files do not appear in recursive listings, and no staging directories remain
after abort or failed publication. The two intentional writer errors and
the publication error are expected; all other errors fail the test.

s3_abort_retry.sql
------------------
Also needs boto3 and an existing test bucket. Set DATALAKE_TEST_S3_ENDPOINT, DATALAKE_TEST_S3_BUCKET,
DATALAKE_TEST_S3_ACCESS_KEY and DATALAKE_TEST_S3_SECRET_KEY. The endpoint must
be reachable from both the coordinator and runner. This fixture uses static
credentials, region us-east-1 and path-style access.

A uploads uncompressed data and blocks before an intentional query error.
The runner first verifies exactly one incomplete multipart upload exists.
B injects one failed AbortMultipartUpload outcome and releases A. The error
aborts A's stream; its destructor must retry cleanup. The fault must be hit
once, and ListMultipartUploads must show no remaining upload afterward.
This exercises destructor retry, rather than explicitly calling Abort twice
from SQL, and does not test SDK-internal retries.

The runner returns nonzero on an unexpected SQL error, missing assertion,
missing fault hit, or executor failure. The deliberate writer errors and local publication collision are
expected. It redacts credentials in displayed SQL and aborts any leaked
upload for its unique test key in a finally block. Local files are removed
by the temporary-directory context. Normal completion drops SQL fixtures
and resets the fault. After external interruption, remove the test schemas
pr2044_local_isolation / pr2044_s3_isolation and server
pr2044_isolation_volume, and reset datalake_local_abort_before_cleanup and
datalake_s3_abort_multipart_upload before
rerunning. Session exit releases advisory locks.

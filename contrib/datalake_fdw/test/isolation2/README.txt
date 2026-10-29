Storage abort isolation tests
============================

Run against an existing dedicated Cloudberry test database with datalake_fdw
preloaded, datalake_fdw_test installed, and a superuser connection. Set normal
PGHOST/PGPORT/PGUSER variables. Python needs PyGreSQL (import pg).

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
runner's private temporary directory. A creates a file and blocks during its
second row fetch. B waits for A's exact advisory lock wait, deletes A's path,
creates and closes a replacement, then releases A. A raises an intentional
error and aborts its stream. B's contents must remain readable afterward.

s3_abort_retry.sql
------------------
Needs a FAULT_INJECTOR build, gp_inject_fault, boto3, and an existing test
bucket. Set DATALAKE_TEST_S3_ENDPOINT, DATALAKE_TEST_S3_BUCKET,
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
missing fault hit, or executor failure. The one deliberate writer error is
expected. It redacts credentials in displayed SQL and aborts any leaked
upload for its unique test key in a finally block. Local files are removed
by the temporary-directory context. Normal completion drops SQL fixtures
and resets the fault. After external interruption, remove the test schemas
pr2044_local_isolation / pr2044_s3_isolation and server
pr2044_isolation_volume, and reset datalake_s3_abort_multipart_upload before
rerunning. Session exit releases advisory locks.

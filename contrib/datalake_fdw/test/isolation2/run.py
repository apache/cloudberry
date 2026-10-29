#!/usr/bin/env python3
"""Run SQL with the repository's real isolation2 executor and check its results."""
import argparse
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import uuid

suite = Path(__file__).resolve().parent
repo = suite.parents[3]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('case', choices=['local', 's3'])
parser.add_argument('--dbname', default=os.environ.get('PGDATABASE', 'postgres'))
args = parser.parse_args()
env = dict(os.environ, PR2044_PYTHON=sys.executable, PR2044_SUITE=str(suite))
executor = Path(env.get('ISOLATION2_EXECUTOR', str(repo / 'src/test/isolation2/sql_isolation_testcase.py')))
for target, source in [('PR2044_S3_ENDPOINT', 'DATALAKE_TEST_S3_ENDPOINT'),
                       ('PR2044_S3_BUCKET', 'DATALAKE_TEST_S3_BUCKET'),
                       ('AWS_ACCESS_KEY_ID', 'DATALAKE_TEST_S3_ACCESS_KEY'),
                       ('AWS_SECRET_ACCESS_KEY', 'DATALAKE_TEST_S3_SECRET_KEY')]:
    if env.get(source):
        env[target] = env[source]

def quote(value):
    return "'" + value.replace("'", "''") + "'"

with tempfile.TemporaryDirectory(prefix='pr2044-isolation-') as tmp:
    values = {'LOCAL_URI': 'file://' + tmp + '/replacement'}
    if args.case == 's3':
        for name in ['PR2044_S3_ENDPOINT', 'PR2044_S3_BUCKET',
                     'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']:
            if not env.get(name):
                parser.error('set ' + name)
        # Import before running SQL so missing dependencies cannot leave fixtures behind.
        import boto3
        env['PR2044_S3_KEY'] = 'pr2044-isolation/' + uuid.uuid4().hex + '/aborted.parquet'
        uri = 's3://' + env['PR2044_S3_BUCKET'] + '/' + env['PR2044_S3_KEY']
        values.update(S3_URI=uri.rsplit('/', 1)[0], S3_OBJECT_URI=uri,
                      S3_ENDPOINT=env['PR2044_S3_ENDPOINT'],
                      ACCESS_KEY=env['AWS_ACCESS_KEY_ID'], SECRET_KEY=env['AWS_SECRET_ACCESS_KEY'])
    name = 'local_abort_ownership' if args.case == 'local' else 's3_abort_retry'
    sql = (suite / 'sql' / (name + '.sql')).read_text()
    for key, value in values.items():
        sql = sql.replace('@' + key + '@', quote(value))
    try:
        result = subprocess.run([sys.executable, str(executor), '--dbname', args.dbname],
                                input=sql, text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, env=env, timeout=180)
        output = result.stdout
        # The executor echoes SQL. Only a standalone result row is a pass marker.
        markers = ['PR2044_LOCAL_PASS'] if args.case == 'local' else [
            'PR2044_S3_BEFORE_PASS', 'PR2044_S3_AFTER_PASS']
        passed = result.returncode == 0 and 'FAILED:' not in output and all(
            re.search(r'^\s*' + marker + r'\s*$', output, re.M) for marker in markers)
        expected_error = 'PR2044 injected writer A failure' if args.case == 'local' else 'PR2044 injected S3 writer failure'
        errors = re.findall(r'^.*ERROR:.*$', output, re.M)
        passed = passed and len(errors) == 1 and expected_error in errors[0]
        if args.case == 's3':
            passed = passed and "num times hit:'1'" in output
            # SQL echo includes the mapping: redact credentials before displaying it.
            for value in (env['AWS_SECRET_ACCESS_KEY'], env['AWS_ACCESS_KEY_ID']):
                output = output.replace(value, '<redacted>')
        print(output, end='')
        print('PASS' if passed else 'FAIL: expected storage cleanup contract was not satisfied')
    finally:
        if args.case == 's3':
            subprocess.run([sys.executable, str(suite / 'check_uploads.py'), 'cleanup'],
                           env=env, check=True)
sys.exit(0 if passed else 1)

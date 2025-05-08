'''
pg_qs_test_runner.py
Copyright (c) 2016-2024, Postgres Professional
'''

import argparse
import getpass
import os
import sys

import psycopg2

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from test_cases import *

class PasswordPromptAction(argparse.Action):
	def __call__(self, parser, args, values, option_string=None):
		password = getpass.getpass()
		setattr(args, self.dest, password)

class SetupException(Exception): pass
class TeardownException(Exception): pass
 
unlock_if_eq_1 = """
	CREATE OR REPLACE FUNCTION unlock_if_eq_1(x integer) RETURNS integer AS $$
		DECLARE
		BEGIN
			IF x = 1 THEN
				perform pg_advisory_unlock(1);
				perform pg_sleep(1);
				perform pg_advisory_lock(2);
			END IF;
			return x;
		END;
	$$ LANGUAGE plpgsql
	"""

setup_cmd = [
	'drop extension if exists perfmon cascade',
	'drop table if exists foo cascade',
	'drop table if exists bar cascade',
	'drop table if exists tt cascade',
	'create extension perfmon',
	'create table foo(c1 integer, c2 text) distributed randomly;',
	'create table bar(c1 integer, c2 boolean) distributed replicated;',
	'create table tt(c1 integer, c2 text)',
	'insert into foo select 1 from gp_dist_random(\'gp_id\')',
	'insert into foo select i, md5(random()::text) from generate_series(1, 1000000) as i',
	'delete from foo where gp_segment_id > 0 and c1 = 1',
	'insert into bar select i, i%2=1 from generate_series(1, 500000) as i',
	'insert into tt select i, md5(random()::text) from generate_series(1, 1000000) as i ',
	'analyze foo',
	'analyze bar',
	'analyze tt',
	unlock_if_eq_1,
]

teardown_cmd = [
	'drop table foo cascade',
	'drop table bar cascade',
	'drop extension perfmon cascade',
]

tests = [
	test_deadlock,
	test_simple_query,
	test_concurrent_access,
	test_nested_call,
#	test_trigger,
	test_costs,
#	test_buffers,
	test_timing,
#	test_formats,
#	test_timing_buffers_conflicts,
	test_insert_on_conflict,
	test_skip_explain_analyze,
	test_init_plan,
	test_qe_cache_query,
]

def setup(con):
	''' Creates pg_query_state extension, creates tables for tests, fills it with data '''
	print('setting up...')
	try:
		cur = con.cursor()
		for cmd in setup_cmd:
			cur.execute(cmd)
		con.commit()
		cur.close()
	except Exception as e:
		raise SetupException('Setup failed: %s' % e)
	print('done!')

def teardown(con):
	''' Drops table and extension '''
	print('tearing down...')
	try:
		cur = con.cursor()
		for cmd in teardown_cmd:
			cur.execute(cmd)
		con.commit()
		cur.close()
	except Exception as e:
		raise TeardownException('Teardown failed: %s' % e)
	print('done!')

def main(config):
	''' Main test function '''
	conn_params = {
		key:config.__dict__[key] for key in ('host', 'port', 'user', 'database', 'password')
	}

	if config.tpcds_setup:
		print('Setup database for TPC-DS bench')
		tpcds.setup_tpcds(conn_params)
		print('Database is setup successfully')
		return

	if config.tpcds_run:
		print('Starting stress test')
		tpcds.run_tpcds(conn_params)
		print('Stress finished successfully')
		return

	# run default tests
	init_conn = psycopg2.connect(**conn_params)
	setup(init_conn)
	for i, test in enumerate(tests):
		if test.__doc__:
			descr = test.__doc__
		else:
			descr = 'test case %d' % (i+1)
		print(("%s..." % descr))
		sys.stdout.flush()
		test(conn_params)
		print('ok!')
	teardown(init_conn)
	init_conn.close()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Query state of running backends tests')

	parser.add_argument('--host', default='localhost', help='postgres server host')
	parser.add_argument('--port', type=int, default=5432, help='postgres server port')
	parser.add_argument('--user', dest='user', default='postgres', help='user name')
	parser.add_argument('--database', dest='database', default='postgres', help='database name')
	parser.add_argument('--password', dest='password', nargs=0, action=PasswordPromptAction, default='', help='password')
	parser.add_argument('--tpc-ds-setup', dest='tpcds_setup', action='store_true', help='setup database to run TPC-DS benchmark')
	parser.add_argument('--tpc-ds-run', dest='tpcds_run', action='store_true', help='run only stress test based on TPC-DS benchmark')

	args = parser.parse_args()
	main(args)

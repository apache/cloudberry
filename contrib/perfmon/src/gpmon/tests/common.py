'''
common.py
Copyright (c) 2016-2024, Postgres Professional
'''

import psycopg2
import psycopg2.extensions
import select
import time

BACKEND_IS_IDLE_INFO = 'INFO:  state of backend is idle\n'
BACKEND_IS_ACTIVE_INFO = 'INFO:  state of backend is active\n'

def wait(conn):
	"""wait for some event on connection to postgres"""
	while 1:
		state = conn.poll()
		if state == psycopg2.extensions.POLL_OK:
			break
		elif state == psycopg2.extensions.POLL_WRITE:
			select.select([], [conn.fileno()], [])
		elif state == psycopg2.extensions.POLL_READ:
			select.select([conn.fileno()], [], [])
		else:
			raise psycopg2.OperationalError("poll() returned %s" % state)

def n_async_connect(config, n=1):
	"""establish n asynchronious connections to the postgres with specified config"""

	aconfig = config.copy()
	aconfig['async'] = True

	result = []
	for _ in range(n):
		conn = psycopg2.connect(**aconfig)
		wait(conn)
		result.append(conn)
	return result

def n_close(conns):
	"""close connections to postgres"""

	for conn in conns:
		conn.close()

def pg_query_state_locks(config, pid, conn, verbose=False, costs=False, timing=False, \
								buffers=False, triggers=False, format='text'):
	"""
	Get query state from backend with specified pid and optional parameters.
	Save any warning, info, notice and log data in global variable 'notices'
	"""

	curs = conn.cursor()
	curs.callproc('pg_query_state', (pid, verbose, costs, timing, buffers, triggers, format))
	wait(conn)
	result = curs.fetchall()
	notices = conn.notices[:]

	return result, notices

def pg_query_state(config, pid, verbose=False, costs=False, timing=False, \
								buffers=False, triggers=False, format='text'):
	"""
	Get query state from backend with specified pid and optional parameters.
	Save any warning, info, notice and log data in global variable 'notices'
	"""

	conn = psycopg2.connect(**config)
	curs = conn.cursor()
	curs.callproc('pg_query_state', (pid, verbose, costs, timing, buffers, triggers, format))
	result = curs.fetchall()
	notices = conn.notices[:]
	conn.close()

	return result, notices

def onetime_query_state_locks(config, acon_query, acon_pg, query, args={}, num_workers=0):
	"""
	Get intermediate state of 'query' on connection 'acon_query' after number of 'steps'
	of node executions from start of query
	"""

	curs_query = acon_query.cursor()
	curs_pg = acon_pg.cursor()
	set_guc(acon_query, 'optimizer', 'off')
	curs_query.execute("select pg_advisory_lock(1) FROM gp_dist_random('gp_id') where gp_segment_id = 0")
	curs_pg.execute("select pg_advisory_lock(2) FROM gp_dist_random('gp_id') where gp_segment_id = 0")
	wait(acon_query)
	wait(acon_pg)
	curs_pg.execute("select pg_advisory_lock(1) FROM gp_dist_random('gp_id') where gp_segment_id = 0")
	# set_guc(acon_query, 'enable_mergejoin', 'off')
	# set_guc(acon_query, 'max_parallel_workers_per_gather', num_workers)
	curs_query.execute(query)
	# extract current state of query progress
	MAX_PG_QS_RETRIES = 10
	DELAY_BETWEEN_RETRIES = 0.3
	pg_qs_args = {
			'config': config,
			'pid': acon_query.get_backend_pid(),
			'conn': acon_pg
			}
	for k, v in args.items():
		pg_qs_args[k] = v
	n_retries = 0

	wait(acon_pg)

	while True:
		result, notices = pg_query_state_locks(**pg_qs_args)
		n_retries += 1
		if len(result) > 0 :
			break
		if n_retries >= MAX_PG_QS_RETRIES:
			# pg_query_state callings don't return any result, more likely run
			# query has completed
			break
		time.sleep(DELAY_BETWEEN_RETRIES)

	curs_pg.execute("select pg_advisory_unlock(2) FROM  gp_dist_random('gp_id') where gp_segment_id = 0;")
	wait(acon_pg)
	wait(acon_query)

	set_guc(acon_query, 'enable_mergejoin', 'on')
	curs_query.execute("select pg_advisory_unlock(2) FROM  gp_dist_random('gp_id') where gp_segment_id = 0;")
	curs_pg.execute("select pg_advisory_unlock(1) FROM  gp_dist_random('gp_id') where gp_segment_id = 0;")
	return result, notices

def onetime_query_state(config, async_conn, query, args={}, num_workers=0):
	"""
	Get intermediate state of 'query' on connection 'async_conn' after number of 'steps'
	of node executions from start of query
	"""

	acurs = async_conn.cursor()

	set_guc(async_conn, 'enable_mergejoin', 'off')
	set_guc(async_conn, 'max_parallel_workers_per_gather', num_workers)
	set_guc(async_conn, 'optimizer', 'off')
	acurs.execute(query)

	# extract current state of query progress
	MAX_PG_QS_RETRIES = 10
	DELAY_BETWEEN_RETRIES = 0.1
	pg_qs_args = {
			'config': config,
			'pid': async_conn.get_backend_pid()
			}
	for k, v in args.items():
		pg_qs_args[k] = v
	n_retries = 0
	while True:
		result, notices = pg_query_state(**pg_qs_args)
		n_retries += 1
		if len(result) > 0:
			break
		if n_retries >= MAX_PG_QS_RETRIES:
			# pg_query_state callings don't return any result, more likely run
			# query has completed
			break
		time.sleep(DELAY_BETWEEN_RETRIES)
	wait(async_conn)

	set_guc(async_conn, 'enable_mergejoin', 'on')
	return result, notices

def set_guc(async_conn, param, value):
	acurs = async_conn.cursor()
	acurs.execute('set %s to %s' % (param, value))
	wait(async_conn)

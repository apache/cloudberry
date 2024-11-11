'''
test_cases.py
Copyright (c) 2016-2024, Postgres Professional
'''

import json
import re
import select
import time
import xml.etree.ElementTree as ET

import psycopg2
import yaml

import common

def test_deadlock(config):
	"""test when two backends try to extract state of each other"""

	acon1, acon2 = common.n_async_connect(config, 2)
	acurs1 = acon1.cursor()
	acurs2 = acon2.cursor()

	while True:
		acurs1.callproc('pg_query_state', (acon2.get_backend_pid(),))
		acurs2.callproc('pg_query_state', (acon1.get_backend_pid(),))

		# listen acon1, acon2 with timeout = 3 sec to determine deadlock
		r, w, x = select.select([acon1.fileno(), acon2.fileno()], [], [], 3)
		assert (r or w or x), "Deadlock is happened under cross reading of query states"

		common.wait(acon1)
		common.wait(acon2)

		# exit from loop if one backend could read state of execution 'pg_query_state'
		# from other backend
		if acurs1.fetchone() or acurs2.fetchone():
			break

	common.n_close((acon1, acon2))

def test_simple_query(config):
	"""test statistics of simple query"""
	acon1, acon2 = common.n_async_connect(config, 2)
	query = 'select count(*) from foo join bar on foo.c1=bar.c1 and unlock_if_eq_1(foo.c1)=bar.c1'

	expected = r"""Query Text: select count\(\*\) from foo join bar on foo.c1=bar.c1 and unlock_if_eq_1\(foo.c1\)=bar.c1
Finalize Aggregate \(node status: .*\) \(actual rows=\d+, loops=1\)
  ->  Gather Motion \d+:1  \(slice1; segments: \d+\) \(node status: .*\) \(actual rows=\d+, loops=1\)
        ->  Partial Aggregate \(node status: .*\) \(actual rows=\d+, loops=1\)
              ->  Hash Join \(node status: Executing\) \(actual rows=\d+, loops=1\)
                    Hash Cond: \(bar.c1 = foo.c1\)
                    Join Filter: \(unlock_if_eq_1\(foo.c1\) = bar.c1\)
                    ->  Seq Scan on bar \(node status: Executing\) \(actual rows=\d+, loops=1\)
                    ->  Hash \(node status: Executing\) \(actual rows=\d+, loops=1\)
                          ->  Seq Scan on foo \(node status: .*\) \(actual rows=\d+, loops=1\)""" 

	qs, _ = common.onetime_query_state_locks(config, acon1, acon2, query)
	assert qs[0][0] == acon1.get_backend_pid()
	assert qs[0][1] == 0
	assert qs[0][2] == query
	if not re.match(expected, qs[0][3]):
		print(qs[0][3])
	assert re.match(expected, qs[0][3])
	assert qs[0][4] == None
	# assert qs[0][0] == acon.get_backend_pid() and qs[0][1] == 0 \
	# 	and qs[0][2] == query and re.match(expected, qs[0][3]) and qs[0][4] == None

	common.n_close((acon1, acon2))

def test_concurrent_access(config):
	"""test when two backends compete with each other to extract state from third running backend"""

	acon1, acon2, acon3 = common.n_async_connect(config, 3)
	acurs1, acurs2, acurs3 = acon1.cursor(), acon2.cursor(), acon3.cursor()
	query = 'select pg_sleep(2)'

	common.set_guc(acon3, 'max_parallel_workers_per_gather', 0)
	acurs3.execute(query)
	time.sleep(0.1)
	acurs1.callproc('pg_query_state', (acon3.get_backend_pid(),))
	acurs2.callproc('pg_query_state', (acon3.get_backend_pid(),))
	common.wait(acon1)
	common.wait(acon2)
	common.wait(acon3)

	qs1, qs2 = acurs1.fetchall(), acurs2.fetchall()
	assert len(qs1) == len(qs2) == 1 \
		and qs1[0][0] == qs2[0][0] == acon3.get_backend_pid() \
		and qs1[0][1] == qs2[0][1] == 0 \
		and qs1[0][2] == qs2[0][2] == query \
		and len(qs1[0][3]) > 0 and len(qs2[0][3]) > 0

	common.n_close((acon1, acon2, acon3))

# in hashdata-lightning, only print the top level query state
def test_nested_call(config):
	"""test statistics under calling function"""

	acon1, acon2 = common.n_async_connect(config, 2)
	util_conn = psycopg2.connect(**config)
	util_curs = util_conn.cursor()
	create_function = """
		create or replace function n_join_foo_bar() returns integer as $$
			begin
				return (select count(*) from foo join bar on foo.c1=bar.c1 and unlock_if_eq_1(foo.c1)=bar.c1);
			end;
		$$ language plpgsql"""
	drop_function = 'drop function n_join_foo_bar()'
	call_function = 'select * from n_join_foo_bar()'
	nested_query1 = '(select count(*) from foo join bar on foo.c1=bar.c1 and unlock_if_eq_1(foo.c1)=bar.c1)'
	expected = r"""Query Text: select \* from n_join_foo_bar\(\)
Result \(node status: .*\) \(actual rows=\d+, loops=1\)
  InitPlan 1 \(returns \$0\)
    ->  Result \(node status: .*\) \(actual rows=\d+, loops=1\)"""
	util_curs.execute(create_function)
	util_conn.commit()

	qs, notices = common.onetime_query_state_locks(config, acon1, acon2, call_function)

	assert len(qs) == 1
	assert qs[0][0] == acon1.get_backend_pid()
	assert qs[0][1] == 0
	assert qs[0][2] == call_function
	if not re.match(expected, qs[0][3]):
		print(qs[0][3])
	assert re.match(expected, qs[0][3])
	assert qs[0][4] == None
	assert len(notices) == 0

	util_curs.execute(drop_function)

	util_conn.close()
	common.n_close((acon1, acon2))

def test_insert_on_conflict(config):
	"""test statistics on conflicting tuples under INSERT ON CONFLICT query"""

	acon, = common.n_async_connect(config)
	util_conn = psycopg2.connect(**config)
	util_curs = util_conn.cursor()
	add_field_uniqueness = 'alter table tt add constraint unique_c1 unique(c1)'
	drop_field_uniqueness = 'alter table tt drop constraint unique_c1'
	query = 'insert into tt select i, md5(random()::text) from generate_series(1, 30000) as i on conflict do nothing'

	expected = r"""Query Text: insert into tt select i, md5\(random\(\)::text\) from generate_series\(1, 30000\) as i on conflict do nothing
Insert on tt \(node status: .*\) \(actual rows=\d+, loops=1\)
  Conflict Resolution: NOTHING
  Conflicting Tuples: \d+
  ->  Redistribute Motion 1:\d+  \(slice1; segments: 1\) \(node status: .*\) \(actual rows=\d+, loops=1\)
        Hash Key: i.i
        ->  Function Scan on generate_series i \(node status: .*\) \(actual rows=\d+, loops=1\)"""

	util_curs.execute(add_field_uniqueness)
	util_conn.commit()

	qs, notices = common.onetime_query_state(config, acon, query)
	assert qs[0][0] == acon.get_backend_pid()
	assert qs[0][1] == 0
	assert qs[0][2] == query
	if not re.match(expected, qs[0][3]):
		print(qs[0][3])
	assert re.match(expected, qs[0][3]) 
	assert qs[0][4] == None
	assert len(notices) == 0

	util_curs.execute(drop_field_uniqueness)
	util_conn.commit()

	util_conn.close()
	common.n_close((acon,))

def test_trigger(config):
	"""test trigger statistics"""

	acon, = common.n_async_connect(config)
	acurs = acon.cursor()
	util_conn = psycopg2.connect(**config)
	util_curs = util_conn.cursor()
	create_trigger_function = """
		create or replace function unique_c1_in_foo() returns trigger as $$
			begin
				if new.c1 in (select c1 from foo) then
					return null;
				end if;
				return new;
			end;
		$$ language plpgsql"""
	create_trigger = """
		create trigger unique_foo_c1
			before insert or update of c1 on foo for row
			execute procedure unique_c1_in_foo()"""
	drop_temps = 'drop function unique_c1_in_foo() cascade'
	query = 'insert into foo select i, md5(random()::text) from generate_series(1, 10000) as i'
	expected_upper = r"""Insert on foo \(Current loop: actual rows=\d+, loop number=1\)
  ->  Function Scan on generate_series i \(Current loop: actual rows=\d+, loop number=1\)"""
	trigger_suffix = r"""Trigger unique_foo_c1: calls=\d+"""

	util_curs.execute(create_trigger_function)
	util_curs.execute(create_trigger)
	util_conn.commit()

	qs, notices = common.onetime_query_state(config, acon, query, {'triggers': True})
	assert qs[0][0] == acon.get_backend_pid() and qs[0][1] == 0 \
		and qs[0][2] == query and re.match(expected_upper, qs[0][3]) \
		and qs[0][4] == None
	assert len(notices) == 0

	qs, notices = common.onetime_query_state(config, acon, query, {'triggers': False})
	assert qs[0][0] == acon.get_backend_pid() and qs[0][1] == 0 \
		and qs[0][2] == query and re.match(expected_upper, qs[0][3]) \
		and qs[0][4] == None
	assert len(notices) == 0

	util_curs.execute(drop_temps)

	util_conn.close()
	common.n_close((acon,))

def test_costs(config):
	"""test plan costs"""

	acon1, acon2 = common.n_async_connect(config, 2)
	
	
	query = 'select count(*) from foo join bar on foo.c1=bar.c1 and unlock_if_eq_1(foo.c1)=bar.c1'
	expected = r"""Query Text: select count\(\*\) from foo join bar on foo.c1=bar.c1 and unlock_if_eq_1\(foo.c1\)=bar.c1
Finalize Aggregate  \(cost=\d+.\d+..\d+.\d+ rows=\d+ width=\d+\) \(node status: .*\) \(actual rows=\d+, loops=1\)
  ->  Gather Motion \d+:1  \(slice1; segments: \d+\)  \(cost=\d+.\d+..\d+.\d+ rows=\d+ width=\d+\) \(node status: .*\) \(actual rows=\d+, loops=1\)
        ->  Partial Aggregate  \(cost=\d+.\d+..\d+.\d+ rows=\d+ width=\d+\) \(node status: .*\) \(actual rows=\d+, loops=1\)
              ->  Hash Join  \(cost=\d+.\d+..\d+.\d+ rows=\d+ width=\d+\) \(node status: .*\) \(actual rows=\d+, loops=1\)
                    Hash Cond: \(bar.c1 = foo.c1\)
                    Join Filter: \(unlock_if_eq_1\(foo.c1\) = bar.c1\)
                    ->  Seq Scan on bar  \(cost=\d+.\d+..\d+.\d+ rows=\d+ width=\d+\) \(node status: .*\) \(actual rows=\d+, loops=1\)
                    ->  Hash  \(cost=\d+.\d+..\d+.\d+ rows=\d+ width=\d+\) \(node status: .*\) \(actual rows=\d+, loops=1\)
                          ->  Seq Scan on foo  \(cost=\d+.\d+..\d+.\d+ rows=\d+ width=\d+\) \(node status: .*\) \(actual rows=\d+, loops=1\)"""

	qs, notices = common.onetime_query_state_locks(config, acon1, acon2, query, {'costs': True})

	assert len(qs) == 1
	if not re.match(expected, qs[0][3]):
		print(qs[0][3])
	assert re.match(expected, qs[0][3])
	assert len(notices) == 0

	common.n_close((acon1, acon2))

def test_buffers(config):
	"""test buffer statistics"""

	acon1, acon2 = common.n_async_connect(config, 2)
	query = 'select count(*) from foo join bar on foo.c1=bar.c1 and unlock_if_eq_1(foo.c1)=bar.c1'
	temporary = r"""Aggregate \(Current loop: actual rows=0, loop number=1\)
  ->  Hash Join \(Current loop: actual rows=\d+, loop number=1\)
        Hash Cond: \(foo.c1 = bar.c1\)
        Join Filter: \(unlock_if_eq_1\(foo.c1\) = bar.c1\)"""
	expected = temporary
	expected_15 = temporary
	expected += r"""
        Buffers: shared hit=\d+, temp read=\d+ written=\d+"""
	expected_15 += r"""
        Buffers: shared hit=\d+, temp written=\d+"""
	temporary = r"""
        ->  Seq Scan on foo \(Current loop: actual rows=\d+, loop number=1\)
              Buffers: [^\n]*
        ->  Hash \(Current loop: actual rows=500000, loop number=1\)
              Buckets: \d+  Batches: \d+  Memory Usage: \d+kB
              Buffers: shared hit=\d+, temp written=\d+
              ->  Seq Scan on bar \(Current loop: actual rows=\d+, loop number=1\)
                    Buffers: .*"""
	expected += temporary
	expected_15 += temporary

	common.set_guc(acon1, 'pg_query_state.enable_buffers', 'on')

	qs, notices = common.onetime_query_state_locks(config, acon1, acon2, query, {'buffers': True})

	assert len(qs) == 2
	assert (re.match(expected, qs[0][3]) or re.match(expected_15, qs[0][3]))
	assert len(notices) == 0

	common.n_close((acon1, acon2))

def test_timing(config):
	"""test timing statistics"""

	acon1, acon2 = common.n_async_connect(config, 2)
	query = 'select count(*) from foo join bar on foo.c1=bar.c1 and unlock_if_eq_1(foo.c1)=bar.c1'
	expected = r"""Query Text: select count\(\*\) from foo join bar on foo.c1=bar.c1 and unlock_if_eq_1\(foo.c1\)=bar.c1
Finalize Aggregate \(node status: .*\) \(actual time=\d+.\d+.* rows=\d+, loops=1\)
  ->  Gather Motion \d+:1  \(slice1; segments: \d+\) \(node status: .*\) \(actual time=\d+.\d+.* rows=\d+, loops=1\)
        ->  Partial Aggregate \(node status: .*\) \(actual time=\d+.\d+.* rows=\d+, loops=1\)
              ->  Hash Join \(node status: .*\) \(actual time=\d+.\d+.* rows=\d+, loops=1\)
                    Hash Cond: \(bar.c1 = foo.c1\)
                    Join Filter: \(unlock_if_eq_1\(foo.c1\) = bar.c1\)
                    ->  Seq Scan on bar \(node status: .*\) \(actual time=\d+.\d+.* rows=\d+, loops=1\)
                    ->  Hash \(node status: .*\) \(actual time=\d+.\d+.* rows=\d+, loops=1\)
                          ->  Seq Scan on foo \(node status: .*\) \(actual time=\d+.\d+.* rows=\d+, loops=1\)"""
	common.set_guc(acon1, 'pg_query_state.enable_timing', 'on')

	qs, notices = common.onetime_query_state_locks(config, acon1, acon2, query, {'timing': True})
	if not re.match(expected, qs[0][3]):
		print(qs[0][3])
	assert re.match(expected, qs[0][3])
	assert len(notices) == 0

	common.n_close((acon1, acon2))

def check_plan(plan):
	assert 'Current loop' in plan
	cur_loop = plan['Current loop']
	assert 'Actual Loop Number' in cur_loop\
		and 'Actual Rows' in cur_loop

	if not 'Plans' in plan:
		return

	for subplan in plan['Plans']:
		check_plan(subplan)

def check_xml(root):
	prefix = '{http://www.postgresql.org/2009/explain}'
	for plan in root.iter(prefix + 'Plan'):
		cur_loop = plan.find(prefix + 'Current-loop')
		assert cur_loop != None \
			and cur_loop.find(prefix + 'Actual-Loop-Number') != None \
			and cur_loop.find(prefix + 'Actual-Rows') != None

def test_formats(config):
	"""test all formats of pg_query_state output"""

	acon, = common.n_async_connect(config)
	query = 'select count(*) from foo join bar on foo.c1=bar.c1'
	expected = r"""Query Text: select count\(\*\) from foo join bar on foo.c1=bar.c1
Finalize Aggregate \(node status: .*\) \(actual rows=\d+, loops=1\)
  ->  Gather Motion \d+:1  \(slice1; segments: \d+\) \(node status: .*\) \(actual rows=\d+, loops=1\)
        ->  Partial Aggregate \(node status: .*\) \(actual rows=\d+, loops=1\)
              ->  Hash Join \(node status: .*\) \(actual rows=\d+, loops=1\)
                    Hash Cond: \(foo.c1 = bar.c1\)
                    ->  Seq Scan on foo \(node status: .*\) \(actual rows=\d+, loops=1\)
                    ->  Hash \(node status: .*\) \(actual rows=\d+, loops=1\)
                          ->  Seq Scan on bar \(node status: .*\) \(actual rows=\d+, loops=1\)"""

	qs, notices = common.onetime_query_state(config, acon, query, {'format': 'text'})
	assert len(qs) == 1 
	assert re.match(expected, qs[0][3])
	assert len(notices) == 0

	qs, notices = common.onetime_query_state(config, acon, query, {'format': 'json'})
	try:
		js_obj = json.loads(qs[0][3])
	except ValueError:
		assert False, 'Invalid json format'
	assert len(qs) == 1
	assert len(notices) == 0
	check_plan(js_obj['Plan'])

	qs, notices = common.onetime_query_state(config, acon, query, {'format': 'xml'})
	assert len(qs) == 1
	assert len(notices) == 0
	try:
		xml_root = ET.fromstring(qs[0][3])
	except:
		assert False, 'Invalid xml format'
	check_xml(xml_root)

	qs, _ = common.onetime_query_state(config, acon, query, {'format': 'yaml'})
	try:
		yaml_doc = yaml.load(qs[0][3], Loader=yaml.FullLoader)
	except:
		assert False, 'Invalid yaml format'
	assert len(qs) == 1
	assert len(notices) == 0
	check_plan(yaml_doc['Plan'])

	common.n_close((acon,))

def test_timing_buffers_conflicts(config):
	"""test when caller requests timing and buffers but counterpart turned off its"""

	acon, = common.n_async_connect(config)
	query = 'select count(*) from foo join bar on foo.c1=bar.c1'
	timing_pattern = '(?:running time=\d+.\d+)|(?:actual time=\d+.\d+..\d+.\d+)'
	buffers_pattern = 'Buffers:'

	common.set_guc(acon, 'pg_query_state.enable_timing', 'off')
	common.set_guc(acon, 'pg_query_state.enable_buffers', 'off')

	qs, notices = common.onetime_query_state(config, acon, query, {'timing': True, 'buffers': False})
	assert len(qs) == 1 and not re.search(timing_pattern, qs[0][3])
	assert notices == ['WARNING:  timing statistics disabled\n']

	qs, notices = common.onetime_query_state(config, acon, query, {'timing': False, 'buffers': True})
	assert len(qs) == 1 and not re.search(buffers_pattern, qs[0][3])
	assert notices == ['WARNING:  buffers statistics disabled\n']

	qs, notices = common.onetime_query_state(config, acon, query, {'timing': True, 'buffers': True})
	assert len(qs) == 1 and not re.search(timing_pattern, qs[0][3]) \
						 and not re.search(buffers_pattern, qs[0][3])
	assert len(notices) == 2 and 'WARNING:  timing statistics disabled\n' in notices \
							 and 'WARNING:  buffers statistics disabled\n' in notices

	common.n_close((acon,))
def test_skip_explain_analyze(config):
	"""test skip explain analyze query"""

	acon1, acon2 = common.n_async_connect(config, 2)
	query = 'explain analyze select count(*) from foo join bar on foo.c1=bar.c1 and unlock_if_eq_1(foo.c1)=bar.c1'
	common.set_guc(acon1, 'pg_query_state.enable_timing', 'off')
	common.set_guc(acon1, 'pg_query_state.enable_buffers', 'off')

	qs, notices = common.onetime_query_state_locks(config, acon1, acon2, query, {})
	assert len(notices) > 0
	assert notices[0] == 'INFO:  state of backend is active\n'
	common.n_close((acon1, acon2))

	# in hashdata-lightning, only print the top level query state
def test_init_plan(config):
	"""test statistics of init plan"""
	acon1, acon2 = common.n_async_connect(config, 2)
	query = 'select (select count(*) from foo join bar on foo.c1=bar.c1 and unlock_if_eq_1(foo.c1)=bar.c1) as t, c1 from bar'
	expected = r"""Query Text: select \(select count\(\*\) from foo join bar on foo.c1=bar.c1 and unlock_if_eq_1\(foo.c1\)=bar.c1\) as t, c1 from bar
Gather Motion 1:1  \(slice1; segments: 1\) \(node status: Initialize\) \(actual rows=0, loops=1\)
  InitPlan 1 \(returns \$0\)  \(slice2\)
    ->  Finalize Aggregate \(node status: Initialize\) \(actual rows=0, loops=1\)
          ->  Gather Motion \d+:1  \(slice3; segments: \d+\) \(node status: .*\) \(actual rows=0, loops=1\)
                ->  Partial Aggregate \(node status: Initialize\) \(actual rows=0, loops=1\)
                      ->  Hash Join \(node status: Initialize\) \(actual rows=0, loops=1\)
                            Hash Cond: \(bar_1.c1 = foo.c1\)
                            Join Filter: \(unlock_if_eq_1\(foo.c1\) = bar_1.c1\)
                            ->  Seq Scan on bar bar_1 \(node status: Initialize\) \(actual rows=0, loops=1\)
                            ->  Hash \(node status: Initialize\) \(actual rows=0, loops=1\)
                                  ->  Seq Scan on foo \(node status: Initialize\) \(actual rows=0, loops=1\)
  ->  Seq Scan on bar \(node status: Initialize\) \(actual rows=0, loops=1\)"""

	qs,notices = common.onetime_query_state_locks(config, acon1, acon2, query, {})
	assert qs[0][0] == acon1.get_backend_pid()
	assert qs[0][1] == 0
	assert qs[0][2] == query
	if not re.match(expected, qs[0][3]):
		print(qs[0][3])
	assert re.match(expected, qs[0][3])
	assert qs[0][4] == None
	# assert qs[0][0] == acon.get_backend_pid() and qs[0][1] == 0 \
	# 	and qs[0][2] == query and re.match(expected, qs[0][3]) and qs[0][4] == None

	common.n_close((acon1, acon2))
def test_qe_cache_query(config):
	"""test qe can cache query state when finished"""
	acon1, acon2 = common.n_async_connect(config, 2)

	query = 'select count(*) from foo left join tt on foo.c1 = tt.c1 and tt.c1 < 10 where unlock_if_eq_1(foo.c1) < 100'
	expected = r"""Query Text: select count\(\*\) from foo left join tt on foo.c1 = tt.c1 and tt.c1 < 10 where unlock_if_eq_1\(foo.c1\) < 100
Finalize Aggregate \(node status: .*\) \(actual rows=0, loops=1\)
  ->  Gather Motion \d+:1  \(slice1; segments: \d+\) \(node status: .*\) \(actual rows=0, loops=1\)
        ->  Partial Aggregate \(node status: .*\) \(actual rows=\d+, loops=1\)
              ->  Hash Left Join \(node status: Executing\) \(actual rows=\d+, loops=1\)
                    Hash Cond: \(foo.c1 = tt.c1\)
                    ->  Seq Scan on foo \(node status: Executing\) \(actual rows=\d+, loops=1\)
                          Filter: \(unlock_if_eq_1\(c1\) < 100\)
                    ->  Hash \(node status: .*\) \(actual rows=\d+, loops=1\)
                          ->  Broadcast Motion \d+:\d+  \(slice2; segments: \d+\) \(node status: Finished\) \(actual rows=\d+, loops=1\)
                                ->  Seq Scan on tt \(node status: Finished\) \(actual rows=\d+, loops=1\)
                                      Filter: \(c1 < 10\)"""
	time.sleep(1)
	qs,notices = common.onetime_query_state_locks(config, acon1, acon2, query, {})
	assert qs[0][0] == acon1.get_backend_pid()
	assert qs[0][1] == 0
	assert qs[0][2] == query
	if not re.match(expected, qs[0][3]):
		print(qs[0][3])
	assert re.match(expected, qs[0][3])
	assert qs[0][4] == None

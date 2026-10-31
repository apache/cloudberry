#!/usr/bin/env python3
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

"""Check gpstate's report for content 0 in the hot_standby schedule."""

import argparse
from contextlib import ExitStack, closing
import os
import subprocess
import time

from gppylib.db import dbconn
from gppylib.gparray import GpArray


def query(conn, sql, args=()):
    with conn.cursor() as cursor:
        cursor.execute(sql, args)
        return cursor.fetchall() if cursor.description else []


def scalar(conn, sql, args=()):
    return query(conn, sql, args)[0][0]


def wait_until(description, predicate):
    deadline = time.monotonic() + 60
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError('timed out waiting for ' + description)
        time.sleep(0.1)


def streaming(conn):
    return scalar(conn, "SELECT count(*) = 1 FROM pg_stat_replication "
                  "WHERE application_name = 'gp_walreceiver' AND state = 'streaming'")


def check_status(coordinator_conn, mirror_conn, mirror, disconnected):
    # Check the premise independently of the status inferred by gpstate.
    assert scalar(mirror_conn, 'SELECT pg_is_in_recovery()'), 'mirror was promoted'
    assert scalar(coordinator_conn,
                  "SELECT role = 'm' AND status = 'u' FROM gp_segment_configuration "
                  "WHERE dbid = %s", (mirror.dbid,)), 'mirror catalog state changed'

    env = os.environ.copy()
    env['PGOPTIONS'] = '-c gp_role=utility'
    ready = subprocess.run([os.path.join(os.environ['GPHOME'], 'bin', 'pg_isready'),
                            '-q', '-h', mirror.hostname, '-p', str(mirror.port),
                            '-d', 'template1'], env=env, timeout=15)
    assert ready.returncode == 0, 'hot standby does not accept connections'

    result = subprocess.run([os.path.join(os.environ['GPHOME'], 'bin', 'gpstate'), '-s'],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            universal_newlines=True, timeout=60)
    # Warnings about disconnected replication are legitimate; a command error is not.
    assert result.returncode in (0, 1), result.stdout

    records = []
    record = None
    for line in result.stdout.splitlines():
        message = line.split(':-', 1)[-1].strip()
        if message == 'Segment Info':
            record = {}
            records.append(record)
        elif record is not None and '=' in message:
            key, value = message.split('=', 1)
            record[key.strip()] = value.strip().rstrip('<').rstrip()

    matches = [r for r in records
               if r.get('Hostname') == mirror.hostname
               and r.get('Port') == str(mirror.port)
               and r.get('Datadir') == mirror.datadir]
    assert len(matches) == 1, 'cannot identify mirror in gpstate output:\n' + result.stdout
    status = matches[0].get('Segment status', '')
    # The process is accepting SQL and is still in recovery. Replication warnings
    # belong in the replication fields; a failed status fetch must not pass either.
    assert status == 'Up', '{} hot standby reported as {!r}'.format(
        'disconnected' if disconnected else 'streaming', status)
    assert scalar(mirror_conn, 'SELECT pg_is_in_recovery()'), 'mirror was promoted during gpstate'
    print('content 0 mirror: recovery=true, status=' + status)


def check_disconnected(coordinator_conn, primary_conn, mirror_conn, coordinator, mirror):
    original = scalar(mirror_conn, 'SHOW primary_conninfo')
    overrides = query(mirror_conn,
                      "SELECT setting FROM pg_file_settings WHERE name = 'primary_conninfo' "
                      "AND sourcefile = current_setting('data_directory') || '/postgresql.auto.conf' "
                      "ORDER BY seqno DESC LIMIT 1")
    changed_conninfo = False
    query(coordinator_conn, "SELECT gp_inject_fault_infinite('fts_probe', 'skip', %s)",
          (coordinator.dbid,))
    try:
        query(coordinator_conn, 'SELECT gp_request_fts_probe_scan()')
        query(coordinator_conn, "SELECT gp_wait_until_triggered_fault('fts_probe', 1, %s)",
              (coordinator.dbid,))
        # /dev/null cannot contain a Unix socket, on either local or remote mirrors.
        # Prevent reconnection instead of racing a restarting walreceiver process.
        changed_conninfo = True
        query(mirror_conn, "ALTER SYSTEM SET primary_conninfo = 'host=/dev/null port=1 connect_timeout=2'")
        query(mirror_conn, 'SELECT pg_reload_conf()')
        wait_until('the WAL connection to disappear', lambda: scalar(primary_conn,
                   "SELECT count(*) = 0 FROM pg_stat_replication WHERE application_name = 'gp_walreceiver'"))
        check_status(coordinator_conn, mirror_conn, mirror, disconnected=True)
    finally:
        try:
            if changed_conninfo:
                if overrides:
                    query(mirror_conn, 'ALTER SYSTEM SET primary_conninfo = %s', (overrides[0][0],))
                else:
                    query(mirror_conn, 'ALTER SYSTEM RESET primary_conninfo')
                query(mirror_conn, 'SELECT pg_reload_conf()')
                wait_until('primary_conninfo to be restored', lambda:
                           scalar(mirror_conn, 'SHOW primary_conninfo') == original)
                wait_until('replication to resume', lambda: streaming(primary_conn))
        finally:
            query(coordinator_conn, "SELECT gp_inject_fault('fts_probe', 'reset', %s)",
                  (coordinator.dbid,))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scenario', choices=('streaming', 'disconnected'))
    parser.add_argument('--dbname', default='isolation2-hot-standby')
    args = parser.parse_args()

    array = GpArray.initFromCatalog(dbconn.DbURL(dbname=args.dbname), utility=True)
    pair = next(p for p in array.segmentPairs if p.primaryDB.content == 0)
    primary, mirror = pair.primaryDB, pair.mirrorDB
    assert mirror is not None, 'content 0 has no mirror'

    with ExitStack() as stack:
        def connect(segment, dbname, utility=True):
            url = dbconn.DbURL(hostname=segment.hostname, port=segment.port,
                               dbname=dbname, timeout=10, retries=1)
            conn = stack.enter_context(closing(dbconn.connect(
                url, utility=utility, unsetSearchPath=False, logConn=False)))
            conn.autocommit = True
            query(conn, "SET statement_timeout = '15s'")
            return conn

        coordinator_conn = connect(array.coordinator, args.dbname, utility=False)
        primary_conn = connect(primary, 'template1')
        mirror_conn = connect(mirror, 'template1')
        assert scalar(mirror_conn, 'SHOW hot_standby') == 'on', 'run hot_standby/setup first'
        wait_until('the mirror to stream WAL', lambda: streaming(primary_conn))

        if args.scenario == 'streaming':
            check_status(coordinator_conn, mirror_conn, mirror, disconnected=False)
        else:
            check_disconnected(coordinator_conn, primary_conn, mirror_conn, array.coordinator, mirror)


if __name__ == '__main__':
    main()

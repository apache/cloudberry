#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# uds_drain.py -- minimal Unix-domain-socket sink for gp_stats_collector.
#
# gpsc.logging_mode = UDS makes every backend push per-node batches and plan
# docs to gpsc.uds_path.  In the crash test we do not run the real yagpcc; this
# drain accepts every connection and reads/discards whatever arrives, so the
# full C++ serialize+send path runs without backpressure drops -- but nothing
# downstream is exercised or asserted.
#
# One drain covers a single-host demo cluster (all segments share the socket
# path).  Runs until killed.

import argparse
import os
import socket
import sys
import threading


def drain_conn(conn):
    with conn:
        while True:
            try:
                if not conn.recv(65536):
                    return
            except OSError:
                return


def main():
    ap = argparse.ArgumentParser(description="gp_stats_collector UDS drain")
    ap.add_argument("--path", required=True, help="unix socket path to listen on")
    ap.add_argument("--backlog", type=int, default=128)
    args = ap.parse_args()

    if os.path.exists(args.path):
        os.unlink(args.path)

    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(args.path)
    srv.listen(args.backlog)
    os.chmod(args.path, 0o777)  # any backend user must be able to connect
    print("uds_drain: listening on {}".format(args.path), flush=True)

    try:
        while True:
            conn, _ = srv.accept()
            threading.Thread(target=drain_conn, args=(conn,), daemon=True).start()
    except KeyboardInterrupt:
        pass
    finally:
        srv.close()
        if os.path.exists(args.path):
            os.unlink(args.path)
    return 0


if __name__ == "__main__":
    sys.exit(main())

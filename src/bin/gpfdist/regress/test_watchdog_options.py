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

import os
import socket
import subprocess
import tempfile
import unittest


class WatchdogOptionsTest(unittest.TestCase):
    def test_invalid_timers(self):
        with tempfile.TemporaryDirectory() as directory:
            for value in ("", "abc", "2junk", "2.5", "-1", "-4294967295",
                          "2147483648", "9223372036854775808"):
                with self.subTest(value=value):
                    # Avoid depending on the default port being free.
                    with socket.socket() as sock:
                        sock.bind(("127.0.0.1", 0))
                        port = sock.getsockname()[1]
                    env = dict(os.environ, GPFDIST_WATCHDOG_TIMER=value)
                    result = subprocess.run(
                        ["gpfdist", "-p", str(port), "-d", directory],
                        env=env, capture_output=True, text=True, timeout=10)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("incorrect watchdog timer", result.stderr)


if __name__ == "__main__":
    unittest.main()

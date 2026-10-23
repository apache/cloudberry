
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

use strict;
use warnings;

use PostgreSQL::Test::Utils;
use Test::More;

# No cluster is started.  The program constructs the connection state it needs
# by hand and never reads or writes a socket, which is the point: the state
# under test is "the bytes are already in our buffer and the socket has gone
# quiet", awkward to provoke against a live server and trivial to build.
command_ok(
	['libpq_isbusy_notify'],
	'PQisBusy() stays true while a complete notification is queued');

done_testing();

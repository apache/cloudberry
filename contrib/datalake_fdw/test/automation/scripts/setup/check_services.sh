#!/usr/bin/env bash
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
#
# Report which external services are reachable.
#
# Informational on purpose: it exits 0 whether or not anything answered, because
# its output decides which categories the runner skips, and a missing service is
# a reason to skip a category rather than to fail a run.

set -u

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../utils/common_functions.sh
. "$script_dir/../utils/common_functions.sh"

dl_load_config

report()
{
	local name="$1" where="$2" state="$3"

	printf '%-16s %-32s %s\n' "$name" "$where" "$state"
}

printf '%-16s %-32s %s\n' 'SERVICE' 'ADDRESS' 'STATE'

if dl_tcp_is_open "$DL_HMS_HOST" "$DL_HMS_PORT" "$DL_PROBE_TIMEOUT"; then
	report 'hive-metastore' "$DL_HMS_HOST:$DL_HMS_PORT" 'available'
else
	report 'hive-metastore' "$DL_HMS_HOST:$DL_HMS_PORT" 'absent'
fi

if dl_http_is_open "$DL_S3_ENDPOINT" "$DL_PROBE_TIMEOUT"; then
	report 's3' "$DL_S3_ENDPOINT" 'available'
else
	report 's3' "$DL_S3_ENDPOINT" 'absent'
fi

if dl_tcp_is_open "$DL_HDFS_HOST" "$DL_HDFS_PORT" "$DL_PROBE_TIMEOUT"; then
	report 'hdfs' "$DL_HDFS_HOST:$DL_HDFS_PORT" 'available'
else
	report 'hdfs' "$DL_HDFS_HOST:$DL_HDFS_PORT" 'absent'
fi

exit 0

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
# Shared helpers.  Sourced, never executed.

# Absolute path of the automation directory, whichever directory the caller
# started from.
dl_automation_dir()
{
	local here
	here="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
	cd -- "$here/../.." && pwd
}

dl_load_config()
{
	local automation_dir
	automation_dir="$(dl_automation_dir)"

	# shellcheck source=../../config/test_config.env
	. "$automation_dir/config/test_config.env"
}

dl_info()
{
	printf '[automation] %s\n' "$*"
}

dl_warn()
{
	printf '[automation] %s\n' "$*" >&2
}

dl_die()
{
	dl_warn "$*"
	exit 1
}

# Name of a working timeout command, or empty when there is none.  macOS ships
# neither; coreutils installs it as gtimeout.
dl_timeout_command()
{
	local candidate

	for candidate in timeout gtimeout; do
		if command -v "$candidate" >/dev/null 2>&1; then
			printf '%s' "$candidate"
			return 0
		fi
	done

	return 1
}

# True when something is listening on host:port.  Uses bash's own /dev/tcp so
# that a probe needs no tool that might not be installed.
#
# Host and port are passed as arguments rather than interpolated into the
# program text: they come from the environment, and a shell metacharacter in one
# would otherwise be executed.  Being unable to probe is reported as a harness
# failure, not as "service absent" -- silently skipping a category because a
# tool is missing is how a suite stops testing anything without saying so.
dl_tcp_is_open()
{
	local host="$1" port="$2" seconds="${3:-3}" timeout_cmd

	if ! timeout_cmd="$(dl_timeout_command)"; then
		dl_die "no timeout command found (install coreutils for gtimeout)"
	fi

	"$timeout_cmd" "$seconds" bash -c \
		'exec 3<>/dev/tcp/"$1"/"$2"' _ "$host" "$port" 2>/dev/null
}

# True when an HTTP endpoint answers at all; any status counts, because a probe
# asks whether the service is there, not whether a request would succeed.
dl_http_is_open()
{
	local url="$1" timeout="${2:-3}"

	command -v curl >/dev/null 2>&1 || return 1
	curl --silent --show-error --output /dev/null \
		 --max-time "$timeout" "$url" 2>/dev/null
}

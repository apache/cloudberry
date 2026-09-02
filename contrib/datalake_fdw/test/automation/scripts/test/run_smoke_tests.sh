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
# Run the smoke categories under sqlrepo/smoke.
#
# A category names its required services in REQUIRED_SERVICES below.  One with
# none is always run; one whose services are absent is skipped and reported as
# skipped, so a developer without a Hive metastore still gets a useful run and
# nobody mistakes a skip for a pass.

set -u -o pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../utils/common_functions.sh
. "$script_dir/../utils/common_functions.sh"

automation_dir="$(dl_automation_dir)"
module_dir="$(cd -- "$automation_dir/../.." && pwd)"

dl_load_config

# category:services   -- an empty service list means "no external dependency"
CATEGORY_SERVICES="
iceberg_am:
"

categories="${CATEGORIES:-iceberg_am}"

services_for()
{
	local category="$1" line

	while read -r line; do
		[ -n "$line" ] || continue
		case "$line" in
			"$category":*) printf '%s' "${line#*:}"; return 0 ;;
		esac
	done <<-EOF
	$CATEGORY_SERVICES
	EOF

	printf 'unknown'
}

service_is_available()
{
	case "$1" in
		hive-metastore)
			dl_tcp_is_open "$DL_HMS_HOST" "$DL_HMS_PORT" "$DL_PROBE_TIMEOUT" ;;
		s3)
			dl_http_is_open "$DL_S3_ENDPOINT" "$DL_PROBE_TIMEOUT" ;;
		hdfs)
			dl_tcp_is_open "$DL_HDFS_HOST" "$DL_HDFS_PORT" "$DL_PROBE_TIMEOUT" ;;
		*)
			return 1 ;;
	esac
}

run_iceberg_am()
{
	# These cases are expected-output cases, so pg_regress runs them; the module
	# Makefile already points it at sqlrepo/smoke/iceberg_am.
	make -C "$module_dir" USE_PGXS=1 installcheck
}

failed=0
skipped=0
ran=0

for category in $categories; do
	required="$(services_for "$category")"

	if [ "$required" = 'unknown' ]; then
		dl_warn "unknown category \"$category\""
		failed=$((failed + 1))
		continue
	fi

	missing=''
	for service in $required; do
		service_is_available "$service" || missing="$missing $service"
	done

	if [ -n "$missing" ]; then
		dl_info "SKIP $category (absent:$missing)"
		skipped=$((skipped + 1))
		continue
	fi

	dl_info "RUN  $category"
	case "$category" in
		iceberg_am) run_iceberg_am ;;
		*) dl_warn "category \"$category\" has no runner"; false ;;
	esac

	if [ $? -eq 0 ]; then
		ran=$((ran + 1))
	else
		dl_warn "FAIL $category"
		failed=$((failed + 1))
	fi
done

dl_info "passed=$ran skipped=$skipped failed=$failed"
[ "$failed" -eq 0 ]

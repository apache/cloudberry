#!/bin/bash
# --------------------------------------------------------------------
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership. The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License. You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied. See the License for the specific language governing
# permissions and limitations under the License.
#
# --------------------------------------------------------------------
# extract_failures.sh <make-log>
#
# Prints the sorted, unique set of test names that pg_regress / isolation2
# reported as FAILED in an installcheck-world make log, one per line.  Lines
# look like "test foo ... FAILED" or "     foo   ... FAILED"; the test name is
# the token immediately before the "..." separator.
# --------------------------------------------------------------------
set -euo pipefail

log="${1:?usage: extract_failures.sh <make-log>}"

awk '
    /\.\.\.[[:space:]]*FAILED/ {
        for (i = 1; i <= NF; i++)
            if ($i == "...") { print $(i - 1); break }
    }
' "${log}" | sort -u

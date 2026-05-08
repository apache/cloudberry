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

USAGE() {
    echo ""
    echo " `basename $0` {-c | -d | -h} <-K>"
    echo " -c : Check if demo is possible."
    echo " -d : Delete the demo."
    echo " -K : Create cluster without data checksums."
    echo " -h : Usage, prints this message."
    echo ""
}

PORT_BASE=${PORT_BASE:=7000}
NUM_PRIMARY_MIRROR_PAIRS=${NUM_PRIMARY_MIRROR_PAIRS:=3}
if [ "${NUM_PRIMARY_MIRROR_PAIRS}" -eq "0" ]; then
  BLDWRAP_POSTGRES_CONF_ADDONS="fsync=on ${BLDWRAP_POSTGRES_CONF_ADDONS}"  # always enable fsync if singlenode
fi
if [[ ! ${BLDWRAP_POSTGRES_CONF_ADDONS} == *"fsync=on"* ]]; then
  BLDWRAP_POSTGRES_CONF_ADDONS="fsync=on ${BLDWRAP_POSTGRES_CONF_ADDONS}" # always fsync=on
else
  BLDWRAP_POSTGRES_CONF_ADDONS="${BLDWRAP_POSTGRES_CONF_ADDONS}"
fi
WITH_MIRRORS=${WITH_MIRRORS:="true"}
if [[ "${WITH_MIRRORS}" == "true" ]]; then
  WITH_STANDBY="true"
fi

export -f USAGE

export enable_gpfdist
export with_openssl

export DEMO_PORT_BASE="$PORT_BASE"
export NUM_PRIMARY_MIRROR_PAIRS
export WITH_MIRRORS
export WITH_STANDBY
export BLDWRAP_POSTGRES_CONF_ADDONS
export DEFAULT_QD_MAX_CONNECT=150
#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../../
GPDB_CONCOURSE_DIR=${TOP_DIR}/gpdb_src/concourse/scripts
CUT_NUMBER=5

source "${GPDB_CONCOURSE_DIR}/common.bash"
source "${TOP_DIR}/diskquota_src/concourse/scripts/test_common.sh"
function _main() {
	time install_gpdb
	time setup_gpadmin_user

	time make_cluster
	time install_diskquota
	if [ "${DISKQUOTA_OS}" == "ubuntu18.04" ]; then
		CUT_NUMBER=6
	fi

	time test ${TOP_DIR}/diskquota_src/ true
}

_main "$@"

#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../../
GPDB_CONCOURSE_DIR=${TOP_DIR}/gpdb_src/concourse/scripts
CUT_NUMBER=6

source "${GPDB_CONCOURSE_DIR}/common.bash"
source "${TOP_DIR}/diskquota_src/concourse/scripts/test_common.sh"

# those two functions are called by upgrade_test
function install_old_version_diskquota() {
	tar -xzf ../../bin_diskquota_old/*.tar.gz -C /usr/local/greenplum-db-devel
}

function install_new_version_diskquota() {
	# the current dir is upgrade_test
	tar -xzf ../../bin_diskquota_new/*.tar.gz -C /usr/local/greenplum-db-devel
}

function _main() {
	time install_gpdb
	time setup_gpadmin_user

	time make_cluster
	if [ "${DISKQUOTA_OS}" == "ubuntu18.04" ]; then
		CUT_NUMBER=6
	fi
	# firstly install an old version diskquota to start diskquota
	tar -xzf bin_diskquota_old/*.tar.gz -C /usr/local/greenplum-db-devel
	# export install_old_version_diskquota install_new_version_diskquota function, becuase they will be called by upgrade_test
	export -f install_old_version_diskquota
	export -f install_new_version_diskquota
	time test  ${TOP_DIR}/diskquota_src/upgrade_test false
}

_main "$@"

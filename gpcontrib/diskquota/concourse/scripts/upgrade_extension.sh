#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../../
GPDB_CONCOURSE_DIR=${TOP_DIR}/gpdb_src/concourse/scripts
CUT_NUMBER=5

source "${GPDB_CONCOURSE_DIR}/common.bash"
source "${TOP_DIR}/diskquota_src/concourse/scripts/test_common.sh"

function install_old_version_diskquota() {
	tar -xzf bin_diskquota_old/*.tar.gz -C /usr/local/greenplum-db-devel
}

# this function is called by upgrade_test/sql/upgrade_extension.sql
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

	# install old_version diskquota
	time install_old_version_diskquota
	# export install_new_version_diskquota function, becuase it will
	# be called by upgrade_test/sql/upgrade_extension.sql
	export -f install_new_version_diskquota
	time test  ${TOP_DIR}/diskquota_src/upgrade_test
}

_main "$@"

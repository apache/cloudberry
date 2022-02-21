#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../../
GPDB_CONCOURSE_DIR=${TOP_DIR}/gpdb_src/concourse/scripts
CUT_NUMBER=6

source "${GPDB_CONCOURSE_DIR}/common.bash"
source "${TOP_DIR}/diskquota_src/concourse/scripts/test_common.sh"

## Currently, isolation2 testing framework relies on pg_isolation2_regress, we
## should build it from source. However, in concourse, the gpdb_bin is fetched
## from remote machine, the $(abs_top_srcdir) variable points to a non-existing
## location, we fixes this issue by creating a symbolic link for it.
function create_fake_gpdb_src() {
	pushd gpdb_src
	./configure --prefix=/usr/local/greenplum-db-devel \
		    --without-zstd \
		    --disable-orca --disable-gpcloud --enable-debug-extensions
	popd

	FAKE_GPDB_SRC=/tmp/build/"$(grep -rnw '/usr/local/greenplum-db-devel' -e 'abs_top_srcdir = .*' | head -n 1 | awk -F"/" '{print $(NF-1)}')"
	mkdir -p ${FAKE_GPDB_SRC}
	ln -s ${TOP_DIR}/gpdb_src ${FAKE_GPDB_SRC}/gpdb_src
}

function _main() {
	time install_gpdb
	create_fake_gpdb_src
	time setup_gpadmin_user

	time make_cluster
	time install_diskquota

	time test ${TOP_DIR}/diskquota_src/ true
}

_main "$@"

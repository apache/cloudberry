#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../../

source "${TOP_DIR}/gpdb_src/concourse/scripts/common.bash"
function pkg() {
    source /opt/gcc_env.sh
    source /usr/local/greenplum-db-devel/greenplum_path.sh

    export USE_PGXS=1
    pushd diskquota_src/
    if [ "${DEV_RELEASE}" == "release" ]; then
        if git describe --tags >/dev/null 2>&1 ; then
            echo "git describe failed" || exit 1
        fi
        DISKQUOTA_VERSION=$(git describe --tags)
    fi
    make clean
    make install
    popd

	pushd /usr/local/greenplum-db-devel/
	echo 'cp -r lib share $GPHOME || exit 1'> install_gpdb_component
	chmod a+x install_gpdb_component
	tar -czf $TOP_DIR/diskquota_artifacts/component_diskquota.tar.gz \
		lib/postgresql/diskquota.so \
		share/postgresql/extension/diskquota.control \
		share/postgresql/extension/diskquota--1.0.sql \
		install_gpdb_component
	popd
    if [ "${DEV_RELEASE}" == "release" ]; then
        case "$OSVER" in
        centos6|centos7)
            cp $TOP_DIR/diskquota_artifacts/component_diskquota.tar.gz $TOP_DIR/diskquota_artifacts/diskquota-${DISKQUOTA_VERSION}.tar.gz
            ;;
        *) echo "Unknown OS: $OSVER"; exit 1 ;;
        esac
    fi
}

function _main() {	
	time install_gpdb
	time pkg
}

_main "$@"

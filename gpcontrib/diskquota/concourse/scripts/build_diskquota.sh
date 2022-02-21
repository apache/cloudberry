#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../../

source "${TOP_DIR}/gpdb_src/concourse/scripts/common.bash"
function pkg() {
    [ -f /opt/gcc_env.sh ] && source /opt/gcc_env.sh
    source /usr/local/greenplum-db-devel/greenplum_path.sh

    export USE_PGXS=1
    pushd diskquota_src/
    DISKQUOTA_VERSION=$(git describe --tags)
    make clean
    make install
    popd

    pushd /usr/local/greenplum-db-devel/
    echo 'cp -r lib share $GPHOME || exit 1'> install_gpdb_component
    chmod a+x install_gpdb_component
    tar -czf "$TOP_DIR/diskquota_artifacts/diskquota-${DISKQUOTA_VERSION}-${DISKQUOTA_OS}_x86_64.tar.gz" \
        "lib/postgresql/diskquota.so" \
        "share/postgresql/extension/diskquota.control" \
        "share/postgresql/extension/diskquota--1.0.sql" \
        "share/postgresql/extension/diskquota--2.0.sql" \
        "share/postgresql/extension/diskquota--1.0--2.0.sql" \
        "share/postgresql/extension/diskquota--2.0--1.0.sql" \
        "install_gpdb_component"
    popd
}

function _main() {
    time install_gpdb
    time pkg
}

_main "$@"

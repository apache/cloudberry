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
    case "$DISKQUOTA_OS" in
    rhel6)
        tar -czf $TOP_DIR/diskquota_artifacts/diskquota-${DISKQUOTA_VERSION}-rhel6_x86_64.tar.gz \
        lib/postgresql/diskquota.so \
        share/postgresql/extension/diskquota.control \
        share/postgresql/extension/diskquota--1.0.sql \
        share/postgresql/extension/diskquota--2.0.sql \
        share/postgresql/extension/diskquota--1.0--2.0.sql \
        share/postgresql/extension/diskquota--2.0--1.0.sql \
        install_gpdb_component
      ;;
    rhel7)
        tar -czf $TOP_DIR/diskquota_artifacts/diskquota-${DISKQUOTA_VERSION}-rhel7_x86_64.tar.gz \
        lib/postgresql/diskquota.so \
        share/postgresql/extension/diskquota.control \
        share/postgresql/extension/diskquota--1.0.sql \
        share/postgresql/extension/diskquota--2.0.sql \
        share/postgresql/extension/diskquota--1.0--2.0.sql \
        share/postgresql/extension/diskquota--2.0--1.0.sql \
        install_gpdb_component
      ;;
    rhel8)
        tar -czf $TOP_DIR/diskquota_artifacts/diskquota-${DISKQUOTA_VERSION}-rhel8_x86_64.tar.gz \
        lib/postgresql/diskquota.so \
        share/postgresql/extension/diskquota.control \
        share/postgresql/extension/diskquota--1.0.sql \
        install_gpdb_component
      ;;
    ubuntu18.04)
        tar -czf $TOP_DIR/diskquota_artifacts/diskquota-${DISKQUOTA_VERSION}-ubuntu18.04_x86_64.tar.gz \
        lib/postgresql/diskquota.so \
        share/postgresql/extension/diskquota.control \
        share/postgresql/extension/diskquota--1.0.sql \
        share/postgresql/extension/diskquota--2.0.sql \
        share/postgresql/extension/diskquota--1.0--2.0.sql \
        share/postgresql/extension/diskquota--2.0--1.0.sql \
        install_gpdb_component
      ;;
    *) echo "Unknown OS: $DISKQUOTA_OS"; exit 1 ;;
    esac
    popd
}

function _main() {
    time install_gpdb
    time pkg
}

_main "$@"

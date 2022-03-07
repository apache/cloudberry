#!/bin/bash -l

set -exo pipefail

function pkg() {
    [ -f /opt/gcc_env.sh ] && source /opt/gcc_env.sh
    source /usr/local/greenplum-db-devel/greenplum_path.sh

    if [ "${DISKQUOTA_OS}" = "rhel6" ]; then
        export CC="$(which gcc)"
    fi

    pushd /home/gpadmin/diskquota_artifacts
    cmake /home/gpadmin/diskquota_src
    cmake --build . --target package
    popd
}

function _main() {
    time pkg
}

_main "$@"

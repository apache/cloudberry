#!/bin/bash -l

set -exo pipefail

function pkg() {
    [ -f /opt/gcc_env.sh ] && source /opt/gcc_env.sh
    source /usr/local/greenplum-db-devel/greenplum_path.sh

    if [ "${DISKQUOTA_OS}" = "rhel6" ]; then
        export CC="$(which gcc)"
    fi

    pushd /home/gpadmin/diskquota_artifacts
    local last_release_path
    last_release_path=$(readlink -e /home/gpadmin/last_released_diskquota_bin/diskquota-*.tar.gz)
    cmake /home/gpadmin/diskquota_src \
        -DDISKQUOTA_LAST_RELEASE_PATH="${last_release_path}" \
        -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"
    cmake --build . --target package
    popd
}

function _main() {
    time pkg
}

_main "$@"

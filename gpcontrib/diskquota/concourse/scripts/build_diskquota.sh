#!/bin/bash -l

set -exo pipefail

function pkg() {
    [ -f /opt/gcc_env.sh ] && source /opt/gcc_env.sh
    source /usr/local/greenplum-db-devel/greenplum_path.sh

    # Always use the gcc from $PATH, to avoid using a lower version compiler by /usr/bin/cc
    export CC="$(which gcc)"
    export CXX="$(which g++)"

    pushd /home/gpadmin/diskquota_artifacts
    local last_release_path
    last_release_path=$(readlink -eq /home/gpadmin/last_released_diskquota_bin/diskquota-*.tar.gz)
    cmake /home/gpadmin/diskquota_src \
        -DDISKQUOTA_LAST_RELEASE_PATH="${last_release_path}" \
        -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"
    cmake --build . --target create_artifact
    popd
}

function _main() {
    time pkg
}

_main "$@"

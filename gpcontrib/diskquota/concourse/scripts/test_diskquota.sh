#!/bin/bash -l

set -exo pipefail

function activate_standby() {
    gpstop -may -M immediate
    if [[ $PGPORT -eq 6000 ]]
    then
        export PGPORT=6001
    else
        export PGPORT=7001
    fi
    export MASTER_DATA_DIRECTORY=/home/gpadmin/gpdb_src/gpAux/gpdemo/datadirs/standby
    gpactivatestandby -a -f -d $MASTER_DATA_DIRECTORY
}

function _main() {
    local tmp_dir="$(mktemp -d)"
    tar -xzf /home/gpadmin/bin_diskquota/diskquota-*-*.tar.gz -C "$tmp_dir"
    pushd "$tmp_dir"
    ./install_gpdb_component
    popd

    source /home/gpadmin/gpdb_src/gpAux/gpdemo/gpdemo-env.sh

    pushd /home/gpadmin/gpdb_src
        make -C src/test/isolation2 install
    popd

    pushd /home/gpadmin/diskquota_artifacts
    # Show regress diff if test fails
    export SHOW_REGRESS_DIFF=1
    time cmake --build . --target installcheck
    # Run test again with standby master
    # FIXME: enable test for GPDB7
    if [[ $PGPORT -eq 6000 ]]
    then
        activate_standby
        time cmake --build . --target installcheck
    fi
    # Run upgrade test (with standby master)
    time cmake --build . --target upgradecheck
    popd
}

_main

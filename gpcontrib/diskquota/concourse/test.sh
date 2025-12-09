#!/bin/bash -l

set -exo pipefail

function activate_standby() {
    gpstop -may -M immediate
    export MASTER_DATA_DIRECTORY=$(readlink /home/gpadmin/gpdb_src)/gpAux/gpdemo/datadirs/standby
    if [[ $PGPORT -eq 6000 ]]
    then
        export PGPORT=6001
    else
        export PGPORT=7001
        export COORDINATOR_DATA_DIRECTORY=$MASTER_DATA_DIRECTORY
    fi
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

    pushd /home/gpadmin/bin_diskquota
    # Show regress diff if test fails
    export SHOW_REGRESS_DIFF=1
    time cmake --build . --target installcheck
    # Run test again with standby master
    activate_standby
    time cmake --build . --target installcheck
    if [[ $OS_NAME != "rhel9" ]]
    then
    # Run upgrade test (with standby master)
    time cmake --build . --target upgradecheck
    fi
    popd

    if [[ $OS_NAME != "rhel9" ]]
    then
    time /home/gpadmin/diskquota_src/upgrade_test/alter_test.sh
    fi
}

_main

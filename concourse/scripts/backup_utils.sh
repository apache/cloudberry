#!/bin/bash

destroy_gpdb() {
    # Setup environment
    source /usr/local/greengage-db-devel/greengage_path.sh;
    export PGPORT=5432;
    export MASTER_DATA_DIRECTORY=/data/gpdata/coordinator/gpseg-1;

    yes|gpdeletesystem -f -d $MASTER_DATA_DIRECTORY
    rm -rf $GPHOME/*
    rm -rf /home/gpadmin/gpdb_src/gpMgmt/bin/pythonSrc/ext/install
}

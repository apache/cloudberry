#!/usr/bin/env bash

## ==================================================================
## Required: regression database generated from src/test/regress
##
## Test binary swap from current Cloudberry install to another
## Cloudberry install. The flow goes like this:
##   1. Run pg_dumpall on current database
##   2. Start binaries provided by -b (required)
##   3. Run pg_dumpall on other binary and diff dumps
##   4. Run some inserts and do another pg_dumpall
##   5. Start binaries provided by -c (default current $GPHOME).
##   6. Run pg_dumpall on current binary and diff dumps
##   7. Run some more inserts and run another pg_dumpall
##
## Providing the binaries can be interchangeable between -b and -c
## but technically shouldn't matter. However, the binary provided in
## -c will be the active binary at the end of the run.
## ==================================================================

## Clean previous generated test output
clean_output()
{
    rm -rf results
    rm -f dump_current.sql dump_other.sql
}

## Run tests via pg_regress with given schedule name
run_tests()
{
    SCHEDULE_NAME=$1
    IF_USE_EXISTING_DB=""
    if [ ! -z "$2" ]; then
      IF_USE_EXISTING_DB="--use-existing"
    fi 
    ../regress/pg_regress --dbname=binswap_connect ${IF_USE_EXISTING_DB} --init-file=../regress/init_file --schedule=${SCHEDULE_NAME}
    if [ $? != 0 ]; then
        exit 1
    fi
}

##  Start up a Cloudberry binary using same $MASTER_DATA_DIRECTORY
start_binary()
{
    BINARY_PATH=$1
    gpstop -ai
    source $BINARY_PATH/greenplum_path.sh
    gpstart -a
    echo "Select our Cloudberry version just to be sure..."
    psql -c "select version()" postgres
}

usage()
{
    appname=`basename $0`
    echo "$appname usage:"
    echo " -b <dir>   Cloudberry install path for another binary to test upgrade/downgrade from (Required User Input)"
    echo " -c <dir>   Cloudberry install path for current binary to test upgrade/downgrade to (Default: \$GPHOME)"
    echo " -m <dir>   Cloudberry Master Data Directory (Default: \$MASTER_DATA_DIRECTORY)"
    echo " -p <port>  Cloudberry Master Port (Default: \$PGPORT)"
    echo " -v <variant> Variant of the test plan (Default: '')"
    exit 0
}

while getopts ":c:b:m:p:v:" opt; do
    case ${opt} in
        c)
            GPHOME_CURRENT=$OPTARG
            ;;
        b)
            GPHOME_OTHER=$OPTARG
            ;;
        m)
            MDD_CURRENT=$OPTARG
            ;;
        p)
            PGPORT_CURRENT=$OPTARG
            ;;
        v)
            VARIANT=$OPTARG
            ;;
        *)
            usage
            ;;
    esac
done

## Main script
## ==================================================

## Argument checking
GPHOME_CURRENT=${GPHOME_CURRENT:=$GPHOME}
MDD_CURRENT=${MDD_CURRENT:=$MASTER_DATA_DIRECTORY}
PGPORT_CURRENT=${PGPORT_CURRENT:=$PGPORT}

if [ "${GPHOME_OTHER}x" == "x" ] || ! [ -f $GPHOME_OTHER/greenplum_path.sh ]; then
    echo "Use -b to provide a valid Cloudberry install path to upgrade/downgrade from"
    exit 1
fi

if [ "${GPHOME_CURRENT}x" == "x" ] || ! [ -f $GPHOME_CURRENT/greenplum_path.sh ]; then
    echo "Use -c to provide a valid Cloudberry install path to upgrade/downgrade to (Default: \$GPHOME)"
    exit 1
fi

if [ "${MDD_CURRENT}x" == "x" ]; then
    echo "Use -m to provide a valid Cloudberry Master Data Directory (Default: \$MASTER_DATA_DIRECTORY)"
    exit 1
fi

if [ "${PGPORT_CURRENT}x" == "x" ]; then
    echo "Use -p to provide a valid Cloudberry Master Port (Default: \$PGPORT)"
    exit 1
fi

if ! [ -e schedule1${VARIANT} -a \
       -e schedule2${VARIANT} -a \
       -e schedule3${VARIANT} ]; then
    echo "Use -v to provide a valid variant of the test plan (Default: '')"
    exit 1
fi

## Grab the Cloudberry versions of each binary for display
CURRENT_VERSION=`$GPHOME_CURRENT/bin/gpstart --version | awk '{ for (i=3; i<NF; i++) printf $i " "; print $NF }'`
OTHER_VERSION=`$GPHOME_OTHER/bin/gpstart --version | awk '{ for (i=3; i<NF; i++) printf $i " "; print $NF }'`

echo "Binary Swap tests"
echo "=================================================="
echo "Current binaries: ${CURRENT_VERSION}"
echo "                  ${GPHOME_CURRENT}"
echo "  Other binaries: ${OTHER_VERSION}"
echo "                  ${GPHOME_OTHER}"
echo "         Variant: ${VARIANT}"
echo "=================================================="

## Clean our directory of any previous test output
clean_output

run_tests schedule0${VARIANT}

## Start/restart current Cloudberry and do initial dump to compare against
start_binary $GPHOME_CURRENT
run_tests schedule1${VARIANT} use-existing

## Change the binary, dump, and then compare the two dumps generated
## by both binaries. Then we do some inserts and dump again. We source
## $GPHOME_CURRENT/greenplum_path.sh here after starting Cloudberry
## with $GPHOME_OTHER to use latest pg_dumpall to prevent catching
## diffs due to changes made to pg_dump. The running binaries are
## still from $GPHOME_OTHER. Only pg_regress, pg_dumpall, psql, and
## gpcheckcat should be from $GPHOME_CURRENT during the pg_regress
## test.
start_binary $GPHOME_OTHER
run_tests schedule2${VARIANT} use-existing

## Change the binary back, dump, and then compare the two new dumps
## generated by both binaries. Then we do some inserts and check to see
## if dump still works fine.
start_binary $GPHOME_CURRENT
run_tests schedule3${VARIANT} use-existing

## Print unnecessary success output
echo "SUCCESS! Provided binaries are swappable."

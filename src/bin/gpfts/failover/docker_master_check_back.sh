#!/usr/bin/env bash

if !type gpssh >/dev/null 2>&1; then 
    echo "[FATAL]:- gpssh not exist, script Exits!"
    exit 1
fi

if !type gpinitstandby >/dev/null 2>&1; then 
    echo "[FATAL]:- gpssh not exist, script Exits!"
    exit 1
fi


addr=""
port=""
datadir=""
user="gpadmin"

function dohelp() {
    echo "usage:"
    echo "  -a(require) hostname"
    echo "  -p(require) port"
    echo "  -d(require) datadir"
    echo "  -u(default gpadmin) user"
}

while getopts "a:p:d:u:h" optname
do
    case "$optname" in
        "h")
            dohelp
            exit 1
            ;;
        "a")
            addr="$OPTARG"
            ;;
        "p")
            port="$OPTARG"
            ;;
        "d")
            datadir="$OPTARG"
            ;;
        "u")
            user="$OPTARG"
            ;;
    esac
done

if [[ -z $addr || -z $port || -z $datadir ]];
then
    dohelp 
    exit -1
fi

# notice that, the PGPORT and COORDINATOR_DATA_DIRECTORY in $addr should change to current master.
# otherwise it will failed.
gpssh -h $addr -u $user -e "gpinitstandby -s $addr -P $port -S $datadir -a"

if [[ $? -ne 0 ]];
then
    echo "[FATAL]:- gpinitstandby -s $addr -P $port -S $datadir -a failed."
    exit -1
fi

echo "all done!"

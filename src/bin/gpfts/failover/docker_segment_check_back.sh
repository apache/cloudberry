#!/usr/bin/env bash

if !type gpssh >/dev/null 2>&1; then 
    echo "[FATAL]:- gpssh not exist, script Exits!"
    exit 1
fi

if !type pg_ctl >/dev/null 2>&1; then 
    echo "[FATAL]:- pg_ctl not exist, script Exits!"
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

echo "[INFO]:- addr: $addr, port: $port, datadir: $datadir"

gpssh -h $addr -u $user -e "pg_ctl -D $datadir -t 600 start -o '-c gp_role=execute -p $port'"

if [[ $? -ne 0 ]];
then
    echo "[FATAL]:- pg_ctl -D $segment_datadir start failed."
    exit -1
fi



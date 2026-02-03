#!/usr/bin/env bash

if !type gprecoverseg >/dev/null 2>&1; then 
    echo "[FATAL]:- gprecoverseg not exist, script Exits!"
    exit 1
fi

if !type gpssh >/dev/null 2>&1; then 
    echo "[FATAL]:- gpssh not exist, script Exits!"
    exit 1
fi

gprecoverseg_config_file="/tmp/gprecoverseg_config_file"

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

gprecoverseg_content_str="$addr|$port|$datadir"


gpssh -h $addr -u $user -e "rm -rf $gprecoverseg_config_file"
gpssh -h $addr -u $user -e "echo \"$gprecoverseg_content_str\" >> $gprecoverseg_config_file"

if [[ $? -ne 0 ]];
then
    echo "[FATAL]:- gpssh -h $addr -u $user -e echo $gprecoverseg_content_str >> $gprecoverseg_config_file failed."
    exit -1
fi


gpssh -h $addr -u $user -e "gprecoverseg -i $gprecoverseg_config_file -a"

if [[ $? -ne 0 ]];
then
    echo "[FATAL]:- gprecoverseg -i $gprecoverseg_config_file -a failed."
    exit -1
fi

# Looks like no need use gpssh
gprecoverseg -ra 
if [[ $? -ne 0 ]];
then
    echo "[FATAL]:- gprecoverseg -r failed."
    exit -1
fi

gpssh -h $addr -u $user -e "rm -rf $gprecoverseg_config_file"

echo "[INFO]:- all done!"
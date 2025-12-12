#!/usr/bin/env bash

if !type pg_ctl >/dev/null 2>&1; then 
    echo "[FATAL]:- pg_ctl not exist, script Exits!"
    exit 1
fi

if !type gprecoverseg >/dev/null 2>&1; then 
    echo "[FATAL]:- gprecoverseg not exist, script Exits!"
    exit 1
fi

if !type gpssh >/dev/null 2>&1; then 
    echo "[FATAL]:- gpssh not exist, script Exits!"
    exit 1
fi

gprecoverseg_config_file="/tmp/gprecoverseg_config_file"
segment_addr=""
segment_port=""
segment_datadir=""
mirror_addr=""
mirror_port=""
mirror_datadir=""
user="gpadmin"


function dohelp() {
    echo "usage:"
    echo "  -r(require) segment hostname"
    echo "  -c(require) segment port"
    echo "  -s(require) segment datadir"
    echo "  -a(require) mirror hostname"
    echo "  -p(require) mirror port"
    echo "  -d(require) mirror datadir"
    echo "  -u(default gpadmin) user"
}

while getopts "r:c:s:a:p:d:u:h" optname
do
    case "$optname" in
        "h")
            dohelp
            exit 1
            ;;
        "r")
            segment_addr="$OPTARG"
            ;;
        "c")
            segment_port="$OPTARG"
            ;;
        "s")
            segment_datadir="$OPTARG"
            ;;
        "a")
            mirror_addr="$OPTARG"
            ;;
        "p")
            mirror_port="$OPTARG"
            ;;
        "d")
            mirror_datadir="$OPTARG"
            ;;
        "u")
            user="$OPTARG"
            ;;
    esac
done

if [[ -z $segment_addr || -z $segment_port || -z $segment_datadir || -z $mirror_addr || -z $mirror_port || -z $mirror_datadir ]];
then
    dohelp 
    exit -1
fi

echo "[INFO]:- segment_addr: $segment_addr, segment_datadir: $segment_datadir, mirror_addr: $mirror_addr, mirror_port: $mirror_port, mirror_datadir: $mirror_datadir,"


gpssh -h $segment_addr -u $user -e "pg_ctl -D $segment_datadir -t 600 start -o '-c gp_role=execute -p $segment_port'"

if [[ $? -ne 0 ]];
then
    echo "[FATAL]:- pg_ctl -D $segment_datadir start failed."
    exit -1
fi

sleep 60

gprecoverseg_content_str="$mirror_addr|$mirror_port|$mirror_datadir"


gpssh -h $mirror_addr -u $user -e "rm -rf $gprecoverseg_config_file"
gpssh -h $mirror_addr -u $user -e "echo \"$gprecoverseg_content_str\" >> $gprecoverseg_config_file"

if [[ $? -ne 0 ]];
then
    echo "[FATAL]:- gpssh -h $mirror_addr -u $user -e echo \"$gprecoverseg_content_str\" >> $gprecoverseg_config_file failed."
    exit -1
fi

gpssh -h $mirror_addr -u $user -e "gprecoverseg -i $gprecoverseg_config_file -a"

if [[ $? -ne 0 ]];
then
    echo "[FATAL]:- gprecoverseg -i $gprecoverseg_config_file -a failed."
    exit -1
fi

gprecoverseg -ra 
if [[ $? -ne 0 ]];
then
    echo "[FATAL]:- gprecoverseg -r failed."
    exit -1
fi

gpssh -h $mirror_addr -u $user -e "rm -rf $gprecoverseg_config_file"

echo "[INFO]:- all done!"


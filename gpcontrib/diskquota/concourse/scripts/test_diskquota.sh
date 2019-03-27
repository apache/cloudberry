#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../../
if [ "$GPDBVER" == "GPDB4.3" ]; then
    GPDB_CONCOURSE_DIR=${TOP_DIR}/gpdb_src/ci/concourse/scripts
else
    GPDB_CONCOURSE_DIR=${TOP_DIR}/gpdb_src/concourse/scripts
fi
source "${GPDB_CONCOURSE_DIR}/common.bash"
function test(){	
	sudo chown -R gpadmin:gpadmin ${TOP_DIR};
	cat > /home/gpadmin/test.sh <<-EOF
		set -exo pipefail
		source /usr/local/greenplum-db-devel/greenplum_path.sh
		export PGPORT=15432
		createdb diskquota
		gpconfig -c shared_preload_libraries -v 'diskquota'
		gpstop -arf
		gpconfig -c diskquota.naptime -v 2
		gpstop -arf
		pushd diskquota_src
		[ -s regression.diffs ] && cat regression.diffs && exit 1
		make installcheck
		ps -ef | grep postgres| grep qddir| cut -d ' ' -f 6 | xargs kill -9
		export PGPORT=16432
		rm /tmp/.s.PGSQL.15432*
		gpactivatestandby -ad ${TOP_DIR}/gpdb_src/gpAux/gpdemo/datadirs/standby
		make installcheck
		popd
	EOF
	export MASTER_DATA_DIRECTORY=${TOP_DIR}/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1
	chown gpadmin:gpadmin /home/gpadmin/test.sh
	chmod a+x /home/gpadmin/test.sh
	su gpadmin -c "bash /home/gpadmin/test.sh"
}	

function setup_gpadmin_user() {
    case "$OSVER" in
        suse*)
        ${GPDB_CONCOURSE_DIR}/setup_gpadmin_user.bash "sles"
        ;;
        centos*)
        ${GPDB_CONCOURSE_DIR}/setup_gpadmin_user.bash "centos"
        ;;
        *) echo "Unknown OS: $OSVER"; exit 1 ;;
    esac
	
}

function install_diskquota() {
	tar -xzf bin_diskquota/component_diskquota.tar.gz -C /usr/local/greenplum-db-devel
}
function _main() {
	time install_gpdb
	time setup_gpadmin_user

	time make_cluster
	time install_diskquota

	time test
}

_main "$@"

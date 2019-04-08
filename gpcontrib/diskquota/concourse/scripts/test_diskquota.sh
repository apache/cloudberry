#!/bin/bash -l

set -exo pipefail

CWDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
TOP_DIR=${CWDIR}/../../../
GPDB_CONCOURSE_DIR=${TOP_DIR}/gpdb_src/concourse/scripts
source "${GPDB_CONCOURSE_DIR}/common.bash"
function test(){	
	sudo chown -R gpadmin:gpadmin ${TOP_DIR};
	cat > /home/gpadmin/test.sh <<-EOF
		set -exo pipefail
		source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
		echo "export MASTER_DATA_DIRECTORY=\$MASTER_DATA_DIRECTORY" >> /usr/local/greenplum-db-devel/greenplum_path.sh
		source /usr/local/greenplum-db-devel/greenplum_path.sh
		createdb diskquota
		gpconfig -c shared_preload_libraries -v 'diskquota'
		gpstop -arf
		gpconfig -c diskquota.naptime -v 2
		gpstop -arf
		pushd diskquota_src
		make installcheck
		[ -s regression.diffs ] && cat regression.diffs && exit 1
		ps -ef | grep postgres| grep qddir| cut -d ' ' -f 6 | xargs kill -9
		export PGPORT=16432
		echo "export PGPROT=\$PGPORT" >> /usr/local/greenplum-db-devel/greenplum_path.sh
		source /usr/local/greenplum-db-devel/greenplum_path.sh
		rm /tmp/.s.PGSQL.15432*
		gpactivatestandby -ad ${TOP_DIR}/gpdb_src/gpAux/gpdemo/datadirs/standby
		make installcheck
		[ -s regression.diffs ] && cat regression.diffs && exit 1
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

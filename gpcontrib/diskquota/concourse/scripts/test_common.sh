# the directory to run the "make install" as the first param
# the second param is a bool var, used to judge if need to active the standby
# and run the regress test again
function test(){
	chown -R gpadmin:gpadmin ${TOP_DIR};
	cat > /home/gpadmin/test.sh <<-EOF
		set -exo pipefail
		source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
		echo "export MASTER_DATA_DIRECTORY=\$MASTER_DATA_DIRECTORY" >> /usr/local/greenplum-db-devel/greenplum_path.sh
		source /usr/local/greenplum-db-devel/greenplum_path.sh
		createdb diskquota
		gpconfig -c shared_preload_libraries -v 'diskquota'
		gpstop -arf
		gpconfig -c diskquota.naptime -v 1
		gpstop -arf
		# the dir to run the "make install" command
		pushd $1

		function look4diffs() {
			diff_files=\`find .. -name regression.diffs\`
			for diff_file in \${diff_files}; do
				if [ -f "\${diff_file}" ]; then
					cat <<-FEOF
						======================================================================
						DIFF FILE: \${diff_file}
						======================================================================

						\$(grep -v GP_IGNORE "\${diff_file}")
					FEOF
				fi
			done
			exit 1
		}

		trap look4diffs ERR
		make installcheck

		if $2 ; then
			## Bring down the QD.
			gpstop -may -M immediate
			export PGPORT=6001
			export MASTER_DATA_DIRECTORY=${TOP_DIR}/gpdb_src/gpAux/gpdemo/datadirs/standby
			gpactivatestandby -ad \$MASTER_DATA_DIRECTORY
			echo "export PGPORT=\$PGPORT" >> /usr/local/greenplum-db-devel/greenplum_path.sh
			echo "export MASTER_DATA_DIRECTORY=\$MASTER_DATA_DIRECTORY" >> /usr/local/greenplum-db-devel/greenplum_path.sh
			source /usr/local/greenplum-db-devel/greenplum_path.sh
			make installcheck
		fi
		popd
	EOF
	export MASTER_DATA_DIRECTORY=${TOP_DIR}/gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1
	chown gpadmin:gpadmin /home/gpadmin/test.sh
	chmod a+x /home/gpadmin/test.sh
	su gpadmin -c "bash /home/gpadmin/test.sh"
}

function setup_gpadmin_user() {
    ${GPDB_CONCOURSE_DIR}/setup_gpadmin_user.bash
}

function install_diskquota() {
  tar -xzf bin_diskquota/*.tar.gz -C /usr/local/greenplum-db-devel
}

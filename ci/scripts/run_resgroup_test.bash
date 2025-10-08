#!/bin/bash
set -eox pipefail

project="resgroup"

function cleanup {
  docker compose -p $project -f ci/docker-compose.yaml --env-file ci/.env down
}

mkdir ssh_keys -p
if [ ! -e "ssh_keys/id_rsa" ]
then
  ssh-keygen -P "" -f ssh_keys/id_rsa
fi

trap cleanup EXIT

#install gpdb and setup gpadmin user
bash ci/scripts/init_containers.sh $project cdw sdw1

for service in 'cdw' 'sdw1'
do
  #grant access rights to group controllers
  docker compose -p $project -f ci/docker-compose.yaml exec -T $service bash -c "
    chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset} &&
    mkdir /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb &&
    chmod -R 777 /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb &&
    chown -R gpadmin:gpadmin /sys/fs/cgroup/{memory,cpu,cpuset}/gpdb"
done

#create cluster
docker compose -p $project -f ci/docker-compose.yaml exec -T cdw \
 bash -c "source gpdb_src/concourse/scripts/common.bash && HOSTS_LIST='sdw1' make_cluster"

#disable exit on error to allow log collection regardless of return code
set +e
#run tests
docker compose -p $project -f ci/docker-compose.yaml exec -Tu gpadmin cdw bash -ex <<EOF
        source /usr/local/greengage-db-devel/greengage_path.sh
        source gpdb_src/gpAux/gpdemo/gpdemo-env.sh
        export LDFLAGS="-L\${GPHOME}/lib"
        export CPPFLAGS="-I\${GPHOME}/include"
        export USER=gpadmin

        cd /home/gpadmin/gpdb_src
        ./configure --prefix=/usr/local/greengage-db-devel \
            --without-zlib --without-rt --without-libcurl \
            --without-libedit-preferred --without-docdir --without-readline \
            --disable-gpcloud --disable-gpfdist --disable-orca \
            ${CONFIGURE_FLAGS}

        make -C /home/gpadmin/gpdb_src/src/test/regress
        ssh sdw1 mkdir -p /home/gpadmin/gpdb_src/src/test/{regress,isolation2} </dev/null
        scp /home/gpadmin/gpdb_src/src/test/regress/regress.so \
            gpadmin@sdw1:/home/gpadmin/gpdb_src/src/test/regress/

        make PGOPTIONS="-c optimizer=off" installcheck-resgroup || (
            errcode=\$?
            find src/test/isolation2 -name regression.diffs \
            | while read diff; do
                cat <<EOF1

======================================================================
DIFF FILE: \$diff
----------------------------------------------------------------------

EOF1
                cat \$diff
              done
            exit \$errcode
        )
EOF

exitcode=$?
docker compose -p $project -f ci/docker-compose.yaml exec -T cdw bash -ex <<EOF
  cd /home/gpadmin
  tar -czf /logs/gpAdminLogs.tar.gz gpAdminLogs/
  tar -czf /logs/gpAux.tar.gz gpdb_src/gpAux/gpdemo/datadirs/gpAdminLogs/
  tar -czf /logs/pg_log.tar.gz gpdb_src/gpAux/gpdemo/datadirs/qddir/demoDataDir-1/pg_log/ gpdb_src/gpAux/gpdemo/datadirs/standby/pg_log
  #regression.diffs may not exist if tests were successful
  tar --ignore-failed-read -czf /logs/results.tar.gz gpdb_src/src/test/isolation2/results/resgroup/ gpdb_src/src/test/isolation2/regression.diffs
EOF

docker compose -p $project -f ci/docker-compose.yaml exec -T sdw1 bash -ex <<EOF
  cd /home/gpadmin
  tar -czf /logs/gpAdminLogs.tar.gz gpAdminLogs/
  tar -czf /logs/gpAux.tar.gz gpdb_src/gpAux/gpdemo/datadirs/gpAdminLogs/
  tar -czf /logs/pg_log.tar.gz \
    gpdb_src/gpAux/gpdemo/datadirs/dbfast1/demoDataDir0/pg_log \
    gpdb_src/gpAux/gpdemo/datadirs/dbfast2/demoDataDir1/pg_log \
    gpdb_src/gpAux/gpdemo/datadirs/dbfast3/demoDataDir2/pg_log \
    gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror1/demoDataDir0/pg_log \
    gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror2/demoDataDir1/pg_log \
    gpdb_src/gpAux/gpdemo/datadirs/dbfast_mirror3/demoDataDir2/pg_log
EOF

exit $exitcode

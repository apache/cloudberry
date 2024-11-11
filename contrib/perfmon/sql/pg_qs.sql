-- start_ignore
\! sudo /bin/bash -c 'source /usr/local/cloudberry-db-devel/greenplum_path.sh;pip3 install psycopg2;pip3 install progressbar'
-- end_ignore
\!python3 src/gpmon/tests/pg_qs_test_runner.py --port $PGPORT --database gpperfmon --user gpadmin

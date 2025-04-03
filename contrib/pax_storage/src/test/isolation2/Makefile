#
# Makefile for isolation2 tests
#

CC = gcc

subdir = src/test/isolation2
top_builddir = ../../../../../
include $(top_builddir)/src/Makefile.global
PG_REGRESS = $(top_builddir)/src/test/regress/pg_regress
PG_REGRESS_DIR = $(top_builddir)/src/test/regress
PG_ISOLATION2_REGRESS = $(top_builddir)/src/test/isolation2/pg_isolation2_regress
PG_ISOLATION2_DIR = $(top_builddir)/src/test/isolation2

installcheck: isolation2_link
	PGOPTIONS="$(PGOPTIONS)" ./pg_isolation2_regress --init-file=./init_file --init-file=./init_file_isolation2 --schedule=isolation2_schedule

# Add a new rule for running installcheck with parallel mode enabled.
installcheck-cbdb-parallel: install
	(\
	export PGOPTIONS='-c optimizer=off -c enable_parallel=true'; \
	$(pg_isolation2_regress_installcheck) --init-file=$(top_builddir)/src/test/regress/init_file --init-file=./init_file_isolation2 --schedule=$(srcdir)/isolation2_schedule \
	)


isolation2_link: pg_isolation2_regress
	ln -sf $(PG_REGRESS_DIR)/explain.pm explain.pm
	ln -sf $(PG_REGRESS_DIR)/atmsort.pm atmsort.pm
	ln -sf $(PG_REGRESS_DIR)/GPTest.pm GPTest.pm
	ln -sf $(PG_ISOLATION2_DIR)/pg_isolation2_regress pg_isolation2_regress
	ln -sf $(PG_ISOLATION2_DIR)/sql_isolation_testcase.py sql_isolation_testcase.py
	ln -sf $(PG_ISOLATION2_DIR)/isolation2_regress.so isolation2_regress.so

pg_isolation2_regress:
	make -C $(top_builddir)/src/backend generated-headers
	make -C $(PG_ISOLATION2_DIR) install

clean:
	rm -f pg_isolation2_regress regress.out regress.diffs gpdiff.pl gpstringsubs.pl explain.pm atmsort.pm GPTest.pm sql_isolation_testcase.py
	rm -rf results
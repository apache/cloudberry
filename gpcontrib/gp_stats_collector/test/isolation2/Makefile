# isolation2 suite for the gp_stats_collector pg_query_state signal API.
#
# Multi-session tests that a plain pg_regress run cannot express (one backend
# polling another).  Reuses the core pg_isolation2_regress harness rather than
# rebuilding it.
#
# Prerequisites (handled by the CI isolation2 job):
#   - gp_inject_fault available (--enable-faultinjector, on by default) for the
#     happy-path spec.
#   - Extension installed and gp_stats_collector in shared_preload_libraries.
#   - Harness built: make -C $(top_builddir)/src/test/isolation2 install
#
# Run:
#   make -C gpcontrib/gp_stats_collector/test/isolation2 installcheck

top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

ISO2 = $(top_builddir)/src/test/isolation2

# isolation2_main.c hardcodes "python3 ./sql_isolation_testcase.py", resolved
# from the current directory, so symlink the core driver here before running.
installcheck:
	@ln -sf $(ISO2)/sql_isolation_testcase.py ./sql_isolation_testcase.py
	$(ISO2)/pg_isolation2_regress \
	  --init-file=$(top_builddir)/src/test/regress/init_file \
	  --init-file=$(ISO2)/init_file_isolation2 \
	  --inputdir=. --outputdir=. \
	  --bindir='$(bindir)' \
	  --schedule=./isolation2_schedule

clean:
	rm -rf results/ regression.diffs regression.out sql_isolation_testcase.py

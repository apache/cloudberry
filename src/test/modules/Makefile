# src/test/modules/Makefile

subdir = src/test/modules
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

SUBDIRS = \
		  brin \
		  commit_ts \
		  delay_execution \
		  dummy_index_am \
		  dummy_seclabel \
		  libpq_pipeline \
		  plsample \
		  snapshot_too_old \
		  spgist_name_ops \
		  test_bloomfilter \
		  test_ddl_deparse \
		  test_escape \
		  test_ginpostinglist \
		  test_integerset \
		  test_misc \
		  test_parser \
		  test_pg_dump \
		  test_predtest \
		  test_rbtree \
		  test_regex \
		  test_rls_hooks \
		  test_shm_mq \
		  worker_spi

#ignore cases:
# test_extensions
#	it uses PREPARE TRANSACTION which is not supported in GPDB.
# unsafe_tests
#	special check for DML on system relations in GPDB

# GPDB subdirs
SUBDIRS += test_planner
ifeq ($(with_ssl),openssl)
SUBDIRS += ssl_passphrase_callback
else
ALWAYS_SUBDIRS += ssl_passphrase_callback
endif

$(recurse)
$(recurse_always)

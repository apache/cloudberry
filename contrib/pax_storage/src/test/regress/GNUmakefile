CC = gcc
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
PGXS_DIR := $(dir $(PGXS))
REGRESS_DIR = $(PGXS_DIR)/../../src/test/regress
PG_REGRESS = $(REGRESS_DIR)/pg_regress

PKGLIBDIR = $(shell pg_config --pkglibdir)

# add arrow.so and headers
PG_CFLAGS += -I$(shell pg_config --includedir-server)
PG_LDFLAGS += -L$(shell pg_config --libdir)
# support libpq
PG_CFLAGS += -I $(shell pg_config --includedir)

all: regress_link

.PHONY: tablespace-setup
tablespace-setup:
	rm -rf ./testtablespace ./testtablespace_*
	mkdir -p ./testtablespace ./testtablespace_otherloc ./testtablespace_unlogged \
	./testtablespace_existing_version_dir/1/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/2/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/3/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/4/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/5/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/6/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/7/GPDB_99_399999991/ \
	./testtablespace_existing_version_dir/8/GPDB_99_399999991/ \
        ./testtablespace_1111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000/ \
	./testtablespace_default_tablespace \
	./testtablespace_temp_tablespace \
	./testtablespace_mytempsp0 \
	./testtablespace_mytempsp1 \
	./testtablespace_mytempsp2 \
	./testtablespace_mytempsp3 \
	./testtablespace_mytempsp4 \
	./testtablespace_database_tablespace

icw_test: tablespace-setup 
	$(PG_REGRESS) \
	--init-file=init_file --schedule=parallel_schedule --schedule=greenplum_schedule \
	--load-extension=gp_inject_fault
	

regress_link:
	ln -sf $(REGRESS_DIR)/twophase_pqexecparams twophase_pqexecparams
	ln -sf $(PKGLIBDIR)/regress.so regress.so
	rm -rf hooktest && mkdir hooktest
	ln -sf $(PKGLIBDIR)/test_hook.so hooktest/test_hook.so
	rm -rf query_info_hook_test && mkdir query_info_hook_test
	ln -sf $(PKGLIBDIR)/query_info_hook_test.so query_info_hook_test/query_info_hook_test.so

REGRESS = dummy
REGRESS_OPTS = --schedule=diskquota_schedule --init-file=../init_file --init-file=./regress_init_file

# FIXME: This check is hacky, since test_fetch_table_stat relies on the
# gp_inject_fault extension, we detect if the extension is built with
# greenplum by checking the output of the command 'pg_config --configure'.
# In the future, if the diskquota is built with GPDB7, or we backport the
# commit below to 6X_STABLE, we don't need this check.
# https://github.com/greenplum-db/gpdb/commit/8b897b12f6cb13753985faacab8e4053bf797a8b
ifneq (,$(findstring '--enable-debug-extensions',$(shell pg_config --configure)))
REGRESS_OPTS += --load-extension=gp_inject_fault
else
REGRESS_OPTS += --exclude-tests=test_fetch_table_stat
endif

PGXS := $(shell pg_config --pgxs)
include $(PGXS)

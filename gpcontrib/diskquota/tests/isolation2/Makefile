PGXS := $(shell pg_config --pgxs)
include $(PGXS)

## Build pg_isolation2_regress and install auxiliary scripts to the correct locations.
.PHONY: pg_isolation2_regress
pg_isolation2_regress:
	$(MAKE) -C $(abs_top_srcdir)/src/test/isolation2 install
	cp $(abs_top_srcdir)/src/test/isolation2/sql_isolation_testcase.py ./

.PHONY: installcheck
installcheck: pg_isolation2_regress
	$(abs_top_srcdir)/src/test/isolation2/pg_isolation2_regress --init-file=../init_file --psqldir=$(PSQLDIR) --inputdir=./sql --schedule=./isolation2_schedule --load-extension=gp_inject_fault

DATA         = $(wildcard sql/*.sql)
MODULES      = $(patsubst %.c,%,$(wildcard src/*.c))
EXTENSION    = gp_url_tools

TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PG_CONFIG    = pg_config
PGXS        := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

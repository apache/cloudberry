MODULE_big = fixeddecimal
OBJS = fixeddecimal.o

EXTENSION = fixeddecimal
DATA = fixeddecimal--1.0.0.sql
MODULES = fixeddecimal

CFLAGS=`pg_config --includedir-server`

TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --outputdir=test --load-extension=fixeddecimal

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

fixeddecimal.so: fixeddecimal.o

fixeddecimal.o: fixeddecimal.c


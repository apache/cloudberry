

MODULE_big = fixeddecimal
OBJS = fixeddecimal.o

EXTENSION = fixeddecimal
DATA = $(shell $(PG_CONFIG) --version | grep -qE " 9\.[5-9]| 10\.0" && \
		cat fixeddecimal--1.0.0_base.sql fixeddecimal--brin.sql > fixeddecimal--1.0.0.sql | echo fixeddecimal--1.0.0.sql || \
		cat fixeddecimal--1.0.0_base.sql > fixeddecimal--1.0.0.sql | echo fixeddecimal--1.0.0.sql)
MODULES = fixeddecimal

CFLAGS=`pg_config --includedir-server`

TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(shell $(PG_CONFIG) --version | grep -qE " 9\.[5-9]| 10\.0" && \
				echo aggregate brin cast comparison index overflow || \
				echo aggregate cast comparison index overflow)

REGRESS_OPTS = --inputdir=test --outputdir=test --load-extension=fixeddecimal

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

fixeddecimal.so: fixeddecimal.o

fixeddecimal.o: fixeddecimal.c


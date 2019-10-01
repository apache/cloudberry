MODULE_big = fixeddecimal
OBJS = fixeddecimal.o

EXTENSION = fixeddecimal

DATA = fixeddecimal--1.0.0--1.1.0.sql
DATA_built = fixeddecimal--1.1.0.sql

CFLAGS = `pg_config --includedir-server`

TESTS = $(wildcard test/sql/*.sql)

REGRESS_BRIN := $(shell pg_config --version | grep -qE "XL 9\.[5-9]| 10\.0| 11\.[0-9]| 12\.[0-9]" && echo brin-xl)
REGRESS_BRIN += $(shell pg_config --version | grep -E "9\.[5-9]| 10\.0| 11\.[0-9]| 12\.[0-9]" | grep -qEv "XL" && echo brin)
REGRESS_VERSION_SPECIFIC := $(shell pg_config --version | grep -qE "XL" && echo index-xl || echo index)
REGRESS = $(shell echo aggregate cast comparison overflow $(REGRESS_BRIN) $(REGRESS_VERSION_SPECIFIC))

REGRESS_OPTS = --inputdir=test --outputdir=test --load-extension=fixeddecimal

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

AGGSTATESQL := $(shell pg_config --version | grep -qE "XL" && echo fixeddecimalaggstate.sql)
AGGFUNCSSQL := $(shell pg_config --version | grep -qE "XL" && echo fixeddecimal--xlaggs.sql)

AGGFUNCSSQL := $(shell pg_config --version | grep -qE "9\.[6-9]| 10\.[0-9]| 11\.[0-9]| 12\.[0-9]" && echo fixeddecimal--parallelaggs.sql || echo fixeddecimal--aggs.sql)

BRINSQL := $(shell pg_config --version | grep -qE "9\.[5-9]| 10\.[0-9]| 11\.[0-9]| 12\.[0-9]" && echo fixeddecimal--brin.sql)

# 9.6 was the dawn of parallel query, so we'll use the parallel enabled .sql file from then on.
BASESQL := $(shell pg_config --version | grep -qE "9\.[6-9]| 10\.[0-9]| 11\.[0-9]| 12\.[0-9]" && echo fixeddecimal--1.1.0_base_parallel.sql || echo fixeddecimal--1.1.0_base.sql)

OBJECTS := $(addprefix $(srcdir)/, $(AGGSTATESQL) $(BASESQL) $(AGGFUNCSSQL) $(BRINSQL))

fixeddecimal--1.1.0.sql: $(OBJECTS)
	cat $^ > $@


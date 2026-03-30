MODULE_big = fixeddecimal
OBJS = fixeddecimal.o

EXTENSION = fixeddecimal

DATA = fixeddecimal--1.0.0--1.1.0.sql
DATA_built = fixeddecimal--1.1.0.sql

REGRESS_OPTS = --inputdir=test --outputdir=test --load-extension=fixeddecimal

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)

PG_VERSION_STR := $(shell $(PG_CONFIG) --version)
ifeq (,$(findstring XL,$(PG_VERSION_STR)))
REGRESS_BRIN := brin
REGRESS_VERSION_SPECIFIC := index
AGGSTATESQL :=
AGGFUNCSSQL := fixeddecimal--parallelaggs.sql
BRINSQL := fixeddecimal--brin.sql
BASESQL := fixeddecimal--1.1.0_base_parallel.sql
else
REGRESS_BRIN := brin-xl
REGRESS_VERSION_SPECIFIC := index-xl
AGGSTATESQL := fixeddecimalaggstate.sql
AGGFUNCSSQL := fixeddecimal--xlaggs.sql
BRINSQL :=
BASESQL := fixeddecimal--1.1.0_base.sql
endif
else
subdir = contrib/fixeddecimal
top_builddir = ../..

# Cloudberry in-tree builds are non-XL and support parallel/BRIN paths.
REGRESS_BRIN := brin
REGRESS_VERSION_SPECIFIC := index
AGGSTATESQL :=
AGGFUNCSSQL := fixeddecimal--parallelaggs.sql
BRINSQL := fixeddecimal--brin.sql
BASESQL := fixeddecimal--1.1.0_base_parallel.sql
endif

REGRESS = aggregate cast comparison overflow $(REGRESS_BRIN) $(REGRESS_VERSION_SPECIFIC)

ifeq ($(srcdir),)
FD_SRC_PREFIX :=
else
FD_SRC_PREFIX := $(srcdir)/
endif

OBJECTS := $(addprefix $(FD_SRC_PREFIX), $(AGGSTATESQL) $(BASESQL) $(AGGFUNCSSQL) $(BRINSQL))

fixeddecimal--1.1.0.sql: $(OBJECTS)
	cat $^ > $@

ifdef USE_PGXS
include $(PGXS)
else
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

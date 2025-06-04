# contrib/try_convert/Makefile

MODULE_big	= try_convert
OBJS		= try_convert.o $(WIN32RES)

EXTENSION = try_convert
DATA = try_convert--1.0.sql
PGFILEDESC = "try_convert - function for type cast"
REGRESS = try_convert

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/try_convert
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

.PHONY: generate-tests
generate-tests:
	mkdir -p input
	mkdir -p output
	python3 scripts/generate_test.py

all: generate-tests

.PHONY: verify
verify:
	python3 scripts/verify.py

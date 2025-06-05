# contrib/postgres_fdw/Makefile

MODULE_big = postgres_fdw
OBJS_FRONTEND = \
	link-canary.o

OBJS = \
	$(WIN32RES) \
	$(OBJS_FRONTEND) \
	connection.o \
	deparse.o \
	option.o \
	postgres_fdw.o \
	shippable.o

PGFILEDESC = "postgres_fdw - foreign data wrapper for PostgreSQL"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = -Wl,-Bsymbolic -Wl,-Bstatic -Wl,-Bstatic $(libpq) -Wl,-Bdynamic

EXTENSION = postgres_fdw
DATA = postgres_fdw--1.0.sql postgres_fdw--1.0--1.1.sql

REGRESS = gp2pg_postgres_fdw gp_postgres_fdw mpp_postgres_fdw #postgres_fdw
REGRESS_OPTS += --init-file=$(top_srcdir)/src/test/regress/init_file

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/postgres_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

link-canary.c : % : $(top_srcdir)/src/common/%
	rm -f $@ && $(LN_S) $< .

link-canary.o: link-canary.c
	$(CC) $(CFLAGS) -DFRONTEND $(CPPFLAGS) -c $< -o $@

clean: clean-symlinks

clean-symlinks:
	rm -f link-canary.c

# For postgres_fdw test
export PG_PORT=5432
installcheck: install prep_postgres
clean: clean_postgres
prep_postgres:
	./postgres_setup.bash
clean_postgres:
	./postgres_clean.bash
.PHONY: prep_postgres clean_postgres

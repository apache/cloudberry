# contrib/cloudberry_fdw/Makefile

MODULE_big = cloudberry_fdw
OBJS_FRONTEND = \
	link-canary.o

OBJS = \
	$(WIN32RES) \
	$(OBJS_FRONTEND) \
	connection.o \
	deparse.o \
	option.o \
	cloudberry_fdw.o \
	shippable.o

PGFILEDESC = "cloudberry_fdw - foreign data wrapper for Cloudberry"

PG_CPPFLAGS = -I$(libpq_srcdir)
SHLIB_LINK_INTERNAL = -Wl,-Bsymbolic -Wl,-Bstatic -Wl,-Bstatic $(libpq) -Wl,-Bdynamic

EXTENSION = cloudberry_fdw
DATA = cloudberry_fdw--1.0.sql cloudberry_fdw--1.0--1.1.sql

REGRESS = gp2pg_cloudberry_fdw gp_cloudberry_fdw mpp_cloudberry_fdw #cloudberry_fdw
REGRESS_OPTS += --init-file=$(top_srcdir)/src/test/regress/init_file

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
SHLIB_PREREQS = submake-libpq
subdir = contrib/cloudberry_fdw
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

# For cloudberry_fdw test
export PG_PORT=5432
installcheck: install prep_postgres
clean: clean_postgres
prep_postgres:
	./postgres_setup.bash
clean_postgres:
	./postgres_clean.bash
.PHONY: prep_postgres clean_postgres

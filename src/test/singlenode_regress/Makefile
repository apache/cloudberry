#
# Makefile for singlenode regress tests
#

PGFILEDESC = "singlenode_pg_regress - test driver"
PGAPPICON = win32

subdir = src/test/singlenode_regress
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

override CPPFLAGS := -I. -I$(srcdir) -I$(libpq_srcdir) \
	-I$(srcdir)/../regress $(CPPFLAGS)

OBJS = \
	$(WIN32RES)

submake-regress:
	$(MAKE) -C $(top_builddir)/src/test/regress all

regress.so: | submake-regress
	rm -f $@ && $(LN_S) $(top_builddir)/src/test/regress/regress.so .

refint$(DLSUFFIX): $(top_builddir)/contrib/spi/refint$(DLSUFFIX)
	cp $< $@

autoinc$(DLSUFFIX): $(top_builddir)/contrib/spi/autoinc$(DLSUFFIX)
	cp $< $@

all: refint$(DLSUFFIX) autoinc$(DLSUFFIX) regress.so

clean distclean:
	rm -f refint$(DLSUFFIX)
	rm -f autoinc$(DLSUFFIX)
	rm -f regress.so

##
## Run tests
##

REGRESS_OPTS = --dlpath=. --max-concurrent-tests=24 --init-file=$(srcdir)/init_file \
	$(EXTRA_REGRESS_OPTS)
tablespace-setup:
	rm -rf ./testtablespace
	mkdir -p ./testtablespace

installcheck-singlenode: installcheck-good-singlenode

installcheck-small-singlenode: all tablespace-setup
	(\
	gpconfig -c gp_appendonly_insert_files -v 0; \
	gpstop -u; \
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/parallel_schedule $(EXTRA_TESTS) \
	)

installcheck-good-singlenode: all tablespace-setup
	(\
	gpconfig -c gp_appendonly_insert_files -v 0; \
	gpstop -u; \
	$(pg_regress_installcheck) $(REGRESS_OPTS) --schedule=$(srcdir)/parallel_schedule --schedule=$(srcdir)/greenplum_schedule $(EXTRA_TESTS) \
	)

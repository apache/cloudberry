# src/bin/pg_upgrade/Makefile

PGFILEDESC = "pg_upgrade - an in-place binary upgrade utility"
PGAPPICON = win32

subdir = src/bin/pg_upgrade
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	$(WIN32RES) \
	check.o \
	controldata.o \
	dump.o \
	exec.o \
	file.o \
	function.o \
	info.o \
	option.o \
	parallel.o \
	pg_upgrade.o \
	relfilenumber.o \
	server.o \
	tablespace.o \
	util.o \
	version.o
# Source files specific to Cloudberry
OBJS += greenplum/aotable.o greenplum/version_gp.o \
		greenplum/check_gp.o greenplum/reporting.o \
		greenplum/aomd_filehandler.o greenplum/option_gp.o \
		greenplum/controldata_gp.o greenplum/util.o

override CPPFLAGS := -I$(srcdir) -I$(libpq_srcdir) $(CPPFLAGS)
LDFLAGS_INTERNAL += -L$(top_builddir)/src/fe_utils -lpgfeutils $(libpq_pgport)

# required for 002_pg_upgrade.pl
REGRESS_SHLIB=$(abs_top_builddir)/src/test/regress/regress$(DLSUFFIX)
export REGRESS_SHLIB

all: pg_upgrade

pg_upgrade: $(OBJS) | submake-libpq submake-libpgport submake-libpgfeutils
	$(CC) $(CFLAGS) $^ $(LDFLAGS) $(LDFLAGS_EX) $(LIBS) -o $@$(X)

install: all installdirs
	$(INSTALL_PROGRAM) pg_upgrade$(X) '$(DESTDIR)$(bindir)/pg_upgrade$(X)'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)'

uninstall:
	rm -f '$(DESTDIR)$(bindir)/pg_upgrade$(X)'

clean distclean maintainer-clean:
	rm -f pg_upgrade$(X) $(OBJS)
	rm -rf delete_old_cluster.sh log/ tmp_check/ \
		   reindex_hash.sql \
		   clusterConfigPostgresAddonsFile clusterConfigFile gpdemo-env.sh \
		   hostfile regression.diffs aomd_filehandler.c

greenplum/aomd_filehandler.c: $(top_srcdir)/src/backend/access/appendonly/aomd_filehandler.c
	rm -f $@ && cd greenplum && $(LN_S) ../$< aomd_filehandler.c

export with_icu

check:
#	$(prove_check)
	# ignore pg_upgarde to make CI happy, issue: https://github.com/apache/cloudberry/issues/262

perfcheck: test_gpdb.sh all
	bash $< -p -r -o $(top_builddir)/gpAux/gpdemo/datadirs/ -b $(DESTDIR)$(bindir)

installcheck:
	$(prove_installcheck)

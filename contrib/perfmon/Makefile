NAME = perfmon
EXTVERSION = 1.1.0

REGRESS = pre_run_check guc_config query extension_test post_run

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
top_builddir = ../../
subdir = contrib/perfmon
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

$(NAME)--$(EXTVERSION).sql: $(NAME).sql
	    cp $< $@

.PHONY: clean_perfmon
clean_perfmon:
	$(MAKE) -C src/gpmon clean
	$(MAKE) -C src/gpmmon clean
	$(MAKE) -C src/gpsmon clean
	rm -rf gpmon.so
	rm -rf gpsmon
	rm -rf gpperfmon.so
clean distclean: clean_perfmon

all: $(NAME)--$(EXTVERSION).sql
	$(MAKE) -C src/gpmon all
	$(MAKE) -C src/gpmmon all
	$(MAKE) -C src/gpsmon all

.dir)/extension/PHONY: installdirs
installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)/../sbin'
install:  installdirs
	$(MAKE) -C src/gpmon install
	$(MAKE) -C src/gpmmon install
	$(MAKE) -C src/gpsmon install
	$(INSTALL_SCRIPT) gpperfmon_install '$(bindir)'
	$(INSTALL_SCRIPT) gpperfmoncat.sh '$(DESTDIR)$(bindir)'
	$(INSTALL_SCRIPT) gpmon_catqrynow.py '$(DESTDIR)$(bindir)/../sbin/'
	$(INSTALL_SCRIPT) gpperfmon.conf '$(DESTDIR)$(datadir)'
	$(INSTALL_SCRIPT) $(wildcard $(NAME)*--*.sql) '$(DESTDIR)$(datadir)/extension/'
	$(INSTALL_SCRIPT) $(NAME).control '$(DESTDIR)$(datadir)/extension/'

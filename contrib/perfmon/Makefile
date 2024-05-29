NAME = gpperfmon
top_builddir = ../../


ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/perfmon
include $(top_builddir)/src/Makefile.global
endif

clean distclean:
	$(MAKE) -C src/gpmon clean
	$(MAKE) -C src/gpmmon clean
	$(MAKE) -C src/gpsmon clean
	rm -rf gpmon.so
	rm -rf gpsmon
	rm -rf gpperfmon.so

all:
	$(MAKE) -C src/gpmon all
	$(MAKE) -C src/gpmmon all
	$(MAKE) -C src/gpsmon all

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)/../sbin'
install:  installdirs
	$(MAKE) -C src/gpmon install
	$(MAKE) -C src/gpmmon install
	$(MAKE) -C src/gpsmon install
	$(INSTALL_SCRIPT) gpperfmon_install '$(bindir)'
	$(INSTALL_SCRIPT) gpperfmoncat.sh '$(DESTDIR)$(bindir)'
	$(INSTALL_SCRIPT) gpmon_catqrynow.py '$(DESTDIR)$(bindir)/../sbin/'
	mkdir -p '$(DESTDIR)$(libdir)/$(NAME)'
	$(INSTALL_SCRIPT) $(NAME).sql '$(DESTDIR)$(libdir)/$(NAME)'
	$(INSTALL_SCRIPT) $(NAME).conf '$(DESTDIR)$(libdir)/$(NAME)'

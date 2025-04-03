# contrib/pax_storage/Makefile

MODULE_big = pax
OBJS = \
	$(WIN32RES) 
PG_CPPFLAGS = -I/usr/local/include
PG_CXXFLAGS = -std=c++17

PGFILEDESC = "pax - PAX table access method"
SHLIB_LINK += -luuid

PAX_REGRESS_DIR = src/test/regress
PAX_ISOLATION2_DIR = src/test/isolation2

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
PGXS_DIR := $(dir $(PGXS))
PG_REGRESS = $(PGXS_DIR)/../../src/test/regress/pg_regress
else
subdir = contrib/pax_storage
top_builddir = ../../
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
PG_REGRESS = $(top_builddir)/src/test/regress/pg_regress
endif

.PHONY: all
all: build

DIRS = $(shell find src/cpp -maxdepth 2 -type d)
SRC = $(foreach dir, $(DIRS), $(wildcard $(dir)/*.cc))
CSRC = $(foreach dir, $(DIRS), $(wildcard $(dir)/*.c))

# read options from ENV
USE_MANIFEST_API := $(shell echo $$USE_MANIFEST_API)
USE_PAX_CATALOG := $(shell echo $$USE_PAX_CATALOG)

# if the ENVs are not set, set default value
ifeq ($(USE_MANIFEST_API),)
    USE_MANIFEST_API := OFF
endif

ifeq ($(USE_PAX_CATALOG),)
    USE_PAX_CATALOG := ON
endif

.PHONY: install-data build
build: $(SRC) $(CSRC)
	@echo "build pax, USE_MANIFEST_API=$(USE_MANIFEST_API) USE_PAX_CATALOG=$(USE_PAX_CATALOG)"
	@if [ ! -f build/Makefile ]; then \
		mkdir -p build && \
		cd build && \
		cmake -DCMAKE_INSTALL_PREFIX=$(DESTDIR)$(prefix) \
			-DUSE_MANIFEST_API=$(USE_MANIFEST_API) \
			-DUSE_PAX_CATALOG=$(USE_PAX_CATALOG) .. ; \
	fi
	cd build && make -j8
	@cp -f build/src/cpp/libpax.so pax.so

pax-unit-test: 
ifeq ($(enable_debug), yes)
	cd build && ./src/cpp/test_main
endif

install-data: build
	@echo "install data"
	$(INSTALL_DATA) pax-cdbinit--1.0.sql '$(DESTDIR)$(datadir)/cdb_init.d/pax-cdbinit--1.0.sql'

.PHONY: install
install: install-data
	$(INSTALL_SHLIB) pax.so '$(DESTDIR)$(pkglibdir)/pax.so'
	@if [ -f build/src/cpp/pax_dump ]; then $(INSTALL_PROGRAM) build/src/cpp/pax_dump '$(DESTDIR)$(bindir)/pax_dump'; fi;
	make -j10 -C build install

.PHONY: pax-test
pax-test: 
	@echo "Begin running pax tests"
	PGOPTIONS="$(PGOPTIONS)" \
	$(PG_REGRESS) \
		--init-file=init_file --schedule=pax_schedule \
		--load-extension=gp_inject_fault --dbname=pax_test
	$(bindir)/dropdb --if-exists pax_test

.PHONY: icw_test
icw_test: pax-test regress_test isolation2_test

.PHONY: regress_test isolation2_test
regress_test:
	@echo "Begin running pax regression tests"
	make -C $(PAX_REGRESS_DIR) regress_link
	@echo "Current PGOPTIONS=$(PGOPTIONS)"
	make -C $(PAX_REGRESS_DIR) icw_test

isolation2_test:
	@echo "Begin running pax isolation2 tests"
	make -C $(PAX_REGRESS_DIR) regress_link
	@echo "Current PGOPTIONS=$(PGOPTIONS)"
	make -C $(PAX_ISOLATION2_DIR) installcheck

.PHONY: uninstall-data
uninstall-data:
	$(RM) '$(DESTDIR)$(datadir)/cdb_init.d/pax-cdbinit--1.0.sql'

uninstall: uninstall-data
clean-data:
	$(RM) pax-cdbinit--1.0.sql
	$(RM) -r build

clean_isolation2_test:
	make -C $(PAX_ISOLATION2_DIR) clean

clean: clean-data clean_isolation2_test

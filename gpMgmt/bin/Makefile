# gpMgmt/bin/Makefile

default: install

top_builddir=../..
ifneq "$(wildcard $(top_builddir)/src/Makefile.global)" ""
include $(top_builddir)/src/Makefile.global
endif

SUBDIRS = stream gpcheckcat_modules gpconfig_modules gpssh_modules gppylib lib
SUBDIRS += ifaddrs

$(recurse)

PROGRAMS= analyzedb gpactivatestandby gpaddmirrors gpcheckcat gpcheckperf \
	gpcheckresgroupimpl gpconfig gpdeletesystem gpexpand gpshrink gpinitstandby \
	gpinitsystem gpload gpload.py gplogfilter gpmovemirrors \
	gppkg gprecoverseg gpreload gpsync gpsd gpssh gpssh-exkeys gpstart \
	gpstate gpstop minirepro gpmemwatcher gpmemreport gpdemo gpdirtableload \
	gpcheckresgroupv2impl

	GPDEMO_LIBS = gpdemo-defaults.sh lalshell generate_certs.sh demo_cluster.sh \
					probe_config.sh README

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(bindir)/lib'
	$(MKDIR_P) '$(DESTDIR)$(bindir)/lib/gpdemo'

installprograms: installdirs
	for file in $(PROGRAMS); do \
		$(INSTALL_SCRIPT) $$file '$(DESTDIR)$(bindir)/'$$file ; \
		$(PERL) $(top_builddir)/putversion '$(DESTDIR)$(bindir)/'$$file ; \
	done
	# install dependencies of gpdemo
	for file in $(GPDEMO_LIBS); do \
		$(INSTALL_SCRIPT) $(top_builddir)/gpAux/gpdemo/$$file '$(DESTDIR)$(bindir)/lib/gpdemo/'$$file ; \
	done
	# Symlink gpcheckcat from bin to bin/lib to maintain backward compatibility
	if [ ! -L $(DESTDIR)$(bindir)/lib/gpcheckcat  ]; then \
		cd $(DESTDIR)$(bindir)/lib/ && $(LN_S) ../gpcheckcat gpcheckcat; \
	fi
	$(INSTALL_DATA) gpload.bat '$(DESTDIR)$(bindir)/gpload.bat'

uninstall:
	for file in $(PROGRAMS); do \
		rm -f '$(DESTDIR)$(bindir)/'$$file ; \
	done
	for file in $(GPDEMO_LIBS); do \
		rm -f '$(DESTDIR)$(bindir)/lib/gpdemo/'$$file ; \
	done
	rm -f '$(DESTDIR)$(bindir)/gpload.bat'

#
# SOURCE DIRECTORIES
#
SRC=$(CURDIR)
PYLIB_SRC=$(SRC)/pythonSrc
PYLIB_SRC_EXT=$(PYLIB_SRC)/ext
SBIN_DIR=$(SRC)/../sbin
SERVER_SRC=$(SRC)
SERVER_SBIN=$(SERVER_SRC)/../sbin


#
# INSTALL DIRECTORY
#
LIB_DIR=$(SRC)/lib
PYLIB_DIR=$(SRC)/ext

core:
	python3 gpconfig_modules/parse_guc_metadata.py $(DESTDIR)$(prefix)

install: installdirs installprograms core

#
# Python Libraries
#

#
# PYLINT
#
MOCK_VERSION=1.0.1
MOCK_DIR=mock-$(MOCK_VERSION)
SETUP_TOOLS_VERSION=36.6.0
PARSE_VERSION=1.8.2
SETUP_TOOLS_DIR=setuptools-$(SETUP_TOOLS_VERSION)
PARSE_DIR=parse-$(PARSE_VERSION)
PYTHONSRC_INSTALL=$(PYLIB_SRC_EXT)/install
PYTHON_VERSION=$(shell python3 -c "import sys; print ('%s.%s' % (sys.version_info[0:2]))")
PYTHONSRC_INSTALL_SITE=$(PYLIB_SRC_EXT)/install/lib/python$(PYTHON_VERSION)/site-packages
PYTHONSRC_INSTALL_PYTHON_PATH=$(PYTHONPATH):$(PYTHONSRC_INSTALL_SITE)
# TODO: mock-1.0.1-py2.6.egg package should be updated.
MOCK_BIN=$(PYTHONSRC_INSTALL)/lib/python$(PYTHON_VERSION)/site-packages/mock-1.0.1-py2.6.egg
UBUNTU_PLATFORM=$(shell if lsb_release -a 2>/dev/null | grep -q 'Ubuntu' ; then echo "Ubuntu"; fi)

$(MOCK_BIN):
	@echo "--- mock for platform $(UBUNTU_PLATFORM)"
	@if [ "$(UBUNTU_PLATFORM)" = "Ubuntu" ]; then\
       pip3 install mock;\
     else\
       mkdir -p $(PYTHONSRC_INSTALL_SITE) && \
	   cd $(PYLIB_SRC_EXT)/ && $(TAR) xzf $(MOCK_DIR).tar.gz && \
	   cd $(PYLIB_SRC_EXT)/$(MOCK_DIR)/ && \
	   PYTHONPATH=$(PYTHONSRC_INSTALL_PYTHON_PATH) python3 setup.py install --prefix $(PYTHONSRC_INSTALL) ; \
	 fi;

# -----------------------------------------------------------------------------
#
# Ruff installation
#
# We dynamically retrieve the latest Ruff binary using the official install script.
# No tarballs or source files for Ruff are stored in this repository.
# This simplifies maintenance and ensures the linter is up-to-date.
#
# Note: Internet access is required for the initial installation.
# -----------------------------------------------------------------------------
RUFF_INSTALL_DIR ?= $(HOME)/.local/bin
RUFF_BIN=$(RUFF_INSTALL_DIR)/ruff

ruff:
	@if [ ! -x "$(RUFF_BIN)" ]; then \
		echo "--- Installing ruff linter"; \
		mkdir -p $(RUFF_INSTALL_DIR); \
		curl -LsSf https://astral.sh/ruff/install.sh | BIN_DIR=$(RUFF_INSTALL_DIR) sh; \
		echo "--- ruff installed at $(RUFF_BIN)"; \
	else \
		echo "--- ruff already installed at $(RUFF_BIN)"; \
	fi
	@$(RUFF_BIN) --version

PYTHON_FILES=`grep -l --exclude=Makefile \
		      --exclude=ruff.txt \
		      --exclude=gplogfilter \
		      "/bin/env python3" *`\
	      `grep -l "/bin/env python3" \
		       $(SRC)/../sbin/*`\
	      `find ./gppylib -name "*.py"`\
	      `find $(SRC)/../sbin -name "*.py"`

checkcode: ruff
	@echo "Running ruff on management scripts..."
	@RUST_LOG=error $(RUFF_BIN) check -v $(PYTHON_FILES) > $(SRC)/ruff.txt || true
	@echo "Ruff lint completed. Results saved to $(SRC)/ruff.txt"

check: $(MOCK_BIN)
	@echo "Running pure unit and also "unit" tests that require cluster to be up..."
	@TMPDIR=/tmp PYTHONPATH=$(SERVER_SRC):$(SERVER_SBIN):$(PYTHONPATH):$(PYTHONSRC_INSTALL_PYTHON_PATH):$(SRC)/ext:$(SBIN_DIR):$(LIB_DIR):$(PYLIB_DIR)/mock-1.0.1 \
	gppylib/gpunit discover --verbose -s $(SRC)/gppylib -p "test_unit*.py" 2> $(SRC)/../gpMgmt_testunit_results.log 1> $(SRC)/../gpMgmt_testunit_output.log
	@TMPDIR=/tmp PYTHONPATH=$(SERVER_SRC):$(SERVER_SBIN):$(PYTHONPATH):$(PYTHONSRC_INSTALL_PYTHON_PATH):$(SRC)/ext:$(SBIN_DIR):$(LIB_DIR):$(PYLIB_DIR)/mock-1.0.1 \
	gppylib/gpunit discover --verbose -s $(SRC)/gppylib -p "test_cluster*.py" 2>> $(SRC)/../gpMgmt_testunit_results.log 1>> $(SRC)/../gpMgmt_testunit_output.log

unitdevel:
	@echo "Running pure unit tests..."
	PYTHONPATH=$(SERVER_SRC):$(SERVER_SBIN):$(PYTHONPATH):$(PYTHONSRC_INSTALL_PYTHON_PATH):$(SRC)/ext:$(SBIN_DIR):$(LIB_DIR):$(PYLIB_DIR)/mock-1.0.1 \
	    python3 -m unittest discover --verbose -s $(SRC)/gppylib -p "test_unit*.py"


.PHONY: installcheck-bash
installcheck-bash:
	./test/suite.bash

.PHONY: installcheck
installcheck: installcheck-bash
	$(MAKE) -C gpload_test $@

clean distclean:
	rm -rf $(PYLIB_SRC_EXT)/$(PYLINT_DIR)
	rm -rf *.pyc
	rm -f analyzedbc gpactivatestandbyc gpaddmirrorsc gpcheckcatc \
		  gpcheckperfc gpcheckresgroupimplc gpchecksubnetcfgc gpconfigc \
		  gpdeletesystemc gpexpandc gpshrinkc gpinitstandbyc gplogfilterc gpmovemirrorsc \
		  gppkgc gprecoversegc gpreloadc gpscpc gpsyncc gpsdc gpssh-exkeysc gpsshc \
		  gpstartc gpstatec gpstopc minireproc gpcheckresgroupv2implc
	rm -f gpconfig_modules/gucs_disallowed_in_file.txt

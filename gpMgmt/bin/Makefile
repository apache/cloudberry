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
SBIN_DIR=$(SRC)/../sbin
SERVER_SRC=$(SRC)
SERVER_SBIN=$(SERVER_SRC)/../sbin

#
# INSTALL DIRECTORY
#
LIB_DIR=$(SRC)/lib

core:
	python3 gpconfig_modules/parse_guc_metadata.py $(DESTDIR)$(prefix)

install_python_deps:
	pip3 install --target=$(DESTDIR)$(bindir)/lib PyGreSQL==5.2.0
	pip3 install --target=$(DESTDIR)$(bindir)/lib psutil==5.7.0
	pip3 install --target=$(DESTDIR)$(bindir)/lib PyYAML==5.3.1
	pip3 install --target=$(DESTDIR)$(bindir)/lib mock==1.0.1
	pip3 install --target=$(DESTDIR)$(bindir)/lib parse==1.8.2
	pip3 install --target=$(DESTDIR)$(bindir)/lib pylint==0.21.0

install: installdirs installprograms install_python_deps core

clean distclean:
	rm -rf *.pyc
	rm -f analyzedbc gpactivatestandbyc gpaddmirrorsc gpcheckcatc \
		  gpcheckperfc gpcheckresgroupimplc gpchecksubnetcfgc gpconfigc \
		  gpdeletesystemc gpexpandc gpshrinkc gpinitstandbyc gplogfilterc gpmovemirrorsc \
		  gppkgc gprecoversegc gpreloadc gpscpc gpsyncc gpsdc gpssh-exkeysc gpsshc \
		  gpstartc gpstatec gpstopc minireproc gpcheckresgroupv2implc
	rm -f gpconfig_modules/gucs_disallowed_in_file.txt
top_builddir = ..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/gpMgmt/Makefile.behave

SUBDIRS= sbin bin doc

$(recurse)

generate_greenplum_path_file:
	mkdir -p $(DESTDIR)$(prefix)
	unset LIBPATH; \
	bin/generate-greenplum-path.sh > $(DESTDIR)$(prefix)/greenplum_path.sh

install: generate_greenplum_path_file
	mkdir -p $(DESTDIR)$(prefix)/lib/python

	# Install Python packages using pip
	pip install --target=$(DESTDIR)$(prefix)/lib/python psutil==5.7.0
	pip install --target=$(DESTDIR)$(prefix)/lib/python PyGreSQL==5.2
	pip install --target=$(DESTDIR)$(prefix)/lib/python PyYAML==5.3.1

	# Create __init__.py if not exists
	touch $(DESTDIR)$(prefix)/lib/python/__init__.py
	
clean distclean:
	$(MAKE) -C bin $@

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
	
    # Remove PyGreSQL, psutil and PyYAML specific copy operations
    # These will be installed via pip

clean distclean:
	$(MAKE) -C bin $@

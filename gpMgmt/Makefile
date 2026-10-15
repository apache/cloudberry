top_builddir = ..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/gpMgmt/Makefile.behave

SUBDIRS= sbin bin doc

$(recurse)

generate_cloudberry_env_file:
	mkdir -p $(DESTDIR)$(prefix)
	unset LIBPATH; \
	bin/generate-cloudberry-env.sh > $(DESTDIR)$(prefix)/cloudberry-env.sh

install: generate_cloudberry_env_file
	mkdir -p $(DESTDIR)$(prefix)/lib/python

	# Setup /lib/python contents
	# Remove previously installed copies first: cp -r merges into existing
	# directories, so stale files (e.g. compiled extensions from an older
	# bundled version, built for another Python ABI) would survive an
	# upgrade and break the new pure-Python sources.
	if [ -e bin/ext/__init__.py ]; then \
	    cp -rp bin/ext/__init__.py $(DESTDIR)$(prefix)/lib/python ; \
	fi
	if [ -e bin/ext/psutil ]; then \
	    rm -rf $(DESTDIR)$(prefix)/lib/python/psutil ; \
	    cp -rp bin/ext/psutil $(DESTDIR)$(prefix)/lib/python ; \
	fi
	if [ -e bin/ext/pgdb.py ]; then \
	    rm -f $(DESTDIR)$(prefix)/lib/python/_pg*.so ; \
	    cp -rp bin/ext/pgdb.py $(DESTDIR)$(prefix)/lib/python && \
	    cp -rp bin/ext/pg.py $(DESTDIR)$(prefix)/lib/python && \
	    cp -rp bin/ext/_pg*.so $(DESTDIR)$(prefix)/lib/python ; \
	fi
	if [ -e bin/ext/yaml ]; then \
	    rm -rf $(DESTDIR)$(prefix)/lib/python/yaml ; \
	    rm -f $(DESTDIR)$(prefix)/lib/python/_yaml*.so ; \
	    cp -rp bin/ext/yaml $(DESTDIR)$(prefix)/lib/python ; \
	fi

clean distclean:
	$(MAKE) -C bin $@

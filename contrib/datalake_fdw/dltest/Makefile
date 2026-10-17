# contrib/datalake_fdw/dltest/Makefile
#
# A storage backend in a library of its own.  It is not installed as part of
# datalake_fdw's own module: a server started to run the regression suite
# preloads it, and a production server never has it.  What it demonstrates is
# the boundary -- it is built against the five installed headers and reaches
# the extension only through datalake_storage_register().

MODULE_big = datalake_fdw_dltest

# The create-only local filesystem is compiled in from the same source the
# extension uses rather than linked against it, which is what a backend
# written outside this tree would have to do, and leaves this library with no
# symbol it expects the other one to supply.
OBJS = dltest_backend.o local_file_system_dltest.o

PKG_CONFIG ?= pkg-config
ARROW_MODULES = arrow
# Same reason as the parent: without the filter Arrow's own .pc file decides
# the C++ standard, and -std=c++11 does not compile Arrow's headers.
ARROW_CPPFLAGS := $(filter-out -std=%,\
	$(shell $(PKG_CONFIG) --cflags $(ARROW_MODULES) 2>/dev/null))
ARROW_LIBS := $(shell $(PKG_CONFIG) --libs $(ARROW_MODULES) 2>/dev/null)

PG_CXXFLAGS = -fvisibility=hidden -fvisibility-inlines-hidden -std=c++17
PG_CPPFLAGS = -I$(srcdir)/../src $(ARROW_CPPFLAGS)

# Shared libraries are linked with $(CC), so a module containing C++
# translation units pulls in the C++ runtime itself.
SHLIB_LINK += -lstdc++ $(ARROW_LIBS)

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/datalake_fdw/dltest
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

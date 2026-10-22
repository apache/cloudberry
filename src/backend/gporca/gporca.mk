override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libgpos/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libgpopt/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libnaucrates/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libgpdbcost/include $(CPPFLAGS)
# Do not omit frame pointer. Even with RELEASE builds, it is used for
# backtracing.
# Linux gcc tolerates -Werror -Wextra -Wpedantic on ORCA; Apple clang
# emits many more diagnostics under the same flags (unused-but-set,
# inconsistent-missing-override, mismatched-tags, ...) and breaks the
# build with no real safety benefit (per-feature -Werror= flags in the
# base CXXFLAGS still catch real bugs). Keep the strict triple on
# Linux, drop it on darwin.
ifeq ($(PORTNAME), darwin)
override CXXFLAGS := -fno-omit-frame-pointer $(CXXFLAGS)
else
override CXXFLAGS := -Werror -Wextra -Wpedantic -fno-omit-frame-pointer $(CXXFLAGS)
endif

# orca is not accessed in JIT (executor stage), avoid the generation of .bc here
# NOTE: accordingly we MUST avoid them in install step (install-postgres-bitcode
# in src/backend/Makefile)
with_llvm = no

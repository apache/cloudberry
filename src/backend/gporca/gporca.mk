override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libgpos/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libgpopt/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libnaucrates/include $(CPPFLAGS)
override CPPFLAGS := -I$(top_srcdir)/src/backend/gporca/libgpdbcost/include $(CPPFLAGS)
# Do not omit frame pointer. Even with RELEASE builds, it is used for
# backtracing.
ifeq ($(PORTNAME), darwin)
  # macOS SDK 26.2 compatibility: disable specific warnings
  override CXXFLAGS := -Wextra -Wpedantic -fno-omit-frame-pointer -Wno-unused-but-set-variable -Wno-deprecated-declarations -Wno-cast-function-type-strict -Wno-uninitialized -Wno-enum-conversion -Wno-incompatible-function-pointer-types $(CXXFLAGS)
else
  override CXXFLAGS := -Werror -Wextra -Wpedantic -fno-omit-frame-pointer $(CXXFLAGS)
endif

# orca is not accessed in JIT (executor stage), avoid the generation of .bc here
# NOTE: accordingly we MUST avoid them in install step (install-postgres-bitcode
# in src/backend/Makefile)
with_llvm = no

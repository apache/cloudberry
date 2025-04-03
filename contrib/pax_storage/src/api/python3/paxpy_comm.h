#pragma once
#include "paxpy_config_d.h"
#define FORMAT_CODE_PY_SSIZE_T "%" PY_FORMAT_SIZE_T "d"

/* GCC 4.0 and later have support for specifying symbol visibility */
#if __GNUC__ >= 4
#define HIDDEN __attribute__((visibility("hidden")))
#else
#define HIDDEN
#endif

/* debug printf-like function */
#ifdef PAXPY_DEBUG
extern HIDDEN int paxpy_debug_enabled;
#endif

#if defined(__GNUC__) && !defined(__APPLE__)
#ifdef PAXPY_DEBUG
#define PAXPY_PRINT(fmt, args...) \
  if (!paxpy_debug_enabled)       \
    ;                             \
  else                            \
    fprintf(stderr, "[%d] " fmt "\n", (int)getpid(), ##args)
#else
#define PAXPY_PRINT(fmt, args...)
#endif
#else /* !__GNUC__ or __APPLE__ */
#ifdef PAXPY_DEBUG
#include <stdarg.h>
static void PAXPY_PRINT(const char *fmt, ...) {
  va_list ap;

  if (!paxpy_debug_enabled) return;
  printf("[%d] ", (int)getpid());
  va_start(ap, fmt);
  vprintf(fmt, ap);
  va_end(ap);
  printf("\n");
}
#else
static void PAXPY_PRINT(const char *fmt, ...) {}
#endif
#endif

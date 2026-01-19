#include "postgres.h"
#include "cdb/cdbvars.h"
#include "funcapi.h"
#include "utils/builtins.h"

#include "hook_wrappers.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);
PG_FUNCTION_INFO_V1(yagp_stat_messages_reset);
PG_FUNCTION_INFO_V1(yagp_stat_messages);
PG_FUNCTION_INFO_V1(yagp_init_log);
PG_FUNCTION_INFO_V1(yagp_truncate_log);

void _PG_init(void) {
  if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE) {
    hooks_init();
  }
}

void _PG_fini(void) {
  if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE) {
    hooks_deinit();
  }
}

Datum yagp_stat_messages_reset(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    yagp_functions_reset();
  }

  funcctx = SRF_PERCALL_SETUP();
  SRF_RETURN_DONE(funcctx);
}

Datum yagp_stat_messages(PG_FUNCTION_ARGS) {
  return yagp_functions_get(fcinfo);
}

Datum yagp_init_log(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    init_log();
  }

  funcctx = SRF_PERCALL_SETUP();
  SRF_RETURN_DONE(funcctx);
}

Datum yagp_truncate_log(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    truncate_log();
  }

  funcctx = SRF_PERCALL_SETUP();
  SRF_RETURN_DONE(funcctx);
}

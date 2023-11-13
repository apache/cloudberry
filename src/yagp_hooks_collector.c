#include "postgres.h"
#include "cdb/cdbvars.h"
#include "utils/builtins.h"

#include "hook_wrappers.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);
PG_FUNCTION_INFO_V1(yagp_stat_messages_reset);
PG_FUNCTION_INFO_V1(yagp_stat_messages);

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
  yagp_functions_reset();
  PG_RETURN_VOID();
}

Datum yagp_stat_messages(PG_FUNCTION_ARGS) {
  return yagp_functions_get(fcinfo);
}
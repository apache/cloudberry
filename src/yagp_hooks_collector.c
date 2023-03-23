#include "postgres.h"
#include "cdb/cdbvars.h"
#include "fmgr.h"

#include "hook_wrappers.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

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

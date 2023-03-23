#include "postgres.h"
#include "cdb/cdbvars.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

void _PG_init(void) {
  if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE) {
    //greenplum_hook_init();
  }
}

void _PG_fini(void) {
  if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE) {
    //greenplum_hook_deinit();
  }
}

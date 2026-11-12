/*
 * This is a mock version of src/backend/utils/fmgrtab.c. The real fmgrtab.c
 * contains a large table, fmgr_builtins, which contains a pointer to all
 * the built-in functions that are exposed at SQL-level, in the pg_proc
 * catalog. We don't need the table in mock tests, and if we leave them out,
 * we don't need to link in the .o files containing them, which helps to cut
 * down the size of the test programs.
 */
#include "postgres.h"

#include "utils/fmgrtab.h"

const FmgrBuiltin fmgr_builtins[] = { };

const int fmgr_nbuiltins = 0;

/*
 * fmgr_isbuiltin() looks up an OID in this index (PG13 and later), so an
 * empty table still needs both of these: no OID is built in here, and the
 * index has the one entry the lookup of OID 0 may touch.
 */
const Oid	fmgr_last_builtin_oid = 0;

const uint16 fmgr_builtin_oid_index[1] = {InvalidOidBuiltinMapping};

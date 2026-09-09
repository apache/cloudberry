#ifndef CLOUDBERRY_PRIVACY_OUTPUT_H
#define CLOUDBERRY_PRIVACY_OUTPUT_H
#include "postgres.h"
#include "nodes/plannodes.h"
#include "nodes/pg_list.h"
/* Per-field final delivery hook. Must not mutate executor input slots. */
typedef Datum (*privacy_output_hook_type)(PlannedStmt *plan, List *targetlist,
    Oid relation, AttrNumber attribute, Oid type, Datum value, bool isnull,
    int operation);
/* Export a metadata-only plan: origins, policy versions and scoped grant. */
typedef PlannedStmt *(*privacy_endpoint_hook_type)(PlannedStmt *plan);
#define PRIVACY_SELECT 1
#define PRIVACY_COPY 2
#define PRIVACY_RETURNING 3
#define PRIVACY_EXTERNAL 4
#define PRIVACY_RETRIEVE 5
extern PGDLLIMPORT int cloudberry_privacy_output_abi;
extern PGDLLIMPORT privacy_output_hook_type cloudberry_privacy_output_hook;
extern PGDLLIMPORT privacy_endpoint_hook_type cloudberry_privacy_endpoint_hook;
#endif

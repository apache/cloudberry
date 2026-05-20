/*-------------------------------------------------------------------------
 *
 * matview.h
 *	  prototypes for matview.c.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/matview.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef MATVIEW_H
#define MATVIEW_H

#include "catalog/objectaddress.h"
#include "executor/execdesc.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/relcache.h"


extern void SetMatViewPopulatedState(Relation relation, bool newstate);

extern void SetMatViewIVMState(Relation relation, bool newstate);

extern void SetDynamicTableState(Relation relation);

extern ObjectAddress ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
										ParamListInfo params, QueryCompletion *qc);

extern DestReceiver *CreateTransientRelDestReceiver(Oid oid, Oid oldreloid, bool concurrent,
													char relpersistence, bool skipdata);

extern bool MatViewIncrementalMaintenanceIsEnabled(void);

extern void transientrel_init(QueryDesc *queryDesc);
extern void transientenr_init(QueryDesc *queryDesc);

extern Datum ivm_immediate_before(PG_FUNCTION_ARGS);
extern Datum ivm_immediate_maintenance(PG_FUNCTION_ARGS);
extern Datum ivm_immediate_cleanup(PG_FUNCTION_ARGS);
extern Datum ivm_visible_in_prestate(PG_FUNCTION_ARGS);
extern void AtAbort_IVM(void);
extern void AtEOXact_IVM(bool isCommit);
extern bool isIvmName(const char *s);
extern void mv_InitHashTables(void);
extern Size mv_TableShmemSize(void);
extern void AddPreassignedMVEntry(Oid matview_id, Oid table_id, const char* snapname);

extern Query *get_matview_query(Relation matviewRel);
#endif							/* MATVIEW_H */

#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "optimizer/orcaopt.h"

/* Declared in C++ headers (gpopt/utils/funcs.h, gpopt/CGPOptimizer.h) */
extern char *SerializeDXLPlan(Query *pquery);
extern PlannedStmt *GPOPTOptimizedPlan(Query *pquery, bool pfUnexpectedFailure, OptimizerOptions *opts);
extern Datum LibraryVersion(void);
extern Datum EnableXform(PG_FUNCTION_ARGS);
extern Datum DisableXform(PG_FUNCTION_ARGS);
extern void InitGPOPT(void);
extern void TerminateGPOPT(void);

char *
SerializeDXLPlan(Query *pquery)
{
	elog(ERROR, "mock implementation of SerializeDXLPlan called");
	return NULL;
}

PlannedStmt *
GPOPTOptimizedPlan(Query *pquery, bool pfUnexpectedFailure, OptimizerOptions *opts)
{
	elog(ERROR, "mock implementation of GPOPTOptimizedPlan called");
	return NULL;
}

Datum
LibraryVersion(void)
{
	elog(ERROR, "mock implementation of LibraryVersion called");
	PG_RETURN_VOID();
}

Datum
EnableXform(PG_FUNCTION_ARGS)
{
	elog(ERROR, "mock implementation of EnableXform called");
	PG_RETURN_VOID();
}

Datum
DisableXform(PG_FUNCTION_ARGS)
{
	elog(ERROR, "mock implementation of EnableXform called");
	PG_RETURN_VOID();
}

void
InitGPOPT(void)
{
	elog(ERROR, "mock implementation of InitGPOPT called");
}

void
TerminateGPOPT(void)
{
	elog(ERROR, "mock implementation of TerminateGPOPT called");
}

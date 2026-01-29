#include "postgres.h"

#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "optimizer/orcaopt.h"

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
InitGPOPT ()
{
	elog(ERROR, "mock implementation of InitGPOPT called");
}

void
TerminateGPOPT ()
{
	elog(ERROR, "mock implementation of TerminateGPOPT called");
}

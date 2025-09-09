/*-------------------------------------------------------------------------
*
* hook_test.c
*
* DENTIFICATION
*	src/test/regress/hootest/hook_test.c
*--------------------------------------------------------------------------
*/
#include "postgres.h"

#include "fmgr.h"
#include "optimizer/planner.h"

PG_MODULE_MAGIC;

static planner_hook_type prev_planner_hook = NULL;

static PlannedStmt *test_planner_hook(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams, OptimizerOptions *optimizer_options);

void		_PG_init(void);
void		_PG_fini(void);

void
_PG_init(void)
{
	prev_planner_hook = planner_hook;
	planner_hook = test_planner_hook;
}

void
_PG_fini(void)
{
	planner_hook = prev_planner_hook;
}

static PlannedStmt *
test_planner_hook(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams, OptimizerOptions *optimizer_options)
{
	PlannedStmt *stmt;

	elog(LOG, "In test_planner_hook");

	if (prev_planner_hook)
		stmt = (*prev_planner_hook) (parse, query_string, cursorOptions, boundParams, optimizer_options);
	else
		stmt = standard_planner(parse, query_string, cursorOptions, boundParams, optimizer_options);

	return stmt;
}

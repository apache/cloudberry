/*-------------------------------------------------------------------------
 *
 * parallel_foreign_scan_test_fdw.c
 *		Mock FDW that generates synthetic rows and supports parallel scan.
 *		Used to test kernel-level Gather + Parallel Foreign Scan paths
 *		without requiring an external data source (e.g., HDFS/PXF).
 *
 * Generates rows (id integer, val text) where id = 1..N and val = 'row_N'.
 * The number of rows is controlled by the 'num_rows' foreign table option
 * (default 10000).
 *
 * Parallel scan support:
 *   - IsForeignScanParallelSafe returns true
 *   - EstimateDSMForeignScan / InitializeDSMForeignScan / etc. manage a
 *     shared counter in DSM so parallel workers divide rows among themselves.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/table.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "storage/spin.h"
#include "utils/builtins.h"
#include "utils/rel.h"

#include "cdb/cdbpathlocus.h"
#include "cdb/cdbutil.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(parallel_foreign_scan_test_fdw_handler);

/*
 * Shared state in DSM for parallel workers.
 */
typedef struct ParallelTestState
{
	slock_t		mutex;
	int			next_row;
	int			total_rows;
} ParallelTestState;

/*
 * Per-backend scan state.
 */
typedef struct TestFdwState
{
	int			current_row;
	int			total_rows;
	bool		is_parallel;
	ParallelTestState *pstate;
} TestFdwState;

/*
 * Read the 'num_rows' option from the foreign table, default 10000.
 */
static int
get_num_rows(Oid foreigntableid)
{
	ForeignTable *ft = GetForeignTable(foreigntableid);
	ListCell   *lc;

	foreach(lc, ft->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "num_rows") == 0)
			return atoi(defGetString(def));
	}
	return 10000;
}

/*
 * GetForeignRelSize: estimate relation size.
 */
static void
testGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
					  Oid foreigntableid)
{
	baserel->rows = get_num_rows(foreigntableid);
}

/*
 * GetForeignPaths: create scan paths.
 *
 * Always creates a normal (non-parallel) path.
 * If parallel mode is possible, also creates a partial path for Gather.
 */
static void
testGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
					Oid foreigntableid)
{
	Cost		startup_cost = 0;
	Cost		total_cost = baserel->rows * 0.01;

	/* Normal path */
	add_path(baserel,
			 (Path *) create_foreignscan_path(root, baserel,
											  NULL,		/* target */
											  baserel->rows,
											  startup_cost,
											  total_cost,
											  NIL,		/* pathkeys */
											  NULL,		/* required_outer */
											  NULL,		/* fdw_outerpath */
											  NIL),		/* fdw_private */
			 root);

	/* Parallel partial path */
	if (baserel->consider_parallel && max_parallel_workers_per_gather > 0)
	{
		int			workers;
		Cost		parallel_total_cost;
		ForeignPath *ppath;

		workers = max_parallel_workers_per_gather;

		/*
		 * Per-worker cost: divide total by (workers + 1) so the Gather path
		 * is cheaper than the non-parallel path.
		 */
		parallel_total_cost = total_cost / (workers + 1);

		ppath = create_foreignscan_path(root, baserel,
										NULL,	/* target */
										baserel->rows,
										startup_cost,
										parallel_total_cost,
										NIL,	/* pathkeys */
										NULL,	/* required_outer */
										NULL,	/* fdw_outerpath */
										NIL);	/* fdw_private */

		ppath->path.parallel_safe = true;
		ppath->path.parallel_aware = true;
		ppath->path.parallel_workers = workers;
		ppath->path.rows = baserel->rows / (workers + 1);

		/* Set locus parallel_workers for Cloudberry Gather support */
		ppath->path.locus.parallel_workers = workers;

		add_partial_path(baserel, (Path *) ppath);
	}
}

/*
 * GetForeignPlan: create a ForeignScan plan node.
 */
static ForeignScan *
testGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
				   Oid foreigntableid,
				   ForeignPath *best_path,
				   List *tlist,
				   List *scan_clauses,
				   Plan *outer_plan)
{
	scan_clauses = extract_actual_clauses(scan_clauses, false);

	return make_foreignscan(tlist,
							scan_clauses,
							baserel->relid,
							NIL,	/* fdw_exprs */
							NIL,	/* fdw_private */
							NIL,	/* fdw_scan_tlist */
							NIL,	/* fdw_recheck_quals */
							outer_plan);
}

/*
 * BeginForeignScan: initialize scan state.
 */
static void
testBeginForeignScan(ForeignScanState *node, int eflags)
{
	TestFdwState *fdw_state;

	fdw_state = (TestFdwState *) palloc0(sizeof(TestFdwState));
	fdw_state->current_row = 0;
	fdw_state->total_rows = get_num_rows(RelationGetRelid(node->ss.ss_currentRelation));
	fdw_state->is_parallel = false;
	fdw_state->pstate = NULL;

	node->fdw_state = fdw_state;
}

/*
 * IterateForeignScan: return next tuple.
 *
 * In parallel mode, atomically claim the next row from shared state.
 * In non-parallel mode, simply increment current_row.
 */
static TupleTableSlot *
testIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	TestFdwState *fdw_state = (TestFdwState *) node->fdw_state;
	int			row_id;

	ExecClearTuple(slot);

	if (fdw_state->is_parallel && fdw_state->pstate != NULL)
	{
		ParallelTestState *pstate = fdw_state->pstate;

		SpinLockAcquire(&pstate->mutex);
		if (pstate->next_row < pstate->total_rows)
		{
			pstate->next_row++;
			row_id = pstate->next_row;
		}
		else
		{
			row_id = 0;		/* done */
		}
		SpinLockRelease(&pstate->mutex);
	}
	else
	{
		if (fdw_state->current_row < fdw_state->total_rows)
		{
			fdw_state->current_row++;
			row_id = fdw_state->current_row;
		}
		else
		{
			row_id = 0;		/* done */
		}
	}

	if (row_id > 0)
	{
		char		buf[64];

		slot->tts_values[0] = Int32GetDatum(row_id);
		slot->tts_isnull[0] = false;

		snprintf(buf, sizeof(buf), "row_%d", row_id);
		slot->tts_values[1] = PointerGetDatum(cstring_to_text(buf));
		slot->tts_isnull[1] = false;

		ExecStoreVirtualTuple(slot);
	}

	return slot;
}

/*
 * ReScanForeignScan: restart the scan.
 */
static void
testReScanForeignScan(ForeignScanState *node)
{
	TestFdwState *fdw_state = (TestFdwState *) node->fdw_state;

	fdw_state->current_row = 0;
}

/*
 * EndForeignScan: clean up.
 */
static void
testEndForeignScan(ForeignScanState *node)
{
	TestFdwState *fdw_state = (TestFdwState *) node->fdw_state;

	if (fdw_state)
		pfree(fdw_state);
}

/* ---- Parallel scan support ---- */

/*
 * IsForeignScanParallelSafe: allow parallel scan.
 */
static bool
testIsForeignScanParallelSafe(PlannerInfo *root, RelOptInfo *rel,
							  RangeTblEntry *rte)
{
	return true;
}

/*
 * EstimateDSMForeignScan: report shared memory needed.
 */
static Size
testEstimateDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt)
{
	return sizeof(ParallelTestState);
}

/*
 * InitializeDSMForeignScan: set up shared state for parallel scan (leader).
 */
static void
testInitializeDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt,
							 void *coordinate)
{
	ParallelTestState *pstate = (ParallelTestState *) coordinate;
	TestFdwState *fdw_state = (TestFdwState *) node->fdw_state;

	SpinLockInit(&pstate->mutex);
	pstate->next_row = 0;
	pstate->total_rows = fdw_state->total_rows;

	fdw_state->pstate = pstate;
	fdw_state->is_parallel = true;
}

/*
 * ReInitializeDSMForeignScan: reset shared state for rescan.
 */
static void
testReInitializeDSMForeignScan(ForeignScanState *node, ParallelContext *pcxt,
							   void *coordinate)
{
	ParallelTestState *pstate = (ParallelTestState *) coordinate;

	SpinLockAcquire(&pstate->mutex);
	pstate->next_row = 0;
	SpinLockRelease(&pstate->mutex);
}

/*
 * InitializeWorkerForeignScan: attach worker to shared state.
 */
static void
testInitializeWorkerForeignScan(ForeignScanState *node, shm_toc *toc,
								void *coordinate)
{
	ParallelTestState *pstate = (ParallelTestState *) coordinate;
	TestFdwState *fdw_state = (TestFdwState *) node->fdw_state;

	fdw_state->pstate = pstate;
	fdw_state->is_parallel = true;
}

/*
 * ShutdownForeignScan: no-op.
 */
static void
testShutdownForeignScan(ForeignScanState *node)
{
	/* nothing to do */
}

/*
 * FDW handler function — returns the FdwRoutine.
 */
Datum
parallel_foreign_scan_test_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Required scan callbacks */
	routine->GetForeignRelSize = testGetForeignRelSize;
	routine->GetForeignPaths = testGetForeignPaths;
	routine->GetForeignPlan = testGetForeignPlan;
	routine->BeginForeignScan = testBeginForeignScan;
	routine->IterateForeignScan = testIterateForeignScan;
	routine->ReScanForeignScan = testReScanForeignScan;
	routine->EndForeignScan = testEndForeignScan;

	/* Parallel scan callbacks */
	routine->IsForeignScanParallelSafe = testIsForeignScanParallelSafe;
	routine->EstimateDSMForeignScan = testEstimateDSMForeignScan;
	routine->InitializeDSMForeignScan = testInitializeDSMForeignScan;
	routine->ReInitializeDSMForeignScan = testReInitializeDSMForeignScan;
	routine->InitializeWorkerForeignScan = testInitializeWorkerForeignScan;
	routine->ShutdownForeignScan = testShutdownForeignScan;

	PG_RETURN_POINTER(routine);
}

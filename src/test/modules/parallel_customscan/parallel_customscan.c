#include "postgres.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "catalog/pg_type.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/extensible.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "cdb/cdbpathlocus.h"
#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "port/atomics.h"
#include "storage/shm_toc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

void		_PG_init(void);
void		_PG_fini(void);

PG_FUNCTION_INFO_V1(pcs_get_hook_calls);

/* GUC */
static bool pcs_enabled = false;

/*
 * Per-process hook-call counters.  Reset in EstimateDSMCustomScan (which is
 * called once per parallel-mode invocation in the leader).  The cross-worker
 * InitializeWorkerCustomScan count is aggregated into the DSM atomic and
 * harvested back into the leader-local counter in ShutdownCustomScan.
 */
static int64 pcs_n_estimate = 0;
static int64 pcs_n_init_dsm = 0;
static int64 pcs_n_reinit_dsm = 0;
static int64 pcs_n_init_worker = 0;
static int64 pcs_n_shutdown = 0;

/*
 * DSM header.  The child SeqScan owns the parallel table scan descriptor, so
 * the wrapper only needs a tiny shared area to count InitializeWorker calls
 * across workers.
 */
typedef struct PcsDSM
{
	pg_atomic_uint32 init_worker_calls;
} PcsDSM;

typedef struct PcsState
{
	CustomScanState csstate;
	PcsDSM	   *dsm;				/* set in InitDSM / InitWorker */
} PcsState;

static set_rel_pathlist_hook_type prev_pathlist_hook = NULL;

static Plan *pcs_plan_path(PlannerInfo *root, RelOptInfo *rel,
						   CustomPath *best_path, List *tlist,
						   List *clauses, List *custom_plans);
static Node *pcs_create_state(CustomScan *cscan);
static void pcs_begin(CustomScanState *node, EState *estate, int eflags);
static TupleTableSlot *pcs_exec(CustomScanState *node);
static void pcs_end(CustomScanState *node);
static void pcs_rescan(CustomScanState *node);
static Size pcs_estimate_dsm(CustomScanState *node, ParallelContext *pcxt);
static void pcs_init_dsm(CustomScanState *node, ParallelContext *pcxt,
						 void *coord);
static void pcs_reinit_dsm(CustomScanState *node, ParallelContext *pcxt,
						   void *coord);
static void pcs_init_worker(CustomScanState *node, shm_toc *toc, void *coord);
static void pcs_shutdown(CustomScanState *node);

static const CustomPathMethods pcs_path_methods =
{
	.CustomName = "ParallelCustomScan",
	.PlanCustomPath = pcs_plan_path,
};

static const CustomScanMethods pcs_scan_methods =
{
	.CustomName = "ParallelCustomScan",
	.CreateCustomScanState = pcs_create_state,
};

static const CustomExecMethods pcs_exec_methods =
{
	.CustomName = "ParallelCustomScan",
	.BeginCustomScan = pcs_begin,
	.ExecCustomScan = pcs_exec,
	.EndCustomScan = pcs_end,
	.ReScanCustomScan = pcs_rescan,
	.EstimateDSMCustomScan = pcs_estimate_dsm,
	.InitializeDSMCustomScan = pcs_init_dsm,
	.ReInitializeDSMCustomScan = pcs_reinit_dsm,
	.InitializeWorkerCustomScan = pcs_init_worker,
	.ShutdownCustomScan = pcs_shutdown,
};

static Plan *
pcs_plan_path(PlannerInfo *root, RelOptInfo *rel, CustomPath *best_path,
			  List *tlist, List *clauses, List *custom_plans)
{
	CustomScan *cs = makeNode(CustomScan);

	cs->scan.plan.targetlist = tlist;
	/*
	 * The child SeqScan does the filtering (create_scan_plan attaches the
	 * base restrictions to it), so the wrapper itself carries no qual.
	 */
	cs->scan.plan.qual = NIL;
	cs->scan.plan.parallel_aware = best_path->path.parallel_aware;
	cs->scan.plan.parallel_safe = best_path->path.parallel_safe;
	/*
	 * The wrapper delegates scanning to its child, so it is not itself a
	 * base-relation scan: scanrelid = 0.  custom_scan_tlist must then
	 * describe the scan tuple we hand upward; we mirror the child's output
	 * targetlist, and set_customscan_references() rewrites our own
	 * targetlist to reference it via INDEX_VAR.  This keeps projection
	 * correct for multi-column relations -- e.g. the catalog scans
	 * (pg_class) that ANALYZE issues internally, which a fixed
	 * base-relation descriptor would mis-deform.
	 */
	cs->scan.scanrelid = 0;
	cs->flags = best_path->flags;
	cs->custom_plans = custom_plans;
	cs->custom_exprs = NIL;
	cs->custom_private = NIL;
	cs->custom_scan_tlist =
		copyObject(((Plan *) linitial(custom_plans))->targetlist);
	cs->methods = &pcs_scan_methods;

	return (Plan *) cs;
}

static Node *
pcs_create_state(CustomScan *cscan)
{
	PcsState   *st = (PcsState *) newNode(sizeof(PcsState), T_CustomScanState);

	st->csstate.methods = &pcs_exec_methods;
	return (Node *) st;
}

static void
pcs_begin(CustomScanState *node, EState *estate, int eflags)
{
	CustomScan *cscan = (CustomScan *) node->ss.ps.plan;
	Plan	   *childplan = (Plan *) linitial(cscan->custom_plans);

	/*
	 * ExecInitCustomScan has already opened the scan relation and set up our
	 * scan/result slots and projection.  All we add is the child plan state,
	 * which becomes our sole custom_ps entry.  That child performs the actual
	 * (parallel) heap scan and is the node the MPP planstate walkers recurse
	 * into via the T_CustomScanState arm.
	 */
	node->custom_ps = list_make1(ExecInitNode(childplan, estate, eflags));
}

static TupleTableSlot *
pcs_child_next(CustomScanState *node)
{
	PlanState  *child = (PlanState *) linitial(node->custom_ps);
	TupleTableSlot *childslot = ExecProcNode(child);

	if (TupIsNull(childslot))
		return NULL;

	/*
	 * Move the child's tuple into our scan slot.  That slot's descriptor was
	 * built from custom_scan_tlist, which is an exact copy of the child's
	 * output targetlist, so the columns line up positionally and ExecScan's
	 * projection (compiled against the same slot) reads correct values.
	 */
	ExecCopySlot(node->ss.ss_ScanTupleSlot, childslot);
	return node->ss.ss_ScanTupleSlot;
}

static bool
pcs_recheck(CustomScanState *node, TupleTableSlot *slot)
{
	return true;
}

static TupleTableSlot *
pcs_exec(CustomScanState *node)
{
	return ExecScan(&node->ss,
					(ExecScanAccessMtd) pcs_child_next,
					(ExecScanRecheckMtd) pcs_recheck);
}

static void
pcs_end(CustomScanState *node)
{
	ExecEndNode((PlanState *) linitial(node->custom_ps));
}

static void
pcs_rescan(CustomScanState *node)
{
	ExecReScan((PlanState *) linitial(node->custom_ps));
}

static Size
pcs_estimate_dsm(CustomScanState *node, ParallelContext *pcxt)
{
	/* Reset per-process counters at the start of a parallel run. */
	pcs_n_estimate++;
	pcs_n_init_dsm = 0;
	pcs_n_reinit_dsm = 0;
	pcs_n_init_worker = 0;
	pcs_n_shutdown = 0;

	return sizeof(PcsDSM);
}

static void
pcs_init_dsm(CustomScanState *node, ParallelContext *pcxt, void *coord)
{
	PcsState   *st = (PcsState *) node;
	PcsDSM	   *dsm = (PcsDSM *) coord;

	pg_atomic_init_u32(&dsm->init_worker_calls, 0);
	st->dsm = dsm;
	pcs_n_init_dsm++;
}

static void
pcs_reinit_dsm(CustomScanState *node, ParallelContext *pcxt, void *coord)
{
	PcsState   *st = (PcsState *) node;
	PcsDSM	   *dsm = (PcsDSM *) coord;

	pg_atomic_write_u32(&dsm->init_worker_calls, 0);
	st->dsm = dsm;
	pcs_n_reinit_dsm++;
}

static void
pcs_init_worker(CustomScanState *node, shm_toc *toc, void *coord)
{
	PcsState   *st = (PcsState *) node;
	PcsDSM	   *dsm = (PcsDSM *) coord;

	st->dsm = dsm;

	/* This runs in a worker; the leader reads the aggregate in Shutdown. */
	pg_atomic_fetch_add_u32(&dsm->init_worker_calls, 1);
}

static void
pcs_shutdown(CustomScanState *node)
{
	PcsState   *st = (PcsState *) node;

	if (st->dsm != NULL)
		pcs_n_init_worker = pg_atomic_read_u32(&st->dsm->init_worker_calls);
	pcs_n_shutdown++;
}

static void
pcs_set_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, Index rti,
					 RangeTblEntry *rte)
{
	CustomPath *cp;
	CustomPath *pp;
	double		rows;
	CdbPathLocus inherited_locus;

	if (prev_pathlist_hook)
		prev_pathlist_hook(root, rel, rti, rte);

	if (!pcs_enabled)
		return;
	if (rte->rtekind != RTE_RELATION || rte->relkind != RELKIND_RELATION)
		return;

	/*
	 * In Cloudberry every Path must carry a CdbPathLocus.  Inherit it from
	 * the first existing pathlist entry (set up by set_plain_rel_pathlist)
	 * so the planner can dispatch our CustomScan the same way a regular
	 * SeqScan would be dispatched.
	 */
	if (rel->pathlist == NIL)
		return;
	inherited_locus = ((Path *) linitial(rel->pathlist))->locus;

	rows = rel->tuples > 0 ? rel->tuples : 1.0;

	cp = makeNode(CustomPath);
	cp->path.pathtype = T_CustomScan;
	cp->path.parent = rel;
	cp->path.pathtarget = rel->reltarget;
	cp->path.param_info = NULL;
	cp->path.parallel_aware = false;
	cp->path.parallel_safe = true;
	cp->path.parallel_workers = 0;
	cp->path.pathkeys = NIL;
	cp->path.rows = rows;
	cp->path.startup_cost = 0;
	cp->path.total_cost = rows * 0.001;	/* cheaper than seqscan to win */
	cp->path.locus = inherited_locus;
	cp->flags = 0;
	/*
	 * A freshly built SeqScan path is the wrapper's child.  It must be a new
	 * node (not one already in rel->pathlist): add_path() below may pfree a
	 * dominated seqscan path, which would leave a dangling child pointer.
	 */
	cp->custom_paths = list_make1(create_seqscan_path(root, rel, NULL, 0));
	cp->custom_private = NIL;
	cp->methods = &pcs_path_methods;
	add_path(rel, &cp->path, root);

	if (rel->consider_parallel)
	{
		CdbPathLocus parallel_locus = cdbpathlocus_from_baserel(root, rel, 2);

		pp = makeNode(CustomPath);
		pp->path.pathtype = T_CustomScan;
		pp->path.parent = rel;
		pp->path.pathtarget = rel->reltarget;
		pp->path.param_info = NULL;
		pp->path.parallel_aware = true;
		pp->path.parallel_safe = true;
		pp->path.parallel_workers = parallel_locus.parallel_workers;
		pp->path.pathkeys = NIL;
		pp->path.rows = rows / Max(2, parallel_locus.parallel_workers);
		pp->path.startup_cost = 0;
		pp->path.total_cost = pp->path.rows * 0.0001;	/* much cheaper */
		pp->path.locus = parallel_locus;
		pp->flags = 0;
		/* Parallel-aware child so it splits blocks across workers. */
		pp->custom_paths =
			list_make1(create_seqscan_path(root, rel, NULL,
										   parallel_locus.parallel_workers));
		pp->custom_private = NIL;
		pp->methods = &pcs_path_methods;
		add_partial_path(rel, &pp->path);
	}
}

Datum
pcs_get_hook_calls(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		values[5];
	bool		nulls[5] = {false, false, false, false, false};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = Int64GetDatum(pcs_n_estimate);
	values[1] = Int64GetDatum(pcs_n_init_dsm);
	values[2] = Int64GetDatum(pcs_n_reinit_dsm);
	values[3] = Int64GetDatum(pcs_n_init_worker);
	values[4] = Int64GetDatum(pcs_n_shutdown);

	tuple = heap_form_tuple(tupdesc, values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}

void
_PG_init(void)
{
	DefineCustomBoolVariable("parallel_customscan.enabled",
							 "Replace seqscan paths with TestParallelCustomScan.",
							 NULL,
							 &pcs_enabled,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	RegisterCustomScanMethods(&pcs_scan_methods);

	prev_pathlist_hook = set_rel_pathlist_hook;
	set_rel_pathlist_hook = pcs_set_rel_pathlist;
}

void
_PG_fini(void)
{
	set_rel_pathlist_hook = prev_pathlist_hook;
}

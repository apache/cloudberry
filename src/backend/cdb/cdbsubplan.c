/*-------------------------------------------------------------------------
 *
 * cdbsubplan.c
 *	  Provides routines for preprocessing initPlan subplans
 *		and executing queries with initPlans
 *
 * Portions Copyright (c) 2003-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/cdb/cdbsubplan.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "executor/nodeSubplan.h"	/* For ExecSetParamPlan */
#include "executor/executor.h"	/* For CreateExprContext */
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbplan.h"
#include "cdb/cdbllize.h"
#include "cdb/cdbsubplan.h"
#include "cdb/cdbvars.h"		/* currentSliceId */
#include "utils/tuplestore.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbtm.h"			/* isCurrentDtxActivated */
#include "cdb/cdbmotion.h"
#include "cdb/ml_ipc.h"
#include "nodes/nodeFuncs.h"		/* planstate_tree_walker */
#include "nodes/pg_list.h"

static bool isParamExecutableNow(SubPlanState *spstate, ParamExecData *prmList, Bitmapset *setparams);
static int	initplan_root_slice(QueryDesc *queryDesc, SubPlanState *sps);
static int	count_qe_slices(SliceTable *sliceTable, int sliceIndex);
static bool plan_tree_uses_params(PlanState *planstate, Bitmapset *params);
static void run_initplans_together(QueryDesc *queryDesc, List *group);

/*
 * The most QE slices that run_initplans_together() dispatches at once. Each
 * QE slice needs a gang of its own. Run one at a time, initplans can reuse
 * the same gangs; run together, they need as many gangs as they have QE
 * slices. So larger groups of initplans are split into batches.
 */
#define MAX_INITPLAN_GROUP_SLICES	8

/*
 * Function preprocess_initplans() is called from ExecutorRun running a
 * parallel plan on the QD.  The call happens prior to dispatch of the
 * main plan, and only if there are some initplans.
 *
 * Argument queryDesc is the one passed in to ExecutorRun.
 *
 * The function loops through the estate->es_param_exec_vals array, which
 * has plan->nParamExec elements.  Each element is a ParamExecData struct,
 * and the index of the element in the array is the paramid of the Param
 * node in the Plan that corresponds to the result of the subquery.
 *
 * The execPlan member points to a SubPlanState struct for the
 * subquery.  The value and isnull members hold the result
 * of executing the SubPlan.
 * I think that the order of the elements in this array guarantees
 * that for a subplan X within a subplan Y, X will come before Y in the array.
 * If a subplan returns multiple columns (like a MULTIEXPR_SUBLINK), each will be
 * a separate entry in the es_param_exec_vals array, but they will all have
 * the same value for execPlan.
 * In order to evaluate a subplan, we call ExecSetParamPlan.
 * This is a postgres function, but has been modified from its original form
 * to parallelize subplans. Inside ExecSetParamPlan, the
 * datum result(s) of the subplan are stuffed into the value field
 * of the ParamExecData struct(s).	It finds the proper one based on the
 * setParam list in the SubPlan node.
 * In order to handle SubPlans of SubPlans, we pass in the values of the
 * estate->es_param_exec_vals as ParamListInfo structs to the ExecSetParamPlan call.
 * These are then serialized into the mppexec all as parameters.  In this manner, the
 * result of a SubPlan of a SubPlan is available.
 */
void
preprocess_initplans(QueryDesc *queryDesc)
{
	int			nParamExec;
	int			i;
	EState	   *estate = queryDesc->estate;
	int			originalSlice,
				rootIndex;
	Bitmapset  *setparams = NULL;

	nParamExec = list_length(queryDesc->plannedstmt->paramExecTypes);
	if (nParamExec == 0)
		return;

	originalSlice = LocallyExecutingSliceIndex(queryDesc->estate);
	Assert(originalSlice == 0); /* Original slice being executed is slice 0 */

	/*
	 * Loop through the estate->es_param_exec_vals and collect all setparams
	 * which should only execute initplan once so that we could get result.
	 */
	for(i = 0; i < nParamExec; i++)
	{
		ParamExecData *prm;
		SubPlanState *sps;
		ListCell *lc;

		prm = &estate->es_param_exec_vals[i];
		sps = (SubPlanState *) prm->execPlan;

		if (!sps)
			continue;

		SubPlan *subplan = sps->subplan;
		if (subplan->setParam)
		{
			foreach(lc, subplan->setParam)
			{
				setparams = bms_add_member(setparams, lfirst_int(lc));
			}
		}
	}

	/*
	 * Loop through the estate->es_param_exec_vals. This array has an element
	 * for each PARAM_EXEC (internal) param, and a pointer to the SubPlanState
	 * to execute to evaluate it. It seems that they are created in the proper
	 * order, i.e. if a subplan x has a sublan y, then y will come before x in
	 * the es_param_exec_vals array.
	 */

	/*
	 * CDB: Initplans that don't depend on any other param are independent of
	 * each other, so instead of dispatching and running them one at a time,
	 * we can dispatch them all at once and let their QE slices run
	 * concurrently. Their root slices still run one after another on the QD,
	 * but by then the QEs have done most of the work. To bound the number of
	 * gangs, they are dispatched in batches of at most
	 * MAX_INITPLAN_GROUP_SLICES QE slices. Whatever is left, or isn't safe to
	 * run that way, is handled by the loop below as before.
	 */
	if (queryDesc->plannedstmt->subplan_sliceIds)
	{
		List	   *group = NIL;
		List	   *batch = NIL;
		int			batchSlices = 0;
		ListCell   *lc;

		for (i = 0; i < nParamExec; i++)
		{
			ParamExecData *prm = &estate->es_param_exec_vals[i];
			SubPlanState *sps = (SubPlanState *) prm->execPlan;

			if (!sps || list_member_ptr(group, sps))
				continue;
			if (sps->subplan->extParam != NIL ||
				sps->subplan->subLinkType == INITPLAN_FUNC_SUBLINK)
				continue;
			/*
			 * The param values are sent with the dispatch, before any
			 * initplan in the group has run. So none of them may use the
			 * output of another initplan anywhere in its plan tree. Note
			 * that extParam doesn't tell: when an initplan uses the result of
			 * a nested initplan, e.g. "WHERE x = (SELECT min(y) ...)" inside
			 * it, that param is not external to it. Nor does allParam of the
			 * top plan node: it only includes the extParam of the node's
			 * children, and a param set by an initplan attached to a node
			 * below is not in that node's extParam. So check every node.
			 */
			if (plan_tree_uses_params(sps->planstate, setparams))
				continue;
			/*
			 * With EXPLAIN ANALYZE, ExecSetParamPlan() collects the QE
			 * statistics of each initplan from its own dispatch; we don't
			 * handle that for a shared dispatch.
			 */
			if (sps->planstate->instrument && sps->planstate->instrument->need_cdb)
				continue;
			if (initplan_root_slice(queryDesc, sps) <= 0)
				continue;
			group = lappend(group, sps);
		}

		elog(DEBUG1, "preprocess_initplans: found %d independent initplans to run together",
			 list_length(group));
		if (list_length(group) >= 2)
		{
			foreach(lc, group)
			{
				SubPlanState *sps = (SubPlanState *) lfirst(lc);
				int			nslices;

				nslices = count_qe_slices(estate->es_sliceTable,
										  initplan_root_slice(queryDesc, sps));
				if (batch != NIL && batchSlices + nslices > MAX_INITPLAN_GROUP_SLICES)
				{
					if (list_length(batch) >= 2)
						run_initplans_together(queryDesc, batch);
					list_free(batch);
					batch = NIL;
					batchSlices = 0;
				}
				batch = lappend(batch, sps);
				batchSlices += nslices;
			}
			if (list_length(batch) >= 2)
				run_initplans_together(queryDesc, batch);
			list_free(batch);
		}
		list_free(group);
	}

	for (i = 0; i < nParamExec; i++)
	{
		ParamExecData *prm;
		SubPlanState *sps;

		prm = &estate->es_param_exec_vals[i];
		sps = (SubPlanState *) prm->execPlan;

		if (isParamExecutableNow(sps, estate->es_param_exec_vals, setparams))
		{
			SubPlan    *subplan = sps->subplan;
			int			qDispSliceId;

			Assert(IsA(subplan, SubPlan));

			if (queryDesc->plannedstmt->subplan_sliceIds)
			{
				qDispSliceId = queryDesc->plannedstmt->subplan_sliceIds[subplan->plan_id - 1];
				if (queryDesc->plannedstmt->slices[qDispSliceId].parentIndex != -1)
					qDispSliceId = 0;		/* not an init plan */
			}
			else
				qDispSliceId = 0;

			if (qDispSliceId > 0)
			{
				/*
				 * Adjust for the slice to execute on the QD.
				 */
				rootIndex = qDispSliceId;
				queryDesc->estate->es_sliceTable->localSlice = rootIndex;

				/* set our global sliceid variable for elog. */
				currentSliceId = rootIndex;

				/*
				 * Use ExprContext to set the param. If ExprContext is not
				 * initialized, create a new one here. (see MPP-3511)
				 */
				if (sps->planstate->ps_ExprContext == NULL)
					sps->planstate->ps_ExprContext = CreateExprContext(estate);

				/* MPP-12048: Set the right slice index before execution. */
				Assert(qDispSliceId < queryDesc->plannedstmt->numSlices);

				Assert(LocallyExecutingSliceIndex(sps->planstate->state) == qDispSliceId);

				/*
				 * Dispatch this initplan query. ExecSetParamPlan() calls
				 * CdbDispatchPlan() to dispatch it.
				 *
				 * Note that this will rebuild the array of dispatched PARAM_EXEC
				 * values on every iteration. That's important because the set
				 * of valid PARAM_EXEC values grows on every iteration, and
				 * later Init plans can depend on previous ones.
				 */
				ExecSetParamPlan(sps, sps->planstate->ps_ExprContext, queryDesc);
			}
		}

		queryDesc->estate->es_sliceTable->localSlice = originalSlice;
		currentSliceId = originalSlice;
	}
}

/* Does no slice in the tree under 'sliceIndex' write? */
static bool
slice_tree_is_read_only(SliceTable *sliceTable, int sliceIndex)
{
	ExecSlice  *slice = &sliceTable->slices[sliceIndex];
	ListCell   *lc;

	if (slice->gangType == GANGTYPE_PRIMARY_WRITER)
		return false;
	foreach(lc, slice->children)
	{
		if (!slice_tree_is_read_only(sliceTable, lfirst_int(lc)))
			return false;
	}
	return true;
}

/*
 * Return the index of the QD root slice of an initplan, if it's one that
 * run_initplans_together() can handle: it must run on the QD and have QE
 * slices below it, none of which writes. Otherwise return -1.
 */
static int
initplan_root_slice(QueryDesc *queryDesc, SubPlanState *sps)
{
	PlannedStmt *stmt = queryDesc->plannedstmt;
	SliceTable *sliceTable = queryDesc->estate->es_sliceTable;
	int			sliceIndex;
	ExecSlice  *slice;

	sliceIndex = stmt->subplan_sliceIds[sps->subplan->plan_id - 1];
	if (sliceIndex <= 0 || stmt->slices[sliceIndex].parentIndex != -1)
		return -1;

	slice = &sliceTable->slices[sliceIndex];
	if (slice->gangType != GANGTYPE_UNALLOCATED || slice->children == NIL)
		return -1;
	if (!slice_tree_is_read_only(sliceTable, sliceIndex))
		return -1;

	return sliceIndex;
}

/*
 * Does any node in the plan tree under 'planstate', including its initplans
 * and subplans, refer to any of 'params'?
 */
static bool
plan_tree_uses_params(PlanState *planstate, Bitmapset *params)
{
	if (planstate == NULL)
		return false;
	if (bms_overlap(planstate->plan->allParam, params))
		return true;
	return planstate_tree_walker(planstate, plan_tree_uses_params, params);
}

/* Number of QE slices in the tree under the QD slice 'sliceIndex' */
static int
count_qe_slices(SliceTable *sliceTable, int sliceIndex)
{
	ExecSlice  *slice = &sliceTable->slices[sliceIndex];
	ListCell   *lc;
	int			n = 0;

	foreach(lc, slice->children)
		n += 1 + count_qe_slices(sliceTable, lfirst_int(lc));
	return n;
}

static void
set_slice_tree_root(SliceTable *sliceTable, int sliceIndex, int rootIndex,
					List **saved)
{
	ExecSlice  *slice = &sliceTable->slices[sliceIndex];
	ListCell   *lc;

	*saved = lappend_int(*saved, sliceIndex);
	*saved = lappend_int(*saved, slice->rootIndex);
	slice->rootIndex = rootIndex;
	foreach(lc, slice->children)
		set_slice_tree_root(sliceTable, lfirst_int(lc), rootIndex, saved);
}

/*
 * Run a group of independent initplans with a single dispatch.
 *
 * The dispatcher and the interconnect both work on the slice tree under one
 * root slice. So we temporarily graft the QE slices of all the initplans in
 * the group under the root slice of the first one: they are dispatched
 * together, start running concurrently, and send their results to the QD.
 * The QD then runs the root slice of each initplan in turn, each reading
 * from its own Motions, which are identified by the sending slice. Finally
 * we tear down the interconnect, wait for the QEs, and restore the slice
 * table, which the main plan's dispatch uses afterwards.
 */
static void
run_initplans_together(QueryDesc *queryDesc, List *group)
{
	EState	   *estate = queryDesc->estate;
	SliceTable *sliceTable = estate->es_sliceTable;
	int			originalSlice = sliceTable->localSlice;
	int			firstRoot = -1;
	ExecSlice  *rootSlice = NULL;
	List	   *savedRootChildren = NIL;
	List	   *grafted = NIL;		/* (slice, old parentIndex) pairs */
	List	   *savedRoots = NIL;	/* (slice, old rootIndex) pairs */
	ListCell   *lc;

	foreach(lc, group)
	{
		SubPlanState *sps = (SubPlanState *) lfirst(lc);
		int			sliceIndex = initplan_root_slice(queryDesc, sps);
		ListCell   *lc2;

		if (firstRoot < 0)
		{
			firstRoot = sliceIndex;
			rootSlice = &sliceTable->slices[firstRoot];
			savedRootChildren = list_copy(rootSlice->children);
			continue;
		}
		foreach(lc2, sliceTable->slices[sliceIndex].children)
		{
			int			child = lfirst_int(lc2);

			grafted = lappend_int(grafted, child);
			grafted = lappend_int(grafted, sliceTable->slices[child].parentIndex);
			sliceTable->slices[child].parentIndex = firstRoot;
			rootSlice->children = lappend_int(rootSlice->children, child);
			set_slice_tree_root(sliceTable, child, firstRoot, &savedRoots);
		}
	}

	sliceTable->localSlice = firstRoot;
	currentSliceId = firstRoot;

	PG_TRY();
	{
		CdbDispatchPlan(queryDesc, estate->es_param_exec_vals,
						isCurrentDtxActivated(), true);
		CurrentMotionIPCLayer->SetupInterconnect(estate);
		UpdateMotionExpectedReceivers(estate->motionlayer_context, sliceTable);

		foreach(lc, group)
		{
			SubPlanState *sps = (SubPlanState *) lfirst(lc);

			if (sps->planstate->ps_ExprContext == NULL)
				sps->planstate->ps_ExprContext = CreateExprContext(estate);
			ExecSetParamPlanPredispatched(sps, sps->planstate->ps_ExprContext, queryDesc);
		}

		CurrentMotionIPCLayer->TeardownInterconnect(estate->interconnect_context, false);
		estate->interconnect_context = NULL;
		estate->es_interconnect_is_setup = false;

		if (estate->dispatcherState && estate->dispatcherState->primaryResults)
		{
			CdbDispatcherState *ds = estate->dispatcherState;
			ErrorData  *qeError = NULL;

			cdbdisp_checkDispatchResult(ds, DISPATCH_WAIT_NONE);
			cdbdisp_getDispatchResults(ds, &qeError);
			estate->dispatcherState = NULL;
			if (qeError)
			{
				FlushErrorState();
				ThrowErrorData(qeError);
			}
			cdbdisp_destroyDispatcherState(ds);
		}
	}
	PG_CATCH();
	{
		if (estate->es_interconnect_is_setup)
		{
			CurrentMotionIPCLayer->TeardownInterconnect(estate->interconnect_context, true);
			estate->interconnect_context = NULL;
			estate->es_interconnect_is_setup = false;
		}
		if (estate->dispatcherState)
		{
			CdbDispatcherState *ds = estate->dispatcherState;

			estate->dispatcherState = NULL;
			CdbDispatchHandleError(ds);
		}
		PG_RE_THROW();
	}
	PG_END_TRY();

	/* Restore the slice table */
	rootSlice->children = savedRootChildren;
	for (int i = 0; i < list_length(grafted); i += 2)
		sliceTable->slices[list_nth_int(grafted, i)].parentIndex = list_nth_int(grafted, i + 1);
	for (int i = 0; i < list_length(savedRoots); i += 2)
		sliceTable->slices[list_nth_int(savedRoots, i)].rootIndex = list_nth_int(savedRoots, i + 1);
	sliceTable->localSlice = originalSlice;
	currentSliceId = originalSlice;
}

/*
 * CDB: Post processing INITPLAN to clean up resource with long life cycle
 * 
 * INITPLAN usually communicate with main plan through scalar PARAM, but in some case,
 * the main plan need to get more data from INITPLAN which long life cycle resource like
 * temp file will be used.
 * Take INITPLAN function case as an example, INITPLAN will store its result into
 * tuplestore, which will be read by entryDB in main plan. Tuplestore and corresponding
 * files should not be cleaned before the main plan finished.
 *
 * postprocess_initplans is used to clean these resources in ExecutorEnd of main plan.
 */
void
postprocess_initplans(QueryDesc *queryDesc)
{
	EState	   *estate = queryDesc->estate;
	int			nParamExec;
	ParamExecData *prm;
	SubPlanState *sps;
	int			i;

	nParamExec = list_length(queryDesc->plannedstmt->paramExecTypes);

	/* clean ntuplestore used by INITPLAN function */
	for (i = 0; i < nParamExec; i++)
	{
		prm = &estate->es_param_exec_vals[i];
		sps = (SubPlanState *) prm->execPlan;
		if (sps && sps->ts_state)
		{
			tuplestore_end(sps->ts_state);
			sps->ts_state = NULL;
		}
	}
}

static bool
isParamExecutableNow(SubPlanState *spstate, ParamExecData *prmList, Bitmapset *setparams)
{
	Assert(prmList);
	if (!spstate)
		return false;

	List	   *extParam = spstate->subplan->extParam;

	if (extParam == NIL)
		return true;

	ListCell   *lc = NULL;
	ParamExecData *prmData = NULL;
	int			paramId = 0;

	foreach(lc, extParam)
	{
		paramId = lfirst_int(lc);
		prmData = &prmList[paramId];

		/*
		 * we have collected all setparams of each query level. If extParams
		 * from initplan came from setparams, no matter which level setparam
		 * is related to, we just treat it as non-changable result of initplan.
		 */
		if (bms_is_member(paramId, setparams))
			continue;

		/*
		 * preprocess_initplans assumes that the params in the
		 * es_param_exec_vals are in order, so the not-evaluated depending
		 * params yet imply that they cannot be preprocessed, neither do this
		 * current param.
		 */
		if (prmData->execPlan)
			return false;
		else if (prmData->value == 0 && !prmData->isnull)
		{
			return false;
		}
	}
	return true;
}

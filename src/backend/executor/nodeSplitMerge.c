/*-------------------------------------------------------------------------
 *
 * nodeSplitMerge.c
 *	  Implementation of nodeSplitMerge.
 *
 * Portions Copyright (c) 2012, EMC Corp.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/backend/executor/nodeSplitMerge.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/tableam.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbutil.h"
#include "commands/tablecmds.h"
#include "executor/instrument.h"
#include "executor/nodeSplitMerge.h"

#include "utils/memutils.h"


typedef struct MTTargetRelLookup
{
	Oid			relationOid;	/* hash key, must be first */
	int			relationIndex;	/* rel's index in resultRelInfo[] array */
} MTTargetRelLookup;


/*
 * Evaluate the hash keys, and compute the target segment ID for the new row.
 */
static uint32
evalHashKey(SplitMergeState *node, Datum *values, bool *isnulls)
{
	SplitMerge *plannode = (SplitMerge *) node->ps.plan;
	ExprContext *econtext = node->ps.ps_ExprContext;
	MemoryContext oldContext;
	unsigned int target_seg;
	CdbHash	   *h = node->cdbhash;

	ResetExprContext(econtext);

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	cdbhashinit(h);

	for (int i = 0; i < plannode->numHashAttrs; i++)
	{
		AttrNumber	keyattno = plannode->hashAttnos[i];

		/*
		 * Compute the hash function
		 */
		cdbhash(h, i + 1, values[keyattno - 1], isnulls[keyattno - 1]);
	}
	target_seg = cdbhashreduce(h);

	MemoryContextSwitchTo(oldContext);

	return target_seg;
}



static TupleTableSlot *
MergeTupleTableSlot(TupleTableSlot *slot, SplitMerge *plannode, SplitMergeState *node, ResultRelInfo *resultRelInfo)
{
	ExprContext *econtext = node->ps.ps_ExprContext;

	List	   *actionStates = NIL;
	ListCell   *l;
	TupleTableSlot *newslot = NULL;

	/*
	 * For INSERT actions, the root relation's merge action is OK since the
	 * INSERT's targetlist and the WHEN conditions can only refer to the
	 * source relation and hence it does not matter which result relation we
	 * work with.
	 *
	 * XXX does this mean that we can avoid creating copies of actionStates on
	 * partitioned tables, for not-matched actions?
	 */
	actionStates = resultRelInfo->ri_notMatchedMergeAction;

	/*
	 * Make source tuple available to ExecQual and ExecProject. We don't need
	 * the target tuple, since the WHEN quals and targetlist can't refer to
	 * the target columns.
	 */
	econtext->ecxt_scantuple = NULL;
	econtext->ecxt_innertuple = slot;
	econtext->ecxt_outertuple = NULL;

	foreach(l, actionStates)
	{
		MergeActionState *action = (MergeActionState *) lfirst(l);
		CmdType		commandType = action->mas_action->commandType;

		/*
		 * Test condition, if any.
		 *
		 * In the absence of any condition, we perform the action
		 * unconditionally (no need to check separately since ExecQual() will
		 * return true if there are no conditions to evaluate).
		 */
		if (!ExecQual(action->mas_whenqual, econtext))
			continue;

		/* Perform stated action */
		switch (commandType)
		{
			case CMD_INSERT:

				/*
				 * Project the tuple.  In case of a partitioned table, the
				 * projection was already built to use the root's descriptor,
				 * so we don't need to map the tuple here.
				 */
				newslot = ExecProject(action->mas_proj);

				break;
			case CMD_NOTHING:
				/* Do nothing */
				break;
			default:
				elog(ERROR, "unknown action in MERGE WHEN NOT MATCHED clause");
		}

		/*
		 * We've activated one of the WHEN clauses, so we don't search
		 * further. This is required behaviour, not an optimization.
		 */
		break;
	}

	if (newslot)
	{
		/* Compute segment ID for the new row */
		int32		target_seg;

		target_seg = evalHashKey(node, newslot->tts_values, newslot->tts_isnull);

		slot->tts_values[node->segid_attno - 1] = Int32GetDatum(target_seg);
		slot->tts_isnull[node->segid_attno - 1] = false;
	}
	else
	{
		/* 
		 * No newslot generated means that insert action will not be triggered.
		 * So we just redistributed tuple to any segment, like segment 0.
		 */
		slot->tts_values[node->segid_attno - 1] = Int32GetDatum(0);
		slot->tts_isnull[node->segid_attno - 1] = false;
	}

	return slot;
}

/*
 * ExecLookupResultRelByOid
 * 		If the table with given OID is among the result relations to be
 * 		updated by the given ModifyTable node, return its ResultRelInfo.
 *
 * If not found, return NULL if missing_ok, else raise error.
 *
 * If update_cache is true, then upon successful lookup, update the node's
 * one-element cache.  ONLY ExecModifyTable may pass true for this.
 */
static ResultRelInfo *
MergeExecLookupResultRelByOid(SplitMergeState *node, Oid resultoid,
						 bool missing_ok, bool update_cache)
{
	if (node->mt_resultOidHash)
	{
		/* Use the pre-built hash table to locate the rel */
		MTTargetRelLookup *mtlookup;

		mtlookup = (MTTargetRelLookup *)
			hash_search(node->mt_resultOidHash, &resultoid, HASH_FIND, NULL);
		if (mtlookup)
		{
			if (update_cache)
			{
				node->mt_lastResultOid = resultoid;
				node->mt_lastResultIndex = mtlookup->relationIndex;
			}
			return node->resultRelInfo + mtlookup->relationIndex;
		}
	}
	else
	{
		/* With few target rels, just search the ResultRelInfo array */
		for (int ndx = 0; ndx < node->nrel; ndx++)
		{
			ResultRelInfo *rInfo = node->resultRelInfo + ndx;

			if (RelationGetRelid(rInfo->ri_RelationDesc) == resultoid)
			{
				if (update_cache)
				{
					node->mt_lastResultOid = resultoid;
					node->mt_lastResultIndex = ndx;
				}
				return rInfo;
			}
		}
	}

	if (!missing_ok)
		elog(ERROR, "incorrect result relation OID %u", resultoid);
	return NULL;
}

/**
 * Splits every TupleTableSlot into two TupleTableSlots: DELETE and INSERT.
 */
static TupleTableSlot *
ExecSplitMerge(PlanState *pstate)
{
	SplitMergeState *node = castNode(SplitMergeState, pstate);
	PlanState *outerNode = outerPlanState(node);
	SplitMerge *plannode = (SplitMerge *) node->ps.plan;
	ResultRelInfo *resultRelInfo = node->resultRelInfo + node->mt_lastResultIndex;
	Datum		datum;
	bool		isNull;
	Oid			resultoid;
	

	TupleTableSlot *slot = NULL;
	TupleTableSlot *result = NULL;

	Assert(outerNode != NULL);

	slot = ExecProcNode(outerNode);

	if (TupIsNull(slot))
	{
		return NULL;
	}

	datum = ExecGetJunkAttribute(slot, resultRelInfo->ri_RowIdAttNo, &isNull);

	/* ctid is NULL means that not matched, then check the insert action */
	if (isNull)
		result = MergeTupleTableSlot(slot, plannode, node, resultRelInfo);
	else
	{
		/* if partion table must switch resultRelInfo */
		if (AttributeNumberIsValid(node->mt_resultOidAttno))
		{
			datum = ExecGetJunkAttribute(slot, node->mt_resultOidAttno, &isNull);
			Assert(!isNull);
			resultoid = DatumGetObjectId(datum);
			if (resultoid != node->mt_lastResultOid)
				resultRelInfo = MergeExecLookupResultRelByOid(node, resultoid,
															false, true);
		}
		result = slot;
	}

	return result;
}




/*
 * Initializes the tuple slots in a ResultRelInfo for any MERGE action.
 *
 * We mark 'projectNewInfoValid' even though the projections themselves
 * are not initialized here.
 */
static void
ExecInitMergeTupleSlots(SplitMergeState *mtstate,
						ResultRelInfo *resultRelInfo)
{
	EState	   *estate = mtstate->ps.state;

	Assert(!resultRelInfo->ri_projectNewInfoValid);

	resultRelInfo->ri_oldTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc,
						  &estate->es_tupleTable);
	resultRelInfo->ri_newTupleSlot =
		table_slot_create(resultRelInfo->ri_RelationDesc,
						  &estate->es_tupleTable);
	resultRelInfo->ri_projectNewInfoValid = true;
}
/*
 * Init SplitMerge Node. A memory context is created to hold Split Tuples.
 * */
SplitMergeState*
ExecInitSplitMerge(SplitMerge *node, EState *estate, int eflags)
{
	SplitMergeState *splitmergestate;
	ResultRelInfo *resultRelInfo;
	ExprContext *econtext;
	ListCell   *lc;
	int i;


	/* Check for unsupported flags */
	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND)));

	splitmergestate = makeNode(SplitMergeState);
	splitmergestate->ps.plan = (Plan *)node;
	splitmergestate->ps.state = estate;
	splitmergestate->ps.ExecProcNode = ExecSplitMerge;

	/*
	 * then initialize outer plan
	 */
	Plan *outerPlan = outerPlan(node);
	outerPlanState(splitmergestate) = ExecInitNode(outerPlan, estate, eflags);

	
	ExecAssignExprContext(estate, &splitmergestate->ps);
	econtext = splitmergestate->ps.ps_ExprContext;

	splitmergestate->nrel = list_length(node->resultRelations);
	splitmergestate->resultRelInfo = (ResultRelInfo *)palloc(splitmergestate->nrel * sizeof(ResultRelInfo));

	resultRelInfo = splitmergestate->resultRelInfo;
	i = 0;
	foreach(lc, node->resultRelations)
	{
		Index		resultRelation = lfirst_int(lc);
		
		ExecInitResultRelation(estate, resultRelInfo, resultRelation);

		resultRelInfo->ri_RowIdAttNo = ExecFindJunkAttributeInTlist(outerPlan->targetlist, "ctid");
		if (!AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
			elog(ERROR, "could not find junk ctid column");
		
		resultRelInfo++;
		i++;
	}

	splitmergestate->mt_lastResultIndex = 0;
	splitmergestate->mt_lastResultOid = InvalidOid;


	i = 0;
	foreach(lc, node->mergeActionLists)
	{
		List	   *mergeActionList = lfirst(lc);
		TupleDesc	relationDesc;
		ListCell   *l;

		resultRelInfo = splitmergestate->resultRelInfo + i;
		i++;
		relationDesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

		/* initialize slots for MERGE fetches from this rel */
		if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
			ExecInitMergeTupleSlots(splitmergestate, resultRelInfo);

		foreach(l, mergeActionList)
		{
			MergeAction *action = (MergeAction *) lfirst(l);
			MergeActionState *action_state;
			TupleTableSlot *tgtslot;
			TupleDesc	tgtdesc;
			List	  **list;

			/*
			 * Build action merge state for this rel.  (For partitions,
			 * equivalent code exists in ExecInitPartitionInfo.)
			 */
			action_state = makeNode(MergeActionState);
			action_state->mas_action = action;
			action_state->mas_whenqual = ExecInitQual((List *) action->qual,
													  &splitmergestate->ps);

			/*
			 * We create two lists - one for WHEN MATCHED actions and one for
			 * WHEN NOT MATCHED actions - and stick the MergeActionState into
			 * the appropriate list.
			 */
			if (action_state->mas_action->matched)
				list = &resultRelInfo->ri_matchedMergeAction;
			else
				list = &resultRelInfo->ri_notMatchedMergeAction;
			*list = lappend(*list, action_state);

			switch (action->commandType)
			{
				case CMD_INSERT:

					/*
					 * If the MERGE targets a partitioned table, any INSERT
					 * actions must be routed through it, not the child
					 * relations. Initialize the routing struct and the root
					 * table's "new" tuple slot for that, if not already done.
					 * The projection we prepare, for all relations, uses the
					 * root relation descriptor, and targets the plan's root
					 * slot.  (This is consistent with the fact that we
					 * checked the plan output to match the root relation,
					 * above.)
					 */
					/* not partitioned? use the stock relation and slot */
					tgtslot = resultRelInfo->ri_newTupleSlot;
					tgtdesc = RelationGetDescr(resultRelInfo->ri_RelationDesc);

					action_state->mas_proj =
						ExecBuildProjectionInfo(action->targetList, econtext,
												tgtslot,
												&splitmergestate->ps,
												tgtdesc);
					break;
				case CMD_UPDATE:
				case CMD_DELETE:
				case CMD_NOTHING:
					break;
				default:
					elog(ERROR, "unknown action in MERGE WHEN clause");
					break;
			}
		}
	}

	/*
	 * Look up the positions of the gp_segment_id in the subplan's target
	 * list, and in the result.
	 */
	splitmergestate->segid_attno =
		ExecFindJunkAttributeInTlist(outerPlan->targetlist, "gp_segment_id");
	
	splitmergestate->mt_resultOidAttno =
		ExecFindJunkAttributeInTlist(outerPlan->targetlist, "tableoid");
	
	Assert(AttributeNumberIsValid(splitmergestate->mt_resultOidAttno) || splitmergestate->nrel == 1);

	/*
	 * DML nodes do not project.
	 */
	ExecInitResultTupleSlotTL(&splitmergestate->ps, &TTSOpsVirtual);
	splitmergestate->ps.ps_ProjInfo = NULL;

	/*
	 * Initialize for computing hash key
	 */
	if (node->numHashAttrs > 0)
	{
		splitmergestate->cdbhash = makeCdbHash(node->numHashSegments,
												node->numHashAttrs,
												node->hashFuncs);
	}

	if (estate->es_instrument && (estate->es_instrument & INSTRUMENT_CDB))
	{
		splitmergestate->ps.cdbexplainbuf = makeStringInfo();
	}

	return splitmergestate;
}

/* Release Resources Requested by SplitMerge node. */
void
ExecEndSplitMerge(SplitMergeState *node)
{
	
	for (int i = 0; i < node->nrel; i++)
	{
		ResultRelInfo *resultRelInfo = node->resultRelInfo + i;
		/*
		 * Cleanup the initialized batch slots. This only matters for FDWs
		 * with batching, but the other cases will have ri_NumSlotsInitialized
		 * == 0.
		 */
		for (int j = 0; j < resultRelInfo->ri_NumSlotsInitialized; j++)
		{
			ExecDropSingleTupleTableSlot(resultRelInfo->ri_Slots[j]);
			ExecDropSingleTupleTableSlot(resultRelInfo->ri_PlanSlots[j]);
		}
	}

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ps);
	
	
	/*
	 * clean out the tuple table
	 */
	if (node->ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ps.ps_ResultTupleSlot);
	ExecEndNode(outerPlanState(node));
}


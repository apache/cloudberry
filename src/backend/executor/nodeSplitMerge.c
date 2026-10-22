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

#include "nodes/nodeFuncs.h"
#include "utils/memutils.h"

/*
 * Action value for rows that should be processed by ModifyTable's
 * normal ExecMerge path (NOT MATCHED or MATCHED pass-through).
 */
#define SPLITMERGE_ACTION_PASSTHROUGH	(-1)

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

/*
 * Compute target segment ID from the given slot's values.
 * Returns 0 if no hash is configured (DISTRIBUTED RANDOMLY).
 */
static int32
computeTargetSegment(SplitMergeState *node, TupleTableSlot *slot)
{
	SplitMerge *plannode = (SplitMerge *) node->ps.plan;

	if (node->cdbhash)
		return evalHashKey(node, slot->tts_values, slot->tts_isnull);
	else
		return cdbhashrandomseg(plannode->numHashSegments);
}

/*
 * Build a tuple in the N+M+1 format for hasSplitUpdate.
 *
 * The output slot layout is:
 *   [0..N-1]     target table columns (from projSlot, or NULL)
 *   [N..N+M-1]   subplan columns (from inputSlot)
 *   [N+M]        DMLAction
 *
 * N = node->subplan_offset, M = inputSlot column count.
 */
static void
BuildSplitMergeTuple(SplitMergeState *node, TupleTableSlot *outSlot,
					 TupleTableSlot *inputSlot, TupleTableSlot *projSlot,
					 int dmlAction, int32 segid)
{
	int			offset = node->subplan_offset;
	int			natts_input = inputSlot->tts_tupleDescriptor->natts;
	int			natts_out = outSlot->tts_tupleDescriptor->natts;

	ExecClearTuple(outSlot);

	memset(outSlot->tts_values, 0, natts_out * sizeof(Datum));
	memset(outSlot->tts_isnull, true, natts_out * sizeof(bool));

	/* Positions 0..N-1: projected target table values (if provided) */
	if (projSlot)
	{
		int natts_proj = projSlot->tts_tupleDescriptor->natts;
		slot_getallattrs(projSlot);
		memcpy(outSlot->tts_values, projSlot->tts_values,
			   natts_proj * sizeof(Datum));
		memcpy(outSlot->tts_isnull, projSlot->tts_isnull,
			   natts_proj * sizeof(bool));
	}

	/* Positions N..N+M-1: subplan columns */
	slot_getallattrs(inputSlot);
	memcpy(outSlot->tts_values + offset, inputSlot->tts_values,
		   natts_input * sizeof(Datum));
	memcpy(outSlot->tts_isnull + offset, inputSlot->tts_isnull,
		   natts_input * sizeof(bool));

	/* gp_segment_id within subplan region */
	outSlot->tts_values[offset + node->segid_attno - 1] = Int32GetDatum(segid);
	outSlot->tts_isnull[offset + node->segid_attno - 1] = false;

	/* DMLAction at the end */
	outSlot->tts_values[node->action_attno - 1] = Int32GetDatum(dmlAction);
	outSlot->tts_isnull[node->action_attno - 1] = false;

	ExecStoreVirtualTuple(outSlot);
}

/*
 * MergeTupleTableSlot
 *
 * Handle a NOT MATCHED row: evaluate WHEN NOT MATCHED actions,
 * project INSERT values, and compute target segment for routing.
 */
static TupleTableSlot *
MergeTupleTableSlot(TupleTableSlot *slot, SplitMerge *plannode,
					SplitMergeState *node, ResultRelInfo *resultRelInfo)
{
	ExprContext *econtext = node->ps.ps_ExprContext;
	ListCell   *l;
	TupleTableSlot *newslot = NULL;
	int32		target_seg = 0;

	econtext->ecxt_scantuple = NULL;
	econtext->ecxt_innertuple = slot;
	econtext->ecxt_outertuple = NULL;

	/* Evaluate NOT MATCHED actions to find INSERT projection */
	foreach(l, resultRelInfo->ri_notMatchedMergeAction)
	{
		MergeActionState *action = (MergeActionState *) lfirst(l);

		if (!ExecQual(action->mas_whenqual, econtext))
			continue;

		if (action->mas_action->commandType == CMD_INSERT)
			newslot = ExecProject(action->mas_proj);
		/* else CMD_NOTHING: do nothing */

		break;	/* only first matching action */
	}

	/* Compute target segment for INSERT, or 0 for DO NOTHING */
	if (newslot)
		target_seg = computeTargetSegment(node, newslot);

	/* Build output in the appropriate format */
	if (plannode->hasSplitUpdate)
	{
		BuildSplitMergeTuple(node, node->ps.ps_ResultTupleSlot,
							 slot, NULL, SPLITMERGE_ACTION_PASSTHROUGH,
							 target_seg);
		return node->ps.ps_ResultTupleSlot;
	}

	/* Non-hasSplitUpdate: modify slot in-place */
	slot->tts_values[node->segid_attno - 1] = Int32GetDatum(target_seg);
	slot->tts_isnull[node->segid_attno - 1] = false;
	return slot;
}

/*
 * ExecLookupResultRelByOid
 *		If the table with given OID is among the result relations to be
 *		updated by the given SplitMerge node, return its ResultRelInfo.
 *
 * If not found, return NULL if missing_ok, else raise error.
 *
 * If update_cache is true, update the node's one-element cache.
 */
static ResultRelInfo *
MergeExecLookupResultRelByOid(SplitMergeState *node, Oid resultoid,
							  bool missing_ok, bool update_cache)
{
	if (node->mt_resultOidHash)
	{
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

/*
 * SwitchResultRelForPartition
 *
 * For partitioned tables, look up the correct ResultRelInfo based on tableoid
 * from the slot. Updates the cached result relation if it changes.
 */
static ResultRelInfo *
SwitchResultRelForPartition(SplitMergeState *node, TupleTableSlot *slot,
							ResultRelInfo *resultRelInfo)
{
	bool		isNull;
	Datum		d;
	Oid			resultoid;

	if (!AttributeNumberIsValid(node->mt_resultOidAttno))
		return resultRelInfo;

	d = ExecGetJunkAttribute(slot, node->mt_resultOidAttno, &isNull);
	Assert(!isNull);
	resultoid = DatumGetObjectId(d);

	if (resultoid != node->mt_lastResultOid)
		resultRelInfo = MergeExecLookupResultRelByOid(node, resultoid,
													  false, true);
	return resultRelInfo;
}

/*
 * MergeMatchedSplitUpdate
 *
 * Handle a MATCHED row when hasSplitUpdate is true.
 *
 * For UPDATE: splits into DELETE + INSERT tuples with DMLAction markers.
 *             Returns DELETE first; INSERT is saved for the next call.
 * For DELETE: emits a single DELETE tuple.
 * For DO NOTHING / no match: emits a pass-through tuple.
 */
static TupleTableSlot *
MergeMatchedSplitUpdate(TupleTableSlot *slot, SplitMerge *plannode,
						SplitMergeState *node, ResultRelInfo *resultRelInfo)
{
	ExprContext *econtext = node->ps.ps_ExprContext;
	ListCell   *l;
	int32		old_segid;

	econtext->ecxt_scantuple = slot;
	econtext->ecxt_innertuple = slot;
	econtext->ecxt_outertuple = NULL;

	old_segid = DatumGetInt32(slot->tts_values[node->segid_attno - 1]);

	foreach(l, resultRelInfo->ri_matchedMergeAction)
	{
		MergeActionState *action = (MergeActionState *) lfirst(l);
		CmdType		commandType = action->mas_action->commandType;

		if (!ExecQual(action->mas_whenqual, econtext))
			continue;

		switch (commandType)
		{
			case CMD_UPDATE:
			{
				TupleTableSlot *newslot;
				int32		new_segid;

				newslot = ExecProject(action->mas_proj);
				new_segid = computeTargetSegment(node, newslot);

				/* DELETE tuple: routed to old segment */
				BuildSplitMergeTuple(node, node->deleteTuple, slot, NULL,
									 (int) DML_DELETE, old_segid);

				/* INSERT tuple: projected new values, routed to new segment */
				BuildSplitMergeTuple(node, node->insertTuple, slot, newslot,
									 (int) DML_INSERT, new_segid);

				/* Return DELETE first, INSERT on next call */
				node->processInsert = true;
				return node->deleteTuple;
			}

			case CMD_DELETE:
				BuildSplitMergeTuple(node, node->ps.ps_ResultTupleSlot,
									 slot, NULL, (int) DML_DELETE, old_segid);
				return node->ps.ps_ResultTupleSlot;

			case CMD_NOTHING:
				break;	/* fall through to pass-through below */

			default:
				elog(ERROR, "unknown action in MERGE WHEN MATCHED clause");
		}

		break;	/* only first matching action */
	}

	/* No UPDATE/DELETE action matched - pass-through */
	BuildSplitMergeTuple(node, node->ps.ps_ResultTupleSlot, slot, NULL,
						 SPLITMERGE_ACTION_PASSTHROUGH, old_segid);
	return node->ps.ps_ResultTupleSlot;
}

/*
 * ExecSplitMerge
 *
 * Main entry point. For each input tuple from the JOIN:
 * - NOT MATCHED: compute target segment for INSERT routing
 * - MATCHED + hasSplitUpdate: split UPDATE into DELETE + INSERT
 * - MATCHED (no split): pass through
 */
static TupleTableSlot *
ExecSplitMerge(PlanState *pstate)
{
	SplitMergeState *node = castNode(SplitMergeState, pstate);
	PlanState  *outerNode = outerPlanState(node);
	SplitMerge *plannode = (SplitMerge *) node->ps.plan;
	ResultRelInfo *resultRelInfo = node->resultRelInfo + node->mt_lastResultIndex;
	Datum		datum;
	bool		isNull;

	TupleTableSlot *slot;

	Assert(outerNode != NULL);

	/* Return pending INSERT tuple from a previous split UPDATE */
	if (node->processInsert)
	{
		node->processInsert = false;
		return node->insertTuple;
	}

	slot = ExecProcNode(outerNode);
	if (TupIsNull(slot))
		return NULL;

	datum = ExecGetJunkAttribute(slot, resultRelInfo->ri_RowIdAttNo, &isNull);

	if (isNull)
	{
		/* NOT MATCHED: compute target segment for INSERT routing */
		return MergeTupleTableSlot(slot, plannode, node, resultRelInfo);
	}

	/* MATCHED: switch to correct partition if needed */
	resultRelInfo = SwitchResultRelForPartition(node, slot, resultRelInfo);

	if (plannode->hasSplitUpdate)
		return MergeMatchedSplitUpdate(slot, plannode, node, resultRelInfo);

	/* No split update: pass through */
	return slot;
}


/*
 * Initializes the tuple slots in a ResultRelInfo for any MERGE action.
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
 * Build a TupleDesc for the root table's column layout from the plan's
 * non-junk target list entries. Used for UPDATE projections so the result
 * matches the SplitMerge output's first N columns regardless of which
 * child partition is being updated.
 */
static TupleDesc
BuildRootUpdateTupleDesc(List *targetlist)
{
	TupleDesc	desc;
	ListCell   *lc;
	int			nnonjunk = 0;
	int			col = 0;

	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if (!tle->resjunk)
			nnonjunk++;
	}

	if (nnonjunk == 0)
		return NULL;

	desc = CreateTemplateTupleDesc(nnonjunk);
	foreach(lc, targetlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		if (!tle->resjunk)
		{
			col++;
			TupleDescInitEntry(desc, col, tle->resname,
							   exprType((Node *) tle->expr),
							   exprTypmod((Node *) tle->expr), 0);
			TupleDescInitEntryCollation(desc, col,
										exprCollation((Node *) tle->expr));
		}
	}
	return desc;
}

/*
 * ExecInitSplitMerge
 */
SplitMergeState*
ExecInitSplitMerge(SplitMerge *node, EState *estate, int eflags)
{
	SplitMergeState *splitmergestate;
	ResultRelInfo *resultRelInfo;
	ExprContext *econtext;
	ListCell   *lc;
	int			i;
	Plan	   *outerPlan = outerPlan(node);

	Assert(!(eflags & (EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK | EXEC_FLAG_REWIND)));

	splitmergestate = makeNode(SplitMergeState);
	splitmergestate->ps.plan = (Plan *) node;
	splitmergestate->ps.state = estate;
	splitmergestate->ps.ExecProcNode = ExecSplitMerge;
	splitmergestate->processInsert = false;

	outerPlanState(splitmergestate) = ExecInitNode(outerPlan, estate, eflags);

	ExecAssignExprContext(estate, &splitmergestate->ps);
	econtext = splitmergestate->ps.ps_ExprContext;

	/* Initialize result relations */
	splitmergestate->nrel = list_length(node->resultRelations);
	splitmergestate->resultRelInfo = (ResultRelInfo *)
		palloc(splitmergestate->nrel * sizeof(ResultRelInfo));

	resultRelInfo = splitmergestate->resultRelInfo;
	foreach(lc, node->resultRelations)
	{
		Index		resultRelation = lfirst_int(lc);

		ExecInitResultRelation(estate, resultRelInfo, resultRelation);
		resultRelInfo->ri_RowIdAttNo =
			ExecFindJunkAttributeInTlist(outerPlan->targetlist, "ctid");
		if (!AttributeNumberIsValid(resultRelInfo->ri_RowIdAttNo))
			elog(ERROR, "could not find junk ctid column");
		resultRelInfo++;
	}

	splitmergestate->mt_lastResultIndex = 0;
	splitmergestate->mt_lastResultOid = InvalidOid;

	/* Build root-table-format slot for UPDATE projections (hasSplitUpdate only) */
	TupleTableSlot *rootUpdateSlot = NULL;
	TupleDesc	rootUpdateDesc = NULL;
	if (node->hasSplitUpdate)
	{
		rootUpdateDesc = BuildRootUpdateTupleDesc(node->plan.targetlist);
		if (rootUpdateDesc)
			rootUpdateSlot = ExecInitExtraTupleSlot(estate, rootUpdateDesc,
													&TTSOpsVirtual);
	}

	/* Initialize merge action states and projections */
	i = 0;
	foreach(lc, node->mergeActionLists)
	{
		List	   *mergeActionList = lfirst(lc);
		ListCell   *l;

		resultRelInfo = splitmergestate->resultRelInfo + i;
		i++;

		if (unlikely(!resultRelInfo->ri_projectNewInfoValid))
			ExecInitMergeTupleSlots(splitmergestate, resultRelInfo);

		foreach(l, mergeActionList)
		{
			MergeAction *action = (MergeAction *) lfirst(l);
			MergeActionState *action_state;
			List	  **list;

			action_state = makeNode(MergeActionState);
			action_state->mas_action = action;
			action_state->mas_whenqual = ExecInitQual((List *) action->qual,
													  &splitmergestate->ps);

			if (action_state->mas_action->matched)
				list = &resultRelInfo->ri_matchedMergeAction;
			else
				list = &resultRelInfo->ri_notMatchedMergeAction;
			*list = lappend(*list, action_state);

			switch (action->commandType)
			{
				case CMD_INSERT:
					action_state->mas_proj =
						ExecBuildProjectionInfo(action->targetList, econtext,
												resultRelInfo->ri_newTupleSlot,
												&splitmergestate->ps,
												RelationGetDescr(resultRelInfo->ri_RelationDesc));
					break;
				case CMD_UPDATE:
					if (node->hasSplitUpdate && rootUpdateSlot != NULL)
					{
						action_state->mas_proj =
							ExecBuildProjectionInfo(action->targetList, econtext,
													rootUpdateSlot,
													&splitmergestate->ps,
													rootUpdateDesc);
					}
					break;
				case CMD_DELETE:
				case CMD_NOTHING:
					break;
				default:
					elog(ERROR, "unknown action in MERGE WHEN clause");
					break;
			}
		}
	}

	/* Look up junk attribute positions in subplan output */
	splitmergestate->segid_attno =
		ExecFindJunkAttributeInTlist(outerPlan->targetlist, "gp_segment_id");
	splitmergestate->mt_resultOidAttno =
		ExecFindJunkAttributeInTlist(outerPlan->targetlist, "tableoid");

	Assert(AttributeNumberIsValid(splitmergestate->mt_resultOidAttno) ||
		   splitmergestate->nrel == 1);

	/* Initialize hasSplitUpdate-specific state */
	if (node->hasSplitUpdate)
	{
		splitmergestate->action_attno =
			ExecFindJunkAttributeInTlist(node->plan.targetlist, "DMLAction");
		Assert(AttributeNumberIsValid(splitmergestate->action_attno));

		/* subplan_offset = N = total output columns - subplan columns - DMLAction */
		splitmergestate->subplan_offset =
			list_length(node->plan.targetlist) -
			list_length(outerPlan->targetlist) - 1;
		Assert(splitmergestate->subplan_offset > 0);

		/* Dedicated slots for split DELETE + INSERT tuple pair */
		{
			TupleDesc tupDesc = ExecTypeFromTL(node->plan.targetlist);
			splitmergestate->deleteTuple =
				ExecInitExtraTupleSlot(estate, tupDesc, &TTSOpsVirtual);
			splitmergestate->insertTuple =
				ExecInitExtraTupleSlot(estate, tupDesc, &TTSOpsVirtual);
		}
	}
	else
	{
		splitmergestate->action_attno = InvalidAttrNumber;
		splitmergestate->subplan_offset = 0;
	}

	ExecInitResultTupleSlotTL(&splitmergestate->ps, &TTSOpsVirtual);
	splitmergestate->ps.ps_ProjInfo = NULL;

	/* Initialize hash for computing target segment */
	if (node->numHashAttrs > 0)
	{
		splitmergestate->cdbhash = makeCdbHash(node->numHashSegments,
											   node->numHashAttrs,
											   node->hashFuncs);
	}

	if (estate->es_instrument && (estate->es_instrument & INSTRUMENT_CDB))
		splitmergestate->ps.cdbexplainbuf = makeStringInfo();

	return splitmergestate;
}

/* Release resources requested by SplitMerge node. */
void
ExecEndSplitMerge(SplitMergeState *node)
{
	for (int i = 0; i < node->nrel; i++)
	{
		ResultRelInfo *resultRelInfo = node->resultRelInfo + i;

		for (int j = 0; j < resultRelInfo->ri_NumSlotsInitialized; j++)
		{
			ExecDropSingleTupleTableSlot(resultRelInfo->ri_Slots[j]);
			ExecDropSingleTupleTableSlot(resultRelInfo->ri_PlanSlots[j]);
		}
	}

	ExecFreeExprContext(&node->ps);

	if (node->ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ps.ps_ResultTupleSlot);
	if (node->insertTuple)
		ExecClearTuple(node->insertTuple);
	if (node->deleteTuple)
		ExecClearTuple(node->deleteTuple);

	ExecEndNode(outerPlanState(node));
}

/*-------------------------------------------------------------------------
 *
 * copyfuncs.c
 *	  Copy functions for Postgres tree nodes.
 *
 * NOTE: we currently support copying all node types found in parse and
 * plan trees.  We do not support copying executor state trees; there
 * is no need for that, and no point in maintaining all the code that
 * would be needed.  We also do not support copying Path trees, mainly
 * because the circular linkages between RelOptInfo and Path nodes can't
 * be handled easily in a simple depth-first traversal.
 *
 *
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/nodes/copyfuncs.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/gp_distribution_policy.h"
#include "catalog/heap.h"
#include "miscadmin.h"
#include "nodes/altertablenodes.h"
#include "nodes/extensible.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "utils/datum.h"
#include "cdb/cdbgang.h"
#include "utils/rel.h"

/*
 * Macros to simplify copying of different kinds of fields.  Use these
 * wherever possible to reduce the chance for silly typos.  Note that these
 * hard-wire the convention that the local variables in a Copy routine are
 * named 'newnode' and 'from'.
 */

/* Copy a simple scalar field (int, float, bool, enum, etc) */
#define COPY_SCALAR_FIELD(fldname) \
	(newnode->fldname = from->fldname)

/* Copy a field that is a pointer to some kind of Node or Node tree */
#define COPY_NODE_FIELD(fldname) \
	(newnode->fldname = copyObjectImpl(from->fldname))

/* Copy a field that is a pointer to a Bitmapset */
#define COPY_BITMAPSET_FIELD(fldname) \
	(newnode->fldname = bms_copy(from->fldname))
/* Copy a field that is a pointer to a C string, or perhaps NULL */
#define COPY_STRING_FIELD(fldname) \
	(newnode->fldname = from->fldname ? pstrdup(from->fldname) : (char *) NULL)

/* Copy a field that is an inline array */
#define COPY_ARRAY_FIELD(fldname) \
        memcpy(newnode->fldname, from->fldname, sizeof(newnode->fldname))

/* Copy a field that is a pointer to a simple palloc'd object of size sz */
#define COPY_POINTER_FIELD(fldname, sz) \
	do { \
		Size	_size = (sz); \
		if (_size > 0 && from->fldname) \
		{ \
			newnode->fldname = palloc(_size); \
			memcpy(newnode->fldname, from->fldname, _size); \
		} \
	} while (0)

#define COPY_BINARY_FIELD(fldname, sz) \
	do { \
		Size _size = (sz); \
		memcpy(&newnode->fldname, &from->fldname, _size); \
	} while (0)

/* Copy a field that is a varlena datum */
#define COPY_VARLENA_FIELD(fldname, len) \
	do { \
		if (from->fldname) \
		{ \
			newnode->fldname = (bytea *) DatumGetPointer( \
					datumCopy(PointerGetDatum(from->fldname), false, len)); \
		} \
	} while (0)

/* Copy a parse location field (for Copy, this is same as scalar case) */
#define COPY_LOCATION_FIELD(fldname) \
	(newnode->fldname = from->fldname)


/* ****************************************************************
 *					 plannodes.h copy functions
 * ****************************************************************
 */
#include "copyfuncs.funcs.c"


static Const *
_copyConst(const Const *from)
{
	Const	   *newnode = makeNode(Const);

	COPY_SCALAR_FIELD(consttype);
	COPY_SCALAR_FIELD(consttypmod);
	COPY_SCALAR_FIELD(constcollid);
	COPY_SCALAR_FIELD(constlen);

	if (from->constbyval || from->constisnull)
	{
		/*
		 * passed by value so just copy the datum. Also, don't try to copy
		 * struct when value is null!
		 */
		newnode->constvalue = from->constvalue;
	}
	else
	{
		/*
		 * passed by reference.  We need a palloc'd copy.
		 */
		newnode->constvalue = datumCopy(from->constvalue,
										from->constbyval,
										from->constlen);
	}

	COPY_SCALAR_FIELD(constisnull);
	COPY_SCALAR_FIELD(constbyval);
	COPY_LOCATION_FIELD(location);

	return newnode;
}

static A_Const *
_copyA_Const(const A_Const *from)
{
	A_Const    *newnode = makeNode(A_Const);

	COPY_SCALAR_FIELD(isnull);
	if (!from->isnull)
	{
		/* This part must duplicate other _copy*() functions. */
		COPY_SCALAR_FIELD(val.node.type);
		switch (nodeTag(&from->val))
		{
			case T_Integer:
				COPY_SCALAR_FIELD(val.ival.ival);
				break;
			case T_Float:
				COPY_STRING_FIELD(val.fval.fval);
				break;
			case T_Boolean:
				COPY_SCALAR_FIELD(val.boolval.boolval);
				break;
			case T_String:
				COPY_STRING_FIELD(val.sval.sval);
				break;
			case T_BitString:
				COPY_STRING_FIELD(val.bsval.bsval);
				break;
			default:
				elog(ERROR, "unrecognized node type: %d",
					 (int) nodeTag(&from->val));
				break;
		}
	}

	COPY_LOCATION_FIELD(location);

	return newnode;
}

static ExtensibleNode *
_copyExtensibleNode(const ExtensibleNode *from)
{
	ExtensibleNode *newnode;
	const ExtensibleNodeMethods *methods;

	methods = GetExtensibleNodeMethods(from->extnodename, false);
	newnode = (ExtensibleNode *) newNode(methods->node_size,
										 T_ExtensibleNode);
	COPY_STRING_FIELD(extnodename);

	/* copy the private fields */
	methods->nodeCopy(newnode, from);

	return newnode;
}

static Bitmapset *
_copyBitmapset(const Bitmapset *from)
{
	return bms_copy(from);
}


static ColumnDef *
_copyColumnDef(const ColumnDef *from)
{
	ColumnDef  *newnode = makeNode(ColumnDef);

	COPY_STRING_FIELD(colname);
	COPY_NODE_FIELD(typeName);
	COPY_STRING_FIELD(compression);
	COPY_SCALAR_FIELD(inhcount);
	COPY_SCALAR_FIELD(is_local);
	COPY_SCALAR_FIELD(is_not_null);
	COPY_SCALAR_FIELD(is_from_type);
	COPY_SCALAR_FIELD(attnum);
	COPY_SCALAR_FIELD(storage);
	COPY_STRING_FIELD(storage_name);
	COPY_NODE_FIELD(raw_default);
	COPY_NODE_FIELD(cooked_default);
	COPY_SCALAR_FIELD(hasCookedMissingVal);
	COPY_SCALAR_FIELD(missingIsNull);
	if (from->hasCookedMissingVal && !from->missingIsNull)
		newnode->missingVal = datumCopy(from->missingVal, false, -1);
	COPY_SCALAR_FIELD(identity);
	COPY_NODE_FIELD(identitySequence);
	COPY_SCALAR_FIELD(generated);
	COPY_NODE_FIELD(collClause);
	COPY_SCALAR_FIELD(collOid);
	COPY_NODE_FIELD(constraints);
	COPY_NODE_FIELD(encoding);
	COPY_NODE_FIELD(fdwoptions);
	COPY_LOCATION_FIELD(location);

	return newnode;
}

static SliceTable *
_copySliceTable(const SliceTable *from)
{
	SliceTable *newnode = makeNode(SliceTable);

	COPY_SCALAR_FIELD(localSlice);
	COPY_SCALAR_FIELD(numSlices);

	newnode->slices = palloc0(from->numSlices * sizeof(ExecSlice));
	for (int i = 0; i < from->numSlices; i++)
	{
		COPY_SCALAR_FIELD(slices[i].sliceIndex);
		COPY_SCALAR_FIELD(slices[i].rootIndex);
		COPY_SCALAR_FIELD(slices[i].planNumSegments);
		COPY_SCALAR_FIELD(slices[i].gangType);
		COPY_NODE_FIELD(slices[i].segments);
		COPY_SCALAR_FIELD(slices[i].useMppParallelMode);
		COPY_SCALAR_FIELD(slices[i].parallel_workers);

		newnode->slices[i].primaryGang = from->slices[i].primaryGang;
		COPY_SCALAR_FIELD(slices[i].parentIndex);
		COPY_NODE_FIELD(slices[i].children);
		COPY_NODE_FIELD(slices[i].primaryProcesses);
		COPY_BITMAPSET_FIELD(slices[i].processesMap);
	}

	COPY_SCALAR_FIELD(instrument_options);
	COPY_SCALAR_FIELD(ic_instance_id);

	return newnode;
}

static AlteredTableInfo *
_copyAlteredTableInfo(const AlteredTableInfo *from)
{
	int	i;

	AlteredTableInfo *newnode = makeNode(AlteredTableInfo);

	COPY_SCALAR_FIELD(relid);
	COPY_SCALAR_FIELD(relkind);
	newnode->oldDesc = CreateTupleDescCopyConstr(from->oldDesc);

	for (i = 0; i < AT_NUM_PASSES; i++)
		COPY_NODE_FIELD(subcmds[i]);

	COPY_NODE_FIELD(constraints);
	COPY_NODE_FIELD(newvals);
	COPY_NODE_FIELD(afterStmts);
	COPY_SCALAR_FIELD(verify_new_notnull);
	COPY_SCALAR_FIELD(rewrite);
	COPY_SCALAR_FIELD(dist_opfamily_changed);
	COPY_SCALAR_FIELD(new_opclass);
	COPY_SCALAR_FIELD(newTableSpace);
	COPY_SCALAR_FIELD(chgPersistence);
	COPY_SCALAR_FIELD(newrelpersistence);
	COPY_NODE_FIELD(partition_constraint);
	COPY_SCALAR_FIELD(validate_default);
	COPY_NODE_FIELD(changedConstraintOids);
	COPY_NODE_FIELD(changedConstraintDefs);
	COPY_NODE_FIELD(changedIndexOids);
	COPY_NODE_FIELD(changedIndexDefs);
	COPY_STRING_FIELD(replicaIdentityIndex);
	COPY_STRING_FIELD(clusterOnIndex);
	COPY_NODE_FIELD(beforeStmtLists);
	COPY_NODE_FIELD(constraintLists);

	return newnode;
}

static PlannedStmt *
_copyPlannedStmt(const PlannedStmt *from)
{
	PlannedStmt *newnode = makeNode(PlannedStmt);

	COPY_SCALAR_FIELD(commandType);
	COPY_SCALAR_FIELD(planGen);
	COPY_SCALAR_FIELD(queryId);
	COPY_SCALAR_FIELD(hasReturning);
	COPY_SCALAR_FIELD(hasModifyingCTE);
	COPY_SCALAR_FIELD(canSetTag);
	COPY_SCALAR_FIELD(transientPlan);
	COPY_SCALAR_FIELD(oneoffPlan);
	COPY_SCALAR_FIELD(simplyUpdatableRel);
	COPY_SCALAR_FIELD(dependsOnRole);
	COPY_SCALAR_FIELD(parallelModeNeeded);
	COPY_SCALAR_FIELD(jitFlags);
	COPY_NODE_FIELD(planTree);
	COPY_SCALAR_FIELD(numSlices);
	newnode->slices = palloc(from->numSlices * sizeof(PlanSlice));
	for (int i = 0; i < from->numSlices; i++)
	{
		COPY_SCALAR_FIELD(slices[i].sliceIndex);
		COPY_SCALAR_FIELD(slices[i].parentIndex);
		COPY_SCALAR_FIELD(slices[i].gangType);
		COPY_SCALAR_FIELD(slices[i].numsegments);
		COPY_SCALAR_FIELD(slices[i].parallel_workers);
		COPY_SCALAR_FIELD(slices[i].segindex);
		COPY_SCALAR_FIELD(slices[i].directDispatch.isDirectDispatch);
		COPY_NODE_FIELD(slices[i].directDispatch.contentIds);
	}
	COPY_NODE_FIELD(rtable);
	COPY_NODE_FIELD(permInfos);
	COPY_NODE_FIELD(resultRelations);
	COPY_NODE_FIELD(appendRelations);
	COPY_NODE_FIELD(subplans);
	COPY_POINTER_FIELD(subplan_sliceIds, list_length(from->subplans) * sizeof(int));
	COPY_BITMAPSET_FIELD(rewindPlanIDs);
	COPY_NODE_FIELD(rowMarks);
	COPY_NODE_FIELD(relationOids);
	COPY_NODE_FIELD(invalItems);
	COPY_NODE_FIELD(paramExecTypes);
	COPY_NODE_FIELD(utilityStmt);
	COPY_LOCATION_FIELD(stmt_location);
	COPY_SCALAR_FIELD(stmt_len);
	COPY_NODE_FIELD(intoPolicy);
	COPY_SCALAR_FIELD(query_mem);
	COPY_NODE_FIELD(intoClause);
	COPY_NODE_FIELD(copyIntoClause);
	COPY_NODE_FIELD(refreshClause);
	COPY_SCALAR_FIELD(metricsQueryType);
	COPY_NODE_FIELD(extensionContext);

	return newnode;
}

static Motion *
_copyMotion(const Motion *from)
{
	Motion *newnode = makeNode(Motion);

	COPY_SCALAR_FIELD(plan.startup_cost);
	COPY_SCALAR_FIELD(plan.total_cost);
	COPY_SCALAR_FIELD(plan.plan_rows);
	COPY_SCALAR_FIELD(plan.plan_width);
	COPY_SCALAR_FIELD(plan.parallel_aware);
	COPY_SCALAR_FIELD(plan.parallel_safe);
	COPY_SCALAR_FIELD(plan.async_capable);
	COPY_SCALAR_FIELD(plan.plan_node_id);
	COPY_NODE_FIELD(plan.targetlist);
	COPY_NODE_FIELD(plan.qual);
	COPY_NODE_FIELD(plan.lefttree);
	COPY_NODE_FIELD(plan.righttree);
	COPY_NODE_FIELD(plan.initPlan);
	COPY_BITMAPSET_FIELD(plan.extParam);
	COPY_BITMAPSET_FIELD(plan.allParam);
	COPY_NODE_FIELD(plan.flow);
	COPY_SCALAR_FIELD(plan.locustype);
	COPY_SCALAR_FIELD(plan.parallel);
	COPY_SCALAR_FIELD(plan.operatorMemKB);
	COPY_SCALAR_FIELD(motionType);
	COPY_SCALAR_FIELD(sendSorted);
	COPY_SCALAR_FIELD(motionID);
	COPY_NODE_FIELD(hashExprs);
	COPY_POINTER_FIELD(hashFuncs, list_length(from->hashExprs) * sizeof(Oid));
	COPY_SCALAR_FIELD(numHashSegments);
	COPY_SCALAR_FIELD(segidColIdx);
	COPY_SCALAR_FIELD(numSortCols);
	COPY_POINTER_FIELD(sortColIdx, from->numSortCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(sortOperators, from->numSortCols * sizeof(Oid));
	COPY_POINTER_FIELD(collations, from->numSortCols * sizeof(Oid));
	COPY_POINTER_FIELD(nullsFirst, from->numSortCols * sizeof(bool));

	if (from->senderSliceInfo)
	{
		newnode->senderSliceInfo = palloc(sizeof(PlanSlice));
		memcpy(newnode->senderSliceInfo, from->senderSliceInfo, sizeof(PlanSlice));
	}

	return newnode;
}

/*
 * copyObjectImpl -- implementation of copyObject(); see nodes/nodes.h
 *
 * Create a copy of a Node tree or list.  This is a "deep" copy: all
 * substructure is copied too, recursively.
 */
void *
copyObjectImpl(const void *from)
{
	void	   *retval;

	if (from == NULL)
		return NULL;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	switch (nodeTag(from))
	{
#include "copyfuncs.switch.c"

		case T_List:
			retval = list_copy_deep(from);
			break;

			/*
			 * Lists of integers, OIDs and XIDs don't need to be deep-copied,
			 * so we perform a shallow copy via list_copy()
			 */
		case T_IntList:
		case T_OidList:
		case T_XidList:
			retval = list_copy(from);
			break;

		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(from));
			retval = 0;			/* keep compiler quiet */
			break;
	}

	return retval;
}

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
		if (_size > 0) \
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


<<<<<<< HEAD
=======
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
	COPY_NODE_FIELD(rtable);
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

	COPY_NODE_FIELD(intoPolicy);

	COPY_SCALAR_FIELD(query_mem);

	COPY_NODE_FIELD(intoClause);
	COPY_NODE_FIELD(copyIntoClause);
	COPY_NODE_FIELD(refreshClause);
	COPY_SCALAR_FIELD(metricsQueryType);
	COPY_NODE_FIELD(extensionContext);

	return newnode;
}

static QueryDispatchDesc *
_copyQueryDispatchDesc(const QueryDispatchDesc *from)
{
	QueryDispatchDesc *newnode = makeNode(QueryDispatchDesc);

	COPY_NODE_FIELD(sliceTable);
	COPY_NODE_FIELD(oidAssignments);
	COPY_NODE_FIELD(cursorPositions);
	COPY_SCALAR_FIELD(useChangedAOOpts);
	COPY_SCALAR_FIELD(secContext);
	COPY_NODE_FIELD(paramInfo);
	COPY_NODE_FIELD(namedRelList);
	COPY_SCALAR_FIELD(matviewOid);
	COPY_SCALAR_FIELD(tableid);
	COPY_SCALAR_FIELD(snaplen);
	COPY_STRING_FIELD(snapname);

	return newnode;
}

static SerializedParams *
_copySerializedParams(const SerializedParams *from)
{
	SerializedParams *newnode = makeNode(SerializedParams);

	COPY_SCALAR_FIELD(nExternParams);
	newnode->externParams = palloc0(from->nExternParams * sizeof(SerializedParamExternData));
	for (int i = 0; i < from->nExternParams; i++)
	{
		COPY_SCALAR_FIELD(externParams[i].isnull);
		COPY_SCALAR_FIELD(externParams[i].pflags);
		COPY_SCALAR_FIELD(externParams[i].ptype);
		COPY_SCALAR_FIELD(externParams[i].plen);
		COPY_SCALAR_FIELD(externParams[i].pbyval);

		if (!from->externParams[i].isnull)
			newnode->externParams[i].value = datumCopy(from->externParams[i].value,
													   from->externParams[i].pbyval,
													   from->externParams[i].plen);
	}

	COPY_SCALAR_FIELD(nExecParams);
	newnode->execParams = palloc0(from->nExecParams * sizeof(SerializedParamExecData));
	for (int i = 0; i < from->nExecParams; i++)
	{
		COPY_SCALAR_FIELD(execParams[i].isnull);
		COPY_SCALAR_FIELD(execParams[i].isvalid);
		COPY_SCALAR_FIELD(execParams[i].plen);
		COPY_SCALAR_FIELD(execParams[i].pbyval);

		if (!from->execParams[i].isnull)
			newnode->execParams[i].value = datumCopy(from->externParams[i].value,
													 from->externParams[i].pbyval,
													 from->externParams[i].plen);
	}

	return newnode;
}

static OidAssignment *
_copyOidAssignment(const OidAssignment *from)
{
	OidAssignment *newnode = makeNode(OidAssignment);

	COPY_SCALAR_FIELD(catalog);
	COPY_STRING_FIELD(objname);
	COPY_SCALAR_FIELD(namespaceOid);
	COPY_SCALAR_FIELD(keyOid1);
	COPY_SCALAR_FIELD(keyOid2);
	COPY_SCALAR_FIELD(oid);

	return newnode;
}

/*
 * CopyPlanFields
 *
 *		This function copies the fields of the Plan node.  It is used by
 *		all the copy functions for classes which inherit from Plan.
 */
static void
CopyPlanFields(const Plan *from, Plan *newnode)
{
	COPY_SCALAR_FIELD(plan_node_id);

	COPY_SCALAR_FIELD(startup_cost);
	COPY_SCALAR_FIELD(total_cost);
	COPY_SCALAR_FIELD(plan_rows);
	COPY_SCALAR_FIELD(plan_width);
	COPY_SCALAR_FIELD(parallel_aware);
	COPY_SCALAR_FIELD(parallel_safe);
	COPY_SCALAR_FIELD(async_capable);
	COPY_SCALAR_FIELD(plan_node_id);
	COPY_NODE_FIELD(targetlist);
	COPY_NODE_FIELD(qual);
	COPY_NODE_FIELD(lefttree);
	COPY_NODE_FIELD(righttree);
	COPY_NODE_FIELD(initPlan);
	COPY_BITMAPSET_FIELD(extParam);
	COPY_BITMAPSET_FIELD(allParam);
	COPY_NODE_FIELD(flow);
	COPY_SCALAR_FIELD(locustype);
	COPY_SCALAR_FIELD(parallel);

	COPY_SCALAR_FIELD(operatorMemKB);
}

/*
 * _copyPlan
 */
static Plan *
_copyPlan(const Plan *from)
{
	Plan	   *newnode = makeNode(Plan);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields(from, newnode);

	return newnode;
}


/*
 * _copyResult
 */
static Result *
_copyResult(const Result *from)
{
	Result	   *newnode = makeNode(Result);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(resconstantqual);

	COPY_SCALAR_FIELD(numHashFilterCols);
	if (from->numHashFilterCols > 0)
	{
		COPY_POINTER_FIELD(hashFilterColIdx, from->numHashFilterCols * sizeof(AttrNumber));
		COPY_POINTER_FIELD(hashFilterFuncs, from->numHashFilterCols * sizeof(Oid));
	}

	return newnode;
}

/*
 * _copyProjectSet
 */
static ProjectSet *
_copyProjectSet(const ProjectSet *from)
{
	ProjectSet *newnode = makeNode(ProjectSet);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	return newnode;
}

/*
 * _copyModifyTable
 */
static ModifyTable *
_copyModifyTable(const ModifyTable *from)
{
	ModifyTable *newnode = makeNode(ModifyTable);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(operation);
	COPY_SCALAR_FIELD(canSetTag);
	COPY_SCALAR_FIELD(nominalRelation);
	COPY_SCALAR_FIELD(rootRelation);
	COPY_SCALAR_FIELD(partColsUpdated);
	COPY_NODE_FIELD(resultRelations);
	COPY_NODE_FIELD(updateColnosLists);
	COPY_NODE_FIELD(withCheckOptionLists);
	COPY_NODE_FIELD(returningLists);
	COPY_NODE_FIELD(fdwPrivLists);
	COPY_BITMAPSET_FIELD(fdwDirectModifyPlans);
	COPY_NODE_FIELD(rowMarks);
	COPY_SCALAR_FIELD(epqParam);
	COPY_SCALAR_FIELD(onConflictAction);
	COPY_NODE_FIELD(arbiterIndexes);
	COPY_NODE_FIELD(onConflictSet);
	COPY_NODE_FIELD(onConflictCols);
	COPY_NODE_FIELD(onConflictWhere);
	COPY_SCALAR_FIELD(exclRelRTI);
	COPY_NODE_FIELD(exclRelTlist);
	COPY_SCALAR_FIELD(splitUpdate);
	COPY_SCALAR_FIELD(forceTupleRouting);

	return newnode;
}

/*
 * _copyAppend
 */
static Append *
_copyAppend(const Append *from)
{
	Append	   *newnode = makeNode(Append);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_BITMAPSET_FIELD(apprelids);
	COPY_NODE_FIELD(appendplans);
	COPY_SCALAR_FIELD(nasyncplans);
	COPY_SCALAR_FIELD(first_partial_plan);
	COPY_NODE_FIELD(part_prune_info);
	COPY_NODE_FIELD(join_prune_paramids);

	return newnode;
}

static Sequence *
_copySequence(const Sequence *from)
{
	Sequence *newnode = makeNode(Sequence);
	CopyPlanFields((Plan *) from, (Plan *) newnode);
	COPY_NODE_FIELD(subplans);

	return newnode;
}

/*
 * _copyMergeAppend
 */
static MergeAppend *
_copyMergeAppend(const MergeAppend *from)
{
	MergeAppend *newnode = makeNode(MergeAppend);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_BITMAPSET_FIELD(apprelids);
	COPY_NODE_FIELD(mergeplans);
	COPY_SCALAR_FIELD(numCols);
	COPY_POINTER_FIELD(sortColIdx, from->numCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(sortOperators, from->numCols * sizeof(Oid));
	COPY_POINTER_FIELD(collations, from->numCols * sizeof(Oid));
	COPY_POINTER_FIELD(nullsFirst, from->numCols * sizeof(bool));
	COPY_NODE_FIELD(part_prune_info);
	COPY_NODE_FIELD(join_prune_paramids);

	return newnode;
}

/*
 * _copyRecursiveUnion
 */
static RecursiveUnion *
_copyRecursiveUnion(const RecursiveUnion *from)
{
	RecursiveUnion *newnode = makeNode(RecursiveUnion);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(wtParam);
	COPY_SCALAR_FIELD(numCols);
	COPY_POINTER_FIELD(dupColIdx, from->numCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(dupOperators, from->numCols * sizeof(Oid));
	COPY_POINTER_FIELD(dupCollations, from->numCols * sizeof(Oid));
	COPY_SCALAR_FIELD(numGroups);

	return newnode;
}

/*
 * _copyBitmapAnd
 */
static BitmapAnd *
_copyBitmapAnd(const BitmapAnd *from)
{
	BitmapAnd  *newnode = makeNode(BitmapAnd);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(bitmapplans);

	return newnode;
}

/*
 * _copyBitmapOr
 */
static BitmapOr *
_copyBitmapOr(const BitmapOr *from)
{
	BitmapOr   *newnode = makeNode(BitmapOr);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(isshared);
	COPY_NODE_FIELD(bitmapplans);

	return newnode;
}

/*
 * _copyGather
 */
static Gather *
_copyGather(const Gather *from)
{
	Gather	   *newnode = makeNode(Gather);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(num_workers);
	COPY_SCALAR_FIELD(rescan_param);
	COPY_SCALAR_FIELD(single_copy);
	COPY_SCALAR_FIELD(invisible);
	COPY_BITMAPSET_FIELD(initParam);

	return newnode;
}

/*
 * _copyGatherMerge
 */
static GatherMerge *
_copyGatherMerge(const GatherMerge *from)
{
	GatherMerge *newnode = makeNode(GatherMerge);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(num_workers);
	COPY_SCALAR_FIELD(rescan_param);
	COPY_SCALAR_FIELD(numCols);
	COPY_POINTER_FIELD(sortColIdx, from->numCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(sortOperators, from->numCols * sizeof(Oid));
	COPY_POINTER_FIELD(collations, from->numCols * sizeof(Oid));
	COPY_POINTER_FIELD(nullsFirst, from->numCols * sizeof(bool));
	COPY_BITMAPSET_FIELD(initParam);

	return newnode;
}

/*
 * CopyScanFields
 *
 *		This function copies the fields of the Scan node.  It is used by
 *		all the copy functions for classes which inherit from Scan.
 */
static void
CopyScanFields(const Scan *from, Scan *newnode)
{
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(scanrelid);
}

/*
 * _copyScan
 */
static Scan *
_copyScan(const Scan *from)
{
	Scan	   *newnode = makeNode(Scan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	return newnode;
}

/*
 * _copySeqScan
 */
static SeqScan *
_copySeqScan(const SeqScan *from)
{
	SeqScan    *newnode = makeNode(SeqScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	return newnode;
}

static DynamicSeqScan *
_copyDynamicSeqScan(const DynamicSeqScan *from)
{
	DynamicSeqScan *newnode = makeNode(DynamicSeqScan);

	CopyScanFields((Scan *) from, (Scan *) newnode);
	COPY_NODE_FIELD(partOids);
	COPY_NODE_FIELD(part_prune_info);
	COPY_NODE_FIELD(join_prune_paramids);

	return newnode;
}

/*
 * _copyExternalScanInfo
 */
static ExternalScanInfo *
_copyExternalScanInfo(const ExternalScanInfo *from)
{
	ExternalScanInfo *newnode = makeNode(ExternalScanInfo);

	COPY_NODE_FIELD(uriList);
	COPY_SCALAR_FIELD(fmtType);
	COPY_SCALAR_FIELD(isMasterOnly);
	COPY_SCALAR_FIELD(rejLimit);
	COPY_SCALAR_FIELD(rejLimitInRows);
	COPY_SCALAR_FIELD(logErrors);
	COPY_SCALAR_FIELD(encoding);
	COPY_SCALAR_FIELD(scancounter);
	COPY_NODE_FIELD(extOptions);

	return newnode;
}

static void
CopyIndexScanFields(const IndexScan *from, IndexScan *newnode)
{
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(indexid);
	COPY_NODE_FIELD(indexqual);
	COPY_NODE_FIELD(indexqualorig);
	COPY_NODE_FIELD(indexorderby);
	COPY_NODE_FIELD(indexorderbyorig);
	COPY_NODE_FIELD(indexorderbyops);
	COPY_SCALAR_FIELD(indexorderdir);
}

/*
 * _copySampleScan
 */
static SampleScan *
_copySampleScan(const SampleScan *from)
{
	SampleScan *newnode = makeNode(SampleScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(tablesample);

	return newnode;
}

/*
 * _copyIndexScan
 */
static IndexScan *
_copyIndexScan(const IndexScan *from)
{
	IndexScan  *newnode = makeNode(IndexScan);

	CopyIndexScanFields(from, newnode);

	return newnode;
}

/*
 * _copyDynamicIndexScan
 */
static DynamicIndexScan *
_copyDynamicIndexScan(const DynamicIndexScan *from)
{
	DynamicIndexScan  *newnode = makeNode(DynamicIndexScan);

	/* DynamicIndexScan has some content from IndexScan */
	CopyIndexScanFields(&from->indexscan, &newnode->indexscan);
	COPY_NODE_FIELD(partOids);
	COPY_NODE_FIELD(part_prune_info);
	COPY_NODE_FIELD(join_prune_paramids);

	return newnode;
}

static void
CopyIndexOnlyScanFields(const IndexOnlyScan *from, IndexOnlyScan *newnode)
{
	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(indexid);
	COPY_NODE_FIELD(indexqual);
	COPY_NODE_FIELD(indexqualorig);
	COPY_NODE_FIELD(recheckqual);
	COPY_NODE_FIELD(indexorderby);
	COPY_NODE_FIELD(indextlist);
	COPY_SCALAR_FIELD(indexorderdir);
}

/*
 * _copyIndexOnlyScan
 */
static IndexOnlyScan *
_copyIndexOnlyScan(const IndexOnlyScan *from)
{
	IndexOnlyScan *newnode = makeNode(IndexOnlyScan);

	CopyIndexOnlyScanFields(from, newnode);

	return newnode;
}

/*
 * _copyDynamicIndexOnlyScan
 */
static DynamicIndexOnlyScan *
_copyDynamicIndexOnlyScan(const DynamicIndexOnlyScan *from)
{
	DynamicIndexOnlyScan  *newnode = makeNode(DynamicIndexOnlyScan);

	/* DynamicIndexScan has some content from IndexScan */
	CopyIndexOnlyScanFields(&from->indexscan, &newnode->indexscan);
	COPY_NODE_FIELD(partOids);
	COPY_NODE_FIELD(part_prune_info);
	COPY_NODE_FIELD(join_prune_paramids);

	return newnode;
}

/*
 * _copyBitmapIndexScan
 */
static void
CopyBitmapIndexScanFields(const BitmapIndexScan *from, BitmapIndexScan *newnode)
{
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	COPY_SCALAR_FIELD(indexid);
	COPY_SCALAR_FIELD(isshared);
	COPY_NODE_FIELD(indexqual);
	COPY_NODE_FIELD(indexqualorig);
}

static BitmapIndexScan *
_copyBitmapIndexScan(const BitmapIndexScan *from)
{
	BitmapIndexScan *newnode = makeNode(BitmapIndexScan);

	CopyBitmapIndexScanFields(from, newnode);

	return newnode;
}

/*
 * _copyDynamicBitmapIndexScan
 */
static DynamicBitmapIndexScan *
_copyDynamicBitmapIndexScan(const DynamicBitmapIndexScan *from)
{
	DynamicBitmapIndexScan *newnode = makeNode(DynamicBitmapIndexScan);

	CopyBitmapIndexScanFields(&from->biscan, &newnode->biscan);

	return newnode;
}

static void
CopyBitmapHeapScanFields(const BitmapHeapScan *from, BitmapHeapScan *newnode)
{
	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(bitmapqualorig);
}

/*
 * _copyBitmapHeapScan
 */
static BitmapHeapScan *
_copyBitmapHeapScan(const BitmapHeapScan *from)
{
	BitmapHeapScan *newnode = makeNode(BitmapHeapScan);

	CopyBitmapHeapScanFields(from, newnode);

	return newnode;
}

/*
 * _copyDynamicBitmapHeapScan
 */
static DynamicBitmapHeapScan *
_copyDynamicBitmapHeapScan(const DynamicBitmapHeapScan *from)
{
	DynamicBitmapHeapScan *newnode = makeNode(DynamicBitmapHeapScan);

	CopyBitmapHeapScanFields(&from->bitmapheapscan, &newnode->bitmapheapscan);
	COPY_NODE_FIELD(partOids);
	COPY_NODE_FIELD(part_prune_info);
	COPY_NODE_FIELD(join_prune_paramids);

	return newnode;
}

/*
 * _copyTidScan
 */
static TidScan *
_copyTidScan(const TidScan *from)
{
	TidScan    *newnode = makeNode(TidScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(tidquals);

	return newnode;
}

/*
 * _copyTidRangeScan
 */
static TidRangeScan *
_copyTidRangeScan(const TidRangeScan *from)
{
	TidRangeScan *newnode = makeNode(TidRangeScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(tidrangequals);

	return newnode;
}

/*
 * _copySubqueryScan
 */
static SubqueryScan *
_copySubqueryScan(const SubqueryScan *from)
{
	SubqueryScan *newnode = makeNode(SubqueryScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(subplan);

	return newnode;
}

/*
 * _copyFunctionScan
 */
static FunctionScan *
_copyFunctionScan(const FunctionScan *from)
{
	FunctionScan *newnode = makeNode(FunctionScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(functions);
	COPY_SCALAR_FIELD(funcordinality);
	COPY_NODE_FIELD(param);
	COPY_SCALAR_FIELD(resultInTupleStore);
	COPY_SCALAR_FIELD(initplanId);

	return newnode;
}

/*
 * _copyTableFunctionScan
 */
static TableFunctionScan *
_copyTableFunctionScan(const TableFunctionScan *from)
{
	TableFunctionScan	*newnode = makeNode(TableFunctionScan);

	CopyScanFields((const Scan *) from, (Scan *) newnode);
	COPY_NODE_FIELD(function);

	return newnode;
}

/*
 * _copyTableFuncScan
 */
static TableFuncScan *
_copyTableFuncScan(const TableFuncScan *from)
{
	TableFuncScan *newnode = makeNode(TableFuncScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(tablefunc);

	return newnode;
}

/*
 * _copyValuesScan
 */
static ValuesScan *
_copyValuesScan(const ValuesScan *from)
{
	ValuesScan *newnode = makeNode(ValuesScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(values_lists);

	return newnode;
}

/*
 * _copyCteScan
 */
static CteScan *
_copyCteScan(const CteScan *from)
{
	CteScan    *newnode = makeNode(CteScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(ctePlanId);
	COPY_SCALAR_FIELD(cteParam);

	return newnode;
}

/*
 * _copyNamedTuplestoreScan
 */
static NamedTuplestoreScan *
_copyNamedTuplestoreScan(const NamedTuplestoreScan *from)
{
	NamedTuplestoreScan *newnode = makeNode(NamedTuplestoreScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_STRING_FIELD(enrname);

	return newnode;
}

/*
 * _copyWorkTableScan
 */
static WorkTableScan *
_copyWorkTableScan(const WorkTableScan *from)
{
	WorkTableScan *newnode = makeNode(WorkTableScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(wtParam);

	return newnode;
}

static void
CopyForeignScanFields(const ForeignScan *from, ForeignScan *newnode)
{
	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(operation);
	COPY_SCALAR_FIELD(resultRelation);
	COPY_SCALAR_FIELD(fs_server);
	COPY_NODE_FIELD(fdw_exprs);
	COPY_NODE_FIELD(fdw_private);
	COPY_NODE_FIELD(fdw_scan_tlist);
	COPY_NODE_FIELD(fdw_recheck_quals);
	COPY_BITMAPSET_FIELD(fs_relids);
	COPY_SCALAR_FIELD(fsSystemCol);

}

/*
 * _copyForeignScan
 */
static ForeignScan *
_copyForeignScan(const ForeignScan *from)
{
	ForeignScan *newnode = makeNode(ForeignScan);

	CopyForeignScanFields(from, newnode);

	return newnode;
}

/*
 * _copyDynamicForeignScan
 */
static DynamicForeignScan *
_copyDynamicForeignScan(const DynamicForeignScan *from)
{
	DynamicForeignScan  *newnode = makeNode(DynamicForeignScan);

	/* DynamicForeignScan has some content from ForeignScan */
	CopyForeignScanFields(&from->foreignscan, &newnode->foreignscan);
	COPY_NODE_FIELD(partOids);
	COPY_NODE_FIELD(part_prune_info);
	COPY_NODE_FIELD(join_prune_paramids);
	COPY_NODE_FIELD(fdw_private_list);

	return newnode;
}

/*
 * _copyCustomScan
 */
static CustomScan *
_copyCustomScan(const CustomScan *from)
{
	CustomScan *newnode = makeNode(CustomScan);

	/*
	 * copy node superclass fields
	 */
	CopyScanFields((const Scan *) from, (Scan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(flags);
	COPY_NODE_FIELD(custom_plans);
	COPY_NODE_FIELD(custom_exprs);
	COPY_NODE_FIELD(custom_private);
	COPY_NODE_FIELD(custom_scan_tlist);
	COPY_BITMAPSET_FIELD(custom_relids);

	/*
	 * NOTE: The method field of CustomScan is required to be a pointer to a
	 * static table of callback functions.  So we don't copy the table itself,
	 * just reference the original one.
	 */
	COPY_SCALAR_FIELD(methods);

	return newnode;
}

/*
 * CopyJoinFields
 *
 *		This function copies the fields of the Join node.  It is used by
 *		all the copy functions for classes which inherit from Join.
 */
static void
CopyJoinFields(const Join *from, Join *newnode)
{
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

    COPY_SCALAR_FIELD(prefetch_inner);
	COPY_SCALAR_FIELD(prefetch_joinqual);
	COPY_SCALAR_FIELD(prefetch_qual);

	COPY_SCALAR_FIELD(jointype);
	COPY_SCALAR_FIELD(inner_unique);
	COPY_NODE_FIELD(joinqual);
}


/*
 * _copyJoin
 */
static Join *
_copyJoin(const Join *from)
{
	Join	   *newnode = makeNode(Join);

	/*
	 * copy node superclass fields
	 */
	CopyJoinFields(from, newnode);

	return newnode;
}


/*
 * _copyNestLoop
 */
static NestLoop *
_copyNestLoop(const NestLoop *from)
{
	NestLoop   *newnode = makeNode(NestLoop);

	/*
	 * copy node superclass fields
	 */
	CopyJoinFields((const Join *) from, (Join *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(nestParams);

    COPY_SCALAR_FIELD(shared_outer);
    COPY_SCALAR_FIELD(singleton_outer); /*CDB-OLAP*/

	return newnode;
}

/*
 * _copyMergeJoin
 */
static MergeJoin *
_copyMergeJoin(const MergeJoin *from)
{
	MergeJoin  *newnode = makeNode(MergeJoin);
	int			numCols;

	/*
	 * copy node superclass fields
	 */
	CopyJoinFields((const Join *) from, (Join *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(skip_mark_restore);
	COPY_NODE_FIELD(mergeclauses);
	numCols = list_length(from->mergeclauses);
	COPY_POINTER_FIELD(mergeFamilies, numCols * sizeof(Oid));
	COPY_POINTER_FIELD(mergeCollations, numCols * sizeof(Oid));
	COPY_POINTER_FIELD(mergeStrategies, numCols * sizeof(int));
	COPY_POINTER_FIELD(mergeNullsFirst, numCols * sizeof(bool));

	COPY_SCALAR_FIELD(unique_outer);

	return newnode;
}

/*
 * _copyHashJoin
 */
static HashJoin *
_copyHashJoin(const HashJoin *from)
{
	HashJoin   *newnode = makeNode(HashJoin);

	/*
	 * copy node superclass fields
	 */
	CopyJoinFields((const Join *) from, (Join *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(hashclauses);

	COPY_NODE_FIELD(hashoperators);
	COPY_NODE_FIELD(hashcollations);
	COPY_NODE_FIELD(hashkeys);
	COPY_NODE_FIELD(hashqualclauses);
	COPY_SCALAR_FIELD(batch0_barrier);
	COPY_SCALAR_FIELD(outer_motionhazard);

	return newnode;
}

/*
 * _copyShareInputScan
 */
static ShareInputScan *
_copyShareInputScan(const ShareInputScan *from)
{
	ShareInputScan *newnode = makeNode(ShareInputScan);

	/* copy node superclass fields */
	CopyPlanFields((Plan *) from, (Plan *) newnode);
	COPY_SCALAR_FIELD(cross_slice);
	COPY_SCALAR_FIELD(share_id);
	COPY_SCALAR_FIELD(producer_slice_id);
	COPY_SCALAR_FIELD(this_slice_id);
	COPY_SCALAR_FIELD(nconsumers);
	COPY_SCALAR_FIELD(discard_output);

	return newnode;
}


/*
 * _copyMaterial
 */
static Material *
_copyMaterial(const Material *from)
{
	Material   *newnode = makeNode(Material);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);
	COPY_SCALAR_FIELD(cdb_strict);
	COPY_SCALAR_FIELD(cdb_shield_child_from_rescans);

    return newnode;
}


/*
 * _copyMemoize
 */
static Memoize *
_copyMemoize(const Memoize *from)
{
	Memoize    *newnode = makeNode(Memoize);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(numKeys);
	COPY_POINTER_FIELD(hashOperators, sizeof(Oid) * from->numKeys);
	COPY_POINTER_FIELD(collations, sizeof(Oid) * from->numKeys);
	COPY_NODE_FIELD(param_exprs);
	COPY_SCALAR_FIELD(singlerow);
	COPY_SCALAR_FIELD(binary_mode);
	COPY_SCALAR_FIELD(est_entries);
	COPY_BITMAPSET_FIELD(keyparamids);

	return newnode;
}


/*
 * CopySortFields
 *
 *		This function copies the fields of the Sort node.  It is used by
 *		all the copy functions for classes which inherit from Sort.
 */
static void
CopySortFields(const Sort *from, Sort *newnode)
{
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(numCols);
	COPY_POINTER_FIELD(sortColIdx, from->numCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(sortOperators, from->numCols * sizeof(Oid));
	COPY_POINTER_FIELD(collations, from->numCols * sizeof(Oid));
	COPY_POINTER_FIELD(nullsFirst, from->numCols * sizeof(bool));
}

/*
 * _copySort
 */
static Sort *
_copySort(const Sort *from)
{
	Sort	   *newnode = makeNode(Sort);

	/*
	 * copy node superclass fields
	 */
	CopySortFields(from, newnode);

	return newnode;
}


/*
 * _copyIncrementalSort
 */
static IncrementalSort *
_copyIncrementalSort(const IncrementalSort *from)
{
	IncrementalSort *newnode = makeNode(IncrementalSort);

	/*
	 * copy node superclass fields
	 */
	CopySortFields((const Sort *) from, (Sort *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(nPresortedCols);

	return newnode;
}


/*
 * _copyAgg
 */
static Agg *
_copyAgg(const Agg *from)
{
	Agg		   *newnode = makeNode(Agg);

	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(aggstrategy);
	COPY_SCALAR_FIELD(aggsplit);
	COPY_SCALAR_FIELD(numCols);
	if (from->numCols > 0)
	{
		COPY_POINTER_FIELD(grpColIdx, from->numCols * sizeof(AttrNumber));
		COPY_POINTER_FIELD(grpOperators, from->numCols * sizeof(Oid));
		COPY_POINTER_FIELD(grpCollations, from->numCols * sizeof(Oid));
	}
	COPY_SCALAR_FIELD(numGroups);
	COPY_SCALAR_FIELD(transitionSpace);
	COPY_BITMAPSET_FIELD(aggParams);
	COPY_NODE_FIELD(groupingSets);
	COPY_NODE_FIELD(chain);
	COPY_SCALAR_FIELD(streaming);

	COPY_SCALAR_FIELD(agg_expr_id);
	return newnode;
}

static DQAExpr *
_copyDQAExpr(const DQAExpr *from)
{
    DQAExpr *newnode = makeNode(DQAExpr);

    COPY_SCALAR_FIELD(agg_expr_id);
    COPY_BITMAPSET_FIELD(agg_args_id_bms);
    COPY_NODE_FIELD(agg_filter);

    return newnode;
}

/*
 * _copyTupleSplit
 */
static TupleSplit *
_copyTupleSplit(const TupleSplit *from)
{
	TupleSplit  *newnode = makeNode(TupleSplit);

	CopyPlanFields((const Plan *) from, (Plan *) newnode);
	COPY_SCALAR_FIELD(numCols);
	if (from->numCols > 0)
	{
		COPY_POINTER_FIELD(grpColIdx, from->numCols * sizeof(AttrNumber));
	}

	COPY_NODE_FIELD(dqa_expr_lst);

	return newnode;
}

/*
 * _copyWindowAgg
 */
static WindowAgg *
_copyWindowAgg(const WindowAgg *from)
{
	WindowAgg  *newnode = makeNode(WindowAgg);

	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(winref);
	COPY_SCALAR_FIELD(partNumCols);
	COPY_POINTER_FIELD(partColIdx, from->partNumCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(partOperators, from->partNumCols * sizeof(Oid));
	COPY_POINTER_FIELD(partCollations, from->partNumCols * sizeof(Oid));
	COPY_SCALAR_FIELD(ordNumCols);

	if (from->ordNumCols > 0)
	{
		COPY_POINTER_FIELD(ordColIdx, from->ordNumCols * sizeof(AttrNumber));
		COPY_POINTER_FIELD(ordOperators, from->ordNumCols * sizeof(Oid));
		COPY_POINTER_FIELD(ordCollations, from->ordNumCols * sizeof(Oid));
	}
	COPY_SCALAR_FIELD(firstOrderCol);
	COPY_SCALAR_FIELD(firstOrderCmpOperator);
	COPY_SCALAR_FIELD(firstOrderNullsFirst);

	COPY_SCALAR_FIELD(frameOptions);
	COPY_NODE_FIELD(startOffset);
	COPY_NODE_FIELD(endOffset);
	COPY_SCALAR_FIELD(startInRangeFunc);
	COPY_SCALAR_FIELD(endInRangeFunc);
	COPY_SCALAR_FIELD(inRangeColl);
	COPY_SCALAR_FIELD(inRangeAsc);
	COPY_SCALAR_FIELD(inRangeNullsFirst);

	return newnode;
}

/*
 * _copyWindowHashAgg
 */
static WindowHashAgg *
_copyWindowHashAgg(const WindowHashAgg *from)
{
	WindowHashAgg  *newnode = makeNode(WindowHashAgg);

	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(winref);
	COPY_SCALAR_FIELD(partNumCols);
	COPY_POINTER_FIELD(partColIdx, from->partNumCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(partOperators, from->partNumCols * sizeof(Oid));
	COPY_POINTER_FIELD(partCollations, from->partNumCols * sizeof(Oid));
	COPY_SCALAR_FIELD(ordNumCols);

	if (from->ordNumCols > 0)
	{
		COPY_POINTER_FIELD(ordColIdx, from->ordNumCols * sizeof(AttrNumber));
		COPY_POINTER_FIELD(ordOperators, from->ordNumCols * sizeof(Oid));
		COPY_POINTER_FIELD(ordCollations, from->ordNumCols * sizeof(Oid));
		COPY_POINTER_FIELD(ordNullsFirst, from->ordNumCols * sizeof(bool));
	}

	COPY_SCALAR_FIELD(frameOptions);
	COPY_NODE_FIELD(startOffset);
	COPY_NODE_FIELD(endOffset);
	COPY_SCALAR_FIELD(startInRangeFunc);
	COPY_SCALAR_FIELD(endInRangeFunc);
	COPY_SCALAR_FIELD(inRangeColl);
	COPY_SCALAR_FIELD(inRangeAsc);
	COPY_SCALAR_FIELD(inRangeNullsFirst);

	return newnode;
}

/*
 * _copyUnique
 */
static Unique *
_copyUnique(const Unique *from)
{
	Unique	   *newnode = makeNode(Unique);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(numCols);
	COPY_POINTER_FIELD(uniqColIdx, from->numCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(uniqOperators, from->numCols * sizeof(Oid));
	COPY_POINTER_FIELD(uniqCollations, from->numCols * sizeof(Oid));

	return newnode;
}

/*
 * _copyHash
 */
static Hash *
_copyHash(const Hash *from)
{
	Hash	   *newnode = makeNode(Hash);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(hashkeys);
	COPY_SCALAR_FIELD(skewTable);
	COPY_SCALAR_FIELD(skewColumn);
	COPY_SCALAR_FIELD(skewInherit);
	COPY_SCALAR_FIELD(rows_total);
	COPY_SCALAR_FIELD(sync_barrier);

	return newnode;
}

/*
 * _copySetOp
 */
static SetOp *
_copySetOp(const SetOp *from)
{
	SetOp	   *newnode = makeNode(SetOp);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_SCALAR_FIELD(cmd);
	COPY_SCALAR_FIELD(strategy);
	COPY_SCALAR_FIELD(numCols);
	COPY_POINTER_FIELD(dupColIdx, from->numCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(dupOperators, from->numCols * sizeof(Oid));
	COPY_POINTER_FIELD(dupCollations, from->numCols * sizeof(Oid));
	COPY_SCALAR_FIELD(flagColIdx);
	COPY_SCALAR_FIELD(firstFlag);
	COPY_SCALAR_FIELD(numGroups);

	return newnode;
}

/*
 * _copyLockRows
 */
static LockRows *
_copyLockRows(const LockRows *from)
{
	LockRows   *newnode = makeNode(LockRows);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(rowMarks);
	COPY_SCALAR_FIELD(epqParam);

	return newnode;
}

/*
 * _copyRuntimeFilter
 */
static RuntimeFilter *
_copyRuntimeFilter(const RuntimeFilter *from)
{
	RuntimeFilter	   *newnode = makeNode(RuntimeFilter);

	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	return newnode;
}

/*
 * _copyLimit
 */
static Limit *
_copyLimit(const Limit *from)
{
	Limit	   *newnode = makeNode(Limit);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((const Plan *) from, (Plan *) newnode);

	/*
	 * copy remainder of node
	 */
	COPY_NODE_FIELD(limitOffset);
	COPY_NODE_FIELD(limitCount);
	COPY_SCALAR_FIELD(limitOption);
	COPY_SCALAR_FIELD(uniqNumCols);
	COPY_POINTER_FIELD(uniqColIdx, from->uniqNumCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(uniqOperators, from->uniqNumCols * sizeof(Oid));
	COPY_POINTER_FIELD(uniqCollations, from->uniqNumCols * sizeof(Oid));

	return newnode;
}

/*
 * _copyNestLoopParam
 */
static NestLoopParam *
_copyNestLoopParam(const NestLoopParam *from)
{
	NestLoopParam *newnode = makeNode(NestLoopParam);

	COPY_SCALAR_FIELD(paramno);
	COPY_NODE_FIELD(paramval);

	return newnode;
}

/*
 * _copyPlanRowMark
 */
static PlanRowMark *
_copyPlanRowMark(const PlanRowMark *from)
{
	PlanRowMark *newnode = makeNode(PlanRowMark);

	COPY_SCALAR_FIELD(rti);
	COPY_SCALAR_FIELD(prti);
	COPY_SCALAR_FIELD(rowmarkId);
	COPY_SCALAR_FIELD(markType);
	COPY_SCALAR_FIELD(allMarkTypes);
	COPY_SCALAR_FIELD(strength);
	COPY_SCALAR_FIELD(waitPolicy);
	COPY_SCALAR_FIELD(isParent);

	return newnode;
}

static PartitionPruneInfo *
_copyPartitionPruneInfo(const PartitionPruneInfo *from)
{
	PartitionPruneInfo *newnode = makeNode(PartitionPruneInfo);

	COPY_NODE_FIELD(prune_infos);
	COPY_BITMAPSET_FIELD(other_subplans);

	return newnode;
}

static PartitionedRelPruneInfo *
_copyPartitionedRelPruneInfo(const PartitionedRelPruneInfo *from)
{
	PartitionedRelPruneInfo *newnode = makeNode(PartitionedRelPruneInfo);

	COPY_SCALAR_FIELD(rtindex);
	COPY_BITMAPSET_FIELD(present_parts);
	COPY_SCALAR_FIELD(nparts);
	COPY_POINTER_FIELD(subplan_map, from->nparts * sizeof(int));
	COPY_POINTER_FIELD(subpart_map, from->nparts * sizeof(int));
	COPY_POINTER_FIELD(relid_map, from->nparts * sizeof(Oid));
	COPY_NODE_FIELD(initial_pruning_steps);
	COPY_NODE_FIELD(exec_pruning_steps);
	COPY_BITMAPSET_FIELD(execparamids);

	return newnode;
}

/*
 * _copyPartitionPruneStepOp
 */
static PartitionPruneStepOp *
_copyPartitionPruneStepOp(const PartitionPruneStepOp *from)
{
	PartitionPruneStepOp *newnode = makeNode(PartitionPruneStepOp);

	COPY_SCALAR_FIELD(step.step_id);
	COPY_SCALAR_FIELD(opstrategy);
	COPY_NODE_FIELD(exprs);
	COPY_NODE_FIELD(cmpfns);
	COPY_BITMAPSET_FIELD(nullkeys);

	return newnode;
}

/*
 * _copyPartitionPruneStepCombine
 */
static PartitionPruneStepCombine *
_copyPartitionPruneStepCombine(const PartitionPruneStepCombine *from)
{
	PartitionPruneStepCombine *newnode = makeNode(PartitionPruneStepCombine);

	COPY_SCALAR_FIELD(step.step_id);
	COPY_SCALAR_FIELD(combineOp);
	COPY_NODE_FIELD(source_stepids);

	return newnode;
}

/*
 * _copyPlanInvalItem
 */
static PlanInvalItem *
_copyPlanInvalItem(const PlanInvalItem *from)
{
	PlanInvalItem *newnode = makeNode(PlanInvalItem);

	COPY_SCALAR_FIELD(cacheId);
	COPY_SCALAR_FIELD(hashValue);

	return newnode;
}

/*
 * _copyMotion
 */
static Motion *
_copyMotion(const Motion *from)
{
	Motion	   *newnode = makeNode(Motion);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(sendSorted);
	COPY_SCALAR_FIELD(motionID);

	COPY_SCALAR_FIELD(motionType);

	COPY_NODE_FIELD(hashExprs);
	COPY_POINTER_FIELD(hashFuncs, list_length(from->hashExprs) * sizeof(Oid));

	COPY_SCALAR_FIELD(numSortCols);
	COPY_POINTER_FIELD(sortColIdx, from->numSortCols * sizeof(AttrNumber));
	COPY_POINTER_FIELD(sortOperators, from->numSortCols * sizeof(Oid));
	COPY_POINTER_FIELD(collations, from->numSortCols * sizeof(Oid));
	COPY_POINTER_FIELD(nullsFirst, from->numSortCols * sizeof(bool));

	COPY_SCALAR_FIELD(segidColIdx);
	COPY_SCALAR_FIELD(numHashSegments);

	if (from->senderSliceInfo)
	{
		newnode->senderSliceInfo = palloc(sizeof(PlanSlice));
		memcpy(newnode->senderSliceInfo, from->senderSliceInfo, sizeof(PlanSlice));
	}

	return newnode;
}

/*
 * _copySplitUpdate
 */
static SplitUpdate *
_copySplitUpdate(const SplitUpdate *from)
{
	SplitUpdate *newnode = makeNode(SplitUpdate);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(actionColIdx);
	COPY_NODE_FIELD(insertColIdx);
	COPY_NODE_FIELD(deleteColIdx);

	COPY_SCALAR_FIELD(numHashSegments);
	COPY_SCALAR_FIELD(numHashAttrs);
	COPY_POINTER_FIELD(hashAttnos, from->numHashAttrs * sizeof(AttrNumber));
	COPY_POINTER_FIELD(hashFuncs, from->numHashAttrs * sizeof(Oid));

	return newnode;
}

/*
 * _copyAssertOp
 */
static AssertOp *
_copyAssertOp(const AssertOp *from)
{
	AssertOp *newnode = makeNode(AssertOp);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(errcode);
	COPY_NODE_FIELD(errmessage);

	return newnode;
}

/*
 * _copyPartitionSelector
 */
static PartitionSelector *
_copyPartitionSelector(const PartitionSelector *from)
{
	PartitionSelector *newnode = makeNode(PartitionSelector);

	/*
	 * copy node superclass fields
	 */
	CopyPlanFields((Plan *) from, (Plan *) newnode);

	COPY_SCALAR_FIELD(paramid);
	COPY_NODE_FIELD(part_prune_info);

	return newnode;
}

/* ****************************************************************
 *					   primnodes.h copy functions
 * ****************************************************************
 */

/*
 * _copyAlias
 */
static Alias *
_copyAlias(const Alias *from)
{
	Alias	   *newnode = makeNode(Alias);

	COPY_STRING_FIELD(aliasname);
	COPY_NODE_FIELD(colnames);

	return newnode;
}

/*
 * _copyRangeVar
 */
static RangeVar *
_copyRangeVar(const RangeVar *from)
{
	RangeVar   *newnode = makeNode(RangeVar);

	Assert(from->schemaname == NULL || strlen(from->schemaname)>0);
	COPY_STRING_FIELD(catalogname);
	COPY_STRING_FIELD(schemaname);
	COPY_STRING_FIELD(relname);
	COPY_SCALAR_FIELD(inh);
	COPY_SCALAR_FIELD(relpersistence);
	COPY_NODE_FIELD(alias);
	COPY_LOCATION_FIELD(location);

	return newnode;
}

/*
 * _copyTableFunc
 */
static TableFunc *
_copyTableFunc(const TableFunc *from)
{
	TableFunc  *newnode = makeNode(TableFunc);

	COPY_NODE_FIELD(ns_uris);
	COPY_NODE_FIELD(ns_names);
	COPY_NODE_FIELD(docexpr);
	COPY_NODE_FIELD(rowexpr);
	COPY_NODE_FIELD(colnames);
	COPY_NODE_FIELD(coltypes);
	COPY_NODE_FIELD(coltypmods);
	COPY_NODE_FIELD(colcollations);
	COPY_NODE_FIELD(colexprs);
	COPY_NODE_FIELD(coldefexprs);
	COPY_BITMAPSET_FIELD(notnulls);
	COPY_SCALAR_FIELD(ordinalitycol);
	COPY_LOCATION_FIELD(location);

	return newnode;
}

/*
 * _copyIntoClause
 */
static IntoClause *
_copyIntoClause(const IntoClause *from)
{
	IntoClause *newnode = makeNode(IntoClause);

	COPY_NODE_FIELD(rel);
	COPY_NODE_FIELD(colNames);
	COPY_STRING_FIELD(accessMethod);
	COPY_NODE_FIELD(options);
	COPY_SCALAR_FIELD(onCommit);
	COPY_STRING_FIELD(tableSpaceName);
	COPY_NODE_FIELD(viewQuery);
	COPY_SCALAR_FIELD(skipData);
	COPY_NODE_FIELD(distributedBy);
	COPY_SCALAR_FIELD(ivm);
	COPY_SCALAR_FIELD(matviewOid);
	COPY_STRING_FIELD(enrname);
	COPY_SCALAR_FIELD(dynamicTbl);
	COPY_STRING_FIELD(schedule);

	return newnode;
}

/*
 * _copyIntoClause
 */
static CopyIntoClause *
_copyCopyIntoClause(const CopyIntoClause *from)
{
	CopyIntoClause *newnode = makeNode(CopyIntoClause);

	COPY_NODE_FIELD(attlist);
	COPY_SCALAR_FIELD(is_program);
	COPY_STRING_FIELD(filename);
	COPY_NODE_FIELD(options);

	return newnode;
}

/*
 * _copyRefreshClause
 */
static RefreshClause *
_copyRefreshClause(const RefreshClause *from)
{
	RefreshClause *newnode = makeNode(RefreshClause);

	COPY_SCALAR_FIELD(concurrent);
	COPY_NODE_FIELD(relation);

	return newnode;
}

/*
 * We don't need a _copyExpr because Expr is an abstract supertype which
 * should never actually get instantiated.  Also, since it has no common
 * fields except NodeTag, there's no need for a helper routine to factor
 * out copying the common fields...
 */

/*
 * _copyVar
 */
static Var *
_copyVar(const Var *from)
{
	Var		   *newnode = makeNode(Var);

	COPY_SCALAR_FIELD(varno);
	COPY_SCALAR_FIELD(varattno);
	COPY_SCALAR_FIELD(vartype);
	COPY_SCALAR_FIELD(vartypmod);
	COPY_SCALAR_FIELD(varcollid);
	COPY_SCALAR_FIELD(varlevelsup);
	COPY_SCALAR_FIELD(varnosyn);
	COPY_SCALAR_FIELD(varattnosyn);
	COPY_LOCATION_FIELD(location);

	return newnode;
}

/*
 * _copyConst
 */
>>>>>>> a840049c146 (ORCA: Introduce hash windows aggregation when use the vectorization exector)
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
<<<<<<< HEAD
#include "copyfuncs.switch.c"
=======
			/*
			 * PLAN NODES
			 */
		case T_PlannedStmt:
			retval = _copyPlannedStmt(from);
			break;
		case T_QueryDispatchDesc:
			retval = _copyQueryDispatchDesc(from);
			break;
		case T_SerializedParams:
			retval = _copySerializedParams(from);
			break;
		case T_OidAssignment:
			retval = _copyOidAssignment(from);
			break;
		case T_Plan:
			retval = _copyPlan(from);
			break;
		case T_Result:
			retval = _copyResult(from);
			break;
		case T_ProjectSet:
			retval = _copyProjectSet(from);
			break;
		case T_ModifyTable:
			retval = _copyModifyTable(from);
			break;
		case T_Append:
			retval = _copyAppend(from);
			break;
		case T_MergeAppend:
			retval = _copyMergeAppend(from);
			break;
		case T_RecursiveUnion:
			retval = _copyRecursiveUnion(from);
			break;
		case T_Sequence:
			retval = _copySequence(from);
			break;
		case T_BitmapAnd:
			retval = _copyBitmapAnd(from);
			break;
		case T_BitmapOr:
			retval = _copyBitmapOr(from);
			break;
		case T_Scan:
			retval = _copyScan(from);
			break;
		case T_Gather:
			retval = _copyGather(from);
			break;
		case T_GatherMerge:
			retval = _copyGatherMerge(from);
			break;
		case T_SeqScan:
			retval = _copySeqScan(from);
			break;
		case T_DynamicSeqScan:
			retval = _copyDynamicSeqScan(from);
			break;
		case T_ExternalScanInfo:
			retval = _copyExternalScanInfo(from);
			break;
		case T_SampleScan:
			retval = _copySampleScan(from);
			break;
		case T_IndexScan:
			retval = _copyIndexScan(from);
			break;
		case T_DynamicIndexScan:
			retval = _copyDynamicIndexScan(from);
			break;
		case T_DynamicIndexOnlyScan:
			retval = _copyDynamicIndexOnlyScan(from);
			break;
		case T_IndexOnlyScan:
			retval = _copyIndexOnlyScan(from);
			break;
		case T_BitmapIndexScan:
			retval = _copyBitmapIndexScan(from);
			break;
		case T_DynamicBitmapIndexScan:
			retval = _copyDynamicBitmapIndexScan(from);
			break;
		case T_BitmapHeapScan:
			retval = _copyBitmapHeapScan(from);
			break;
		case T_DynamicBitmapHeapScan:
			retval = _copyDynamicBitmapHeapScan(from);
			break;
		case T_TidScan:
			retval = _copyTidScan(from);
			break;
		case T_TidRangeScan:
			retval = _copyTidRangeScan(from);
			break;
		case T_SubqueryScan:
			retval = _copySubqueryScan(from);
			break;
		case T_FunctionScan:
			retval = _copyFunctionScan(from);
			break;
		case T_TableFuncScan:
			retval = _copyTableFuncScan(from);
			break;
		case T_ValuesScan:
			retval = _copyValuesScan(from);
			break;
		case T_CteScan:
			retval = _copyCteScan(from);
			break;
		case T_NamedTuplestoreScan:
			retval = _copyNamedTuplestoreScan(from);
			break;
		case T_WorkTableScan:
			retval = _copyWorkTableScan(from);
			break;
		case T_ForeignScan:
			retval = _copyForeignScan(from);
			break;
		case T_DynamicForeignScan:
			retval = _copyDynamicForeignScan(from);
			break;
		case T_CustomScan:
			retval = _copyCustomScan(from);
			break;
		case T_Join:
			retval = _copyJoin(from);
			break;
		case T_NestLoop:
			retval = _copyNestLoop(from);
			break;
		case T_MergeJoin:
			retval = _copyMergeJoin(from);
			break;
		case T_HashJoin:
			retval = _copyHashJoin(from);
			break;
		case T_ShareInputScan:
			retval = _copyShareInputScan(from);
			break;
		case T_Material:
			retval = _copyMaterial(from);
			break;
		case T_Memoize:
			retval = _copyMemoize(from);
			break;
		case T_Sort:
			retval = _copySort(from);
			break;
		case T_IncrementalSort:
			retval = _copyIncrementalSort(from);
			break;
		case T_Agg:
			retval = _copyAgg(from);
			break;
		case T_TupleSplit:
			retval = _copyTupleSplit(from);
			break;
		case T_DQAExpr:
			retval = _copyDQAExpr(from);
			break;
		case T_WindowAgg:
			retval = _copyWindowAgg(from);
			break;
		case T_WindowHashAgg:
			retval = _copyWindowHashAgg(from);
			break;
		case T_TableFunctionScan:
			retval = _copyTableFunctionScan(from);
			break;
		case T_Unique:
			retval = _copyUnique(from);
			break;
		case T_Hash:
			retval = _copyHash(from);
			break;
		case T_SetOp:
			retval = _copySetOp(from);
			break;
		case T_LockRows:
			retval = _copyLockRows(from);
			break;
		case T_RuntimeFilter:
			retval = _copyRuntimeFilter(from);
			break;
		case T_Limit:
			retval = _copyLimit(from);
			break;
		case T_NestLoopParam:
			retval = _copyNestLoopParam(from);
			break;
		case T_PlanRowMark:
			retval = _copyPlanRowMark(from);
			break;
		case T_PartitionPruneInfo:
			retval = _copyPartitionPruneInfo(from);
			break;
		case T_PartitionedRelPruneInfo:
			retval = _copyPartitionedRelPruneInfo(from);
			break;
		case T_PartitionPruneStepOp:
			retval = _copyPartitionPruneStepOp(from);
			break;
		case T_PartitionPruneStepCombine:
			retval = _copyPartitionPruneStepCombine(from);
			break;
		case T_PlanInvalItem:
			retval = _copyPlanInvalItem(from);
			break;
		case T_Motion:
			retval = _copyMotion(from);
			break;
		case T_SplitUpdate:
			retval = _copySplitUpdate(from);
			break;
		case T_AssertOp:
			retval = _copyAssertOp(from);
			break;
		case T_PartitionSelector:
			retval = _copyPartitionSelector(from);
			break;
>>>>>>> a840049c146 (ORCA: Introduce hash windows aggregation when use the vectorization exector)

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

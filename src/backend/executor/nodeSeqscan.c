/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/executor/nodeSeqscan.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 *		ExecSeqScan				sequentially scans a relation.
 *		ExecSeqNext				retrieve next tuple in sequential order.
 *		ExecInitSeqScan			creates and initializes a seqscan node.
 *		ExecEndSeqScan			releases any storage allocated.
 *		ExecReScanSeqScan		rescans the relation
 *
 *		ExecSeqScanEstimate		estimates DSM space needed for parallel scan
 *		ExecSeqScanInitializeDSM initialize DSM for parallel scan
 *		ExecSeqScanReInitializeDSM reinitialize DSM for fresh parallel scan
 *		ExecSeqScanInitializeWorker attach to DSM info in parallel worker
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/relscan.h"
#include "access/session.h"
#include "access/tableam.h"
#include "catalog/pg_namespace.h"
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "utils/datum.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "nodes/nodeFuncs.h"
#include "nodes/makefuncs.h"

#include "cdb/cdbaocsam.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbvars.h"

static TupleTableSlot *SeqNext(SeqScanState *node);
static List *ConvertScanKeysToQuals(List *scankeys, Index scanrelid);
static void ExecSeqScanExplainEnd(PlanState *planstate, struct StringInfoData *buf);

/* ----------------------------------------------------------------
 *						Scan Support
 * ----------------------------------------------------------------
 */
/* ----------------------------------------------------------------
 *		SeqNext
 *
 *		This is a workhorse for ExecSeqScan
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
SeqNext(SeqScanState *node)
{
	TableScanDesc scandesc;
	EState	   *estate;
	ScanDirection direction;
	TupleTableSlot *slot;

	/*
	 * get information from the estate and scan state
	 */
	scandesc = node->ss.ss_currentScanDesc;
	estate = node->ss.ps.state;
	direction = estate->es_direction;
	slot = node->ss.ss_ScanTupleSlot;

	if (scandesc == NULL)
	{
		/*
		 * Just when gp_enable_runtime_filter_pushdown enabled and
		 * node->filter_in_seqscan is false means scankey need to be pushed to
		 * AM.
		 */
		if (gp_enable_runtime_filter_pushdown && !node->filter_in_seqscan &&
			!estate->useMppParallelMode)
		{
			List *quals = NIL;
			Scan *scan = (Scan *)node->ss.ps.plan;
			quals = ConvertScanKeysToQuals(node->filters, scan->scanrelid);
			if (quals)
			{
				node->ss.ps.plan->qual = list_concat(node->ss.ps.plan->qual, quals);
			}
		}

		/*
		* We reach here if the scan is not parallel, or if we're serially
		* executing a scan that was planned to be parallel.
		*/
		scandesc = table_beginscan_es(node->ss.ss_currentRelation,
									  estate->es_snapshot,
									  0, NULL,
									  NULL,
									  &node->ss.ps);
		node->ss.ss_currentScanDesc = scandesc;
	}

	/*
	 * get the next tuple from the table
	 */
	if (node->filter_in_seqscan && node->filters)
	{
		while (table_scan_getnextslot(scandesc, direction, slot))
		{
			if (!PassByBloomFilter(&node->ss.ps, node->filters, slot))
				continue;

			return slot;
		}
	}
	else
	{
		if (table_scan_getnextslot(scandesc, direction, slot))
			return slot;
	}

	return NULL;
}

/*
 * SeqRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool
SeqRecheck(SeqScanState *node, TupleTableSlot *slot)
{
	/*
	 * Note that unlike IndexScan, SeqScan never use keys in heap_beginscan
	 * (and this is very bad) - so, here we do not check are keys ok or not.
	 */
	return true;
}

/* ----------------------------------------------------------------
 *		ExecSeqScan(node)
 *
 *		Scans the relation sequentially and returns the next qualifying
 *		tuple.
 *		We call the ExecScan() routine and pass it the appropriate
 *		access method functions.
 * ----------------------------------------------------------------
 */
static TupleTableSlot *
ExecSeqScan(PlanState *pstate)
{
	SeqScanState *node = castNode(SeqScanState, pstate);

	return ExecScan(&node->ss,
					(ExecScanAccessMtd) SeqNext,
					(ExecScanRecheckMtd) SeqRecheck);
}

/* ----------------------------------------------------------------
 *		ExecInitSeqScan
 * ----------------------------------------------------------------
 */
SeqScanState *
ExecInitSeqScan(SeqScan *node, EState *estate, int eflags)
{
	Relation	currentRelation;

	/*
	 * get the relation object id from the relid'th entry in the range table,
	 * open that relation and acquire appropriate lock on it.
	 */
	currentRelation = ExecOpenScanRelation(estate, node->scanrelid, eflags);

	return ExecInitSeqScanForPartition(node, estate, currentRelation);
}

SeqScanState *
ExecInitSeqScanForPartition(SeqScan *node, EState *estate,
							Relation currentRelation)
{
	SeqScanState *scanstate;

	/*
	 * Once upon a time it was possible to have an outerPlan of a SeqScan, but
	 * not any more.
	 */
	Assert(outerPlan(node) == NULL);
	Assert(innerPlan(node) == NULL);

	/*
	 * create state structure
	 */
	scanstate = makeNode(SeqScanState);
	scanstate->ss.ps.plan = (Plan *) node;
	scanstate->ss.ps.state = estate;
	scanstate->ss.ps.ExecProcNode = ExecSeqScan;

	if (estate->es_instrument && (estate->es_instrument & INSTRUMENT_CDB))
	{
		/* Allocate string buffer. */
		scanstate->ss.ps.cdbexplainbuf = makeStringInfo();

		/* Request a callback at end of query. */
		scanstate->ss.ps.cdbexplainfun = ExecSeqScanExplainEnd;
	}
	/*
	 * Miscellaneous initialization
	 *
	 * create expression context for node
	 */
	ExecAssignExprContext(estate, &scanstate->ss.ps);

	/*
	 * open the scan relation
	 */
	scanstate->ss.ss_currentRelation = currentRelation;

	/* and create slot with the appropriate rowtype */
	ExecInitScanTupleSlot(estate, &scanstate->ss,
						  RelationGetDescr(scanstate->ss.ss_currentRelation),
						  table_slot_callbacks(scanstate->ss.ss_currentRelation));

	/*
	 * Initialize result type and projection.
	 */
	ExecInitResultTypeTL(&scanstate->ss.ps);
	ExecAssignScanProjectionInfo(&scanstate->ss);

	/*
	 * initialize child expressions
	 */
	scanstate->ss.ps.qual =
		ExecInitQual(node->plan.qual, (PlanState *) scanstate);

	/*
	 * check scan slot with bloom filters in seqscan node or not.
	 */
	if (gp_enable_runtime_filter_pushdown
		&& !estate->useMppParallelMode)
	{
		scanstate->filter_in_seqscan = !(table_scan_flags(currentRelation) & SCAN_SUPPORT_RUNTIME_FILTER);
	}

	return scanstate;
}

/* ----------------------------------------------------------------
 *		ExecEndSeqScan
 *
 *		frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void
ExecEndSeqScan(SeqScanState *node)
{
	TableScanDesc scanDesc;

	/*
	 * get information from node
	 */
	scanDesc = node->ss.ss_currentScanDesc;

	/*
	 * Free the exprcontext
	 */
	ExecFreeExprContext(&node->ss.ps);

	/*
	 * clean out the tuple table
	 */
	if (node->ss.ps.ps_ResultTupleSlot)
		ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
	ExecClearTuple(node->ss.ss_ScanTupleSlot);

	/*
	 * close heap scan
	 */
	if (scanDesc != NULL)
		table_endscan(scanDesc);
}

/* ----------------------------------------------------------------
 *						Join Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecReScanSeqScan
 *
 *		Rescans the relation.
 * ----------------------------------------------------------------
 */
void
ExecReScanSeqScan(SeqScanState *node)
{
	TableScanDesc scan;

	scan = node->ss.ss_currentScanDesc;

	if (scan != NULL)
		table_rescan(scan,		/* scan desc */
					 NULL);		/* new scan keys */

	ExecScanReScan((ScanState *) node);
}

/* ----------------------------------------------------------------
 *						Parallel Scan Support
 * ----------------------------------------------------------------
 */

/* ----------------------------------------------------------------
 *		ExecSeqScanEstimate
 *
 *		Compute the amount of space we'll need in the parallel
 *		query DSM, and inform pcxt->estimator about our needs.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanEstimate(SeqScanState *node,
					ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;

	node->pscan_len = table_parallelscan_estimate(node->ss.ss_currentRelation,
												  estate->es_snapshot);
	shm_toc_estimate_chunk(&pcxt->estimator, node->pscan_len);
	shm_toc_estimate_keys(&pcxt->estimator, 1);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeDSM
 *
 *		Set up a parallel heap scan descriptor.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeDSM(SeqScanState *node,
						 ParallelContext *pcxt)
{
	EState	   *estate = node->ss.ps.state;
	ParallelTableScanDesc pscan;
	TableScanDesc scandesc;

	pscan = shm_toc_allocate(pcxt->toc, node->pscan_len);

	Assert(pscan);

	table_parallelscan_initialize(node->ss.ss_currentRelation,
								  pscan,
								  estate->es_snapshot);
	shm_toc_insert(pcxt->toc, node->ss.ps.plan->plan_node_id, pscan);
	if (node->ss.ss_currentRelation->rd_tableam->scan_begin_extractcolumns)
	{
		/* try parallel mode for AOCO extract columns */
		scandesc = table_beginscan_es(node->ss.ss_currentRelation,
									  estate->es_snapshot,
									  0, NULL,
									  pscan,
									  &node->ss.ps);
	}
	else
	{
		/* normal parallel mode */
		scandesc = table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
	}
	node->ss.ss_currentScanDesc = scandesc;
}

/* ----------------------------------------------------------------
 *		ExecSeqScanReInitializeDSM
 *
 *		Reset shared state before beginning a fresh scan.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanReInitializeDSM(SeqScanState *node,
						   ParallelContext *pcxt)
{
	ParallelTableScanDesc pscan;

	pscan = node->ss.ss_currentScanDesc->rs_parallel;
	table_parallelscan_reinitialize(node->ss.ss_currentRelation, pscan);
}

/* ----------------------------------------------------------------
 *		ExecSeqScanInitializeWorker
 *
 *		Copy relevant information from TOC into planstate.
 * ----------------------------------------------------------------
 */
void
ExecSeqScanInitializeWorker(SeqScanState *node,
							ParallelWorkerContext *pwcxt)
{
	ParallelTableScanDesc pscan;
	TableScanDesc scandesc;
	EState	   *estate = node->ss.ps.state;

	pscan = shm_toc_lookup(pwcxt->toc, node->ss.ps.plan->plan_node_id, false);
	if (node->ss.ss_currentRelation->rd_tableam->scan_begin_extractcolumns)
	{
		/* try parallel mode for AOCO extract columns */
		scandesc = table_beginscan_es(node->ss.ss_currentRelation,
									  estate->es_snapshot,
									  0, NULL,
									  pscan,
									  &node->ss.ps);
	}
	else
	{
		/* normal parallel mode */
		scandesc = table_beginscan_parallel(node->ss.ss_currentRelation, pscan);
	}
	node->ss.ss_currentScanDesc = scandesc;
}

/*
 * Returns true if the element may be in the bloom filter.
 */
bool
PassByBloomFilter(PlanState *ps, List *filters, TupleTableSlot *slot)
{
	ScanKey	sk;
	Datum	val;
	bool	isnull;
	ListCell *lc;
	bloom_filter *blm_filter;

	/*
	 * Mark that the pushdown runtime filter is actually taking effect.
	 */
	if (ps->instrument && !ps->instrument->prf_work && list_length(filters))
		ps->instrument->prf_work = true;

	foreach (lc, filters)
	{
		sk = lfirst(lc);
		if (sk->sk_flags != SK_BLOOM_FILTER)
			continue;

		val = slot_getattr(slot, sk->sk_attno, &isnull);
		if (isnull)
			continue;

		blm_filter = (bloom_filter *)DatumGetPointer(sk->sk_argument);
		if (bloom_lacks_element(blm_filter, (unsigned char *)&val, sizeof(Datum)))
		{
			InstrCountFilteredPRF(ps, 1);
			return false;
		}
	}

	return true;
}

/*
 * ConvertScanKeysToQuals
 *		Convert a list of ScanKeys to a list of Expr nodes.
 *
 * This is used to convert the scan keys into a form that can be used as
 * runtime filter qual.
 */
static List *
ConvertScanKeysToQuals(List *scankeys, Index scanrelid)
{
	List *result = NIL;
	ListCell *lc;

	foreach (lc, scankeys)
	{
		ScanKey skey = (ScanKey) lfirst(lc);
		Expr *qual;
		const char *opname;
		Oid oproid;
		int16 typlen;
		bool typbyval;
		/* support SK_BLOOM_FILTER by PassByBloomFilter */
		if (skey->sk_flags == SK_BLOOM_FILTER)
			continue;

		/* Create Var node representing the scan attribute */
		Var *var = makeVar(scanrelid,		 /* ID of scan relation */
						   skey->sk_attno,	 /* Attribute number */
						   skey->sk_subtype, /* Attribute type */
						   -1,				 /* Type modifier */
						   skey->sk_collation, /* Collation */
						   0);				 /* Variable level */
		get_typlenbyval(skey->sk_subtype, &typlen, &typbyval);
		/* Create Const node representing the comparison value */
		Const *constant =
			makeConst(skey->sk_subtype,					/* Constant type */
					  -1,								/* Type modifier */
					  skey->sk_collation,						/* Collation */
					  typlen,							/* Length */
					  datumCopy(skey->sk_argument, typbyval, typlen), /* Value */
					  false,							/* Is null */
					  typbyval);							/* By value */

		/* Determine operator name based on B-tree strategy number */
		switch (skey->sk_strategy)
		{
			case BTGreaterEqualStrategyNumber:
				opname = ">=";
				break;
			case BTLessEqualStrategyNumber:
				opname = "<=";
				break;
			default:
				elog(ERROR, "unsupported strategy number: %d",
					 skey->sk_strategy);
		}

		/* Look up operator OID dynamically */
		oproid = get_operator(InvalidOid, skey->sk_subtype, skey->sk_subtype,
							  PG_CATALOG_NAMESPACE, opname, false);


		/* Create operator expression node */
		OpExpr *opexpr = makeNode(OpExpr);
		opexpr->opno = oproid;				   /* Operator OID */
		opexpr->opfuncid = get_opcode(oproid); /* Implementing function's OID */
		opexpr->opresulttype = BOOLOID;		   /* Result is always boolean */
		opexpr->opretset = false;		  /* Not a set returning operator */
		opexpr->opcollid = InvalidOid;	  /* No collation */
		opexpr->inputcollid = InvalidOid; /* No collation for input */

		/* Add the comparison arguments */
		opexpr->args = list_make2(var, constant);

		qual = (Expr *) opexpr;
		result = lappend(result, qual);
	}

	/* If we have multiple conditions, combine them with AND */
	if (list_length(result) > 1)
	{
		BoolExpr *andExpr = makeNode(BoolExpr);
		andExpr->boolop = AND_EXPR;
		andExpr->args = result;
		andExpr->location = -1;
		return list_make1(andExpr);
	}

	return result;
}

static void
ExecSeqScanExplainEnd(PlanState *planstate, struct StringInfoData *buf)
{
	SeqScanState *scanstate = (SeqScanState *) planstate;
	ListCell	*lc;
	int idx = 0;
	Relation rel;
	TupleDesc tupdesc;

	if (!scanstate->filters)
		return;

	rel = scanstate->ss.ss_currentRelation;
	tupdesc = RelationGetDescr(rel);

	foreach_with_count (lc, scanstate->filters, idx)
	{
		ScanKey sk = lfirst(lc);
		if (sk->sk_flags == SK_BLOOM_FILTER && scanstate->filter_in_seqscan)
		{
			bloom_filter *bf = (bloom_filter *) DatumGetPointer(sk->sk_argument);
			if (bf)
				appendStringInfo(buf, "BloomFilter[%d]: fpr:%f ", idx, bloom_false_positive_rate(bf));
		}
		if (sk->sk_flags == 0 && !scanstate->filter_in_seqscan)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, sk->sk_attno - 1);

			switch(sk->sk_strategy)
			{
				case BTGreaterEqualStrategyNumber:
					appendStringInfo(buf, "ScanKey[%s] >= %ld, ",
									NameStr(attr->attname), DatumGetInt64(sk->sk_argument));
					break;
				case BTLessEqualStrategyNumber:
					appendStringInfo(buf, "ScanKey[%s] <= %ld ",
									NameStr(attr->attname), DatumGetInt64(sk->sk_argument));
					break;
				default:
					break;
			}
		}
	}
}

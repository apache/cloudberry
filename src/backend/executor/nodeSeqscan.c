/*-------------------------------------------------------------------------
 *
 * nodeSeqscan.c
 *	  Support routines for sequential scans of relations.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
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
#include "executor/execdebug.h"
#include "executor/nodeSeqscan.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include "nodes/nodeFuncs.h"

#include "cdb/cdbaocsam.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbvars.h"

static TupleTableSlot *SeqNext(SeqScanState *node);

static bool PassByBloomFilter(SeqScanState *node, TupleTableSlot *slot);
static ScanKey ScanKeyListToArray(List *keys, int *num);

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
		int nkeys = 0;
		ScanKey keys = NULL;

		/*
		 * Just when gp_enable_runtime_filter_pushdown enabled and
		 * node->filter_in_seqscan is false means scankey need to be pushed to
		 * AM.
		 */
		if (gp_enable_runtime_filter_pushdown && !node->filter_in_seqscan)
			keys = ScanKeyListToArray(node->filters, &nkeys);

		/*
		* We reach here if the scan is not parallel, or if we're serially
		* executing a scan that was planned to be parallel.
		*/
		scandesc = table_beginscan_es(node->ss.ss_currentRelation,
									  estate->es_snapshot,
									  nkeys, keys,
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
			if (!PassByBloomFilter(node, slot))
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
	currentRelation = ExecOpenScanRelation(estate, node->scan.scanrelid, eflags);

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
		ExecInitQual(node->scan.plan.qual, (PlanState *) scanstate);

	/*
	 * check scan slot with bloom filters in seqscan node or not.
	 */
	if (gp_enable_runtime_filter_pushdown
		&& !estate->useMppParallelMode)
	{
		scanstate->filter_in_seqscan = true;
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
static bool
PassByBloomFilter(SeqScanState *node, TupleTableSlot *slot)
{
	ScanKey	sk;
	Datum	val;
	bool	isnull;
	ListCell *lc;
	bloom_filter *blm_filter;

	/*
	 * Mark that the pushdown runtime filter is actually taking effect.
	 */
	if (node->ss.ps.instrument &&
		!node->ss.ps.instrument->prf_work &&
		list_length(node->filters))
		node->ss.ps.instrument->prf_work = true;

	foreach (lc, node->filters)
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
			InstrCountFilteredPRF(node, 1);
			return false;
		}
	}

	return true;
}

/*
 * Convert the list of ScanKey to the array, and append an emtpy ScanKey as
 * the end flag of the array.
 */
static ScanKey
ScanKeyListToArray(List *keys, int *num)
{
	ScanKey sk;

	if (list_length(keys) == 0)
		return NULL;

	Assert(num);
	*num = list_length(keys);

	sk = (ScanKey)palloc(sizeof(ScanKeyData) * (*num + 1));
	for (int i = 0; i < *num; ++i)
		memcpy(&sk[i], list_nth(keys, i), sizeof(ScanKeyData));

	/*
	 * SK_EMPYT means the end of the array of the ScanKey
	 */
	sk[*num].sk_flags = SK_EMPYT;

	return sk;
}

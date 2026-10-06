/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * explain_gp.h
 *
 * src/include/commands/explain_gp.h
 * 
 *-------------------------------------------------------------------------
 */
#ifndef EXPLAIN_GP_H
#define EXPLAIN_GP_H


/* EXPLAIN ANALYZE statistics for one plan node of a slice */
typedef struct CdbExplain_StatInst
{
	NodeTag		pstype;			/* PlanState node type */

	/* fields from Instrumentation struct */
	instr_time	starttime;		/* Start time of current iteration of node */
	instr_time	counter;		/* Accumulated runtime for this node */
	double		firsttuple;		/* Time for first tuple of this cycle */
	double		startup;		/* Total startup time (in seconds) */
	double		total;			/* Total total time (in seconds) */
	double		ntuples;		/* Total tuples produced */
	double		ntuples2;
	double		nloops;			/* # of run cycles for this node */
	double		nfiltered1;
	double		nfiltered2;
	bool		prf_work;
	double		nfilteredPRF;
	double		execmemused;	/* executor memory used (bytes) */
	double		workmemused;	/* work_mem actually used (bytes) */
	double		workmemwanted;	/* work_mem to avoid workfile i/o (bytes) */
	bool		workfileCreated;	/* workfile created in this node */
	instr_time	firststart;		/* Start time of first iteration of node */
	int			numPartScanned; /* Number of part tables scanned */

	TuplesortInstrumentation sortstats; /* Sort stats, if this is a Sort node */
	HashInstrumentation hashstats; /* Hash stats, if this is a Hash node */
	IncrementalSortGroupInfo fullsortGroupInfo; /* Full sort group info for Incremental Sort node */
	IncrementalSortGroupInfo prefixsortGroupInfo; /* Prefix sort group info for Incremental Sort node */
	int			bnotes;			/* Offset to beginning of node's extra text */
	int			enotes;			/* Offset to end of node's extra text */
	int 		nworkers_launched; /* Number of workers launched for this node */
	WalUsage	walusage;		/* add WAL usage */
} CdbExplain_StatInst;


/* EXPLAIN ANALYZE statistics for one process working on one slice */
typedef struct CdbExplain_SliceWorker
{
	double		peakmemused;	/* bytes alloc in per-query mem context tree */
	double		vmem_reserved;	/* vmem reserved by a QE */
	int 		nworkers_launched; /* Number of workers launched for this slice */
} CdbExplain_SliceWorker;


/* Header of EXPLAIN ANALYZE statistics message sent from qExec to qDisp */
typedef struct CdbExplain_StatHdr
{
	NodeTag		type;			/* T_CdbExplain_StatHdr */
	int			segindex;		/* segment id */
	int			nInst;			/* num of StatInst entries following StatHdr */
	int			bnotes;			/* offset to extra text area */
	int			enotes;			/* offset to end of extra text area */

	CdbExplain_SliceWorker worker;	/* qExec's overall stats for slice */

	/*
	 * During serialization, we use this as a temporary StatInst and save
	 * "one-at-a-time" StatInst into this variable. We then write this
	 * variable into buffer (serialize it) and then "recycle" the same inst
	 * for next plan node's StatInst. During deserialization, an Array
	 * [0..nInst-1] of StatInst entries is appended starting here.
	 */
	CdbExplain_StatInst inst[1];

	/* extra text is appended after that */
} CdbExplain_StatHdr;

#endif //EXPLAIN_GP_H

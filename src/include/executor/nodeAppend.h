/*-------------------------------------------------------------------------
 *
 * nodeAppend.h
 *
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/executor/nodeAppend.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef NODEAPPEND_H
#define NODEAPPEND_H

#include "access/parallel.h"
#include "nodes/execnodes.h"

extern AppendState *ExecInitAppend(Append *node, EState *estate, int eflags);
extern void ExecEndAppend(AppendState *node);
extern void ExecReScanAppend(AppendState *node);
extern void ExecSquelchAppend(AppendState *node, bool force);
extern void ExecAppendEstimate(AppendState *node, ParallelContext *pcxt);
extern void GpAppendEstimate(AppendState *node, shm_toc_estimator *estimator);
extern void ExecAppendInitializeDSM(AppendState *node, ParallelContext *pcxt);
extern void GpAppendInitializeLWLock(ParallelAppendState *pstate);
extern void ExecAppendReInitializeDSM(AppendState *node, ParallelContext *pcxt);
extern void ExecAppendInitializeWorker(AppendState *node, ParallelWorkerContext *pwcxt);

extern void ExecAsyncAppendResponse(AsyncRequest *areq);

#endif							/* NODEAPPEND_H */

/*-------------------------------------------------------------------------
 *
 * nodeSplitMerge.h
 *        Prototypes for nodeSplitMerge.
 *
 * Portions Copyright (c) 2012, EMC Corp.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/executor/nodeSplitMerge.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef NODESplitMerge_H
#define NODESplitMerge_H

extern SplitMergeState* ExecInitSplitMerge(SplitMerge *node, EState *estate, int eflags);
extern void ExecEndSplitMerge(SplitMergeState *node);

#endif   /* NODESplitMerge_H */


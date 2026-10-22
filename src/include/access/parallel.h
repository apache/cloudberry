/*-------------------------------------------------------------------------
 *
 * parallel.h
 *	  Infrastructure for launching parallel workers
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/parallel.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PARALLEL_H
#define PARALLEL_H

#include "access/xlogdefs.h"
#include "lib/ilist.h"
#include "nodes/execnodes.h"
#include "postmaster/bgworker.h"
#include "storage/barrier.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"

typedef void (*parallel_worker_main_type) (dsm_segment *seg, shm_toc *toc);

typedef struct ParallelWorkerInfo
{
	BackgroundWorkerHandle *bgwhandle;
	shm_mq_handle *error_mqh;
	int32		pid;
} ParallelWorkerInfo;

typedef struct ParallelContext
{
	dlist_node	node;
	SubTransactionId subid;
	int			nworkers;		/* Maximum number of workers to launch */
	int			nworkers_to_launch; /* Actual number of workers to launch */
	int			nworkers_launched;
	char	   *library_name;
	char	   *function_name;
	ErrorContextCallback *error_context_stack;
	shm_toc_estimator estimator;
	dsm_segment *seg;
	void	   *private_memory;
	shm_toc    *toc;
	ParallelWorkerInfo *worker;
	int			nknown_attached_workers;
	bool	   *known_attached_workers;
} ParallelContext;

typedef struct ParallelWorkerContext
{
	dsm_segment *seg;
	shm_toc    *toc;
	int		nworkers;
	int		worker_id;
} ParallelWorkerContext;

extern PGDLLIMPORT volatile sig_atomic_t ParallelMessagePending;
extern PGDLLIMPORT int ParallelWorkerNumber;
extern PGDLLIMPORT bool InitializingParallelWorker;

/* CBDB_PARALLEL: Total parallel workers of a slice including myself, 0 for no parallel */
extern PGDLLIMPORT int ParallelWorkerNumberOfSlice;
extern PGDLLIMPORT int TotalParallelWorkerNumberOfSlice;

typedef struct ParallelEntryTag
{
	int cid;
	int sliceId;
	int sessionId;
} ParallelEntryTag;

#define INIT_PARALLELENTRYTAG(a,xx_cid,xx_sliceId,xx_sessionId) \
do {															\
	(a).cid = (xx_cid);											\
	(a).sliceId = (xx_sliceId);									\
	(a).sessionId = (xx_sessionId);								\
} while(0)

typedef struct GpParallelDSMEntry
{
	ParallelEntryTag	tag;
	int             	pid;
	dsm_handle      	handle;
	shm_toc         	*toc;
	int             	reference;
	int             	tolaunch;
	int			parallel_workers;
	int			temp_worker_id;  /* temproary usage */
	Barrier		build_barrier;	/* synchronization for the build dsm phases */
} GpParallelDSMEntry;

/*
 * CBDB_PARALLEL
 * The Postgres uses ParallelWorkerNumber to handle background workers including
 * parallel workers under Gather node.
 * To avoid mixing them and assertion failure, we use ParallelWorkerNumberOfSlice
 * to indentify CBDB style parallel mode.
 */
#define		IsParallelWorkerOfSlice()	(ParallelWorkerNumberOfSlice >= 0)

#define		IsParallelWorker()		(ParallelWorkerNumber >= 0)

extern ParallelContext *CreateParallelContext(const char *library_name,
											  const char *function_name, int nworkers);
extern void InitializeParallelDSM(ParallelContext *pcxt);
extern void ReinitializeParallelDSM(ParallelContext *pcxt);
extern void ReinitializeParallelWorkers(ParallelContext *pcxt, int nworkers_to_launch);
extern void LaunchParallelWorkers(ParallelContext *pcxt);
extern void WaitForParallelWorkersToAttach(ParallelContext *pcxt);
extern void WaitForParallelWorkersToFinish(ParallelContext *pcxt);
extern void DestroyParallelContext(ParallelContext *pcxt);
extern bool ParallelContextActive(void);

extern void HandleParallelMessageInterrupt(void);
extern void HandleParallelMessages(void);
extern void AtEOXact_Parallel(bool isCommit);
extern void AtEOSubXact_Parallel(bool isCommit, SubTransactionId mySubId);
extern void ParallelWorkerReportLastRecEnd(XLogRecPtr last_xlog_end);

extern void ParallelWorkerMain(Datum main_arg);

extern void InitGpParallelDSMHash(void);

extern GpParallelDSMEntry* GpInsertParallelDSMHash(PlanState *planstate);

extern Size GpParallelDSMHashSize(void);

extern bool EstimateGpParallelDSMEntrySize(PlanState *planstate, ParallelContext *pctx);

extern bool InitializeGpParallelDSMEntry(PlanState *node, ParallelContext *pctx);
extern bool InitializeGpParallelWorkers(PlanState *planstate, ParallelWorkerContext *pwcxt);

extern void GpDestroyParallelDSMEntry(void);

extern void AtEOXact_CBDB_Parallel(void);

extern void AtProcExit_CBDB_Parallel(int code, Datum arg);

#endif							/* PARALLEL_H */

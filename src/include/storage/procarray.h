/*-------------------------------------------------------------------------
 *
 * procarray.h
 *	  POSTGRES process array definitions.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/procarray.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PROCARRAY_H
#define PROCARRAY_H

#include "storage/lock.h"
#include "storage/standby.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

#include "cdb/cdbpublic.h"
#include "cdb/cdbtm.h"

struct DtxContextInfo;         /* cdb/cdbdtxcontextinfo.h */

extern Size ProcArrayShmemSize(void);
extern void CreateSharedProcArray(void);
extern void ProcArrayAdd(PGPROC *proc);
extern void ProcArrayRemove(PGPROC *proc, TransactionId latestXid);
extern void ProcArrayEndTransaction(PGPROC *proc, TransactionId latestXid);
extern void ProcArrayEndGxact(TMGXACT *gxact);
extern void ProcArrayClearTransaction(PGPROC *proc);

extern void ProcArrayInitRecovery(TransactionId initializedUptoXID);
extern void ProcArrayApplyRecoveryInfo(RunningTransactions running);
extern void ProcArrayApplyXidAssignment(TransactionId topxid,
										int nsubxids, TransactionId *subxids);

extern void RecordKnownAssignedTransactionIds(TransactionId xid);
extern void ExpireTreeKnownAssignedTransactionIds(TransactionId xid,
												  int nsubxids, TransactionId *subxids,
												  TransactionId max_xid);
extern void ExpireAllKnownAssignedTransactionIds(void);
extern void ExpireOldKnownAssignedTransactionIds(TransactionId xid);
extern void KnownAssignedTransactionIdsIdleMaintenance(void);

extern int	GetMaxSnapshotXidCount(void);
extern int	GetMaxSnapshotSubxidCount(void);

extern Snapshot GetSnapshotData(Snapshot snapshot, DtxContext distributedTransactionContext);

extern bool ProcArrayInstallImportedXmin(TransactionId xmin,
										 VirtualTransactionId *sourcevxid);
extern bool ProcArrayInstallRestoredXmin(TransactionId xmin, PGPROC *proc);

extern RunningTransactions GetRunningTransactionData(void);

extern bool TransactionIdIsInProgress(TransactionId xid);
extern bool TransactionIdIsActive(TransactionId xid);
extern TransactionId GetOldestNonRemovableTransactionId(Relation rel);
extern TransactionId GetLocalOldestNonRemovableTransactionId(Relation rel, bool updateGlobalVis);
extern TransactionId GetOldestTransactionIdConsideredRunning(void);
extern TransactionId GetLocalOldestTransactionIdConsideredRunning(void);
extern TransactionId GetOldestActiveTransactionId(void);
extern TransactionId GetOldestSafeDecodingTransactionId(bool catalogOnly);
extern void GetReplicationHorizons(TransactionId *xmin, TransactionId *catalog_xmin);

extern VirtualTransactionId *GetVirtualXIDsDelayingChkpt(int *nvxids, int type);
extern VirtualTransactionId *GetVirtualXIDsDelayingChkptEnd(int *nvxids);

extern bool HaveVirtualXIDsDelayingChkpt(VirtualTransactionId *vxids,
										 int nvxids);
extern bool HaveVirtualXIDsDelayingChkptEnd(VirtualTransactionId *vxids,
											int nvxids);

extern PGPROC *BackendPidGetProc(int pid);
extern PGPROC *BackendPidGetProcWithLock(int pid);
extern int	BackendXidGetPid(TransactionId xid);
extern bool IsBackendPid(int pid);

extern VirtualTransactionId *GetCurrentVirtualXIDs(TransactionId limitXmin,
												   bool excludeXmin0, bool allDbs, int excludeVacuum,
												   int *nvxids);
extern VirtualTransactionId *GetConflictingVirtualXIDs(TransactionId limitXmin, Oid dbOid);
extern pid_t CancelVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode);
extern pid_t SignalVirtualTransaction(VirtualTransactionId vxid, ProcSignalReason sigmode,
									  bool conflictPending);

extern bool MinimumActiveBackends(int min);
extern int	CountDBBackends(Oid databaseid);
extern int	CountDBConnections(Oid databaseid);
extern void CancelDBBackends(Oid databaseid, ProcSignalReason sigmode, bool conflictPending);
extern int	SignalMppBackends(int sig);
extern int	CountUserBackends(Oid roleid);
extern bool CountOtherDBBackends(Oid databaseId,
								 int *nbackends, int *nprepared);
extern void TerminateOtherDBBackends(Oid databaseId);

extern void XidCacheRemoveRunningXids(TransactionId xid,
									  int nxids, const TransactionId *xids,
									  TransactionId latestXid);
						  
extern volatile PGPROC *FindProcByGpSessionId(long gp_session_id);
extern void UpdateCommandIdInSnapshot(CommandId curcid);

extern void updateSharedLocalSnapshot(struct DtxContextInfo *dtxContextInfo,
									  DtxContext distributedTransactionContext,
									  Snapshot snapshot,
									  char *debugCaller);

extern void GetSlotTableDebugInfo(void **snapshotArray, int *maxSlots);

extern void getDtxCheckPointInfo(char **result, int *result_size);

extern List *ListAllGxid(void);
extern int GetPidByGxid(DistributedTransactionId gxid);
extern bool IsDtxInProgress(DistributedTransactionId gxid);

extern void ProcArraySetReplicationSlotXmin(TransactionId xmin,
											TransactionId catalog_xmin, bool already_locked);

extern void ProcArrayGetReplicationSlotXmin(TransactionId *xmin,
											TransactionId *catalog_xmin);
extern DistributedTransactionId LocalXidGetDistributedXid(TransactionId xid);
extern int GetSessionIdByPid(int pid);
extern bool ResGroupMoveSignalTarget(int sessionId, void *slot, Oid groupId,
								bool isExecutor);
extern void ResGroupMoveCheckTargetReady(int sessionId, bool *clean, bool *result);
extern void ResGroupMoveNotifyInitiator(pid_t callerPid);

typedef void (*BackendProcCallbackFunction) (volatile PGPROC *proc, void *args);
extern void LoopBackendProc(BackendProcCallbackFunction func, void *args);

#endif							/* PROCARRAY_H */

/*-------------------------------------------------------------------------
 *
 * storage.h
 *	  prototypes for functions in backend/catalog/storage.c
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/storage.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_H
#define STORAGE_H

#include "storage/block.h"
#include "storage/relfilelocator.h"
#include "storage/smgr.h"
#include "utils/relcache.h"

/* GUC variables */
extern PGDLLIMPORT int wal_skip_threshold;

/*
 * We keep a list of all relations (represented as RelFileNode values)
 * that have been created or deleted in the current transaction.  When
 * a relation is created, we create the physical file immediately, but
 * remember it so that we can delete the file again if the current
 * transaction is aborted.  Conversely, a deletion request is NOT
 * executed immediately, but is just entered in the list.  When and if
 * the transaction commits, we can delete the physical file.
 *
 * To handle subtransactions, every entry is marked with its transaction
 * nesting level.  At subtransaction commit, we reassign the subtransaction's
 * entries to the parent nesting level.  At subtransaction abort, we can
 * immediately execute the abort-time actions for all entries of the current
 * nesting level.
 *
 * NOTE: the list is kept in TopMemoryContext to be sure it won't disappear
 * unbetimes.  It'd probably be OK to keep it in TopTransactionContext,
 * but I'm being paranoid.
 */
struct PendingRelDeleteAction;
typedef struct PendingRelDelete
{
	struct PendingRelDeleteAction *action;	/* The action is to do pending
											 * delete */
	RelFileNodePendingDelete rlocator;	/* relation that may need to be
										 * deleted */
	BackendId	backend;                /* InvalidBackendId if not a temp rel */
	bool		atCommit;		/* T=delete at commit; F=delete at abort */
	int			nestLevel;		/* xact nesting level of request */

	struct PendingRelDelete *next;	/* linked-list link */
} PendingRelDelete;

/*
 * functions used to pending delete callbacks
 *
 * Notice that: no xlog generate in these interface.
 * also make sure that NO register same pending delete
 * into smgr
 */
struct PendingRelDeleteAction
{
	/* The flag to tell action support function in current pending delete */
	int			flags;

	/* Used to destroy pending delete item */
	void		(*destroy_pending_rel_delete) (PendingRelDelete *reldelete);

	/* do delete function */
	void		(*do_pending_rel_delete) (PendingRelDelete *reldelete);
};


/*
 * pending delete need delay delete when drop storage happend.
 * The pg pending delete item will insert the xlog and read the
 * xlog in `FinishPreparedTransaction` then do the `unlink`.
 * So after trascation prepared, all of the pending delete items
 * will be removed. But if current pending delete item have not
 * xlog, then should setting this flags, `do_pending_rel_delete`
 * will be called in trascation commit.
 */
#define PENDING_REL_DELETE_NEED_DROP_DELAY_DELETE (1)

/*
 * The flags in pending delete action
 * do NOT register XLOG/SYNC if current relation is not HEAP/AO/AOCS
 * after CBDB support custom WAL resouce manager, then different
 * pending delete item can define different WAL log. But for now,
 * CBDB only support pg pending item record WAL log.
 */

#define PENDING_REL_DELETE_NEED_PRESERVE (1 << 1)
#define PENDING_REL_DELETE_NEED_XLOG (1 << 2)
#define PENDING_REL_DELETE_NEED_SYNC (1 << 3)

/*
 * The default pending delete item won't write the xlog,
 * also need delay do pending delete when storage is dropped.
*/
#define PENDING_REL_DELETE_DEFAULT_FLAG PENDING_REL_DELETE_NEED_DROP_DELAY_DELETE

extern SMgrRelation RelationCreateStorage(RelFileLocator rlocator,
										  char relpersistence,
										  bool register_delete,
										  SMgrImpl smgr_which,
										  Relation rel);

extern void RelationDropStorage(Relation rel);
extern void RelationPreserveStorage(RelFileLocator rlocator, bool atCommit);
extern void RelationPreTruncate(Relation rel);
extern void RelationTruncate(Relation rel, BlockNumber nblocks);
extern void RelationCopyStorage(SMgrRelation src, SMgrRelation dst,
								ForkNumber forkNum, char relpersistence);
extern bool RelFileLocatorSkippingWAL(RelFileLocator rlocator);
extern Size EstimatePendingSyncsSpace(void);
extern void SerializePendingSyncs(Size maxSize, char *startAddress);
extern void RestorePendingSyncs(char *startAddress);

/* register a pending delete item into pending delete list */
void		RegisterPendingDelete(struct PendingRelDelete *pending);

/*
 * These functions used to be in storage/smgr/smgr.c, which explains the
 * naming
 */
extern void smgrDoPendingDeletes(bool isCommit);
extern void smgrDoPendingSyncs(bool isCommit, bool isParallelWorker);
extern int	smgrGetPendingDeletes(bool forCommit, RelFileNodePendingDelete **ptr);
extern void AtSubCommit_smgr(void);
extern void AtSubAbort_smgr(void);
extern void PostPrepare_smgr(void);

#endif							/* STORAGE_H */

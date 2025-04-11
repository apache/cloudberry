/*-------------------------------------------------------------------------
 *
 * storage_pending_deletes_redo.c
 *	  code to support processing of pending deletes (orphaned files) in WAL
 *
 * Copyright (c) 2025 Greengage Community
 *
 *	  src/backend/catalog/storage_pending_deletes_redo.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/clog.h"
#include "access/transam.h"
#include "catalog/storage_pending_deletes_redo.h"
#include "miscadmin.h"
#include "storage/md.h"
#include "utils/elog.h"
#include "utils/guc.h"
#include "utils/hsearch.h"

/*
 * HTAB entry for pending deletes for the given xid.
 */
typedef struct PendingDeleteHtabNode
{
	TransactionId xid;
	List	   *relnode_list;	/* list of RelFileNodePendingDelete */
}	PendingDeleteHtabNode;

/*
 * Hash table for pending deletes relfilenodes for a given xid.
 */
static HTAB *pendingDeletesRedo = NULL;

static bool
PdlTrackingDisabled()
{
	return IsBootstrapProcessingMode() || !gp_track_pending_delete;
}

/*
 * This function inserts XLOG_PENDING_DELETE record into WAL.
 */
void
PdlXLogInsert()
{
	if (PdlTrackingDisabled())
		return;

	PendingRelXactDeleteArray *arr = PdlXLogShmemDump();

	if (arr != NULL)
	{
		XLogRecPtr	rec;
		XLogRecData rdata =
		{
			.buffer = InvalidBuffer,
			.data = (char *) arr,
			.len = PdlDumpSize(arr->count),
			.next = NULL,
			.buffer_std = false
		};

		rec = XLogInsert(RM_XLOG_ID, XLOG_PENDING_DELETE, &rdata);

		XLogFlush(rec);

		elog(DEBUG1, "Pending delete XLog record inserted");

		pfree(arr);
	}
}

/*
 * This function adds pending delete node to a pendingDeletesRedo hash-table
 * during WAL redo processing.
 */
void
PdlRedoAdd(PendingRelXactDelete * pd)
{
	Assert(pd);

	if (PdlTrackingDisabled() || (pd->xid == InvalidTransactionId))
		return;

	if (NULL == pendingDeletesRedo)
	{
		HASHCTL		info =
		{
			.keysize = sizeof(TransactionId),
			.entrysize = sizeof(PendingDeleteHtabNode)
		};

		pendingDeletesRedo = hash_create("pendingDeletesRedo hash",
										 32,
										 &info,
										 HASH_ELEM);
	}

	bool		found = false;

	PendingDeleteHtabNode *entry = (PendingDeleteHtabNode *)
		hash_search(pendingDeletesRedo, &pd->xid, HASH_ENTER, &found);

	if (!found)
	{
		entry->xid = pd->xid;
		entry->relnode_list = NIL;
	}

	RelFileNodePendingDelete *data = (RelFileNodePendingDelete *)
		palloc(sizeof(*data));

	*data = pd->relnode;
	entry->relnode_list = lappend(entry->relnode_list, data);
}

/*
 * This function replays XLOG_PENDING_DELETE xlog record.
 */
void
PdlRedoXLogRecord(XLogRecord *record)
{
	Assert(record);

	if (PdlTrackingDisabled())
		return;

	PendingRelXactDeleteArray *arr = (PendingRelXactDeleteArray *)
		XLogRecGetData(record);

	TransactionId oldest_xid = ShmemVariableCache->oldestXid;

	Assert(arr->count);

	for (int i = 0; i < arr->count; i++)
	{
		PendingRelXactDelete *pd = &(arr->array[i]);

		/*
		 * This function should check transaction status before adding
		 * relfilenode to a pendingDeletesRedo hash table. Concurrent xlog
		 * inserts (concurrent to a checkpointing process) of commit or abort
		 * xlog records may out out-date pending deletes list. We don't want
		 * to use aggressive locking of shared structures in order to avoid
		 * performance drawbacks of concurrent commits or aborts. So the
		 * strategy is to double-check relfilenodes with it's transaction
		 * status. If it's TRANSACTION_STATUS_IN_PROGRESS, then it's
		 * permitted to delete files (it's orphaned), if it's in some other
		 * status - don't touch it. Also we should check transaction xid
		 * doesn't cross "freeze horizon" and compare it with current
		 * oldestXid value. Motivation of this check is that clog might get
		 * truncated after REDO point and before replaying XLOG_PENDING_DELETE
		 * record (though that looks like unlikely will happen in real-world,
		 * but still needs to be considered as possible scenario). So in that
		 * case we can't rely on xid status of that frozen transactions.
		 * Second point is that there is no way that clog would be truncated
		 * when transaction is in progress, so it's either been committed or
		 * aborted before that.
		 */

		if (TransactionIdPrecedes(pd->xid, oldest_xid))
			ereport(LOG, (errmsg(
					"Prevented adding node for XLOG_PENDING_DELETE "
					"record for xid: %u, oldestXid: %u",
					pd->xid, oldest_xid)));
		else
		{
			XLogRecPtr	result;
			XidStatus	status = TransactionIdGetStatus(pd->xid, &result);

			if (status == TRANSACTION_STATUS_IN_PROGRESS)
				PdlRedoAdd(pd);
			else
				ereport(LOG, (errmsg(
						"Prevented adding node for XLOG_PENDING_DELETE "
						"record for xid: %u, status: %d",
						pd->xid, status)));
		}
	}
}

static void
PdlRedoRemove(TransactionId xid)
{
	if ((xid == InvalidTransactionId) ||
		(NULL == pendingDeletesRedo))
		return;

	PendingDeleteHtabNode *entry = (PendingDeleteHtabNode *)
		hash_search(pendingDeletesRedo, &xid, HASH_REMOVE, NULL);

	if (entry)
		list_free_deep(entry->relnode_list);
}

/*
 * This function removes pending delete nodes from redo hash-table
 * (pendingDeleteRedo) for a given transaction identified by it's xid and
 * sub-transactions (if there are).
 */
void
PdlRedoRemoveTree(TransactionId xid,
				  TransactionId *sub_xids, int nsubxacts)
{
	if (PdlTrackingDisabled())
		return;

	for (int i = 0; i < nsubxacts; i++)
		PdlRedoRemove(sub_xids[i]);

	PdlRedoRemove(xid);
}

/*
 * This function serializes the contents of hash table entry into a structure
 * suitable to pass into DropRelationFiles() functions.
 */
static RelFileNodePendingDelete *
PdlRedoPrepareArrayForDrop(PendingDeleteHtabNode *hnode, int *ndelrels)
{
	ListCell   *cell;

	foreach(cell, hnode->relnode_list)
	{
		RelFileNodePendingDelete *pending_delete_node =
			(RelFileNodePendingDelete *) lfirst(cell);
		ListCell   *i_cell = lnext(cell);
		ListCell   *i_cell_prev = cell;

		while (i_cell)
		{
			ListCell   *i_cell_next = lnext(i_cell);
			RelFileNodePendingDelete *i_relnode =
				(RelFileNodePendingDelete *) lfirst(i_cell);

			if (RelFileNodeEquals(pending_delete_node->node, i_relnode->node))
			{
				elog(DEBUG1,
					 "Duplicate pending delete node found: "
					 "(rel: (%u: %u: %u); xid: %u)",
					 pending_delete_node->node.spcNode,
					 pending_delete_node->node.dbNode,
					 pending_delete_node->node.relNode,
					 hnode->xid);

				hnode->relnode_list =
					list_delete_cell(hnode->relnode_list, i_cell, i_cell_prev);
				pfree(i_relnode);
			}
			else
				i_cell_prev = i_cell;

			i_cell = i_cell_next;
		}
	}

	*ndelrels = list_length(hnode->relnode_list);

	if (*ndelrels <= 0)
	{
		ereport(WARNING, (errmsg("Empty list for xid: %u", hnode->xid)));
		return NULL;
	}

	RelFileNodePendingDelete *delrels = (RelFileNodePendingDelete *)
		palloc((*ndelrels) * sizeof(*delrels));

	int			i = 0;

	foreach_with_count(cell, hnode->relnode_list, i)
	{
		RelFileNodePendingDelete *pending_delete_node =
			(RelFileNodePendingDelete *) lfirst(cell);

		ereport(LOG, (errmsg(
				"Prepare to drop node (%u: %u: %u) for xid: %u",
				pending_delete_node->node.spcNode,
				pending_delete_node->node.dbNode,
				pending_delete_node->node.relNode,
				hnode->xid)));

		delrels[i] = *pending_delete_node;
	}

	return delrels;
}

/*
 * This function deletes files for pending delete nodes.
 */
void
PdlRedoDropFiles()
{
	if (PdlTrackingDisabled() ||
		(NULL == pendingDeletesRedo) ||
		(hash_get_num_entries(pendingDeletesRedo) == 0))
		return;

	TransactionId oldest_xid = ShmemVariableCache->oldestXid;
	HASH_SEQ_STATUS scan_status = {0};
	PendingDeleteHtabNode *node;

	hash_seq_init(&scan_status, pendingDeletesRedo);
	while ((node = (PendingDeleteHtabNode *) hash_seq_search(&scan_status)) != NULL)
	{
		if (TransactionIdPrecedes(node->xid, oldest_xid))
			ereport(WARNING, (errmsg(
					"Prevented drop files for xid: %u, oldestXid: %u",
					node->xid, oldest_xid)));
		else
		{
			XLogRecPtr	result;
			XidStatus	status = TransactionIdGetStatus(node->xid, &result);

			if (status != TRANSACTION_STATUS_IN_PROGRESS)
				ereport(WARNING, (errmsg(
						"Prevented drop files for xid: %u, status: %d",
						node->xid, status)));
			else
			{
				int			ndelrels = 0;
				RelFileNodePendingDelete *delrels =
					PdlRedoPrepareArrayForDrop(node, &ndelrels);

				DropRelationFiles(delrels, ndelrels, true);

				ereport(LOG, (errmsg(
						"Pending delete rels were dropped (count: %d; xid: %d).",
						ndelrels,
						node->xid)));

				pfree(delrels);
			}
		}

		list_free_deep(node->relnode_list);
	}

	hash_destroy(pendingDeletesRedo);
	pendingDeletesRedo = NULL;
}

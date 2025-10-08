/*-------------------------------------------------------------------------
 *
 * storage_pending_deletes_redo.h
 *	  prototypes for functions in backend/catalog/storage_pending_deletes_redo.c
 *
 * Copyright (c) 2025 Greengage Community
 *
 * src/include/catalog/storage_pending_deletes_redo.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_PENDING_DELETES_REDO_H
#define STORAGE_PENDING_DELETES_REDO_H

#include "postgres.h"

#include "access/xlog.h"
#include "catalog/storage_pending_deletes.h"

extern void PdlXLogInsert(void);

extern void PdlRedoAdd(PendingRelXactDelete * pd);

extern void PdlRedoXLogRecord(XLogRecord *record);

extern void PdlRedoRemoveTree(TransactionId xid,
				  TransactionId *sub_xids, int nsubxacts);

extern void PdlRedoDropFiles(void);

#endif   /* STORAGE_PENDING_DELETES_REDO_H */

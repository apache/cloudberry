/*-------------------------------------------------------------------------
 *
 * storage_pending_deletes.h
 *	  prototypes for functions in backend/catalog/storage_pending_deletes.c
 *
 * Copyright (c) 2025 Greengage Community
 *
 * src/include/catalog/storage_pending_deletes.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef STORAGE_PENDING_DELETES_H
#define STORAGE_PENDING_DELETES_H

#include "postgres.h"

#include "storage/relfilenode.h"
#include "utils/dsa.h"

/* Pending delete node linked to xact which created it */
typedef struct PendingRelXactDelete
{
	RelFileNodePendingDelete relnode;
	TransactionId xid;
}	PendingRelXactDelete;

typedef struct PendingRelXactDeleteArray
{
	Size		count;
	PendingRelXactDelete array[FLEXIBLE_ARRAY_MEMBER];
}	PendingRelXactDeleteArray;

static inline Size
PdlDumpSize(Size count)
{
	Size array_size = sizeof(PendingRelXactDelete) * count;

	return offsetof(PendingRelXactDeleteArray, array) + array_size;
}

extern Size PdlShmemSize(void);
extern void PdlShmemInit(void);
extern dsa_pointer PdlShmemAdd(const RelFileNodePendingDelete * relnode,
			TransactionId xid);
extern void PdlShmemRemove(dsa_pointer node_ptr);
extern PendingRelXactDeleteArray *PdlXLogShmemDump(void);

#endif   /* STORAGE_PENDING_DELETES_H */

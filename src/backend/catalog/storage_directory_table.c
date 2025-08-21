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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Storage manipulation for directory table.
 *
 *	    src/backend/catalog/storage_directory_table.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/parallel.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/pg_directory_table.h"
#include "catalog/pg_tablespace.h"
#include "catalog/storage.h"
#include "catalog/storage_directory_table.h"
#include "storage/smgr.h"
#include "storage/ufile.h"
#include "utils/acl.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "cdb/cdbvars.h"

bool allow_dml_directory_table = false;

/*
 * TODO: support ufile pending delete xlog
 *
 * Ufile do not support deleteing files during WAL redo, two of reason:
 *
 * 1. deleting files requires a connection to object storage system.
 * In order to establish the connection to the object storage, we
 * need to access the catalog table to retrieve the connection
 * configuration info, which is impossible during WAL redo.
 *
 * 2. no custom xlog entry support.
 * Custom WAL Resource Managers are immature and not reflected in CBDB.
 *
 */
typedef struct UFileNodePendingDelete
{
	char		relkind;
	Oid			spcId;			/* directory table needs an extra tabpespace */
	char	   *relativePath;
}			UFileNodePendingDelete;

typedef struct PendingRelDeleteUFile
{
	PendingRelDelete reldelete; /* base pending delete */
	UFileNodePendingDelete filenode;	/* relation that may need to be
										 * deleted */
}			PendingRelDeleteUFile;


static void
UfileDestroyPendingRelDelete(PendingRelDelete *reldelete)
{
	PendingRelDeleteUFile *ufiledelete;

	Assert(reldelete);
	ufiledelete = (PendingRelDeleteUFile *) reldelete;

	pfree(ufiledelete->filenode.relativePath);
	pfree(ufiledelete);
}

static void
UfileDoPendingRelDelete(PendingRelDelete *reldelete)
{
	PendingRelDeleteUFile *ufiledelete;

	Assert(reldelete);
	ufiledelete = (PendingRelDeleteUFile *) reldelete;

	UFileUnlink(ufiledelete->filenode.spcId, ufiledelete->filenode.relativePath);
}

struct PendingRelDeleteAction ufile_pending_rel_deletes_action = {
	.flags = PENDING_REL_DELETE_DEFAULT_FLAG,
	.destroy_pending_rel_delete = UfileDestroyPendingRelDelete,
	.do_pending_rel_delete = UfileDoPendingRelDelete
};

void
DirectoryTableDropStorage(Oid relid)
{
	Relation rel;
	char	   *filePath;
	DirectoryTable *dirTable;
	TableScanDesc scandesc;
	Relation	spcrel;
	HeapTuple	tuple;
	Form_pg_tablespace spcform;
	ScanKeyData entry[1];
	Oid			tablespaceoid;
	char	   *tablespace_name;

	rel = relation_open(relid, AccessExclusiveLock); 
	dirTable = GetDirectoryTable(RelationGetRelid(rel));

	/*
	 * Find the tablespace by spaceId
	 */
	spcrel = table_open(TableSpaceRelationId, RowExclusiveLock);

	ScanKeyInit(&entry[0],
				Anum_pg_tablespace_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(dirTable->spcId));
	scandesc = table_beginscan_catalog(spcrel, 1, entry);
	tuple = heap_getnext(scandesc, ForwardScanDirection);

	if (!HeapTupleIsValid(tuple))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("tablespace \"%d\" does not exist",
						dirTable->spcId)));
	}

	spcform = (Form_pg_tablespace) GETSTRUCT(tuple);
	tablespaceoid = spcform->oid;
	tablespace_name = pstrdup(NameStr(((Form_pg_tablespace) GETSTRUCT(tuple))->spcname));

	table_endscan(scandesc);
	table_close(spcrel, RowExclusiveLock);

	filePath = psprintf("%s", dirTable->location);

	UFileAddPendingDelete(rel, dirTable->spcId, filePath, true);

	pfree(filePath);

	relation_close(rel, NoLock);
}

void
UFileAddPendingDelete(Relation rel, Oid spcId, char *relativePath, bool atCommit)
{
	PendingRelDeleteUFile *pending;

	/* Add the relation to the list of stuff to delete at abort */
	pending = (PendingRelDeleteUFile *)
		MemoryContextAlloc(TopMemoryContext, sizeof(PendingRelDeleteUFile));
	pending->filenode.relkind = rel->rd_rel->relkind;
	pending->filenode.relativePath = MemoryContextStrdup(TopMemoryContext, relativePath);
	pending->filenode.spcId = spcId;

	pending->reldelete.atCommit = atCommit; /* delete if abort */
	pending->reldelete.nestLevel = GetCurrentTransactionNestLevel();

	pending->reldelete.rlocator.node = rel->rd_locator;
	pending->reldelete.rlocator.isTempRelation = rel->rd_backend == TempRelBackendId;
	pending->reldelete.rlocator.smgr_which = SMGR_INVALID;

	pending->reldelete.action = &ufile_pending_rel_deletes_action;
	RegisterPendingDelete(&pending->reldelete);

	/*
	 * Make sure the spccache to the corresponding tablespace has been cached.
	 */
	forceCacheUFileResource(spcId);
}

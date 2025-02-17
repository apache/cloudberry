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
 * pg_directory_table.c
 *		  support for directory table.
 *
 * IDENTIFICATION
 *		  src/backend/catalog/pg_directory_table.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_opclass.h"
#include "cdb/cdbhash.h"
#include "cdb/cdbutil.h"
#include "parser/parser.h"
#include "parser/parse_func.h"
#include "catalog/pg_directory_table.h"
#include "catalog/gp_distribution_policy.h"
#include "catalog/pg_tablespace.h"
#include "storage/lmgr.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

typedef FileAm* (*File_handler) (void);
/* Hash table for tablespace file handler */
static HTAB *TableSpaceFileHandlerHash = NULL;

typedef struct TableSpaceFileAmEntry
{
	Oid 	spcId;	/* tablespace oid */
	FileAm  *fileAm; /* tablespace file am */
} TableSpaceFileAmEntry;

static void
InvalidateTableSpaceFileAmCallBack(Datum arg, int cacheid, uint32 hashvalue)
{
	HASH_SEQ_STATUS	status;
	TableSpaceFileAmEntry	*fileAmEntry;

	hash_seq_init(&status, TableSpaceFileHandlerHash);
	while ((fileAmEntry = (TableSpaceFileAmEntry *) hash_seq_search(&status)) != NULL)
	{
		hash_search(TableSpaceFileHandlerHash,
					(void *) &fileAmEntry->spcId,
					HASH_REMOVE,
					NULL);
	}
}

static void
InitializeTableSpaceFileHandlerHash(void)
{
	HASHCTL		ctl;

	/* Initialize the hash table. */
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(TableSpaceFileAmEntry);
	TableSpaceFileHandlerHash =
		hash_create("TableSpace File Handler hash", 8, &ctl,
			  		HASH_ELEM | HASH_BLOBS);

	/* Make sure we've initialized CacheMemoryContext. */
	if (!CacheMemoryContext)
		CreateCacheMemoryContext();

	/* Watch for invalidation events. */
	CacheRegisterSyscacheCallback(TABLESPACEOID,
								  InvalidateTableSpaceFileAmCallBack,
								  (Datum) 0);
}

FileAm *
GetTablespaceFileHandler(Oid spcId)
{
	HeapTuple tuple;
	Datum datum;
	bool isNull;
	char *filehandlersrc;
	char *filehandlerbin;
	Form_pg_tablespace tblspcForm;
	void *libraryhandle;
	File_handler file_handler;
	TableSpaceFileAmEntry *fileAmEntry;
	FileAm *fileAm = NULL;
	bool found;

	if (!TableSpaceFileHandlerHash)
		InitializeTableSpaceFileHandlerHash();

	fileAmEntry = (TableSpaceFileAmEntry *) hash_search(TableSpaceFileHandlerHash,
													   (void *) &spcId,
													   HASH_FIND,
													   &found);

	if (found)
		return fileAmEntry->fileAm;

	tuple = SearchSysCache1(TABLESPACEOID, ObjectIdGetDatum(spcId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for table space %u", spcId);

	tblspcForm = (Form_pg_tablespace) GETSTRUCT(tuple);
	datum = SysCacheGetAttr(TABLESPACEOID,
							tuple,
							Anum_pg_tablespace_spcfilehandlerbin,
							&isNull);
	if (!isNull)
	{
		filehandlerbin = TextDatumGetCString(datum);

		datum = SysCacheGetAttr(TABLESPACEOID,
						  		tuple,
						  		Anum_pg_tablespace_spcfilehandlersrc,
						  		&isNull);
		filehandlersrc = TextDatumGetCString(datum);

		file_handler = (File_handler) load_external_function(filehandlerbin, filehandlersrc, true, &libraryhandle);

		if (file_handler)
			fileAm = (*file_handler) ();

		if (fileAm == NULL || fileAm == &localFileAm)
			elog(ERROR, "tablespace file handler did not return a FileAm struct");
	}
	else
	{
		fileAm = &localFileAm;
	}

	ReleaseSysCache(tuple);

	Assert(fileAm != NULL);
	Assert(fileAm->open != NULL);
	Assert(fileAm->close != NULL);
	Assert(fileAm->read != NULL);
	Assert(fileAm->write != NULL);
	Assert(fileAm->size != NULL);
	Assert(fileAm->unlink != NULL);
	Assert(fileAm->formatPathName != NULL);
	Assert(fileAm->exists != NULL);
	Assert(fileAm->name != NULL);
	Assert(fileAm->getLastError != NULL);

	fileAmEntry = (TableSpaceFileAmEntry *) hash_search(TableSpaceFileHandlerHash,
													   (void *) &spcId,
													   HASH_ENTER,
													   &found);
	if (found)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("extra tablespace oid \"%u\" already exists", spcId)));

	fileAmEntry->fileAm = fileAm;

	return fileAm;
}

/*
 * GetDirectoryTable - look up the directory table definition by relId.
 */
DirectoryTable *
GetDirectoryTable(Oid relId)
{
	Form_pg_directory_table dirtableForm;
	HeapTuple tuple;
	Datum datum;
	bool isNull;
	DirectoryTable *dirTable;

	tuple = SearchSysCache1(DIRECTORYTABLEREL, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for directory table %u", relId);

	dirtableForm = (Form_pg_directory_table) GETSTRUCT(tuple);

	dirTable = (DirectoryTable *) palloc(sizeof(DirectoryTable));
	dirTable->relId = relId;
	dirTable->spcId = dirtableForm->dttablespace;
	GetTablespaceFileHandler(dirtableForm->dttablespace);

	datum = SysCacheGetAttr(DIRECTORYTABLEREL,
							tuple,
							Anum_pg_directory_table_dtlocation,
							&isNull);
	Assert(!isNull);

	dirTable->location = TextDatumGetCString(datum);
	ReleaseSysCache(tuple);

	return dirTable;
}

bool
RelationIsDirectoryTable(Oid relId)
{
	HeapTuple tuple;

	tuple = SearchSysCache1(DIRECTORYTABLEREL, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))
		return false;

	ReleaseSysCache(tuple);
	return true;
}

List *
GetDirectoryTableSchema(void)
{
	List *result = NIL;

	ColumnDef *columnDef = makeNode(ColumnDef);
	columnDef->colname = "relative_path";
	columnDef->typeName = SystemTypeName("text");
	columnDef->is_local = true;
	result = lappend(result, columnDef);

	columnDef = makeNode(ColumnDef);
	columnDef->colname = "size";
	columnDef->typeName = SystemTypeName("int8");
	columnDef->is_local = true;
	result = lappend(result, columnDef);

	columnDef = makeNode(ColumnDef);
	columnDef->colname = "last_modified";
	columnDef->typeName = SystemTypeName("timestamptz");
	columnDef->is_local =true;
	result = lappend(result, columnDef);

	columnDef = makeNode(ColumnDef);
	columnDef->colname = "md5";
	columnDef->typeName = SystemTypeName("text");
	columnDef->is_local = true;
	result = lappend(result, columnDef);

	columnDef = makeNode(ColumnDef);
	columnDef->colname = "tag";
	columnDef->typeName = SystemTypeName("text");
	columnDef->is_local = true;
	result = lappend(result, columnDef);

	return result;
}

DistributedBy *
GetDirectoryTableDistributedBy(void)
{
	Oid			opclassoid = InvalidOid;
	HeapTuple	ht_opc;
	Form_pg_opclass opcrec;
	char	   *opcname;
	char	   *nspname;

	DistributedBy *distributedBy = makeNode(DistributedBy);
	distributedBy->ptype = POLICYTYPE_PARTITIONED;
	distributedBy->numsegments = -1;
	DistributionKeyElem *elem = makeNode(DistributionKeyElem);
	elem->name = "relative_path";
	if (gp_use_legacy_hashops)
		opclassoid = get_legacy_cdbhash_opclass_for_base_type(TEXTOID);

	if (!OidIsValid(opclassoid))
		opclassoid = cdb_default_distribution_opclass_for_type(TEXTOID);

	ht_opc = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclassoid));
	if (!HeapTupleIsValid(ht_opc))
		elog(ERROR, "cache lookup failed for opclass %u", opclassoid);
	opcrec = (Form_pg_opclass) GETSTRUCT(ht_opc);
	nspname = get_namespace_name(opcrec->opcnamespace);
	opcname = pstrdup(NameStr(opcrec->opcname));
	elem->opclass = list_make2(makeString(nspname), makeString(opcname));

	elem->location = -1;
	distributedBy->keyCols = lappend(distributedBy->keyCols, elem);
	distributedBy->numsegments = GP_POLICY_DEFAULT_NUMSEGMENTS();

	ReleaseSysCache(ht_opc);

	return distributedBy;
}

Oid
CreateDirectoryTableIndex(Relation rel)
{
	char		dirtable_idxname[NAMEDATALEN];
	Oid 		dirtable_idxid;
	Oid		    collationObjectId;
	IndexInfo	*indexInfo;
	List		*indexColNames;
	Oid			classObjectId[1];
	int16		coloptions[1];
	HeapTuple	tuple;
	Datum		collationDatum;
	bool		isNull;

	snprintf(dirtable_idxname, sizeof(dirtable_idxname),
			 "%s_pkey", RelationGetRelationName(rel));

	classObjectId[0] = TEXT_BTREE_OPS_OID;

	coloptions[0] = 0;

	indexInfo = makeNode(IndexInfo);
	indexInfo->ii_NumIndexAttrs = 1;
	indexInfo->ii_NumIndexKeyAttrs = 1;
	indexInfo->ii_IndexAttrNumbers[0] = 1;
	indexInfo->ii_Expressions = NIL;
	indexInfo->ii_ExpressionsState = NIL;
	indexInfo->ii_Predicate = NIL;
	indexInfo->ii_PredicateState = NULL;
	indexInfo->ii_Unique = true;
	indexInfo->ii_Concurrent = false;
	indexColNames = list_make1("relative_path");

	tuple = SearchSysCache2(ATTNUM,
							ObjectIdGetDatum(RelationGetRelid(rel)),
							Int16GetDatum(1));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for directory_table %u",
			 RelationGetRelid(rel));
	collationDatum = SysCacheGetAttr(ATTNUM, tuple, Anum_pg_attribute_attcollation,
								  	 &isNull);
	collationObjectId = DatumGetObjectId(collationDatum);

	dirtable_idxid = index_create(rel,
								  dirtable_idxname,
								  InvalidOid,
								  InvalidOid,
								  InvalidOid,
								  InvalidOid,
								  indexInfo,
								  indexColNames,
								  BTREE_AM_OID,
								  InvalidOid,
								  &collationObjectId, classObjectId, coloptions, (Datum) 0,
								  INDEX_CREATE_IS_PRIMARY, 0, true, true, NULL);

	ReleaseSysCache(tuple);

	/* Make this table visible, else index creation will fail */
	CommandCounterIncrement();

	/* Unlock the index -- no one can see it anyway */
	UnlockRelationOid(dirtable_idxid, AccessExclusiveLock);

	return dirtable_idxid;
}

void
RemoveDirectoryTableEntry(Oid relId)
{
	Relation	rel;
	HeapTuple 	tuple;

	rel = table_open(DirectoryTableRelationId, RowExclusiveLock);

	tuple = SearchSysCache1(DIRECTORYTABLEREL, ObjectIdGetDatum(relId));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for directory table %u", relId);

	CatalogTupleDelete(rel, &tuple->t_self);

	ReleaseSysCache(tuple);
	table_close(rel, RowExclusiveLock);
}

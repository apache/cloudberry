/*-------------------------------------------------------------------------
 *
 * directorycmds.c
 *	  directory table creation/manipulation commands
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/directorycmds.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_directory_table.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_tablespace.h"
#include "catalog/pg_type.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/storage_directory_table.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdboidsync.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "commands/dirtablecmds.h"
#include "commands/tablespace.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "storage/ufile.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "libpq-fe.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "funcapi.h"

typedef struct TableFunctionContext
{
	Relation 		relation;
	TableScanDesc	scanDesc;
	TupleTableSlot	*slot;
	DirectoryTable 	*dirTable;
} TableFunctionContext;

Datum directory_table(PG_FUNCTION_ARGS);

static Oid
chooseTableSpace(CreateDirectoryTableStmt *stmt)
{
	Oid tablespaceId = InvalidOid;

	/*
	 * Select tablespace to use: an explicitly indicated one, or (in the case
	 * of a partitioned table) the parent's, if it has one.
	 */
	if (stmt->tablespacename)
	{
		/*
		 * Tablespace specified on the command line, or was passed down by
		 * dispatch.
		 */
		tablespaceId = get_tablespace_oid(stmt->tablespacename, false);
	}

	/* still nothing? use the default */
	if (!OidIsValid(tablespaceId))
		tablespaceId = GetDefaultTablespace(stmt->base.relation->relpersistence, false);

	if (!OidIsValid(tablespaceId))
		tablespaceId = tablespaceId ? tablespaceId : MyDatabaseTableSpace;

	/* Check permissions except when using database's default */
	if (OidIsValid(tablespaceId) && tablespaceId != MyDatabaseTableSpace)
	{
		AclResult	aclresult;

		aclresult = object_aclcheck(TableSpaceRelationId, tablespaceId, GetUserId(),
									ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_TABLESPACE,
				  			get_tablespace_name(tablespaceId));
	}

	/* In all cases disallow placing user relations in pg_global */
	if (tablespaceId == GLOBALTABLESPACE_OID)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("only shared relations can be placed in pg_global tablespace")));

	return tablespaceId;
}

void
CreateDirectoryTable(CreateDirectoryTableStmt *stmt, Oid relId)
{
	Relation 	dirRelation;
	Datum 		values[Natts_pg_directory_table];
	bool 		nulls[Natts_pg_directory_table];
	HeapTuple	tuple;
	char 		*dirTablePath;
	Oid 		spcId = chooseTableSpace(stmt);

	if (stmt->location)
	{
		if (spcId == InvalidOid ||
			spcId == DEFAULTTABLESPACE_OID)
			dirTablePath = psprintf("base/%s", stmt->location);
		else if (spcId == GLOBALTABLESPACE_OID)
			dirTablePath = psprintf("global/%s", stmt->location);
		else
			dirTablePath = psprintf("pg_tblspc/%s", stmt->location);
	}
	else
	{
		Form_pg_class pg_class_tuple = NULL;
		HeapTuple	class_tuple = NULL;
		RelFileLocator relFileNode = {0};

		class_tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relId));
		if (!HeapTupleIsValid(class_tuple))
			elog(ERROR, "cache lookup failed for relation %u", relId);
		pg_class_tuple = (Form_pg_class) GETSTRUCT(class_tuple);

		relFileNode.spcOid = spcId;
		relFileNode.dbOid = MyDatabaseId;
		relFileNode.relNumber = pg_class_tuple->relfilenode;

		dirTablePath = UFileFormatPathName(&relFileNode);
		ReleaseSysCache(class_tuple);
	}

	/*
	 * We will check whether the directory path has existed only when use the local file am.
	 */
	if (UFileExists(spcId, dirTablePath) && GetTablespaceFileHandler(spcId) == &localFileAm)
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("directory table path \"%s\" already exists",
				 dirTablePath)));

	/*
	 * Acquire DirectoryTableLock to ensure that no DROP DIRECTORY TABLE
	 * or CREATE DIRECTORY TABLE is running concurrently.
	 */
	LWLockAcquire(DirectoryTableLock, LW_EXCLUSIVE);

	UFileEnsurePath(spcId, dirTablePath);

	/*
	 * Advance command counter to ensure the pg_attribute tuple is visible;
	 * the tuple might be updated to add constraints in previous step.
	 */
	CommandCounterIncrement();

	dirRelation = table_open(DirectoryTableRelationId, RowExclusiveLock);
	/*
	 * Insert tuple into pg_directory_table.
	 */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	values[Anum_pg_directory_table_dttablespace - 1] = spcId;
	values[Anum_pg_directory_table_dtrelid - 1] = ObjectIdGetDatum(relId);
	values[Anum_pg_directory_table_dtlocation - 1] = CStringGetTextDatum(dirTablePath);

	tuple = heap_form_tuple(dirRelation->rd_att, values, nulls);

	CatalogTupleInsert(dirRelation, tuple);

	/* Add dependency with tablespace */
	recordDependencyOnTablespace(RelationRelationId, relId, spcId);

	heap_freetuple(tuple);

	table_close(dirRelation, RowExclusiveLock);

	LWLockRelease(DirectoryTableLock);
}

static Datum
getFileContent(Oid spcId, char *scopedFileUrl)
{
	char 	errorMessage[256];
	char 	buffer[4096];
	char 	*data;
	int 	curPos = 0;
	int		bytesRead;
	bytea 	*content;
	UFile	*file;
	int64_t 	fileSize;

	file = UFileOpen(spcId,
					 scopedFileUrl,
				  	 O_RDONLY,
				  	 errorMessage,
				  	 sizeof(errorMessage));

	if (file == NULL)
		ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to open file \"%s\": %s,", scopedFileUrl, errorMessage)));

	fileSize = UFileSize(file);
	if (fileSize > MaxAllocSize - VARHDRSZ)
		ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory")));

	content = (bytea *) palloc(fileSize + VARHDRSZ);
	if (content == NULL)
		ereport(ERROR,
					(errcode(ERRCODE_OUT_OF_MEMORY),
					 errmsg("out of memory"),
					 errdetail("Failed to allocate %ld bytes memory.", fileSize)));
	SET_VARSIZE(content, fileSize + VARHDRSZ);
	data = VARDATA(content);

	while (true)
	{
		bytesRead = UFileRead(file, buffer, sizeof(buffer));
		if (bytesRead == -1)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to read file \"%s\": %s", scopedFileUrl, UFileGetLastError(file))));

		if (bytesRead == 0)
			break;

		memcpy(data + curPos, buffer, bytesRead);
		curPos += bytesRead;
	}

	int ret = UFileClose(file);
	if (ret < 0)
		ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("failed to close the file \"%s\": %s", scopedFileUrl, UFileGetLastError(file))));

	pfree(file);

	PG_RETURN_BYTEA_P(content);
}

static char *
getScopedFileUrl(DirectoryTable *dirTable, char *relativePath)
{
	return psprintf("%s/%s", dirTable->location, relativePath);
}

Datum
directory_table(PG_FUNCTION_ARGS)
{
#define DIRECTORY_TABLE_FUNCTION_COLUMNS	7
	Oid			relId = PG_GETARG_OID(0);
	Datum 		values[DIRECTORY_TABLE_FUNCTION_COLUMNS];
	bool 		nulls[DIRECTORY_TABLE_FUNCTION_COLUMNS];
	HeapTuple 	tuple;
	Datum 		result;
	FuncCallContext *funcCtx;
	TableFunctionContext *context;

	if (SRF_IS_FIRSTCALL())
	{
		Snapshot 		snapshot;
		TupleDesc 		newTupDesc;
		MemoryContext	oldContext;

		funcCtx = SRF_FIRSTCALL_INIT();

		oldContext = MemoryContextSwitchTo(funcCtx->multi_call_memory_ctx);

		newTupDesc = CreateTemplateTupleDesc(DIRECTORY_TABLE_FUNCTION_COLUMNS);
		TupleDescInitEntry(newTupDesc, (AttrNumber) 1, "scoped_file_url", TEXTOID, -1, 0);
		TupleDescInitEntry(newTupDesc, (AttrNumber) 2, "relative_path", TEXTOID, -1, 0);
		TupleDescInitEntry(newTupDesc, (AttrNumber) 3, "tag", TEXTOID, -1, 0);
		TupleDescInitEntry(newTupDesc, (AttrNumber) 4, "size", INT8OID, -1, 0);
		TupleDescInitEntry(newTupDesc, (AttrNumber) 5, "last_modified", TIMESTAMPTZOID, -1, 0);
		TupleDescInitEntry(newTupDesc, (AttrNumber) 6, "md5", TEXTOID, -1, 0);
		TupleDescInitEntry(newTupDesc, (AttrNumber) 7, "content", BYTEAOID, -1, 0);

		funcCtx->tuple_desc = BlessTupleDesc(newTupDesc);

		context = (TableFunctionContext *) palloc0(sizeof(TableFunctionContext));
		context->relation = table_open(relId, AccessShareLock);

		SIMPLE_FAULT_INJECTOR("directory_table_inject");

		if (!RelationIsDirectoryTable(relId))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("'%s' is not a directory table",
							RelationGetRelationName(context->relation))));

		context->slot = MakeSingleTupleTableSlot(RelationGetDescr(context->relation),
										   		 table_slot_callbacks(context->relation));
		context->dirTable = GetDirectoryTable(RelationGetRelid(context->relation));

		snapshot = GetLatestSnapshot();
		context->scanDesc = table_beginscan(context->relation, snapshot, 0, NULL);

		funcCtx->user_fctx = (void *) context;

		MemoryContextSwitchTo(oldContext);
	}

	funcCtx = SRF_PERCALL_SETUP();
	context = (TableFunctionContext *) funcCtx->user_fctx;

	if (table_scan_getnextslot(context->scanDesc, ForwardScanDirection, context->slot))
	{
		Datum attrRelativePath;
		Datum attrSize;
		Datum attrLastModified;
		Datum attrMd5Sum;
		Datum attrTags;
		bool  isNull;
		char  *scopedFileUrl;

		slot_getallattrs(context->slot);

		attrRelativePath  = slot_getattr(context->slot, 1, &isNull);
		Assert(isNull == false);
		attrSize = slot_getattr(context->slot, 2, &isNull);
		Assert(isNull == false);
		attrLastModified = slot_getattr(context->slot, 3, &isNull);
		Assert(isNull == false);
		attrMd5Sum = slot_getattr(context->slot, 4, &isNull);
		Assert(isNull == false);
		attrTags = slot_getattr(context->slot, 5, &isNull);

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		scopedFileUrl = getScopedFileUrl(context->dirTable, TextDatumGetCString(attrRelativePath));

		values[0] = PointerGetDatum(cstring_to_text(scopedFileUrl));
		values[1] = attrRelativePath;
		values[2] = attrTags;
		nulls[2] = isNull;
		values[3] = attrSize;
		values[4] = attrLastModified;
		values[5] = attrMd5Sum;
		values[6] = getFileContent(context->dirTable->spcId, scopedFileUrl);

		tuple = heap_form_tuple(funcCtx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcCtx, result);
	}

	table_endscan(context->scanDesc);
	ExecDropSingleTupleTableSlot(context->slot);
	table_close(context->relation, AccessShareLock);

	pfree(context);
	funcCtx->user_fctx = NULL;

	SRF_RETURN_DONE(funcCtx);
}

Datum
remove_file_segment(PG_FUNCTION_ARGS)
{
	Relation 	relation;
	ScanKeyData scankey;
	SysScanDesc scan;
	HeapTuple 	tuple;
	Oid 		indexOid;
	List		*indexOids;
	Oid 		relId;
	char 		*relativePath;
	char 		*fullPathName;
	bool 		exist = false;
	DirectoryTable	*dirTable;

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation name cannot be NULL")));
	relId = PG_GETARG_OID(0);

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("file path cannot be null")));
	relativePath = PG_GETARG_CSTRING(1);

	dirTable = GetDirectoryTable(relId);
	relation = table_open(relId, AccessExclusiveLock);

	SIMPLE_FAULT_INJECTOR("remove_file_inject");

	indexOids = RelationGetIndexList(relation);
	Assert(list_length(indexOids) == 1);

	indexOid = list_nth_oid(indexOids, 0);
	ScanKeyInit(&scankey,
			 	1, /* relative_path */
			 	BTEqualStrategyNumber, F_TEXTEQ,
			 	CStringGetTextDatum(relativePath));

	scan = systable_beginscan(relation, indexOid, true, NULL, 1, &scankey);
	tuple = systable_getnext(scan);
	if (HeapTupleIsValid(tuple))
	{
		CatalogTupleDelete(relation, &tuple->t_self);
		fullPathName = psprintf("%s/%s", dirTable->location, relativePath);
		UFileAddPendingDelete(relation, dirTable->spcId, fullPathName, true);
		exist = true;
	}

	systable_endscan(scan);
	list_free(indexOids);

	table_close(relation, NoLock);

	PG_RETURN_BOOL(exist);
}

Datum
remove_file(PG_FUNCTION_ARGS)
{
	Relation 	relation;
	Oid			relId;
	char 		*relativePath;
	char 		*query;
	int			numDeletes;
	int			i;
	CdbPgResults cdbPgresults = {NULL, 0};

	if (PG_ARGISNULL(0))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation name cannot be NULL")));
	relId = PG_GETARG_OID(0);

	if (PG_ARGISNULL(1))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("file path cannot be null")));
	relativePath = PG_GETARG_CSTRING(1);

	relation = table_open(relId, AccessExclusiveLock);

	if (Gp_role != GP_ROLE_DISPATCH)
		ereport(ERROR,
				(errcode(ERRCODE_GP_COMMAND_ERROR),
				 errmsg("remove_file() could only be called on QD")));

	query = psprintf("select pg_catalog.remove_file_segment(%u, '%s')", relId, relativePath);

	CdbDispatchCommand(query, DF_WITH_SNAPSHOT | DF_CANCEL_ON_ERROR, &cdbPgresults);

	numDeletes = 0;
	for (i = 0; i < cdbPgresults.numResults; i++)
	{
		int ntuples, nfields;
		Datum value;
		struct pg_result *pgresult = cdbPgresults.pg_results[i];
		ExecStatusType status = PQresultStatus(pgresult);
		if (status != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdbPgresults);
			ereport(ERROR,
					(errmsg("unexpected result from segment: %d", status)));
		}
		ntuples = PQntuples(pgresult);
		nfields = PQnfields(pgresult);
		if (ntuples != 1 || nfields != 1)
		{
			cdbdisp_clearCdbPgResults(&cdbPgresults);
			ereport(ERROR,
					(errmsg("unexpected shape of result from segment (%d rows, %d cols)",
							ntuples, nfields)));
		}
		if (PQgetisnull(pgresult, 0, 0))
			value = 0;
		else
			value = DirectFunctionCall1(boolin,
							   			CStringGetDatum(PQgetvalue(pgresult, 0, 0)));

		numDeletes += value;
	}

	cdbdisp_clearCdbPgResults(&cdbPgresults);

	table_close(relation, NoLock);

	PG_RETURN_BOOL(numDeletes > 0);
}

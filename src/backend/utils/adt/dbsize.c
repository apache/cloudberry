/*
 * dbsize.c
 *		Database object size functions, and related inquiries
 *
 * Copyright (c) 2002-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/dbsize.c
 *
 */

#include "postgres.h"

#include <sys/stat.h>
#include <glob.h>

#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/catalog.h"
#include "catalog/namespace.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_database.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "common/relpath.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/relfilenumbermap.h"
#include "utils/relmapper.h"
#include "utils/syscache.h"

#include "access/appendonlywriter.h"
#include "access/tableam.h"
#include "catalog/pg_appendonly.h"
#include "libpq-fe.h"
#include "foreign/fdwapi.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "utils/snapmgr.h"
#include "utils/relfilenumbermap.h"
#include "utils/relmapper.h"
#include "utils/syscache.h"

/* Divide by two and round away from zero */
#define half_rounded(x)   (((x) + ((x) < 0 ? -1 : 1)) / 2)

/* Units used in pg_size_pretty functions.  All units must be powers of 2 */
struct size_pretty_unit
{
	const char *name;			/* bytes, kB, MB, GB etc */
	uint32		limit;			/* upper limit, prior to half rounding after
								 * converting to this unit. */
	bool		round;			/* do half rounding for this unit */
	uint8		unitbits;		/* (1 << unitbits) bytes to make 1 of this
								 * unit */
};

/* When adding units here also update the docs and the error message in pg_size_bytes */
static const struct size_pretty_unit size_pretty_units[] = {
		{"bytes", 10 * 1024, false, 0},
		{"kB", 20 * 1024 - 1, true, 10},
		{"MB", 20 * 1024 - 1, true, 20},
		{"GB", 20 * 1024 - 1, true, 30},
		{"TB", 20 * 1024 - 1, true, 40},
		{"PB", 20 * 1024 - 1, true, 50},
		{NULL, 0, false, 0}
};

/* Additional unit aliases accepted by pg_size_bytes */
struct size_bytes_unit_alias
{
	const char *alias;
	int			unit_index;		/* corresponding size_pretty_units element */
};

/* When adding units here also update the docs and the error message in pg_size_bytes */
static const struct size_bytes_unit_alias size_bytes_aliases[] = {
		{"B", 0},
		{NULL}
};

static int64 calculate_total_relation_size(Relation rel);
static HTAB *cbdb_get_size_from_segDBs(const char *cmd, int32 relnum);

/* Hook for plugins to calculate relation size */
relation_size_hook_type relation_size_hook = NULL;

/**
 * Some functions are peculiar in that they do their own dispatching.
 * They do not work on entry db since we do not support dispatching
 * from entry-db currently.
 */
#define ERROR_ON_ENTRY_DB()	\
	if (Gp_role == GP_ROLE_EXECUTE && IS_QUERY_DISPATCHER())	\
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),	\
						errmsg("This query is not currently supported by GPDB.")))

/*
 * Helper function to dispatch a size-returning command.
 *
 * Dispatches the given SQL query to segments, and sums up the results.
 * The query is expected to return one int8 value.
 */
int64
get_size_from_segDBs(const char *cmd)
{
	int64		result;
	CdbPgResults cdb_pgresults = {NULL, 0};
	int			i;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	CdbDispatchCommand(cmd, DF_WITH_SNAPSHOT, &cdb_pgresults);

	result = 0;
	for (i = 0; i < cdb_pgresults.numResults; i++)
	{
		Datum		value;
		ExecStatusType status;
		int ntuples;
		int nfields;

		struct pg_result *pgresult = cdb_pgresults.pg_results[i];

		status = PQresultStatus(pgresult);
		if (status != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			ereport(ERROR,
					(errmsg("unexpected result from segment: %d",
							status)));
		}

		ntuples = PQntuples(pgresult);
		nfields = PQnfields(pgresult);

		if (ntuples != 1 || nfields != 1)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			ereport(ERROR,
					(errmsg("unexpected shape of result from segment (%d rows, %d cols)",
							ntuples, nfields)));
		}
		if (PQgetisnull(pgresult, 0, 0))
			value = 0;
		else
			value = DirectFunctionCall1(int8in,
										CStringGetDatum(PQgetvalue(pgresult, 0, 0)));
		result += value;
	}

	cdbdisp_clearCdbPgResults(&cdb_pgresults);

	return result;
}

/* Return physical size of directory contents, or 0 if dir doesn't exist */
static int64
db_dir_size(const char *path)
{
	int64		dirsize = 0;
	struct dirent *direntry;
	DIR		   *dirdesc;
	char		filename[MAXPGPATH * 2];

	dirdesc = AllocateDir(path);

	if (!dirdesc)
		return 0;

	while ((direntry = ReadDir(dirdesc, path)) != NULL)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(filename, sizeof(filename), "%s/%s", path, direntry->d_name);

		if (stat(filename, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m", filename)));
		}
		dirsize += fst.st_size;
	}

	FreeDir(dirdesc);
	return dirsize;
}

/*
 * calculate size of database in all tablespaces
 */
static int64
calculate_database_size(Oid dbOid)
{
	int64		totalsize;
	DIR		   *dirdesc;
	struct dirent *direntry;
	char		dirpath[MAXPGPATH];
	char		pathname[MAXPGPATH + 13 + MAX_DBID_STRING_LENGTH + 1 + sizeof(GP_TABLESPACE_VERSION_DIRECTORY)];
	AclResult	aclresult;

	/*
	 * User must have connect privilege for target database or have privileges
	 * of pg_read_all_stats
	 */
	aclresult = object_aclcheck(DatabaseRelationId, dbOid, GetUserId(), ACL_CONNECT);
	if (aclresult != ACLCHECK_OK &&
		!has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS))
	{
		aclcheck_error(aclresult, OBJECT_DATABASE,
					   get_database_name(dbOid));
	}

	/* Shared storage in pg_global is not counted */

	/* Include pg_default storage */
	snprintf(pathname, sizeof(pathname), "base/%u", dbOid);
	totalsize = db_dir_size(pathname);

	/* Scan the non-default tablespaces */
	snprintf(dirpath, MAXPGPATH, "pg_tblspc");
	dirdesc = AllocateDir(dirpath);

	while ((direntry = ReadDir(dirdesc, dirpath)) != NULL)
	{
		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(pathname, sizeof(pathname), "pg_tblspc/%s/%s/%u",
				 direntry->d_name, GP_TABLESPACE_VERSION_DIRECTORY, dbOid);
		totalsize += db_dir_size(pathname);
	}

	FreeDir(dirdesc);

	return totalsize;
}

Datum
pg_database_size_oid(PG_FUNCTION_ARGS)
{
	Oid			dbOid = PG_GETARG_OID(0);
	int64		size;

	ERROR_ON_ENTRY_DB();

	size = calculate_database_size(dbOid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_database_size(%u)", dbOid);

		size += get_size_from_segDBs(sql);
	}

	if (size == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}

Datum
pg_database_size_name(PG_FUNCTION_ARGS)
{
	Name		dbName = PG_GETARG_NAME(0);
	Oid			dbOid = get_database_oid(NameStr(*dbName), false);
	int64		size;

	ERROR_ON_ENTRY_DB();

	size = calculate_database_size(dbOid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_database_size(%s)",
					   quote_literal_cstr(NameStr(*dbName)));

		size += get_size_from_segDBs(sql);
	}

	if (size == 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}

/*
 * GPDB: get segment file count of AO/AOCO tables.
 * Could the segment file count be different between segments?
 * Take the average of counts, there is no difference if they are same.
 */
Datum
gp_ao_segment_file_count(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	Relation	rel;
	int16		count = 0;

	ERROR_ON_ENTRY_DB();

	rel = try_relation_open(relOid, AccessShareLock, false);
	if (rel == NULL)
		PG_RETURN_NULL();

	if (!RelationIsAppendOptimized(rel))
	{
		relation_close(rel, AccessShareLock);
		PG_RETURN_NULL();
	}

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;
		sql = psprintf("select pg_catalog.gp_ao_segment_file_count(%u)", relOid);
		count = get_size_from_segDBs(sql)/getgpsegmentCount();
	} else {
		count = GetAppendOnlySegmentFilesCount(rel);
	}
	Assert(count <= MAX_AOREL_CONCURRENCY);
	relation_close(rel, AccessShareLock);
	PG_RETURN_INT16(count);
}

/*
 * Calculate total size of tablespace. Returns -1 if the tablespace directory
 * cannot be found.
 */
static int64
calculate_tablespace_size(Oid tblspcOid)
{
	char		tblspcPath[MAXPGPATH];
	char		pathname[MAXPGPATH * 2];
	int64		totalsize = 0;
	DIR		   *dirdesc;
	struct dirent *direntry;
	AclResult	aclresult;

	/*
	 * User must have privileges of pg_read_all_stats or have CREATE privilege
	 * for target tablespace, either explicitly granted or implicitly because
	 * it is default for current database.
	 */
	if (tblspcOid != MyDatabaseTableSpace &&
		!has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS))
	{
		aclresult = object_aclcheck(TableSpaceRelationId, tblspcOid, GetUserId(), ACL_CREATE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_TABLESPACE,
						   get_tablespace_name(tblspcOid));
	}

	if (tblspcOid == DEFAULTTABLESPACE_OID)
		snprintf(tblspcPath, MAXPGPATH, "base");
	else if (tblspcOid == GLOBALTABLESPACE_OID)
		snprintf(tblspcPath, MAXPGPATH, "global");
	else
		snprintf(tblspcPath, MAXPGPATH, "pg_tblspc/%u/%s", tblspcOid,
				 GP_TABLESPACE_VERSION_DIRECTORY);

	dirdesc = AllocateDir(tblspcPath);

	if (!dirdesc)
		return -1;

	while ((direntry = ReadDir(dirdesc, tblspcPath)) != NULL)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (strcmp(direntry->d_name, ".") == 0 ||
			strcmp(direntry->d_name, "..") == 0)
			continue;

		snprintf(pathname, sizeof(pathname), "%s/%s", tblspcPath, direntry->d_name);

		if (stat(pathname, &fst) < 0)
		{
			if (errno == ENOENT)
				continue;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file \"%s\": %m", pathname)));
		}

		if (S_ISDIR(fst.st_mode))
			totalsize += db_dir_size(pathname);

		totalsize += fst.st_size;
	}

	FreeDir(dirdesc);

	return totalsize;
}

Datum
pg_tablespace_size_oid(PG_FUNCTION_ARGS)
{
	Oid			tblspcOid = PG_GETARG_OID(0);
	int64		size;

	ERROR_ON_ENTRY_DB();

	size = calculate_tablespace_size(tblspcOid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_tablespace_size(%u)", tblspcOid);

		size += get_size_from_segDBs(sql);
	}

	if (size < 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}

Datum
pg_tablespace_size_name(PG_FUNCTION_ARGS)
{
	Name		tblspcName = PG_GETARG_NAME(0);
	Oid			tblspcOid = get_tablespace_oid(NameStr(*tblspcName), false);
	int64		size;

	ERROR_ON_ENTRY_DB();

	size = calculate_tablespace_size(tblspcOid);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_tablespace_size(%s)",
					   quote_literal_cstr(NameStr(*tblspcName)));

		size += get_size_from_segDBs(sql);
	}

	if (size < 0)
		PG_RETURN_NULL();

	PG_RETURN_INT64(size);
}


/*
 * calculate size of (one fork of) a relation
 *
 * Iterator over all files belong to the relation and do stat.
 * The obviously better way is to use glob.  For whatever reason,
 * glob is extremely slow if there are lots of relations in the
 * database.  So we handle all cases, instead. 
 *
 * Note: we can safely apply this to temp tables of other sessions, so there
 * is no check here or at the call sites for that.
 */
static int64
calculate_relation_size(Relation rel, ForkNumber forknum)
{
	int64		totalsize = 0;
	char	   *relationpath;
	char		pathname[MAXPGPATH];
	unsigned int segcount = 0;

	/*
	 * TODO: For non-heap relations, use table_relation_size instead.
	 */
	if (relation_size_hook)
		return (*relation_size_hook) (rel, forknum);

	/* Call into the tableam api if the table is not heap, i.e. AO/CO/PAX relations. */
	if (RelationIsNonblockRelation(rel))
		return table_relation_size(rel, forknum);

	relationpath = relpathbackend(rel->rd_locator, rel->rd_backend, forknum);

	/* Ordinary relation, including heap and index.
	 * They take form of relationpath, or relationpath.%d
	 * There will be no holes, therefore, we can stop when
	 * we reach the first non-existing file.
	 */
	for (segcount = 0;; segcount++)
	{
		struct stat fst;

		CHECK_FOR_INTERRUPTS();

		if (segcount == 0)
			snprintf(pathname, MAXPGPATH, "%s",
					 relationpath);
		else
			snprintf(pathname, MAXPGPATH, "%s.%u",
					 relationpath, segcount);

		if (stat(pathname, &fst) < 0)
		{
			if (errno == ENOENT)
				break;
			else
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not stat file %s: %m", pathname)));
		}
		totalsize += fst.st_size;
	}

	/* RELSTORAGE_VIRTUAL has no space usage */
	return totalsize;
}

Datum
pg_relation_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	text	   *forkName = PG_GETARG_TEXT_PP(1);
	ForkNumber	forkNumber;
	Relation	rel;
	int64		size = 0;

	ERROR_ON_ENTRY_DB();

	rel = try_relation_open(relOid, AccessShareLock, false);

	/*
	 * Before 9.2, we used to throw an error if the relation didn't exist, but
	 * that makes queries like "SELECT pg_relation_size(oid) FROM pg_class"
	 * less robust, because while we scan pg_class with an MVCC snapshot,
	 * someone else might drop the table. It's better to return NULL for
	 * already-dropped tables than throw an error and abort the whole query.
	 */
	if (rel == NULL)
		PG_RETURN_NULL();

	if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		FdwRoutine *fdwroutine;
		bool        ok = false;

		fdwroutine = GetFdwRoutineForRelation(rel, false);

		if (fdwroutine->GetRelationSizeOnSegment != NULL)
			ok = fdwroutine->GetRelationSizeOnSegment(rel, &size);

		if (!ok)
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- cannot calculate this foreign table size",
							RelationGetRelationName(rel))));

		relation_close(rel, AccessShareLock);

		PG_RETURN_INT64(size);

	}

	forkNumber = forkname_to_number(text_to_cstring(forkName));

	// TODO directory table
	size = calculate_relation_size(rel, forkNumber);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_relation_size(%u, '%s')", relOid,
					   forkNames[forkNumber]);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

/*
 * Calculate total on-disk size of a TOAST relation, including its indexes.
 * Must not be applied to non-TOAST relations.
 */
static int64
calculate_toast_table_size(Oid toastrelid)
{
	int64		size = 0;
	Relation	toastRel;
	ForkNumber	forkNum;
	ListCell   *lc;
	List	   *indexlist;

	toastRel = relation_open(toastrelid, AccessShareLock);

	/* toast heap size, including FSM and VM size */
	for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
		size += calculate_relation_size(toastRel, forkNum);

	/* toast index size, including FSM and VM size */
	indexlist = RelationGetIndexList(toastRel);

	/* Size is calculated using all the indexes available */
	foreach(lc, indexlist)
	{
		Relation	toastIdxRel;

		toastIdxRel = relation_open(lfirst_oid(lc),
									AccessShareLock);
		for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
			size += calculate_relation_size(toastIdxRel, forkNum);

		relation_close(toastIdxRel, AccessShareLock);
	}
	list_free(indexlist);
	relation_close(toastRel, AccessShareLock);

	return size;
}

/*
 * Calculate total on-disk size of a given table,
 * including FSM and VM, plus TOAST table if any.
 * Indexes other than the TOAST table's index are not included.
 * GPDB: Also includes aoseg, aoblkdir, and aovisimap tables
 *
 * Note that this also behaves sanely if applied to an index or toast table;
 * those won't have attached toast tables, but they can have multiple forks.
 */
static int64
calculate_table_size(Relation rel)
{
	int64		size = 0;
	ForkNumber	forkNum;

	if (!RelationIsValid(rel))
		return 0;

	/*
	 * heap size, including FSM and VM
	 */
	if (rel->rd_locator.relNumber != 0)
	{
		for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
			size += calculate_relation_size(rel, forkNum);
	}

	/*
	 * Size of toast relation
	 */
	if (OidIsValid(rel->rd_rel->reltoastrelid))
		size += calculate_toast_table_size(rel->rd_rel->reltoastrelid);

	if (RelationStorageIsAO(rel))
	{
		Oid	auxRelIds[3];
		GetAppendOnlyEntryAuxOids(rel, &auxRelIds[0],
								 &auxRelIds[1], NULL,
								 &auxRelIds[2], NULL);

		for (int i = 0; i < 3; i++)
		{
			Relation auxRel;

			if (!OidIsValid(auxRelIds[i]))
				continue;

			if ((auxRel = try_relation_open(auxRelIds[i], AccessShareLock, false)) != NULL)
			{
				size += calculate_total_relation_size(auxRel);
				relation_close(auxRel, AccessShareLock);
			}
			else
			{
				/*
				 * This error may occur when the auxiliary relations' records of
				 * the appendonly table are corrupted.
				 */
				elog(ERROR, "invalid auxiliary relation oid %u for appendonly relation '%s'",
							auxRelIds[i], rel->rd_rel->relname.data);
			}
		}
	}

	return size;
}

/*
 * Calculate total on-disk size of all indexes attached to the given table.
 *
 * Can be applied safely to an index, but you'll just get zero.
 */
static int64
calculate_indexes_size(Relation rel)
{
	int64		size = 0;

	/*
	 * Aggregate all indexes on the given relation
	 */
	if (rel->rd_rel->relhasindex)
	{
		List	   *index_oids = RelationGetIndexList(rel);
		ListCell   *cell;

		foreach(cell, index_oids)
		{
			Oid			idxOid = lfirst_oid(cell);
			Relation	idxRel;
			ForkNumber	forkNum;

			idxRel = try_relation_open(idxOid, AccessShareLock, false);

			if (RelationIsValid(idxRel))
			{
				for (forkNum = 0; forkNum <= MAX_FORKNUM; forkNum++)
					size += calculate_relation_size(idxRel,
													forkNum);

				relation_close(idxRel, AccessShareLock);
			}
		}

		list_free(index_oids);
	}

	return size;
}

Datum
pg_table_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	Relation	rel;
	int64		size;

	ERROR_ON_ENTRY_DB();

	rel = try_relation_open(relOid, AccessShareLock, false);

	if (rel == NULL)
		PG_RETURN_NULL();

	size = calculate_table_size(rel);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_table_size(%u)", relOid);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

Datum
pg_indexes_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	Relation	rel;
	int64		size;

	ERROR_ON_ENTRY_DB();

	rel = try_relation_open(relOid, AccessShareLock, false);

	if (rel == NULL)
		PG_RETURN_NULL();

	size = calculate_indexes_size(rel);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_indexes_size(%u)", relOid);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

/*
 *	Compute the on-disk size of all files for the relation,
 *	including heap data, index data, toast data, FSM, VM.
 */
static int64
calculate_total_relation_size(Relation rel)
{
	int64		size;

	/*
	 * Aggregate the table size, this includes size of the heap, toast and
	 * toast index with free space and visibility map
	 */
	size = calculate_table_size(rel);

	/*
	 * Add size of all attached indexes as well
	 */
	size += calculate_indexes_size(rel);

	return size;
}

Datum
pg_total_relation_size(PG_FUNCTION_ARGS)
{
	Oid			relOid = PG_GETARG_OID(0);
	Relation	rel;
	int64		size;

	ERROR_ON_ENTRY_DB();

	/*
	 * While we scan pg_class with an MVCC snapshot,
	 * someone else might drop the table. It's better to return NULL for
	 * already-dropped tables than throw an error and abort the whole query.
	 */
	if (get_rel_name(relOid) == NULL)
		PG_RETURN_NULL();

	rel = try_relation_open(relOid, AccessShareLock, false);

	if (rel == NULL)
		PG_RETURN_NULL();

	size = calculate_total_relation_size(rel);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		char	   *sql;

		sql = psprintf("select pg_catalog.pg_total_relation_size(%u)",
					   relOid);

		size += get_size_from_segDBs(sql);
	}

	relation_close(rel, AccessShareLock);

	PG_RETURN_INT64(size);
}

/*
 * formatting with size units
 */
Datum
pg_size_pretty(PG_FUNCTION_ARGS)
{
	int64		size = PG_GETARG_INT64(0);
	char		buf[64];
	const struct size_pretty_unit *unit;

	for (unit = size_pretty_units; unit->name != NULL; unit++)
	{
		uint8		bits;
		uint64		abs_size = size < 0 ? 0 - (uint64) size : (uint64) size;

		/*
		 * Use this unit if there are no more units or the absolute size is
		 * below the limit for the current unit.
		 */
		if (unit[1].name == NULL || abs_size < unit->limit)
		{
			if (unit->round)
				size = half_rounded(size);

			snprintf(buf, sizeof(buf), INT64_FORMAT " %s", size, unit->name);
			break;
		}

		/*
		 * Determine the number of bits to use to build the divisor.  We may
		 * need to use 1 bit less than the difference between this and the
		 * next unit if the next unit uses half rounding.  Or we may need to
		 * shift an extra bit if this unit uses half rounding and the next one
		 * does not.  We use division rather than shifting right by this
		 * number of bits to ensure positive and negative values are rounded
		 * in the same way.
		 */
		bits = (unit[1].unitbits - unit->unitbits - (unit[1].round == true)
				+ (unit->round == true));
		size /= ((int64) 1) << bits;
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf));
}

static char *
numeric_to_cstring(Numeric n)
{
	Datum		d = NumericGetDatum(n);

	return DatumGetCString(DirectFunctionCall1(numeric_out, d));
}

static bool
numeric_is_less(Numeric a, Numeric b)
{
	Datum		da = NumericGetDatum(a);
	Datum		db = NumericGetDatum(b);

	return DatumGetBool(DirectFunctionCall2(numeric_lt, da, db));
}

static Numeric
numeric_absolute(Numeric n)
{
	Datum		d = NumericGetDatum(n);
	Datum		result;

	result = DirectFunctionCall1(numeric_abs, d);
	return DatumGetNumeric(result);
}

static Numeric
numeric_half_rounded(Numeric n)
{
	Datum		d = NumericGetDatum(n);
	Datum		zero;
	Datum		one;
	Datum		two;
	Datum		result;

	zero = NumericGetDatum(int64_to_numeric(0));
	one = NumericGetDatum(int64_to_numeric(1));
	two = NumericGetDatum(int64_to_numeric(2));

	if (DatumGetBool(DirectFunctionCall2(numeric_ge, d, zero)))
		d = DirectFunctionCall2(numeric_add, d, one);
	else
		d = DirectFunctionCall2(numeric_sub, d, one);

	result = DirectFunctionCall2(numeric_div_trunc, d, two);
	return DatumGetNumeric(result);
}

static Numeric
numeric_truncated_divide(Numeric n, int64 divisor)
{
	Datum		d = NumericGetDatum(n);
	Datum		divisor_numeric;
	Datum		result;

	divisor_numeric = NumericGetDatum(int64_to_numeric(divisor));
	result = DirectFunctionCall2(numeric_div_trunc, d, divisor_numeric);
	return DatumGetNumeric(result);
}

Datum
pg_size_pretty_numeric(PG_FUNCTION_ARGS)
{
	Numeric		size = PG_GETARG_NUMERIC(0);
	char	   *result = NULL;
	const struct size_pretty_unit *unit;

	for (unit = size_pretty_units; unit->name != NULL; unit++)
	{
		unsigned int shiftby;

		/* use this unit if there are no more units or we're below the limit */
		if (unit[1].name == NULL ||
			numeric_is_less(numeric_absolute(size),
							int64_to_numeric(unit->limit)))
		{
			if (unit->round)
				size = numeric_half_rounded(size);

			result = psprintf("%s %s", numeric_to_cstring(size), unit->name);
			break;
		}

		/*
		 * Determine the number of bits to use to build the divisor.  We may
		 * need to use 1 bit less than the difference between this and the
		 * next unit if the next unit uses half rounding.  Or we may need to
		 * shift an extra bit if this unit uses half rounding and the next one
		 * does not.
		 */
		shiftby = (unit[1].unitbits - unit->unitbits - (unit[1].round == true)
				   + (unit->round == true));
		size = numeric_truncated_divide(size, ((int64) 1) << shiftby);
	}

	PG_RETURN_TEXT_P(cstring_to_text(result));
}

/*
 * Convert a human-readable size to a size in bytes
 */
Datum
pg_size_bytes(PG_FUNCTION_ARGS)
{
	text	   *arg = PG_GETARG_TEXT_PP(0);
	char	   *str,
			   *strptr,
			   *endptr;
	char		saved_char;
	Numeric		num;
	int64		result;
	bool		have_digits = false;

	str = text_to_cstring(arg);

	/* Skip leading whitespace */
	strptr = str;
	while (isspace((unsigned char) *strptr))
		strptr++;

	/* Check that we have a valid number and determine where it ends */
	endptr = strptr;

	/* Part (1): sign */
	if (*endptr == '-' || *endptr == '+')
		endptr++;

	/* Part (2): main digit string */
	if (isdigit((unsigned char) *endptr))
	{
		have_digits = true;
		do
			endptr++;
		while (isdigit((unsigned char) *endptr));
	}

	/* Part (3): optional decimal point and fractional digits */
	if (*endptr == '.')
	{
		endptr++;
		if (isdigit((unsigned char) *endptr))
		{
			have_digits = true;
			do
				endptr++;
			while (isdigit((unsigned char) *endptr));
		}
	}

	/* Complain if we don't have a valid number at this point */
	if (!have_digits)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid size: \"%s\"", str)));

	/* Part (4): optional exponent */
	if (*endptr == 'e' || *endptr == 'E')
	{
		long		exponent;
		char	   *cp;

		/*
		 * Note we might one day support EB units, so if what follows 'E'
		 * isn't a number, just treat it all as a unit to be parsed.
		 */
		exponent = strtol(endptr + 1, &cp, 10);
		(void) exponent;		/* Silence -Wunused-result warnings */
		if (cp > endptr + 1)
			endptr = cp;
	}

	/*
	 * Parse the number, saving the next character, which may be the first
	 * character of the unit string.
	 */
	saved_char = *endptr;
	*endptr = '\0';

	num = DatumGetNumeric(DirectFunctionCall3(numeric_in,
											  CStringGetDatum(strptr),
											  ObjectIdGetDatum(InvalidOid),
											  Int32GetDatum(-1)));

	*endptr = saved_char;

	/* Skip whitespace between number and unit */
	strptr = endptr;
	while (isspace((unsigned char) *strptr))
		strptr++;

	/* Handle possible unit */
	if (*strptr != '\0')
	{
		const struct size_pretty_unit *unit;
		int64		multiplier = 0;

		/* Trim any trailing whitespace */
		endptr = str + VARSIZE_ANY_EXHDR(arg) - 1;

		while (isspace((unsigned char) *endptr))
			endptr--;

		endptr++;
		*endptr = '\0';

		for (unit = size_pretty_units; unit->name != NULL; unit++)
		{
			/* Parse the unit case-insensitively */
			if (pg_strcasecmp(strptr, unit->name) == 0)
				break;
		}

		/* If not found, look in table of aliases */
		if (unit->name == NULL)
		{
			for (const struct size_bytes_unit_alias *a = size_bytes_aliases; a->alias != NULL; a++)
			{
				if (pg_strcasecmp(strptr, a->alias) == 0)
				{
					unit = &size_pretty_units[a->unit_index];
					break;
				}
			}
		}

		/* Verify we found a valid unit in the loop above */
		if (unit->name == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("invalid size: \"%s\"", text_to_cstring(arg)),
					 errdetail("Invalid size unit: \"%s\".", strptr),
					 errhint("Valid units are \"bytes\", \"B\", \"kB\", \"MB\", \"GB\", \"TB\", and \"PB\".")));

		multiplier = ((int64) 1) << unit->unitbits;

		if (multiplier > 1)
		{
			Numeric		mul_num;

			mul_num = int64_to_numeric(multiplier);

			num = DatumGetNumeric(DirectFunctionCall2(numeric_mul,
													  NumericGetDatum(mul_num),
													  NumericGetDatum(num)));
		}
	}

	result = DatumGetInt64(DirectFunctionCall1(numeric_int8,
											   NumericGetDatum(num)));

	PG_RETURN_INT64(result);
}

/*
 * Get the filenode of a relation
 *
 * This is expected to be used in queries like
 *		SELECT pg_relation_filenode(oid) FROM pg_class;
 * That leads to a couple of choices.  We work from the pg_class row alone
 * rather than actually opening each relation, for efficiency.  We don't
 * fail if we can't find the relation --- some rows might be visible in
 * the query's MVCC snapshot even though the relations have been dropped.
 * (Note: we could avoid using the catcache, but there's little point
 * because the relation mapper also works "in the now".)  We also don't
 * fail if the relation doesn't have storage.  In all these cases it
 * seems better to quietly return NULL.
 */
Datum
pg_relation_filenode(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	RelFileNumber result;
	HeapTuple	tuple;
	Form_pg_class relform;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		PG_RETURN_NULL();
	relform = (Form_pg_class) GETSTRUCT(tuple);

	switch (relform->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
		case RELKIND_INDEX:
		case RELKIND_SEQUENCE:
		case RELKIND_TOASTVALUE:
		case RELKIND_AOSEGMENTS:
		case RELKIND_AOBLOCKDIR:
		case RELKIND_AOVISIMAP:
		case RELKIND_DIRECTORY_TABLE:
			/* okay, these have storage */
			if (relform->relfilenode)
				result = relform->relfilenode;
			else				/* Consult the relation mapper */
				result = RelationMapOidToFilenumber(relid,
												  relform->relisshared);
			break;

		default:
			/* no storage, return NULL */
			result = InvalidOid;
			break;
	}

	ReleaseSysCache(tuple);

	if (!RelFileNumberIsValid(result))
		PG_RETURN_NULL();

	PG_RETURN_OID(result);
}

/*
 * Get the relation via (reltablespace, relfilenumber)
 *
 * This is expected to be used when somebody wants to match an individual file
 * on the filesystem back to its table. That's not trivially possible via
 * pg_class, because that doesn't contain the relfilenumbers of shared and nailed
 * tables.
 *
 * We don't fail but return NULL if we cannot find a mapping.
 *
 * InvalidOid can be passed instead of the current database's default
 * tablespace.
 */
Datum
pg_filenode_relation(PG_FUNCTION_ARGS)
{
	Oid			reltablespace = PG_GETARG_OID(0);
	RelFileNumber relfilenumber = PG_GETARG_OID(1);
	Oid			heaprel;

	/* test needed so RelidByRelfilenumber doesn't misbehave */
	if (!RelFileNumberIsValid(relfilenumber))
		PG_RETURN_NULL();

	heaprel = RelidByRelfilenumber(reltablespace, relfilenumber);

	if (!OidIsValid(heaprel))
		PG_RETURN_NULL();
	else
		PG_RETURN_OID(heaprel);
}

/*
 * Get the pathname (relative to $PGDATA) of a relation
 *
 * See comments for pg_relation_filenode.
 */
Datum
pg_relation_filepath(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);
	HeapTuple	tuple;
	Form_pg_class relform;
	RelFileLocator rlocator;
	BackendId	backend;
	char	   *path;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(tuple))
		PG_RETURN_NULL();
	relform = (Form_pg_class) GETSTRUCT(tuple);

	switch (relform->relkind)
	{
		case RELKIND_RELATION:
		case RELKIND_MATVIEW:
		case RELKIND_INDEX:
		case RELKIND_SEQUENCE:
		case RELKIND_TOASTVALUE:
		case RELKIND_AOSEGMENTS:
		case RELKIND_AOVISIMAP:
		case RELKIND_AOBLOCKDIR:
		case RELKIND_DIRECTORY_TABLE:
			/* okay, these have storage */

			/* This logic should match RelationInitPhysicalAddr */
			if (relform->reltablespace)
				rlocator.spcOid = relform->reltablespace;
			else
				rlocator.spcOid = MyDatabaseTableSpace;
			if (rlocator.spcOid == GLOBALTABLESPACE_OID)
				rlocator.dbOid = InvalidOid;
			else
				rlocator.dbOid = MyDatabaseId;
			if (relform->relfilenode)
				rlocator.relNumber = relform->relfilenode;
			else				/* Consult the relation mapper */
				rlocator.relNumber = RelationMapOidToFilenumber(relid,
														 relform->relisshared);
			break;

		default:
			/* no storage, return NULL */
			rlocator.relNumber = InvalidOid;
			/* some compilers generate warnings without these next two lines */
			rlocator.dbOid = InvalidOid;
			rlocator.spcOid = InvalidOid;
			break;
	}

	if (!RelFileNumberIsValid(rlocator.relNumber))
	{
		ReleaseSysCache(tuple);
		PG_RETURN_NULL();
	}

	/* Determine owning backend. */
	switch (relform->relpersistence)
	{
		case RELPERSISTENCE_UNLOGGED:
		case RELPERSISTENCE_PERMANENT:
			backend = InvalidBackendId;
			break;
		case RELPERSISTENCE_TEMP:
			if (isTempOrTempToastNamespace(relform->relnamespace))
				backend = BackendIdForTempRelations();
			else
			{
				/* Do it the hard way. */
				backend = GetTempNamespaceBackendId(relform->relnamespace);
				Assert(backend != InvalidBackendId);
			}
			break;
		default:
			elog(ERROR, "invalid relpersistence: %c", relform->relpersistence);
			backend = InvalidBackendId; /* placate compiler */
			break;
	}

	ReleaseSysCache(tuple);

	path = relpathbackend(rlocator, backend, MAIN_FORKNUM);

	PG_RETURN_TEXT_P(cstring_to_text(path));
}

/**
 * cbdb_relation_size accepts a group of relation
 * oids and return their size.
 * arg0: oid array
 * arg1: fork name
 *
 * cbdb_relation_size is similar to pg_relation_size
 * but when getting multiple relations's size, it can
 * get better performance. On each segment, it gets a
 * group of relations's size once and sum them up on
 * the dispatcher. Compared with pg_relation_size,
 * which only computes one relation's size at one time
 * and dispatches the sql command for different relations
 * multiple times, it saves a lot of work.
 *
 * If there are duplicated oids in the oid array,
 * cbdb_relation_size doesn't deal with that now.
 */
typedef struct
{
	Oid 	reloid;
	int64 	size;
} RelSize;

typedef struct
{
	int32	index;
	int32 	num_entries;
	RelSize *relsize;
} get_relsize_cxt;

Datum
cbdb_relation_size(PG_FUNCTION_ARGS)
{
	FuncCallContext	*funcctx;
	get_relsize_cxt	*cxt;
	int32			len = 0; /* the length of oid array */
	Relation		rel;
	StringInfoData	oidInfo;
	RelSize			*result;

	ForkNumber		forkNumber;
	ArrayType  		*array = PG_GETARG_ARRAYTYPE_P(0);
	text	   		*forkName = PG_GETARG_TEXT_PP(1);
	Oid 			*oidArray =  (Oid *) ARR_DATA_PTR(array);


	if (array_contains_nulls(array))
		ereport(ERROR, (errcode(ERRCODE_ARRAY_ELEMENT_ERROR),
			errmsg("cannot work with arrays containing NULLs")));

	/* caculate all the relation size */
	if (SRF_IS_FIRSTCALL())
	{
#define RELSIZE_NATTS 2
		MemoryContext oldcontext;
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();
		len = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
		forkNumber = forkname_to_number(text_to_cstring(forkName));
		/* Switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		TupleDesc tupdesc = CreateTemplateTupleDesc(RELSIZE_NATTS);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "reloid", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "size", INT8OID, -1, 0);
		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		result = (RelSize*) palloc0(sizeof(RelSize) * len);

		ERROR_ON_ENTRY_DB();

		int relnum = 0; /* the num of oid appended to the oidInfo */
		for (int i = 0; i< len; i++)
		{
			result[i].reloid = oidArray[i];

			rel = try_relation_open(oidArray[i], AccessShareLock, false);

			/*
			 * Before 9.2, we used to throw an error if the relation didn't exist, but
			 * that makes queries like "SELECT pg_relation_size(oid) FROM pg_class"
			 * less robust, because while we scan pg_class with an MVCC snapshot,
			 * someone else might drop the table. It's better to return NULL for
			 * already-dropped tables than throw an error and abort the whole query.
			 *
			 * For cbdb_relation_size, for rel not existed, just set the size to 0
			 */
			if (rel == NULL)
			{
				continue;
			}

			/* for foreign table, only get its size on the dispatcher */
			if (rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
			{
				FdwRoutine *fdwroutine;
				bool        ok = false;

				fdwroutine = GetFdwRoutineForRelation(rel, false);

				if (fdwroutine->GetRelationSizeOnSegment != NULL)
					ok = fdwroutine->GetRelationSizeOnSegment(rel, &result[i].size);

				if (!ok)
					ereport(WARNING,
							(errmsg("skipping \"%s\" --- cannot calculate this foreign table size",
									RelationGetRelationName(rel))));
				relation_close(rel, AccessShareLock);
				continue;
			}

			result[i].size = calculate_relation_size(rel, forkNumber);
			relation_close(rel, AccessShareLock);

			relnum ++;
			if (Gp_role == GP_ROLE_DISPATCH)
			{
				if (relnum == 1)
				{
					initStringInfo(&oidInfo);
					appendStringInfo(&oidInfo, "%u", oidArray[i]);
				}
				else
					appendStringInfo(&oidInfo, ",%u", oidArray[i]);
			}
		}

		if (Gp_role == GP_ROLE_DISPATCH && relnum > 0)
		{
			char	*sql;
			HTAB	*segsize;
			sql = psprintf("select * from pg_catalog.cbdb_relation_size(array[%s]::oid[], '%s')", oidInfo.data,
					forkNames[forkNumber]);
			segsize = cbdb_get_size_from_segDBs(sql, relnum);
			pfree(oidInfo.data);
			pfree(sql);

			for (int i = 0; i< len; i++)
			{
				bool 	found;
				RelSize *entry;
				Oid oid = result[i].reloid;
				entry = hash_search(segsize, &oid, HASH_FIND, &found);
				/* some tables may only exist on dispatcher */
				if (found)
				{
					result[i].size += entry->size;
				}
			}
		}

		cxt = (get_relsize_cxt *) palloc(sizeof(get_relsize_cxt));
		cxt->num_entries = len;
		cxt->index = 0;
		cxt->relsize = result;

		funcctx->user_fctx = cxt;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	cxt = (get_relsize_cxt *) funcctx->user_fctx;

	while (cxt->index < cxt->num_entries)
	{
		RelSize 	*relsize = &cxt->relsize[cxt->index];
		Datum       values[RELSIZE_NATTS];
		bool        nulls[RELSIZE_NATTS];
		HeapTuple	tuple;
		Datum		res;

		MemSet(nulls, 0, sizeof(nulls));
		values[0] = ObjectIdGetDatum(relsize->reloid);
		values[1] = Int64GetDatum(relsize->size);
		cxt->index++;
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		res = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, res);
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * Helper function to dispatch a size-returning command.
 *
 * Dispatches the given SQL query to segments, and sums up the results.
 */
static HTAB*
cbdb_get_size_from_segDBs(const char *cmd, int32 relnum)
{
	CdbPgResults cdb_pgresults = {NULL, 0};
	int			i;
	HTAB       *res_htab = NULL;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	if (!res_htab)
	{
		HASHCTL     hctl;

		memset(&hctl, 0, sizeof(HASHCTL));
		hctl.keysize = sizeof(Oid);
		hctl.entrysize = sizeof(RelSize);
		hctl.hcxt = CurrentMemoryContext;

		res_htab = hash_create("cbdb_get_size_from_segDBs",
				relnum,
				&hctl,
				HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	if (relnum == 0)
		return res_htab;

	CdbDispatchCommand(cmd, DF_WITH_SNAPSHOT, &cdb_pgresults);

	for (i = 0; i < cdb_pgresults.numResults; i++)
	{
		ExecStatusType status;
		int ntuples;
		int nfields;

		struct pg_result *pgresult = cdb_pgresults.pg_results[i];

		status = PQresultStatus(pgresult);
		if (status != PGRES_TUPLES_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			ereport(ERROR,
					(errmsg("unexpected result from segment: %d",
							status)));
		}

		ntuples = PQntuples(pgresult);
		nfields = PQnfields(pgresult);

		if (ntuples != relnum || nfields != RELSIZE_NATTS)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			ereport(ERROR,
					(errmsg("unexpected shape of result from segment (%d rows, %d cols)",
							ntuples, nfields)));
		}

		for ( int j = 0; j < ntuples; j++)
		{
			bool		found;
			RelSize		*entry;
			int64		size;
			if (PQgetisnull(pgresult, j, 0) || PQgetisnull(pgresult, j, 1))
				continue;

			Oid oid = DatumGetObjectId(DirectFunctionCall1(oidin,
						CStringGetDatum(PQgetvalue(pgresult, j, 0))));
			size = DatumGetInt64(DirectFunctionCall1(int8in,
						CStringGetDatum(PQgetvalue(pgresult, j, 1))));
			entry = hash_search(res_htab, &oid, HASH_ENTER, &found);
			if (!found)
			{
				entry->reloid = oid;
				entry->size = size;
			}
			else
			{
				entry->size += size;
			}
		}
	}
	return res_htab;
}

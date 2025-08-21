/*-------------------------------------------------------------------------
 *
 * matview.c
 *	  materialized view support
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/matview.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/amapi.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/catalog.h"
#include "catalog/gp_matview_aux.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_am.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_trigger.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbvars.h"
#include "commands/cluster.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/matview.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "executor/nodeShareInputScan.h"
#include "executor/spi.h"
#include "executor/tstoreReceiver.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/analyze.h"
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "pgstat.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rowsecurity.h"
#include "storage/proc.h"
#include "storage/lmgr.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/typcache.h"


typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	Oid			transientoid;	/* OID of new heap into which to store */
	Oid			oldreloid;
	bool		concurrent;
	bool		skipData;
	char 		relpersistence;
	/* These fields are filled by transientrel_startup: */
	Relation	transientrel;	/* relation to write to */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			ti_options;		/* table_tuple_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */
	uint64		processed;		/* GPDB: number of tuples inserted */
} DR_transientrel;

#define MV_INIT_QUERYHASHSIZE	32
#define MV_INIT_SNAPSHOTHASHSIZE	(2 * MaxBackends)
#define SNAPSHOT_KEYSIZE 128

/*
 * MV_TriggerHashEntry
 *
 * Hash entry for base tables on which IVM trigger is invoked
 */
typedef struct MV_TriggerHashEntry
{
	Oid	matview_id;			/* OID of the materialized view */
	int	before_trig_count;	/* count of before triggers invoked */
	int	after_trig_count;	/* count of after triggers invoked */
	int pid;				/* for debug */
	int reference;			/* reference count */

	Snapshot  snapshot;		/* Snapshot just before table change */
	char  *snapname;		/* Snapshot name for lookup */

	List   *tables;		/* List of MV_TriggerTable */
	bool	has_old;	/* tuples are deleted from any table? */
	bool	has_new;	/* tuples are inserted into any table? */
	MemoryContext context;		/* The session-scoped memory context. */
	ResourceOwner resowner;		/* The session-scoped resource owner. */
} MV_TriggerHashEntry;

/* SnapshotDumpEntry to hold information about a snapshot dump entry */
typedef struct SnapshotDumpEntry
{
	char	snapname[SNAPSHOT_KEYSIZE];  /* Name of the snapshot */
	Oid 	matview_id;			/* OID of the materialized view */
	int 	pid;  				/* Process ID of creater */
	dsm_handle  handle;  		/* Handle to the DSM segment */
	dsm_segment *segment;  		/* Pointer to the DSM segment */
} SnapshotDumpEntry;

/*
 * MV_TriggerTable
 *
 * IVM related data for tables on which the trigger is invoked.
 */
typedef struct MV_TriggerTable
{
	Oid		table_id;			/* OID of the modified table */
	List   *old_tuplestores;	/* tuplestores for deleted tuples */
	List   *new_tuplestores;	/* tuplestores for inserted tuples */

	List   *rte_indexes;		/* List of RTE index of the modified table */
	RangeTblEntry *original_rte;	/* the original RTE saved before rewriting query */

	Relation	rel;			/* relation of the modified table */
	TupleTableSlot *slot;		/* for checking visibility in the pre-state table */
} MV_TriggerTable;

static HTAB *mv_trigger_info = NULL;
static HTAB *mv_trigger_snapshot = NULL;

/* kind of IVM operation for the view */
typedef enum
{
	IVM_ADD,
	IVM_SUB
} IvmOp;

/* ENR name for materialized view delta */
#define NEW_DELTA_ENRNAME "new_delta"
#define OLD_DELTA_ENRNAME "old_delta"

static int	matview_maintenance_depth = 0;

static RefreshClause* MakeRefreshClause(bool concurrent, bool skipData, RangeVar *relation);
static IntoClause* makeIvmIntoClause(const char *enrname, Relation matviewRel);
static void transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo);
static bool transientrel_receive(TupleTableSlot *slot, DestReceiver *self);
static void transientrel_shutdown(DestReceiver *self);
static void transientrel_destroy(DestReceiver *self);
static uint64 refresh_matview_datafill(DestReceiver *dest, Query *query,
									   const char *queryString, RefreshClause *refreshClause);
static uint64 refresh_matview_memoryfill(DestReceiver *dest,Query *query,
						 QueryEnvironment *queryEnv,
						 TupleDesc *resultTupleDesc,
						 const char *queryString, Relation matviewRel);
static char *make_temptable_name_n(char *tempname, int n);
static void refresh_by_match_merge(Oid matviewOid, Oid tempOid, Oid relowner,
								   int save_sec_context);
static void refresh_by_heap_swap(Oid matviewOid, Oid OIDNewHeap, char relpersistence);
static bool is_usable_unique_index(Relation indexRel);
static void OpenMatViewIncrementalMaintenance(void);
static void CloseMatViewIncrementalMaintenance(void);
/* CBDB: move to matview.h */
#if 0
static Query *get_matview_query(Relation matviewRel);
#endif

static Query *rewrite_query_for_preupdate_state(Query *query, List *tables,
								  ParseState *pstate, Oid matviewid);
static void register_delta_ENRs(ParseState *pstate, Query *query, List *tables);
static RangeTblEntry *get_prestate_rte(RangeTblEntry *rte, MV_TriggerTable *table,
				 QueryEnvironment *queryEnv, Oid matviewid);
static RangeTblEntry *replace_rte_with_delta(RangeTblEntry *rte, MV_TriggerTable *table, bool is_new,
		   QueryEnvironment *queryEnv);
static Query *rewrite_query_for_counting_and_aggregates(Query *query, ParseState *pstate);

static char* calc_delta_old(Tuplestorestate *ts,Relation matviewRel, MV_TriggerTable *table, int rte_index, Query *query,
			DestReceiver *dest_old,
			TupleDesc *tupdesc_old,
			QueryEnvironment *queryEnv);
static char* calc_delta_new(Tuplestorestate *ts, Relation matviewRel, MV_TriggerTable *table, int rte_index, Query *query,
			DestReceiver *dest_new,
			TupleDesc *tupdesc_new,
			QueryEnvironment *queryEnv);
static Query *rewrite_query_for_postupdate_state(Query *query, MV_TriggerTable *table, int rte_index);

static void apply_delta(char *old_enr, char *new_enr, MV_TriggerTable *table, Oid matviewOid, Tuplestorestate *old_tuplestores, Tuplestorestate *new_tuplestores,
			TupleDesc tupdesc_old, TupleDesc tupdesc_new,
			Query *query, bool use_count, char *count_colname);
static void append_set_clause_for_count(const char *resname, StringInfo buf_old,
							StringInfo buf_new,StringInfo aggs_list);
static void append_set_clause_for_sum(const char *resname, StringInfo buf_old,
						  StringInfo buf_new, StringInfo aggs_list);
static void append_set_clause_for_avg(const char *resname, StringInfo buf_old,
						  StringInfo buf_new, StringInfo aggs_list,
						  const char *aggtype);
static char *get_operation_string(IvmOp op, const char *col, const char *arg1, const char *arg2,
					 const char* count_col, const char *castType);
static char *get_null_condition_string(IvmOp op, const char *arg1, const char *arg2,
						  const char* count_col);
static void apply_old_delta(const char *matviewname, const char *deltaname_old,
				List *keys);
static void apply_old_delta_with_count(const char *matviewname, Oid matviewRelid, const char *deltaname_old,
				List *keys, StringInfo aggs_list, StringInfo aggs_set,
				const char *count_colname);
static void apply_new_delta(const char *matviewname, const char *deltaname_new,
				StringInfo target_list);
static void apply_new_delta_with_count(const char *matviewname, const char* deltaname_new,
				List *keys, StringInfo target_list, StringInfo aggs_set,
				const char* count_colname);
static char *get_matching_condition_string(List *keys);
static void generate_equal(StringInfo querybuf, Oid opttype,
			   const char *leftop, const char *rightop);

static void clean_up_IVM_hash_entry(MV_TriggerHashEntry *entry, bool is_abort);
static void clean_up_ivm_dsm_entry(MV_TriggerHashEntry *entry);
static void apply_cleanup(Oid matview_id);
static void ivm_export_snapshot(Oid matview_id, char *snapname);
static Snapshot ivm_import_snapshot(const char *idstr);
static void ExecuteTruncateGuts_IVM(Relation matviewRel, Oid matviewOid, Query *query);
static void ivm_set_ts_persitent_name(TriggerData *trigdata, Oid relid, Oid mvid);
/*
 * SetMatViewPopulatedState
 *		Mark a materialized view as populated, or not.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 */
void
SetMatViewPopulatedState(Relation relation, bool newstate)
{
	Relation	pgrel;
	HeapTuple	tuple;

	Assert(relation->rd_rel->relkind == RELKIND_MATVIEW);

	/*
	 * Update relation's pg_class entry.  Crucial side-effect: other backends
	 * (and this one too!) are sent SI message to make them rebuild relcache
	 * entries.
	 */
	pgrel = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID,
								ObjectIdGetDatum(RelationGetRelid(relation)));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(relation));

	((Form_pg_class) GETSTRUCT(tuple))->relispopulated = newstate;

	CatalogTupleUpdate(pgrel, &tuple->t_self, tuple);

	heap_freetuple(tuple);
	table_close(pgrel, RowExclusiveLock);

	/*
	 * Advance command counter to make the updated pg_class row locally
	 * visible.
	 */
	CommandCounterIncrement();
}

static RefreshClause*
MakeRefreshClause(bool concurrent, bool skipData, RangeVar *relation)
{
	RefreshClause *refreshClause;
	refreshClause = makeNode(RefreshClause);

	refreshClause->concurrent = concurrent;
	refreshClause->skipData = skipData;
	refreshClause->relation = relation;

	return refreshClause;
}

/*
 * SetDynamicTableState
 *		Mark a materialized view as Dynamic Table, or not.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 */
void
SetDynamicTableState(Relation relation)
{
	Relation	pgrel;
	HeapTuple	tuple;

	Assert(relation->rd_rel->relkind == RELKIND_MATVIEW);

	/*
	 * Update relation's pg_class entry.  Crucial side-effect: other backends
	 * (and this one too!) are sent SI message to make them rebuild relcache
	 * entries.
	 */
	pgrel = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID,
								ObjectIdGetDatum(RelationGetRelid(relation)));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(relation));

	((Form_pg_class) GETSTRUCT(tuple))->relisdynamic = true;

	CatalogTupleUpdate(pgrel, &tuple->t_self, tuple);

	heap_freetuple(tuple);
	table_close(pgrel, RowExclusiveLock);

	/*
	 * Advance command counter to make the updated pg_class row locally
	 * visible.
	 */
	CommandCounterIncrement();
}

/*
 * SetMatViewIVMState
 *		Mark a materialized view as IVM, or not.
 *
 * NOTE: caller must be holding an appropriate lock on the relation.
 */
void
SetMatViewIVMState(Relation relation, bool newstate)
{
	Relation	pgrel;
	HeapTuple	tuple;

	Assert(relation->rd_rel->relkind == RELKIND_MATVIEW);

	/*
	 * Update relation's pg_class entry.  Crucial side-effect: other backends
	 * (and this one too!) are sent SI message to make them rebuild relcache
	 * entries.
	 */
	pgrel = table_open(RelationRelationId, RowExclusiveLock);
	tuple = SearchSysCacheCopy1(RELOID,
								ObjectIdGetDatum(RelationGetRelid(relation)));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for relation %u",
			 RelationGetRelid(relation));

	((Form_pg_class) GETSTRUCT(tuple))->relisivm = newstate;

	CatalogTupleUpdate(pgrel, &tuple->t_self, tuple);

	heap_freetuple(tuple);
	table_close(pgrel, RowExclusiveLock);

	/*
	 * Advance command counter to make the updated pg_class row locally
	 * visible.
	 */
	CommandCounterIncrement();
}

/*
 * ExecRefreshMatView -- execute a REFRESH MATERIALIZED VIEW command
 *
 * This refreshes the materialized view by creating a new table and swapping
 * the relfilenumbers of the new table and the old materialized view, so the OID
 * of the original materialized view is preserved. Thus we do not lose GRANT
 * nor references to this materialized view.
 *
 * If WITH NO DATA was specified, this is effectively like a TRUNCATE;
 * otherwise it is like a TRUNCATE followed by an INSERT using the SELECT
 * statement associated with the materialized view.  The statement node's
 * skipData field shows whether the clause was used.
 *
 * Indexes are rebuilt too, via REINDEX. Since we are effectively bulk-loading
 * the new heap, it's better to create the indexes afterwards than to fill them
 * incrementally while we load.
 *
 * The matview's "populated" state is changed based on whether the contents
 * reflect the result set of the materialized view's query.
 */
ObjectAddress
ExecRefreshMatView(RefreshMatViewStmt *stmt, const char *queryString,
				   ParamListInfo params, QueryCompletion *qc)
{
	Oid			matviewOid;
	Relation	matviewRel;
	Query	   *dataQuery;
	Query	   *viewQuery;
	Oid			tableSpace;
	Oid			relowner;
	Oid			OIDNewHeap;
	DestReceiver *dest;
	uint64		processed = 0;
	bool		concurrent;
	LOCKMODE	lockmode;
	char		relpersistence;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;
	ObjectAddress address;
	RefreshClause *refreshClause;
	bool oldPopulated;
	bool ao_has_index;

	/* MATERIALIZED_VIEW_FIXME: Refresh MatView is not MPP-fied. */

	/* Determine strength of lock needed. */
	concurrent = stmt->concurrent;
	lockmode = concurrent ? ExclusiveLock : AccessExclusiveLock;

	/*
	 * Get a lock until end of transaction.
	 */
	matviewOid = RangeVarGetRelidExtended(stmt->relation,
										  lockmode, 0,
										  RangeVarCallbackOwnsTable, NULL);
	matviewRel = table_open(matviewOid, NoLock);
	relowner = matviewRel->rd_rel->relowner;

	/*
	 * Switch to the owner's userid, so that any functions are run as that
	 * user.  Also lock down security-restricted operations and arrange to
	 * make GUC variable changes local to this command.
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();
	oldPopulated = RelationIsPopulated(matviewRel);

	/* Make sure it is a materialized view. */
	if (matviewRel->rd_rel->relkind != RELKIND_MATVIEW)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("\"%s\" is not a materialized view",
						RelationGetRelationName(matviewRel))));

	/* Check that CONCURRENTLY is not specified if not populated. */
	if (concurrent && !RelationIsPopulated(matviewRel))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("CONCURRENTLY cannot be used when the materialized view is not populated")));

	/* Check that conflicting options have not been specified. */
	if (concurrent && stmt->skipData)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("%s and %s options cannot be used together",
						"CONCURRENTLY", "WITH NO DATA")));

	viewQuery = get_matview_query(matviewRel);

	/* For IMMV, we need to rewrite matview query */
	if (!stmt->skipData && RelationIsIVM(matviewRel))
		dataQuery = rewriteQueryForIMMV(viewQuery,NIL);
	else
		dataQuery = viewQuery;

	/*
	 * Check that there is a unique index with no WHERE clause on one or more
	 * columns of the materialized view if CONCURRENTLY is specified.
	 */
	if (concurrent)
	{
		List	   *indexoidlist = RelationGetIndexList(matviewRel);
		ListCell   *indexoidscan;
		bool		hasUniqueIndex = false;

		foreach(indexoidscan, indexoidlist)
		{
			Oid			indexoid = lfirst_oid(indexoidscan);
			Relation	indexRel;

			indexRel = index_open(indexoid, AccessShareLock);
			hasUniqueIndex = is_usable_unique_index(indexRel);
			index_close(indexRel, AccessShareLock);
			if (hasUniqueIndex)
				break;
		}

		list_free(indexoidlist);

		if (!hasUniqueIndex)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot refresh materialized view \"%s\" concurrently",
							quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
													   RelationGetRelationName(matviewRel))),
					 errhint("Create a unique index with no WHERE clause on one or more columns of the materialized view.")));
	}

	/*
	 * Check for active uses of the relation in the current transaction, such
	 * as open scans.
	 *
	 * NB: We count on this to protect us against problems with refreshing the
	 * data using TABLE_INSERT_FROZEN.
	 */
	CheckTableNotInUse(matviewRel, "REFRESH MATERIALIZED VIEW");

	/*
	 * Fast path to REFRESH a view:
	 * avoid doing the real REFRESH if the data of view
	 * is up to date. The data should be the logically same as after
	 * REFRESH when there is data changed since latest REFRESH.
	 * In that case we may save a lot, ex: a cron task REFRESH view periodically
	 * or manually executed by users each time.
	 *
	 * Set this feature default to true, but let uesrs decide if they intend
	 * to do a real REFRESH.
	 */
	if (gp_enable_refresh_fast_path &&
		!RelationIsIVM(matviewRel) &&
		!stmt->skipData &&
		MatviewIsUpToDate(matviewOid) &&
		!MatviewHasForeignTables(matviewOid))
	{
		table_close(matviewRel, NoLock);

		/* Roll back any GUC changes */
		AtEOXact_GUC(false, save_nestlevel);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);

		ObjectAddressSet(address, RelationRelationId, matviewOid);
		return address;
	}

	/*
	 * Tentatively mark the matview as populated or not (this will roll back
	 * if we fail later).
	 */
	SetMatViewPopulatedState(matviewRel, !stmt->skipData);

	if (IS_QD_OR_SINGLENODE())
	{
		/*
		 * Update view info:
		 * It's actually a TRUNCATE command if skipData is true.
		 */
		if (stmt->skipData)
			SetMatviewAuxStatus(RelationGetRelid(matviewRel), MV_DATA_STATUS_EXPIRED);
		else
			SetMatviewAuxStatus(RelationGetRelid(matviewRel), MV_DATA_STATUS_UP_TO_DATE);
	}

	/*
	 * The stored query was rewritten at the time of the MV definition, but
	 * has not been scribbled on by the planner.
	 *
	 * GPDB: using original query directly may cause dangling pointers if
	 * shared-inval-queue is overflow, which will cause rebuild the matview
	 * relation. when rebuilding matview relation(relcache), it is found
	 * that oldRel->rule(parentStmtType = PARENTSTMTTYPE_REFRESH_MATVIEW)
	 * is not equal to newRel->rule(parentStmtType = PARENTSTMTTYPE_NONE),
	 * caused oldRel->rule(dataQuery) to be released
	 */
	Assert(IsA(dataQuery, Query));

	dataQuery->parentStmtType = PARENTSTMTTYPE_REFRESH_MATVIEW;

	/* Concurrent refresh builds new data in temp tablespace, and does diff. */
	if (concurrent)
	{
		tableSpace = GetDefaultTablespace(RELPERSISTENCE_TEMP, false);
		relpersistence = RELPERSISTENCE_TEMP;
	}
	else
	{
		tableSpace = matviewRel->rd_rel->reltablespace;
		relpersistence = matviewRel->rd_rel->relpersistence;
	}

	/* delete IMMV triggers. */
	if (RelationIsIVM(matviewRel) && stmt->skipData)
	{
		Relation	tgRel;
		Relation	depRel;
		ScanKeyData key;
		SysScanDesc scan;
		HeapTuple	tup;
		ObjectAddresses *immv_triggers;

		immv_triggers = new_object_addresses();

		tgRel = table_open(TriggerRelationId, RowExclusiveLock);
		depRel = table_open(DependRelationId, RowExclusiveLock);

		/* search triggers that depends on IMMV. */
		ScanKeyInit(&key,
					Anum_pg_depend_refobjid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(matviewOid));
		scan = systable_beginscan(depRel, DependReferenceIndexId, true,
								  NULL, 1, &key);
		while ((tup = systable_getnext(scan)) != NULL)
		{
			ObjectAddress obj;
			Form_pg_depend foundDep = (Form_pg_depend) GETSTRUCT(tup);

			if (foundDep->classid == TriggerRelationId)
			{
				HeapTuple	tgtup;
				ScanKeyData tgkey[1];
				SysScanDesc tgscan;
				Form_pg_trigger tgform;

				/* Find the trigger name. */
				ScanKeyInit(&tgkey[0],
							Anum_pg_trigger_oid,
							BTEqualStrategyNumber, F_OIDEQ,
							ObjectIdGetDatum(foundDep->objid));

				tgscan = systable_beginscan(tgRel, TriggerOidIndexId, true,
											NULL, 1, tgkey);
				tgtup = systable_getnext(tgscan);
				if (!HeapTupleIsValid(tgtup))
					elog(ERROR, "could not find tuple for immv trigger %u", foundDep->objid);

				tgform = (Form_pg_trigger) GETSTRUCT(tgtup);

				/* If trigger is created by IMMV, delete it. */
				if (strncmp(NameStr(tgform->tgname), "IVM_trigger_", 12) == 0)
				{
					obj.classId = foundDep->classid;
					obj.objectId = foundDep->objid;
					obj.objectSubId = foundDep->refobjsubid;
					add_exact_object_address(&obj, immv_triggers);
				}
				systable_endscan(tgscan);
			}
		}
		systable_endscan(scan);

		performMultipleDeletions(immv_triggers, DROP_RESTRICT, PERFORM_DELETION_INTERNAL);

		table_close(depRel, RowExclusiveLock);
		table_close(tgRel, RowExclusiveLock);
		free_object_addresses(immv_triggers);
	}

	/*
	 * Fix issue https://github.com/apache/cloudberry/issues/865
	 * Create blockdir for Matartalized View of AO/AOCS storage has index.
	 */
	ao_has_index = matviewRel->rd_rel->relhasindex && RelationIsAppendOptimized(matviewRel);

	/*
	 * Create the transient table that will receive the regenerated data. Lock
	 * it against access by any other process until commit (by which time it
	 * will be gone).
	 */
	OIDNewHeap = make_new_heap_with_colname(matviewOid, tableSpace, matviewRel->rd_rel->relam, relpersistence,
							   ExclusiveLock, ao_has_index, true, "_$");

	LockRelationOid(OIDNewHeap, AccessExclusiveLock);
	dest = CreateTransientRelDestReceiver(OIDNewHeap, matviewOid, concurrent, relpersistence,
										  stmt->skipData);

	refreshClause = MakeRefreshClause(concurrent, stmt->skipData, stmt->relation);

	/*
	 * Only in dispatcher role, we should set intoPolicy, else it should remain NULL.
	 */
	if (GP_ROLE_DISPATCH == Gp_role)
	{
		dataQuery->intoPolicy = matviewRel->rd_cdbpolicy;
	}
	/* Generate the data, if wanted. */
	/*
	 * In GPDB, we call refresh_matview_datafill() even when WITH NO DATA was
	 * specified, because it will dispatch the operation to the segments.
	 */
	processed = refresh_matview_datafill(dest, dataQuery, queryString, refreshClause);

	/* Make the matview match the newly generated data. */
	if (concurrent)
	{
		int			old_depth = matview_maintenance_depth;

		PG_TRY();
		{
			refresh_by_match_merge(matviewOid, OIDNewHeap, relowner,
								   save_sec_context);
		}
		PG_CATCH();
		{
			matview_maintenance_depth = old_depth;
			PG_RE_THROW();
		}
		PG_END_TRY();
		Assert(matview_maintenance_depth == old_depth);
	}
	else
	{
		refresh_by_heap_swap(matviewOid, OIDNewHeap, relpersistence);

		/*
		 * Inform stats collector about our activity: basically, we truncated
		 * the matview and inserted some new data.  (The concurrent code path
		 * above doesn't need to worry about this because the inserts and
		 * deletes it issues get counted by lower-level code.)
		 *
		 * In GPDB, we should count the pgstat on segments and union them on
		 * QD, so both the segments and coordinator will have pgstat for this
		 * relation. See pgstat_combine_from_qe(pgstat.c) for more details.
		 * Then comment out the below codes on the dispatcher side and leave
		 * the current comment to avoid further upstream merge issues.
		 * The pgstat is updated in function transientrel_shutdown on QE side.
		 * This related to issue: https://github.com/greenplum-db/gpdb/issues/11375
		 */
		// pgstat_count_truncate(matviewRel);
		// if (!stmt->skipData)
		// 	pgstat_count_heap_insert(matviewRel, processed);
	}

	if (!stmt->skipData && RelationIsIVM(matviewRel) && !oldPopulated)
	{
		CreateIvmTriggersOnBaseTables(dataQuery, matviewOid);
	}

	table_close(matviewRel, NoLock);

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	ObjectAddressSet(address, RelationRelationId, matviewOid);

	/*
	 * Save the rowcount so that pg_stat_statements can track the total number
	 * of rows processed by REFRESH MATERIALIZED VIEW command. Note that we
	 * still don't display the rowcount in the command completion tag output,
	 * i.e., the display_rowcount flag of CMDTAG_REFRESH_MATERIALIZED_VIEW
	 * command tag is left false in cmdtaglist.h. Otherwise, the change of
	 * completion tag output might break applications using it.
	 */
	if (qc)
	{
		if (stmt->isdynamic)
			SetQueryCompletion(qc, CMDTAG_REFRESH_DYNAMIC_TABLE, processed);
		else
			SetQueryCompletion(qc, CMDTAG_REFRESH_MATERIALIZED_VIEW, processed);
	}

	return address;
}

/*
 * refresh_matview_datafill
 *
 * Execute the given query, sending result rows to "dest" (which will
 * insert them into the target matview).
 *
 * Returns number of rows inserted.
 */
static uint64
refresh_matview_datafill(DestReceiver *dest, Query *query,
						 const char *queryString, RefreshClause *refreshClause)
{
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	Query	   *copied_query;
	uint64		processed;

	/*
	 * Cloudberry specific behavior:
	 * MPP architecture needs to make sure OIDs of the temp table are the same
	 * among QD and all QEs. It stores the OID in the static variable dispatch_oids.
	 * This variable will be consumed for each dispatch.
	 *
	 * During planning, Cloudberry might pre-evalute some function expr, this will
	 * lead to dispatch if the function is in SQL or PLPGSQL and consumes the above
	 * static variable. So later refresh matview's dispatch will not find the
	 * oid on QEs.
	 *
	 * We first store the OIDs information in a local variable, and then restore
	 * it is for later refresh matview's dispatch to solve the above issue.
	 *
	 * See Github Issue for details: https://github.com/greenplum-db/gpdb/issues/11956
	 */
	List       *saved_dispatch_oids = SaveOidAssignments();

	/* Lock and rewrite, using a copy to preserve the original query. */
	copied_query = copyObject(query);
	AcquireRewriteLocks(copied_query, true, false);
	rewritten = QueryRewrite(copied_query);

	/* SELECT should never rewrite to more or less than one SELECT query */
	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for REFRESH MATERIALIZED VIEW");
	query = (Query *) linitial(rewritten);
	/*
	 * In GPDB, the refresh clause is dispatched to segments when execute the query plan.
	 * But for WITH NO DATA option, it's effectively like a TRUNCATE, so it doesn't need
	 * to take a long time to run the query.
	 *
	 * Add a constant-FALSE to the qual to simulate a plan like this, dispatch the refresh
	 * clause without run the long query:
	 * Motion
	 * 	Result  (cost=0.00..0.01 rows=1 width=0)
	 * 	  One-Time Filter: false
	 * Planner create the motion node on the top according to the matview's distribution
	 * policy in the query->intoPolicy.
	 */
	if (refreshClause->skipData)
		query->jointree->quals = (Node *) makeBoolConst(false, false);

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();

	/* Plan the query which will generate data for the refresh. */

	plan = pg_plan_query(query, queryString, CURSOR_OPT_PARALLEL_OK, NULL);

	plan->refreshClause = refreshClause;

	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.  (This could only matter if
	 * the planner executed an allegedly-stable function that changed the
	 * database contents, but let's do it anyway to be safe.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, NULL, NULL, 0);

	RestoreOidAssignments(saved_dispatch_oids);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* run the plan */
	ExecutorRun(queryDesc, ForwardScanDirection, 0, true);

	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	processed = queryDesc->es_processed;

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	return processed;
}

DestReceiver *
CreateTransientRelDestReceiver(Oid transientoid, Oid oldreloid, bool concurrent,
							   char relpersistence, bool skipdata)
{
	DR_transientrel *self = (DR_transientrel *) palloc0(sizeof(DR_transientrel));

	self->pub.receiveSlot = transientrel_receive;
	self->pub.rStartup = transientrel_startup;
	self->pub.rShutdown = transientrel_shutdown;
	self->pub.rDestroy = transientrel_destroy;
	self->pub.mydest = DestTransientRel;
	self->transientoid = transientoid;
	self->oldreloid = oldreloid;
	self->concurrent = concurrent;
	self->skipData = skipdata;
	self->relpersistence = relpersistence;
	self->processed = 0;

	return (DestReceiver *) self;
}

void
transientrel_init(QueryDesc *queryDesc)
{
	Oid			matviewOid;
	Relation	matviewRel;
	Oid			tableSpace;
	Oid			OIDNewHeap;
	bool		concurrent;
	char		relpersistence;
	LOCKMODE	lockmode;
	RefreshClause *refreshClause;
	bool		ao_has_index;

	refreshClause = queryDesc->plannedstmt->refreshClause;
	/* Determine strength of lock needed. */
	concurrent = refreshClause->concurrent;
	lockmode = concurrent ? ExclusiveLock : AccessExclusiveLock;

	/*
	 * Get a lock until end of transaction.
	 */
	matviewOid = RangeVarGetRelidExtended(refreshClause->relation,
										  lockmode, 0,
										  RangeVarCallbackOwnsTable, NULL);
	matviewRel = heap_open(matviewOid, NoLock);

	/*
	 * Tentatively mark the matview as populated or not (this will roll back
	 * if we fail later).
	 */
	SetMatViewPopulatedState(matviewRel, !refreshClause->skipData);

	/* Concurrent refresh builds new data in temp tablespace, and does diff. */
	if (concurrent)
	{
		tableSpace = GetDefaultTablespace(RELPERSISTENCE_TEMP, false);
		relpersistence = RELPERSISTENCE_TEMP;
	}
	else
	{
		tableSpace = matviewRel->rd_rel->reltablespace;
		relpersistence = matviewRel->rd_rel->relpersistence;
	}

	ao_has_index = matviewRel->rd_rel->relhasindex && RelationIsAppendOptimized(matviewRel);

	/*
	 * Create the transient table that will receive the regenerated data. Lock
	 * it against access by any other process until commit (by which time it
	 * will be gone).
	 */
	OIDNewHeap = make_new_heap(matviewOid, tableSpace, matviewRel->rd_rel->relam,
							   relpersistence,
							   ExclusiveLock, ao_has_index, false);
	LockRelationOid(OIDNewHeap, AccessExclusiveLock);

	queryDesc->dest = CreateTransientRelDestReceiver(OIDNewHeap, matviewOid, concurrent,
													 relpersistence, refreshClause->skipData);

	heap_close(matviewRel, NoLock);
}

/*
 * transientrel_startup --- executor startup
 */
static void
transientrel_startup(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	DR_transientrel *myState = (DR_transientrel *) self;
	Relation	transientrel;

	transientrel = table_open(myState->transientoid, NoLock);

	/*
	 * Fill private fields of myState for use by later routines
	 */
	myState->transientrel = transientrel;
	myState->output_cid = GetCurrentCommandId(true);
	myState->ti_options = TABLE_INSERT_SKIP_FSM | TABLE_INSERT_FROZEN;
	myState->bistate = GetBulkInsertState();
	myState->processed = 0;

	if (RelationIsAoRows(myState->transientrel))
		appendonly_dml_init(myState->transientrel, CMD_INSERT);
	else if (RelationIsAoCols(myState->transientrel))
		aoco_dml_init(myState->transientrel, CMD_INSERT);
	else if (ext_dml_init_hook)
		ext_dml_init_hook(myState->transientrel, CMD_INSERT);

	/*
	 * Valid smgr_targblock implies something already wrote to the relation.
	 * This may be harmless, but this function hasn't planned for it.
	 */
	Assert(RelationGetTargetBlock(transientrel) == InvalidBlockNumber);
}

/*
 * transientrel_receive --- receive one tuple
 */
static bool
transientrel_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_transientrel *myState = (DR_transientrel *) self;

	if (myState->skipData)
		return true;

	/*
	 * Note that the input slot might not be of the type of the target
	 * relation. That's supported by table_tuple_insert(), but slightly less
	 * efficient than inserting with the right slot - but the alternative
	 * would be to copy into a slot of the right type, which would not be
	 * cheap either. This also doesn't allow accessing per-AM data (say a
	 * tuple's xmin), but since we don't do that here...
	 */

	table_tuple_insert(myState->transientrel,
					   slot,
					   myState->output_cid,
					   myState->ti_options,
					   myState->bistate);
	myState->processed++;

	/* We know this is a newly created relation, so there are no indexes */

	return true;
}

/*
 * transientrel_shutdown --- executor end
 */
static void
transientrel_shutdown(DestReceiver *self)
{
	DR_transientrel *myState = (DR_transientrel *) self;

	FreeBulkInsertState(myState->bistate);

	table_finish_bulk_insert(myState->transientrel, myState->ti_options);

	/* close transientrel, but keep lock until commit */
	table_close(myState->transientrel, NoLock);
	myState->transientrel = NULL;
	if (Gp_role == GP_ROLE_EXECUTE && !myState->concurrent)
	{
		Relation	matviewRel;

		matviewRel = table_open(myState->oldreloid, NoLock);
		refresh_by_heap_swap(myState->oldreloid, myState->transientoid, myState->relpersistence);

		/*
		 * In GPDB, we should count the pgstat on segments and union them on
		 * QD, so both the segments and coordinator will have pgstat for this
		 * relation. See pgstat_combine_from_qe(pgstat.c) for more details.
		 * Here each QE will count it's pgstat and report to QD if needed.
		 * This related to issue: https://github.com/greenplum-db/gpdb/issues/11375
		 */
		pgstat_count_truncate(matviewRel);
		if (!myState->skipData)
			pgstat_count_heap_insert(matviewRel, myState->processed);

		table_close(matviewRel, NoLock);
	}
}

/*
 * transientrel_destroy --- release DestReceiver object
 */
static void
transientrel_destroy(DestReceiver *self)
{
	pfree(self);
}


/*
 * Given a qualified temporary table name, append an underscore followed by
 * the given integer, to make a new table name based on the old one.
 * The result is a palloc'd string.
 *
 * As coded, this would fail to make a valid SQL name if the given name were,
 * say, "FOO"."BAR".  Currently, the table name portion of the input will
 * never be double-quoted because it's of the form "pg_temp_NNN", cf
 * make_new_heap().  But we might have to work harder someday.
 */
static char *
make_temptable_name_n(char *tempname, int n)
{
	StringInfoData namebuf;

	initStringInfo(&namebuf);
	appendStringInfoString(&namebuf, tempname);
	appendStringInfo(&namebuf, "_%d", n);
	return namebuf.data;
}

/*
 * refresh_by_match_merge
 *
 * Refresh a materialized view with transactional semantics, while allowing
 * concurrent reads.
 *
 * This is called after a new version of the data has been created in a
 * temporary table.  It performs a full outer join against the old version of
 * the data, producing "diff" results.  This join cannot work if there are any
 * duplicated rows in either the old or new versions, in the sense that every
 * column would compare as equal between the two rows.  It does work correctly
 * in the face of rows which have at least one NULL value, with all non-NULL
 * columns equal.  The behavior of NULLs on equality tests and on UNIQUE
 * indexes turns out to be quite convenient here; the tests we need to make
 * are consistent with default behavior.  If there is at least one UNIQUE
 * index on the materialized view, we have exactly the guarantee we need.
 *
 * The temporary table used to hold the diff results contains just the TID of
 * the old record (if matched) and the ROW from the new table as a single
 * column of complex record type (if matched).
 *
 * Once we have the diff table, we perform set-based DELETE and INSERT
 * operations against the materialized view, and discard both temporary
 * tables.
 *
 * Everything from the generation of the new data to applying the differences
 * takes place under cover of an ExclusiveLock, since it seems as though we
 * would want to prohibit not only concurrent REFRESH operations, but also
 * incremental maintenance.  It also doesn't seem reasonable or safe to allow
 * SELECT FOR UPDATE or SELECT FOR SHARE on rows being updated or deleted by
 * this command.
 */
static void
refresh_by_match_merge(Oid matviewOid, Oid tempOid, Oid relowner,
					   int save_sec_context)
{
	StringInfoData querybuf;
	Relation	matviewRel;
	Relation	tempRel;
	char	   *matviewname;
	char	   *tempname;
	char	   *diffname;
	TupleDesc	tupdesc;
	TupleDesc   newHeapDesc;
	bool		foundUniqueIndex;
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	int16		relnatts;
	Oid		   *opUsedForQual;

	initStringInfo(&querybuf);
	matviewRel = table_open(matviewOid, NoLock);
	matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
											 RelationGetRelationName(matviewRel));
	tempRel = table_open(tempOid, NoLock);
	tempname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(tempRel)),
										  RelationGetRelationName(tempRel));
	diffname = make_temptable_name_n(tempname, 2);

	relnatts = RelationGetNumberOfAttributes(matviewRel);

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* Analyze the temp table with the new contents. */
	appendStringInfo(&querybuf, "ANALYZE %s", tempname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/*
	 * We need to ensure that there are not duplicate rows without NULLs in
	 * the new data set before we can count on the "diff" results.  Check for
	 * that in a way that allows showing the first duplicated row found.  Even
	 * after we pass this test, a unique index on the materialized view may
	 * find a duplicate key problem.
	 *
	 * Note: here and below, we use "tablename.*::tablerowtype" as a hack to
	 * keep ".*" from being expanded into multiple columns in a SELECT list.
	 * Compare ruleutils.c's get_variable().
	 */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					 "SELECT newdata.*::%s FROM %s newdata "
					 "WHERE newdata.* IS NOT NULL AND EXISTS "
					 "(SELECT 1 FROM %s newdata2 WHERE newdata2.* IS NOT NULL "
					 "AND newdata2.* OPERATOR(pg_catalog.*=) newdata.* "
					 "AND newdata2.ctid OPERATOR(pg_catalog.<>) "
					 "newdata.ctid AND newdata2.gp_segment_id "
					 "OPERATOR(pg_catalog.=) newdata.gp_segment_id)",
					 tempname, tempname, tempname);
	if (SPI_execute(querybuf.data, false, 1) != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	if (SPI_processed > 0)
	{
		/*
		 * Note that this ereport() is returning data to the user.  Generally,
		 * we would want to make sure that the user has been granted access to
		 * this data.  However, REFRESH MAT VIEW is only able to be run by the
		 * owner of the mat view (or a superuser) and therefore there is no
		 * need to check for access to data in the mat view.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_CARDINALITY_VIOLATION),
				 errmsg("new data for materialized view \"%s\" contains duplicate rows without any null columns",
						RelationGetRelationName(matviewRel)),
				 errdetail("Row: %s",
						   SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1))));
	}

	/*
	 * Create the temporary "diff" table.
	 *
	 * Temporarily switch out of the SECURITY_RESTRICTED_OPERATION context,
	 * because you cannot create temp tables in SRO context.  For extra
	 * paranoia, add the composite type column only after switching back to
	 * SRO context.
	 *
	 * Greenplum doesn't store diffs in a composite type column, instead it
	 * creates a similar table with the same distribution for performance
	 * considerations.
	 */
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_LOCAL_USERID_CHANGE);
	resetStringInfo(&querybuf);

	appendStringInfo(&querybuf,
					 "CREATE TEMP TABLE %s (LIKE %s)",
					 diffname, tempname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	SetUserIdAndSecContext(relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					 "ALTER TABLE %s ADD COLUMN tid pg_catalog.tid",
					 diffname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					 "ALTER TABLE %s ADD COLUMN sid pg_catalog.int4",
					 diffname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/* Start building the query for populating the diff table. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					 "INSERT INTO %s "
					 "SELECT newdata.*, mv.ctid AS tid, mv.gp_segment_id as sid "
					 "FROM %s mv FULL JOIN %s newdata ON (",
					 diffname, matviewname, tempname);

	/*
	 * Get the list of index OIDs for the table from the relcache, and look up
	 * each one in the pg_index syscache.  We will test for equality on all
	 * columns present in all unique indexes which only reference columns and
	 * include all rows.
	 */
	tupdesc = matviewRel->rd_att;
	newHeapDesc = tempRel->rd_att;
	opUsedForQual = (Oid *) palloc0(sizeof(Oid) * relnatts);
	foundUniqueIndex = false;

	indexoidlist = RelationGetIndexList(matviewRel);

	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		Relation	indexRel;

		indexRel = index_open(indexoid, RowExclusiveLock);
		if (is_usable_unique_index(indexRel))
		{
			Form_pg_index indexStruct = indexRel->rd_index;
			int			indnkeyatts = indexStruct->indnkeyatts;
			oidvector  *indclass;
			Datum		indclassDatum;
			int			i;

			/* Must get indclass the hard way. */
			indclassDatum = SysCacheGetAttrNotNull(INDEXRELID,
												   indexRel->rd_indextuple,
												   Anum_pg_index_indclass);
			indclass = (oidvector *) DatumGetPointer(indclassDatum);

			/* Add quals for all columns from this index. */
			for (i = 0; i < indnkeyatts; i++)
			{
				int			attnum = indexStruct->indkey.values[i];
				Oid			opclass = indclass->values[i];
				Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
				Form_pg_attribute newattr = TupleDescAttr(newHeapDesc, attnum - 1);
				Oid			attrtype = attr->atttypid;
				HeapTuple	cla_ht;
				Form_pg_opclass cla_tup;
				Oid			opfamily;
				Oid			opcintype;
				Oid			op;
				const char *leftop;
				const char *rightop;

				/*
				 * Identify the equality operator associated with this index
				 * column.  First we need to look up the column's opclass.
				 */
				cla_ht = SearchSysCache1(CLAOID, ObjectIdGetDatum(opclass));
				if (!HeapTupleIsValid(cla_ht))
					elog(ERROR, "cache lookup failed for opclass %u", opclass);
				cla_tup = (Form_pg_opclass) GETSTRUCT(cla_ht);
				Assert(IsIndexAccessMethod(cla_tup->opcmethod, BTREE_AM_OID));
				opfamily = cla_tup->opcfamily;
				opcintype = cla_tup->opcintype;
				ReleaseSysCache(cla_ht);

				op = get_opfamily_member(opfamily, opcintype, opcintype,
										 BTEqualStrategyNumber);
				if (!OidIsValid(op))
					elog(ERROR, "missing operator %d(%u,%u) in opfamily %u",
						 BTEqualStrategyNumber, opcintype, opcintype, opfamily);

				/*
				 * If we find the same column with the same equality semantics
				 * in more than one index, we only need to emit the equality
				 * clause once.
				 *
				 * Since we only remember the last equality operator, this
				 * code could be fooled into emitting duplicate clauses given
				 * multiple indexes with several different opclasses ... but
				 * that's so unlikely it doesn't seem worth spending extra
				 * code to avoid.
				 */
				if (opUsedForQual[attnum - 1] == op)
					continue;
				opUsedForQual[attnum - 1] = op;

				/*
				 * Actually add the qual, ANDed with any others.
				 */
				if (foundUniqueIndex)
					appendStringInfoString(&querybuf, " AND ");

				leftop = quote_qualified_identifier("newdata",
													NameStr(newattr->attname));
				rightop = quote_qualified_identifier("mv",
													 NameStr(attr->attname));

				generate_operator_clause(&querybuf,
										 leftop, attrtype,
										 op,
										 rightop, attrtype);

				foundUniqueIndex = true;
			}
		}

		/* Keep the locks, since we're about to run DML which needs them. */
		index_close(indexRel, NoLock);
	}

	list_free(indexoidlist);

	/*
	 * There must be at least one usable unique index on the matview.
	 *
	 * ExecRefreshMatView() checks that after taking the exclusive lock on the
	 * matview. So at least one unique index is guaranteed to exist here
	 * because the lock is still being held.  (One known exception is if a
	 * function called as part of refreshing the matview drops the index.
	 * That's a pretty silly thing to do.)
	 */
	if (!foundUniqueIndex)
		elog(ERROR, "could not find suitable unique index on materialized view");

	appendStringInfoString(&querybuf,
						   " AND newdata.* OPERATOR(pg_catalog.*=) mv.*) "
						   "WHERE newdata.* IS NULL OR mv.* IS NULL "
						   "ORDER BY tid");

	/* Populate the temporary "diff" table. */
	if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/*
	 * We have no further use for data from the "full-data" temp table, but we
	 * must keep it around because its type is referenced from the diff table.
	 */

	/* Analyze the diff table. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf, "ANALYZE %s", diffname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	OpenMatViewIncrementalMaintenance();

	/* Deletes must come before inserts; do them first. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					 "DELETE FROM %s mv WHERE ctid OPERATOR(pg_catalog.=) ANY "
					 "(SELECT diff.tid FROM %s diff "
					 "WHERE diff.tid IS NOT NULL "
					 "AND diff.tid OPERATOR(pg_catalog.=) mv.ctid AND diff.sid "
					 "OPERATOR(pg_catalog.=) mv.gp_segment_id)",
					 matviewname, diffname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/* Inserts go last. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf, "INSERT INTO %s SELECT", matviewname);
	for (int i = 0; i < newHeapDesc->natts; ++i)
	{
		Form_pg_attribute attr = TupleDescAttr(newHeapDesc, i);
		if (i == newHeapDesc->natts - 1)
			appendStringInfo(&querybuf, " %s", NameStr(attr->attname));
		else
			appendStringInfo(&querybuf, " %s,", NameStr(attr->attname));
	}
	appendStringInfo(&querybuf,
					 " FROM %s diff WHERE tid IS NULL",
					 diffname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/* We're done maintaining the materialized view. */
	CloseMatViewIncrementalMaintenance();
	table_close(tempRel, NoLock);
	table_close(matviewRel, NoLock);

	/* Clean up temp tables. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf, "DROP TABLE %s, %s", diffname, tempname);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}

/*
 * Swap the physical files of the target and transient tables, then rebuild
 * the target's indexes and throw away the transient table.  Security context
 * swapping is handled by the called function, so it is not needed here.
 */
static void
refresh_by_heap_swap(Oid matviewOid, Oid OIDNewHeap, char relpersistence)
{
	finish_heap_swap(matviewOid, OIDNewHeap,
					 false, /* is_system_catalog */
					 false, /* swap_toast_by_content */
					 false, /* swap_stats */
					 true, /* check_constraints */
					 true, /* is_internal */
					 RecentXmin, ReadNextMultiXactId(),
					 relpersistence);
}

/*
 * Check whether specified index is usable for match merge.
 */
static bool
is_usable_unique_index(Relation indexRel)
{
	Form_pg_index indexStruct = indexRel->rd_index;

	/*
	 * Must be unique, valid, immediate, non-partial, and be defined over
	 * plain user columns (not expressions).  We also require it to be a
	 * btree.  Even if we had any other unique index kinds, we'd not know how
	 * to identify the corresponding equality operator, nor could we be sure
	 * that the planner could implement the required FULL JOIN with non-btree
	 * operators.
	 */
	if (indexStruct->indisunique &&
		indexStruct->indimmediate &&
        IsIndexAccessMethod(indexRel->rd_rel->relam, BTREE_AM_OID) &&
		indexStruct->indisvalid &&
		RelationGetIndexPredicate(indexRel) == NIL &&
		indexStruct->indnatts > 0)
	{
		/*
		 * The point of groveling through the index columns individually is to
		 * reject both index expressions and system columns.  Currently,
		 * matviews couldn't have OID columns so there's no way to create an
		 * index on a system column; but maybe someday that wouldn't be true,
		 * so let's be safe.
		 */
		int			numatts = indexStruct->indnatts;
		int			i;

		for (i = 0; i < numatts; i++)
		{
			int			attnum = indexStruct->indkey.values[i];

			if (attnum <= 0)
				return false;
		}
		return true;
	}
	return false;
}


/*
 * This should be used to test whether the backend is in a context where it is
 * OK to allow DML statements to modify materialized views.  We only want to
 * allow that for internal code driven by the materialized view definition,
 * not for arbitrary user-supplied code.
 *
 * While the function names reflect the fact that their main intended use is
 * incremental maintenance of materialized views (in response to changes to
 * the data in referenced relations), they are initially used to allow REFRESH
 * without blocking concurrent reads.
 */
bool
MatViewIncrementalMaintenanceIsEnabled(void)
{
	if (Gp_role == GP_ROLE_EXECUTE)
		return true;
	else
		return matview_maintenance_depth > 0;
}

static void
OpenMatViewIncrementalMaintenance(void)
{
	matview_maintenance_depth++;
}

static void
CloseMatViewIncrementalMaintenance(void)
{
	matview_maintenance_depth--;
	Assert(matview_maintenance_depth >= 0);
}

/*
 * get_matview_query - get the Query from a matview's _RETURN rule.
 */
Query *
get_matview_query(Relation matviewRel)
{
	RewriteRule *rule;
	List * actions;

	/*
	 * Check that everything is correct for a refresh. Problems at this point
	 * are internal errors, so elog is sufficient.
	 */
	if (matviewRel->rd_rel->relhasrules == false ||
		matviewRel->rd_rules->numLocks < 1)
		elog(ERROR,
			 "materialized view \"%s\" is missing rewrite information",
			 RelationGetRelationName(matviewRel));

	if (matviewRel->rd_rules->numLocks > 1)
		elog(ERROR,
			 "materialized view \"%s\" has too many rules",
			 RelationGetRelationName(matviewRel));

	rule = matviewRel->rd_rules->rules[0];
	if (rule->event != CMD_SELECT || !(rule->isInstead))
		elog(ERROR,
			 "the rule for materialized view \"%s\" is not a SELECT INSTEAD OF rule",
			 RelationGetRelationName(matviewRel));

	actions = rule->actions;
	if (list_length(actions) != 1)
		elog(ERROR,
			 "the rule for materialized view \"%s\" is not a single action",
			 RelationGetRelationName(matviewRel));

	/*
	 * The stored query was rewritten at the time of the MV definition, but
	 * has not been scribbled on by the planner.
	 */
	return linitial_node(Query, actions);
}


/* ----------------------------------------------------
 *		Incremental View Maintenance routines
 * ---------------------------------------------------
 */

/*
 * ivm_immediate_before
 *
 * IVM trigger function invoked before base table is modified. If this is
 * invoked firstly in the same statement, we save the transaction id and the
 * command id at that time.
 */
Datum
ivm_immediate_before(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	char	   *matviewOid_text = trigdata->tg_trigger->tgargs[0];
	char	   *ex_lock_text = trigdata->tg_trigger->tgargs[1];
	Oid			matviewOid;
	MV_TriggerHashEntry *entry;
	bool	found;
	bool	ex_lock;
	ResourceOwner oldowner;
	MemoryContext oldctx;
	Relation rel = trigdata->tg_relation;

	matviewOid = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(matviewOid_text)));
	ex_lock = DatumGetBool(DirectFunctionCall1(boolin, CStringGetDatum(ex_lock_text)));

	/* If the view has more than one tables, we have to use an exclusive lock. */
	if (ex_lock)
	{
		/*
		 * Wait for concurrent transactions which update this materialized view at
		 * READ COMMITED. This is needed to see changes committed in other
		 * transactions. No wait and raise an error at REPEATABLE READ or
		 * SERIALIZABLE to prevent update anomalies of matviews.
		 * XXX: dead-lock is possible here.
		 */
		if (!IsolationUsesXactSnapshot())
			LockRelationOid(matviewOid, ExclusiveLock);
		else if (!ConditionalLockRelationOid(matviewOid, ExclusiveLock))
		{
			/* try to throw error by name; relation could be deleted... */
			char	   *relname = get_rel_name(matviewOid);

			if (!relname)
				ereport(ERROR,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						errmsg("could not obtain lock on materialized view during incremental maintenance")));

			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					errmsg("could not obtain lock on materialized view \"%s\" during incremental maintenance",
							relname)));
		}
	}
	else
		LockRelationOid(matviewOid, RowExclusiveLock);

	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matviewOid,
											  HASH_ENTER, &found);

	/* On the first BEFORE to update the view, initialize trigger data */
	if (!found)
	{
		/*
		 * Get a snapshot just before the table was modified for checking
		 * tuple visibility in the pre-update state of the table.
		 */
		entry->context = AllocSetContextCreate(TopMemoryContext,
													"IVM Writer Session",
													ALLOCSET_DEFAULT_SIZES);
		entry->resowner = ResourceOwnerCreate(TopTransactionResourceOwner, "IVM Writer Session");
		/* Change to session owner */
		oldowner = CurrentResourceOwner;
		CurrentResourceOwner = entry->resowner;
		oldctx = MemoryContextSwitchTo(entry->context);

		entry->snapname = (char*) palloc0(SNAPSHOT_KEYSIZE);
		entry->matview_id = matviewOid;
		entry->before_trig_count = 0;
		entry->after_trig_count = 0;
		entry->snapshot = NULL;
		entry->tables = NIL;
		entry->has_old = false;
		entry->has_new = false;
		entry->reference = 1;
		entry->pid = MyProcPid;

		entry->snapname[0] = '\0';
		MemoryContextSwitchTo(oldctx);
		CurrentResourceOwner = oldowner;
	}

	entry->before_trig_count++;

	elogif(Debug_print_ivm, INFO, "IVM ivm_immediate_before ref %d, mvid:%d", entry->before_trig_count, matviewOid);

	if (Gp_role == GP_ROLE_DISPATCH && !TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event))
	{
		snprintf(entry->snapname, SNAPSHOT_KEYSIZE, "%08X-%08X-%d-%d",
			 MyProc->backendId, MyProc->lxid, gp_command_count, entry->before_trig_count);
		ivm_export_snapshot(matviewOid, entry->snapname);
	}
	ivm_set_ts_persitent_name(trigdata, rel->rd_id, matviewOid);

	return PointerGetDatum(NULL);
}

/*
 * ivm_immediate_maintenance
 *
 * IVM trigger function invoked after base table is modified.
 * For each table, tuplestores of transition tables are collected.
 * and after the last modification
 */
Datum
ivm_immediate_maintenance(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	Relation	rel;
	Oid			relid;
	Oid			matviewOid;
	Query	   *query;
	Query	   *rewritten = NULL;
	char	   *matviewOid_text = trigdata->tg_trigger->tgargs[0];
	Relation	matviewRel;
	int old_depth = matview_maintenance_depth;

	Oid			relowner;
	Tuplestorestate *old_tuplestore = NULL;
	Tuplestorestate *new_tuplestore = NULL;
	DestReceiver *dest_new = NULL, *dest_old = NULL;
	Oid			save_userid;
	int			save_sec_context;
	int			save_nestlevel;

	MV_TriggerHashEntry *entry;
	MV_TriggerTable		*table;
	bool	found;

	ParseState		 *pstate;
	MemoryContext	oldcxt;
	ListCell   *lc;
	int			i;
	ResourceOwner oldowner;

	QueryEnvironment *queryEnv = create_queryEnv();

	/* Create a ParseState for rewriting the view definition query */
	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	rel = trigdata->tg_relation;
	relid = rel->rd_id;

	matviewOid = DatumGetObjectId(DirectFunctionCall1(oidin, CStringGetDatum(matviewOid_text)));

	/* get the entry for this materialized view */
	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matviewOid,
											  HASH_FIND, &found);
	Assert (found && entry != NULL);
	entry->after_trig_count++;

	elogif(Debug_print_ivm, INFO, "IVM ivm_immediate_maintenance ref %d, mvid:%d", entry->after_trig_count, matviewOid);

	oldowner = CurrentResourceOwner;
	CurrentResourceOwner = entry->resowner;

	oldcxt = MemoryContextSwitchTo(entry->context);
	/* search the entry for the modified table and create new entry if not found */
	found = false;
	foreach(lc, entry->tables)
	{
		table = (MV_TriggerTable *) lfirst(lc);
		if (table->table_id == relid)
		{
			found = true;
			break;
		}
	}

	if (!found)
	{
		table = (MV_TriggerTable *) palloc0(sizeof(MV_TriggerTable));
		table->table_id = relid;
		table->old_tuplestores = NIL;
		table->new_tuplestores = NIL;
		table->rte_indexes = NIL;
		table->slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), table_slot_callbacks(rel));
		table->rel = table_open(RelationGetRelid(rel), NoLock);
		entry->tables = lappend(entry->tables, table);
	}

	/* Save the transition tables and make a request to not free immediately */
	if (trigdata->tg_oldtable)
	{
		table->old_tuplestores = lappend(table->old_tuplestores, trigdata->tg_oldtable);
		entry->has_old = true;
	}
	if (trigdata->tg_newtable)
	{
		table->new_tuplestores = lappend(table->new_tuplestores, trigdata->tg_newtable);
		entry->has_new = true;
	}
	if (entry->has_new || entry->has_old)
	{
		CmdType cmd;

		if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
			cmd = CMD_INSERT;
		else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
			cmd = CMD_DELETE;
		else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
			cmd = CMD_UPDATE;
		else
			elog(ERROR,"unsupported trigger type");
		/* Prolong lifespan of transition tables to the end of the last AFTER trigger */
		SetTransitionTablePreserved(relid, cmd);
	}


	/* If this is not the last AFTER trigger call, immediately exit. */
	Assert (entry->before_trig_count >= entry->after_trig_count);
	if (entry->before_trig_count != entry->after_trig_count)
		return PointerGetDatum(NULL);

	/*
	 * If this is the last AFTER trigger call, continue and update the view.
	 */

	/*
	 * Advance command counter to make the updated base table row locally
	 * visible.
	 */
	CommandCounterIncrement();

	matviewRel = table_open(matviewOid, NoLock);

	/* Make sure it is a materialized view. */
	Assert(matviewRel->rd_rel->relkind == RELKIND_MATVIEW);

	/*
	 * Get and push the latast snapshot to see any changes which is committed
	 * during waiting in other transactions at READ COMMITTED level.
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Check for active uses of the relation in the current transaction, such
	 * as open scans.
	 *
	 * NB: We count on this to protect us against problems with refreshing the
	 * data using TABLE_INSERT_FROZEN.
	 */
	CheckTableNotInUse(matviewRel, "refresh a materialized view incrementally");

	/*
	 * Switch to the owner's userid, so that any functions are run as that
	 * user.  Also arrange to make GUC variable changes local to this command.
	 * We will switch modes when we are about to execute user code.
	 */
	relowner = matviewRel->rd_rel->relowner;
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(relowner,
						   save_sec_context);
	save_nestlevel = NewGUCNestLevel();

	/* get view query*/
	query = get_matview_query(matviewRel);

	/*
	 * When a base table is truncated, the view content will be empty if the
	 * view definition query does not contain an outer join or an aggregate
	 * without a GROUP clause. Therefore, such views can be truncated.
	 *
	 * Aggregate views without a GROUP clause always have one row. Therefore,
	 * if a base table is truncated, the view will not be empty and will contain
	 * a row with NULL value (or 0 for count()). So, in this case, we refresh the
	 * view instead of truncating it.
	 */
	if (TRIGGER_FIRED_BY_TRUNCATE(trigdata->tg_event))
	{
		MemoryContextSwitchTo(oldcxt);

		if (!(query->hasAggs && query->groupClause == NIL))
			ExecuteTruncateGuts(list_make1(matviewRel), list_make1_oid(matviewOid),
							NIL, DROP_RESTRICT, false, false, NULL);
		else if (Gp_role == GP_ROLE_DISPATCH)
			ExecuteTruncateGuts_IVM(matviewRel, matviewOid, query);

		/* Clean up hash entry and delete tuplestores */
		clean_up_IVM_hash_entry(entry, false);

		/* Pop the original snapshot. */
		PopActiveSnapshot();

		table_close(matviewRel, NoLock);

		/* Roll back any GUC changes */
		AtEOXact_GUC(false, save_nestlevel);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);
		/* Restore resource owner */
		CurrentResourceOwner = oldowner;

		return PointerGetDatum(NULL);
	}

	/*
	 * rewrite query for calculating deltas
	 */

	rewritten = copyObject(query);

	/* Replace resnames in a target list with materialized view's attnames */
	i = 0;
	foreach (lc, rewritten->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
		char *resname = NameStr(attr->attname);

		tle->resname = pstrdup(resname);
		i++;
	}
	/*
	 * Step1: collect transition tables in QEs and
	 * Set all tables in the query to pre-update state and
	 */
	rewritten = rewrite_query_for_preupdate_state(rewritten, entry->tables,
												  pstate, matviewOid);
	/* Rewrite for counting duplicated tuples and aggregates functions*/
	rewritten = rewrite_query_for_counting_and_aggregates(rewritten, pstate);

	/* Create tuplestores to store view deltas */
	if (entry->has_old)
	{
		MemoryContext cxt = MemoryContextSwitchTo(TopTransactionContext);

		old_tuplestore = tuplestore_begin_heap(false, false, work_mem);
		dest_old = CreateDestReceiver(DestTuplestore);
		SetTuplestoreDestReceiverParams(dest_old,
									old_tuplestore,
									TopTransactionContext,
									false,
									NULL,
									NULL);

		MemoryContextSwitchTo(cxt);
	}
	if (entry->has_new)
	{
		MemoryContext cxt = MemoryContextSwitchTo(TopTransactionContext);

		new_tuplestore = tuplestore_begin_heap(false, false, work_mem);
		dest_new = CreateDestReceiver(DestTuplestore);
		SetTuplestoreDestReceiverParams(dest_new,
									new_tuplestore,
									TopTransactionContext,
									false,
									NULL,
									NULL);
		MemoryContextSwitchTo(cxt);
	}
	/*
	 * Step2: calculate delta tables
	 * Step3: apply delta tables to the materialized view
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		/* for all modified tables */
		foreach(lc, entry->tables)
		{
			ListCell *lc2;

			table = (MV_TriggerTable *) lfirst(lc);

			/* loop for self-join */
			foreach(lc2, table->rte_indexes)
			{
				int	rte_index = lfirst_int(lc2);
				TupleDesc		tupdesc_old;
				TupleDesc		tupdesc_new;
				Size			snaplen = strlen(entry->snapname);

				bool	use_count = false;
				char	*count_colname = NULL;
				char	*old_enr = NULL;
				char	*new_enr = NULL;

				count_colname = pstrdup("__ivm_count__");
				if (query->hasAggs || query->distinctClause)
					use_count = true;

				configure_queryEnv(queryEnv, matviewOid, table->table_id, entry->snapname, snaplen);

				/* calculate delta tables */
				old_enr = calc_delta_old(old_tuplestore, matviewRel, table, rte_index, rewritten, dest_old,
							&tupdesc_old, queryEnv);

				new_enr = calc_delta_new(new_tuplestore, matviewRel, table, rte_index, rewritten, dest_new,
							&tupdesc_new, queryEnv);

				configure_queryEnv(queryEnv, InvalidOid, InvalidOid, NULL, 0);

				/* Set the table in the query to post-update state */
				rewritten = rewrite_query_for_postupdate_state(rewritten, table, rte_index);

				PG_TRY();
				{
					/* apply the delta tables to the materialized view */
					apply_delta(old_enr, new_enr, table, matviewOid, old_tuplestore, new_tuplestore,
									tupdesc_old, tupdesc_new, query, use_count, count_colname);
				}
				PG_CATCH();
				{
					matview_maintenance_depth = old_depth;
					PG_RE_THROW();
				}
				PG_END_TRY();

				/* clear view delta tuplestores */
				if (old_tuplestore)
					tuplestore_clear(old_tuplestore);
				if (new_tuplestore)
					tuplestore_clear(new_tuplestore);
			}
		}
	}

	if (old_tuplestore)
	{
		dest_old->rDestroy(dest_old);
		tuplestore_end(old_tuplestore);
	}
	if (new_tuplestore)
	{
		dest_new->rDestroy(dest_new);
		tuplestore_end(new_tuplestore);
	}

	MemoryContextSwitchTo(oldcxt);

	/* Pop the original snapshot. */
	PopActiveSnapshot();

	table_close(matviewRel, NoLock);

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	CurrentResourceOwner = oldowner;
	/*
	 * Step4: cleanup stage
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		apply_cleanup(matviewOid);
		DirectFunctionCall1(ivm_immediate_cleanup, ObjectIdGetDatum(matviewOid));
	}
	return PointerGetDatum(NULL);
}

/*
 * rewrite_query_for_preupdate_state
 *
 * Rewrite the query so that base tables' RTEs will represent "pre-update"
 * state of tables. This is necessary to calculate view delta after multiple
 * tables are modified.
 */
static Query*
rewrite_query_for_preupdate_state(Query *query, List *tables,
								  ParseState *pstate, Oid matviewid)
{
	ListCell *lc;
	int num_rte = list_length(query->rtable);
	int i;


	/* register delta ENRs */
	register_delta_ENRs(pstate, query, tables);

	/* XXX: Is necessary? Is this right timing? */
	AcquireRewriteLocks(query, true, false);

	i = 1;

	foreach(lc, query->rtable)
	{
		RangeTblEntry *r = (RangeTblEntry*) lfirst(lc);

		ListCell *lc2;
		foreach(lc2, tables)
		{
			MV_TriggerTable *table = (MV_TriggerTable *) lfirst(lc2);
			/*
			 * if the modified table is found then replace the original RTE with
			 * "pre-state" RTE and append its index to the list.
			 */
			if (r->relid == table->table_id)
			{
				List *securityQuals;
				List *withCheckOptions;
				bool  hasRowSecurity;
				bool  hasSubLinks;

				RangeTblEntry *rte_pre = get_prestate_rte(r, table, pstate->p_queryEnv, matviewid);
				/*
				 * Set a row security policies of the modified table to the subquery RTE which
				 * represents the pre-update state of the table.
				 */
				get_row_security_policies(query, table->original_rte, i,
										  &securityQuals, &withCheckOptions,
										  &hasRowSecurity, &hasSubLinks);

				if (hasRowSecurity)
				{
					query->hasRowSecurity = true;
					rte_pre->security_barrier = true;
				}
				if (hasSubLinks)
					query->hasSubLinks = true;

				rte_pre->securityQuals = securityQuals;
				lfirst(lc) = rte_pre;

				table->rte_indexes = list_append_unique_int(table->rte_indexes, i);
				break;
			}
		}

		/* finish the loop if we processed all RTE included in the original query */
		if (i++ >= num_rte)
			break;
	}

	return query;
}

/*
 * register_delta_ENRs
 *
 * For all modified tables, make ENRs for their transition tables
 * and register them to the queryEnv. ENR's RTEs are also appended
 * into the list in query tree.
 */
static void
register_delta_ENRs(ParseState *pstate, Query *query, List *tables)
{
	QueryEnvironment *queryEnv = pstate->p_queryEnv;
	ListCell *lc;
	RangeTblEntry	*rte;

	foreach(lc, tables)
	{
		MV_TriggerTable *table = (MV_TriggerTable *) lfirst(lc);
		ListCell *lc2;

		foreach(lc2, table->old_tuplestores)
		{
			Tuplestorestate *oldtable = (Tuplestorestate *) lfirst(lc2);
			bool freezed = tuplestore_in_freezed(oldtable);
			EphemeralNamedRelation enr =
				palloc(sizeof(EphemeralNamedRelationData));
			ParseNamespaceItem *nsitem;
			char* shared_name = tuplestore_get_sharedname(oldtable);

			if (freezed || shared_name)
				enr->md.name = pstrdup(shared_name);
			else
				enr->md.name = MakeDeltaName("old", table->table_id, gp_command_count);
			enr->md.reliddesc = table->table_id;
			enr->md.tupdesc = CreateTupleDescCopy(table->rel->rd_att);
			enr->md.enrtype = ENR_NAMED_TUPLESTORE;
			enr->md.enrtuples = tuplestore_tuple_count(oldtable);
			enr->reldata = NULL;
			register_ENR(queryEnv, enr);

			nsitem = addRangeTableEntryForENR(pstate, makeRangeVar(NULL, enr->md.name, -1), true);
			rte = nsitem->p_rte;
			query->rtable = list_append_unique_ptr(query->rtable, rte);

			/* Note: already freezed case */
			if (freezed)
			{
				continue;
			}
			tuplestore_make_sharedV2(oldtable,
						get_shareinput_fileset(),
						enr->md.name,
						tuplestore_get_resowner(oldtable));
			tuplestore_freeze(oldtable);
		}

		foreach(lc2, table->new_tuplestores)
		{
			Tuplestorestate *newtable = (Tuplestorestate *) lfirst(lc2);
			bool freezed = tuplestore_in_freezed(newtable);
			EphemeralNamedRelation enr =
				palloc(sizeof(EphemeralNamedRelationData));
			ParseNamespaceItem *nsitem;
			char* shared_name = tuplestore_get_sharedname(newtable);

			if (freezed || shared_name)
				enr->md.name = pstrdup(shared_name);
			else
				enr->md.name = MakeDeltaName("new", table->table_id, gp_command_count);
			enr->md.reliddesc = table->table_id;
			enr->md.tupdesc = CreateTupleDescCopy(table->rel->rd_att);
			enr->md.enrtype = ENR_NAMED_TUPLESTORE;
			enr->md.enrtuples = tuplestore_tuple_count(newtable);
			enr->reldata = NULL;
			register_ENR(queryEnv, enr);

			nsitem = addRangeTableEntryForENR(pstate, makeRangeVar(NULL, enr->md.name, -1), true);
			rte = nsitem->p_rte;

			query->rtable = list_append_unique_ptr(query->rtable, rte);

			/* Note: already freezed case */
			if (freezed)
			{
				continue;
			}
			tuplestore_make_sharedV2(newtable,
						get_shareinput_fileset(),
						enr->md.name,
						tuplestore_get_resowner(newtable));
			tuplestore_freeze(newtable);
		}
	}
}

#define DatumGetItemPointer(X)	 ((ItemPointer) DatumGetPointer(X))
#define PG_GETARG_ITEMPOINTER(n) DatumGetItemPointer(PG_GETARG_DATUM(n))

/*
 * ivm_visible_in_prestate
 *
 * Check visibility of a tuple specified by the tableoid and item pointer
 * using the snapshot taken just before the table was modified.
 */
Datum
ivm_visible_in_prestate(PG_FUNCTION_ARGS)
{
	Oid			tableoid = PG_GETARG_OID(0);
	ItemPointer itemPtr = PG_GETARG_ITEMPOINTER(1);
	Oid			matviewOid = PG_GETARG_OID(2);
	/*
	CBDB_PG_15_FIXME: Here, we do not use the 4th argument of this function. 
	Either justify its existence by using it, or remove the 4th argument from
	the function definition (catalog change).
	*/
	ListCell   *lc;
	bool	result = true;
	bool	found = false;
	MemoryContext oldcxt;
	ResourceOwner oldowner;

	MV_TriggerHashEntry *entry;
	MV_TriggerTable		*table = NULL;

	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matviewOid,
											  HASH_FIND, &found);
	Assert (found && entry != NULL);

	foreach(lc, entry->tables)
	{
		table = (MV_TriggerTable *) lfirst(lc);
		if (table->table_id == tableoid)
			break;
	}

	Assert (table != NULL);
	if (table->rel == NULL && table->slot == NULL)
	{
		oldowner = CurrentResourceOwner;
		CurrentResourceOwner = entry->resowner;
		oldcxt = MemoryContextSwitchTo(entry->context);
		/* Lazy open relation */
		table->rel = table_open(tableoid, NoLock);
		table->slot = MakeSingleTupleTableSlot(RelationGetDescr(table->rel), table_slot_callbacks(table->rel));

		MemoryContextSwitchTo(oldcxt);
		CurrentResourceOwner = oldowner;
	}

	result = table_tuple_fetch_row_version(table->rel, itemPtr, entry->snapshot, table->slot);

	ExecClearTuple(table->slot);

	elogif(Debug_print_ivm, INFO, "IVM tableoid: %d, ctid: %s, visible %d.", tableoid, ItemPointerToString(itemPtr), result);
	PG_RETURN_BOOL(result);
}

/*
 * get_prestate_rte
 *
 * Rewrite RTE of the modified table to a subquery which represents
 * "pre-state" table. The original RTE is saved in table->rte_original.
 */
static RangeTblEntry*
get_prestate_rte(RangeTblEntry *rte, MV_TriggerTable *table,
				 QueryEnvironment *queryEnv, Oid matviewid)
{
	StringInfoData str;
	RawStmt *raw;
	Query *subquery;
	Relation rel;
	ParseState *pstate;
	char *relname;
	ListCell *lc;

	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	/*
	 * We can use NoLock here since AcquireRewriteLocks should
	 * have locked the relation already.
	 */
	rel = table_open(table->table_id, NoLock);
	relname = quote_qualified_identifier(
					get_namespace_name(RelationGetNamespace(rel)),
									   RelationGetRelationName(rel));
	table_close(rel, NoLock);

	/*
	 * Filtering inserted row using the snapshot taken before the table
	 * is modified. <ctid,gp_segment_id> is required for maintaining outer join views.
	 */
	initStringInfo(&str);
	appendStringInfo(&str,
			"SELECT t.* FROM %s t"
			" WHERE pg_catalog.ivm_visible_in_prestate(t.tableoid, t.ctid, %d::pg_catalog.oid, t.gp_segment_id)",
				relname, matviewid);

	/*
	 * Append deleted rows contained in old transition tables.
	 */
	foreach(lc, table->old_tuplestores)
	{
		Tuplestorestate *tuplestore = (Tuplestorestate *) lfirst(lc);
		appendStringInfo(&str, " UNION ALL ");
		appendStringInfo(&str," SELECT * FROM %s",
			tuplestore_get_sharedname(tuplestore));
	}

	elogif(Debug_print_ivm, INFO, "IVM execute prestate visibilty chek new %s", str.data);

	/* Get a subquery representing pre-state of the table */
	raw = (RawStmt*)linitial(raw_parser(str.data, RAW_PARSE_DEFAULT));
	subquery = transformStmt(pstate, raw->stmt);

	/* save the original RTE */
	table->original_rte = copyObject(rte);

	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = subquery;
	rte->security_barrier = false;

	/* Clear fields that should not be set in a subquery RTE */
	rte->relid = InvalidOid;
	rte->relkind = 0;
	rte->rellockmode = 0;
	rte->tablesample = NULL;
	rte->inh = false;			/* must not be set for a subquery */

	return rte;
}

/*
 * replace_rte_with_delta
 *
 * Replace RTE of the modified table with a single table delta that combine its
 * all transition tables.
 */
static RangeTblEntry*
replace_rte_with_delta(RangeTblEntry *rte, MV_TriggerTable *table, bool is_new,
					   QueryEnvironment *queryEnv)
{
	StringInfoData str;
	ParseState	*pstate;
	RawStmt *raw;
	Query *sub;
	int		i;
	ListCell *lc;
	List	*ts = is_new ? table->new_tuplestores : table->old_tuplestores;

	/* the previous RTE must be a subquery which represents "pre-state" table */
	Assert(rte->rtekind == RTE_SUBQUERY);

	/* Create a ParseState for rewriting the view definition query */
	pstate = make_parsestate(NULL);
	pstate->p_queryEnv = queryEnv;
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	initStringInfo(&str);

	i = 0;
	foreach(lc, ts)
	{
		Tuplestorestate *tuplestore = (Tuplestorestate *) lfirst(lc);
		if (i > 0)
			appendStringInfo(&str, " UNION ALL ");
		appendStringInfo(&str,
			" SELECT * FROM %s",
			tuplestore_get_sharedname(tuplestore));
		i++;
	}

	raw = (RawStmt*)linitial(raw_parser(str.data, RAW_PARSE_DEFAULT));
	sub = transformStmt(pstate, raw->stmt);

	/*
	 * Update the subquery so that it represents the combined transition
	 * table.  Note that we leave the security_barrier and securityQuals
	 * fields so that the subquery relation can be protected by the RLS
	 * policy as same as the modified table.
	 */
	rte->rtekind = RTE_SUBQUERY;
	rte->subquery = sub;
	rte->security_barrier = false;

	/* Clear fields that should not be set in a subquery RTE */
	rte->relid = InvalidOid;
	rte->relkind = 0;
	rte->rellockmode = 0;
	rte->tablesample = NULL;
	rte->inh = false;			/* must not be set for a subquery */

	return rte;
}

/*
 * rewrite_query_for_counting_and_aggregates
 *
 * Rewrite query for counting duplicated tuples and aggregate functions.
 */
static Query *
rewrite_query_for_counting_and_aggregates(Query *query, ParseState *pstate)
{
	TargetEntry *tle_count;
	FuncCall *fn;
	Node *node;

	/* For aggregate views */
	if (query->hasAggs)
	{
		ListCell *lc;
		List *aggs = NIL;
		AttrNumber next_resno = list_length(query->targetList) + 1;

		foreach(lc, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);

			if (IsA(tle->expr, Aggref))
				makeIvmAggColumn(pstate, (Aggref *)tle->expr, tle->resname, &next_resno, &aggs);
		}
		query->targetList = list_concat(query->targetList, aggs);
	}
	/* Add count(*) for counting distinct tuples in views */
	fn = makeFuncCall(SystemFuncName("count"), NIL, COERCE_EXPLICIT_CALL, -1);
	fn->agg_star = true;
	if (!query->groupClause && !query->hasAggs)
		query->groupClause = transformDistinctClause(NULL, &query->targetList, query->sortClause, false);

	node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);

	tle_count = makeTargetEntry((Expr *) node,
								list_length(query->targetList) + 1,
								pstrdup("__ivm_count__"),
								false);
	query->targetList = lappend(query->targetList, tle_count);
	query->hasAggs = true;

	return query;
}

/*
 * calc_delta
 *
 * Calculate view deltas generated under the modification of a table specified
 * by the RTE index.
 */
static char*
calc_delta_old(Tuplestorestate *ts, Relation matviewRel, MV_TriggerTable *table, int rte_index, Query *query,
				DestReceiver *dest_old,
				TupleDesc *tupdesc_old,
				QueryEnvironment *queryEnv)
{
	ListCell *lc = list_nth_cell(query->rtable, rte_index - 1);
	RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
	uint64 es_processed = 0;
	char *enrname = NULL;

	/* Generate old delta */
	if (list_length(table->old_tuplestores) > 0)
	{
		/* Replace the modified table with the old delta table and calculate the old view delta. */
		replace_rte_with_delta(rte, table, false, queryEnv);
		enrname = MakeDeltaName(OLD_DELTA_ENRNAME, RelationGetRelid(matviewRel), gp_command_count);
		es_processed = refresh_matview_memoryfill(dest_old, query, queryEnv,
									tupdesc_old, enrname, matviewRel);
		if (ts)
			tuplestore_set_tuplecount(ts, es_processed);
	}

	return enrname;
}

static char*
calc_delta_new(Tuplestorestate *ts, Relation matviewRel, MV_TriggerTable *table, int rte_index, Query *query,
				DestReceiver *dest_new,
				TupleDesc *tupdesc_new,
				QueryEnvironment *queryEnv)
{
	ListCell *lc = list_nth_cell(query->rtable, rte_index - 1);
	RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
	uint64 es_processed = 0;
	char *enrname = NULL;

	/* Generate new delta */
	if (list_length(table->new_tuplestores) > 0)
	{
		/* Replace the modified table with the new delta table and calculate the new view delta*/
		replace_rte_with_delta(rte, table, true, queryEnv);
		enrname = MakeDeltaName(NEW_DELTA_ENRNAME, RelationGetRelid(matviewRel), gp_command_count);
		es_processed = refresh_matview_memoryfill(dest_new, query, queryEnv,
									tupdesc_new, enrname, matviewRel);
		if (ts)
			tuplestore_set_tuplecount(ts, es_processed);
	}

	return enrname;
}

/*
 * rewrite_query_for_postupdate_state
 *
 * Rewrite the query so that the specified base table's RTEs will represent
 * "post-update" state of tables. This is called after the view delta
 * calculation due to changes on this table finishes.
 */
static Query*
rewrite_query_for_postupdate_state(Query *query, MV_TriggerTable *table, int rte_index)
{
	ListCell *lc = list_nth_cell(query->rtable, rte_index - 1);

	/* Retore the original RTE */
	lfirst(lc) = table->original_rte;

	return query;
}

#define IVM_colname(type, col) makeObjectName("__ivm_" type, col, "_")

/*
 * apply_delta
 *
 * Apply deltas to the materialized view. In outer join cases, this requires
 * the view maintenance graph.
 */
static void
apply_delta(char *old_enr, char *new_enr, MV_TriggerTable *table, Oid matviewOid,
			Tuplestorestate *old_tuplestores, Tuplestorestate *new_tuplestores,
			TupleDesc tupdesc_old, TupleDesc tupdesc_new,
			Query *query, bool use_count, char *count_colname)
{
	StringInfoData querybuf;
	StringInfoData target_list_buf;
	StringInfo	aggs_list_buf = NULL;
	StringInfo	aggs_set_old = NULL;
	StringInfo	aggs_set_new = NULL;
	Relation	matviewRel;
	char	   *matviewname;
	ListCell	*lc;
	int			i;
	List	   *keys = NIL;


	/*
	 * get names of the materialized view and delta tables
	 */

	matviewRel = table_open(matviewOid, NoLock);
	matviewname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(matviewRel)),
											 RelationGetRelationName(matviewRel));

	/*
	 * Build parts of the maintenance queries
	 */

	initStringInfo(&querybuf);
	initStringInfo(&target_list_buf);

	if (query->hasAggs)
	{
		if (old_tuplestores && tuplestore_tuple_count(old_tuplestores) > 0)
			aggs_set_old = makeStringInfo();
		if (new_tuplestores && tuplestore_tuple_count(new_tuplestores) > 0)
			aggs_set_new = makeStringInfo();
		aggs_list_buf = makeStringInfo();
	}

	/* build string of target list */
	for (i = 0; i < matviewRel->rd_att->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
		char   *resname = NameStr(attr->attname);
		if (i != 0)
			appendStringInfo(&target_list_buf, ", ");
		appendStringInfo(&target_list_buf, "%s", quote_qualified_identifier(NULL, resname));
	}

	i = 0;
	foreach (lc, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(lc);
		Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, i);
		char *resname = NameStr(attr->attname);

		i++;
		if (tle->resjunk)
			continue;

		/*
		 * For views without aggregates, all attributes are used as keys to identify a
		 * tuple in a view.
		 */
		if (!query->hasAggs)
			keys = lappend(keys, attr);

		/* For views with aggregates, we need to build SET clause for updating aggregate
		 * values. */
		if (query->hasAggs && IsA(tle->expr, Aggref))
		{
			Aggref *aggref = (Aggref *) tle->expr;
			const char *aggname = get_func_name(aggref->aggfnoid);

			/*
			 * We can use function names here because it is already checked if these
			 * can be used in IMMV by its OID at the definition time.
			 */

			/* count */
			if (!strcmp(aggname, "count"))
				append_set_clause_for_count(resname, aggs_set_old, aggs_set_new, aggs_list_buf);

			/* sum */
			else if (!strcmp(aggname, "sum"))
				append_set_clause_for_sum(resname, aggs_set_old, aggs_set_new, aggs_list_buf);

			/* avg */
			else if (!strcmp(aggname, "avg"))
				append_set_clause_for_avg(resname, aggs_set_old, aggs_set_new, aggs_list_buf,
										  format_type_be(aggref->aggtype));
			else
				elog(ERROR, "unsupported aggregate function: %s", aggname);
		}
	}
	/* If we have GROUP BY clause, we use its entries as keys. */
	if (query->hasAggs && query->groupClause)
	{
		foreach (lc, query->groupClause)
		{
			SortGroupClause *sgcl = (SortGroupClause *) lfirst(lc);
			TargetEntry		*tle = get_sortgroupclause_tle(sgcl, query->targetList);
			Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, tle->resno - 1);

			keys = lappend(keys, attr);
		}
	}
	/* Start maintaining the materialized view. */
	OpenMatViewIncrementalMaintenance();

	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/* For tuple deletion */
	if (old_tuplestores && tuplestore_tuple_count(old_tuplestores) > 0)
	{
		int				rc;
		EphemeralNamedRelation enr = palloc(sizeof(EphemeralNamedRelationData));
		/* convert tuplestores to ENR, and register for SPI */
		enr->md.name = pstrdup(old_enr);
		enr->md.reliddesc = matviewOid;
		enr->md.tupdesc = CreateTupleDescCopy(tupdesc_old);
		enr->md.enrtype = ENR_NAMED_TUPLESTORE;
		enr->md.enrtuples = tuplestore_tuple_count(old_tuplestores);
		enr->reldata = NULL;

		rc = SPI_register_relation(enr);
		if (rc != SPI_OK_REL_REGISTER)
			elog(ERROR, "SPI_register failed");

		elogif(Debug_print_ivm, INFO, "IVM apply old enr %s, command_count: %d", enr->md.name, gp_command_count);
		if (use_count)
			/* apply old delta and get rows to be recalculated */
			apply_old_delta_with_count(matviewname, RelationGetRelid(matviewRel), enr->md.name,
										keys, aggs_list_buf, aggs_set_old,
										count_colname);
		else
			apply_old_delta(matviewname, enr->md.name, keys);
	}
	/* For tuple insertion */
	if (new_tuplestores && tuplestore_tuple_count(new_tuplestores) > 0)
	{
		int rc;
		EphemeralNamedRelation enr = palloc(sizeof(EphemeralNamedRelationData));

		/* convert tuplestores to ENR, and register for SPI */
		enr->md.name = pstrdup(new_enr);
		enr->md.reliddesc = matviewOid;
		enr->md.tupdesc = CreateTupleDescCopy(tupdesc_new);
		enr->md.enrtype = ENR_NAMED_TUPLESTORE;
		enr->md.enrtuples = tuplestore_tuple_count(new_tuplestores);
		enr->reldata = NULL;

		rc = SPI_register_relation(enr);
		if (rc != SPI_OK_REL_REGISTER)
			elog(ERROR, "SPI_register failed");

		elogif(Debug_print_ivm, INFO, "IVM apply new enr %s, command_count: %d", enr->md.name, gp_command_count);
		/* apply new delta */
		if (use_count)
			apply_new_delta_with_count(matviewname, enr->md.name,
										keys, &target_list_buf,
										aggs_set_new, count_colname);
		else
			apply_new_delta(matviewname, enr->md.name, &target_list_buf);
	}

	/* We're done maintaining the materialized view. */
	CloseMatViewIncrementalMaintenance();

	table_close(matviewRel, NoLock);

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
}

/*
 * append_set_clause_for_count
 *
 * Append SET clause string for count aggregation to given buffers.
 * Also, append resnames required for calculating the aggregate value.
 */
static void
append_set_clause_for_count(const char *resname, StringInfo buf_old,
							StringInfo buf_new,StringInfo aggs_list)
{
	/* For tuple deletion */
	if (buf_old)
	{
		/* resname = mv.resname - t.resname */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_SUB, resname, "mv", "t", NULL, NULL));
	}
	/* For tuple insertion */
	if (buf_new)
	{
		/* resname = mv.resname + diff.resname */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_ADD, resname, "mv", "diff", NULL, NULL));
	}

	appendStringInfo(aggs_list, ", %s",
		quote_qualified_identifier("diff", resname)
	);
}

/*
 * append_set_clause_for_sum
 *
 * Append SET clause string for sum aggregation to given buffers.
 * Also, append resnames required for calculating the aggregate value.
 */
static void
append_set_clause_for_sum(const char *resname, StringInfo buf_old,
						  StringInfo buf_new, StringInfo aggs_list)
{
	char *count_col = IVM_colname("count", resname);

	/* For tuple deletion */
	if (buf_old)
	{
		/* sum = mv.sum - t.sum */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_SUB, resname, "mv", "t", count_col, NULL)
		);
		/* count = mv.count - t.count */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
		);
	}
	/* For tuple insertion */
	if (buf_new)
	{
		/* sum = mv.sum + diff.sum */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_ADD, resname, "mv", "diff", count_col, NULL)
		);
		/* count = mv.count + diff.count */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
		);
	}

	appendStringInfo(aggs_list, ", %s, %s",
		quote_qualified_identifier("diff", resname),
		quote_qualified_identifier("diff", IVM_colname("count", resname))
	);
}

/*
 * append_set_clause_for_avg
 *
 * Append SET clause string for avg aggregation to given buffers.
 * Also, append resnames required for calculating the aggregate value.
 */
static void
append_set_clause_for_avg(const char *resname, StringInfo buf_old,
						  StringInfo buf_new, StringInfo aggs_list,
						  const char *aggtype)
{
	char *sum_col = IVM_colname("sum", resname);
	char *count_col = IVM_colname("count", resname);

	/* For tuple deletion */
	if (buf_old)
	{
		/* avg = (mv.sum - t.sum)::aggtype / (mv.count - t.count) */
		appendStringInfo(buf_old,
			", %s = %s OPERATOR(pg_catalog./) %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_SUB, sum_col, "mv", "t", count_col, aggtype),
			get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
		);
		/* sum = mv.sum - t.sum */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, sum_col),
			get_operation_string(IVM_SUB, sum_col, "mv", "t", count_col, NULL)
		);
		/* count = mv.count - t.count */
		appendStringInfo(buf_old,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_SUB, count_col, "mv", "t", NULL, NULL)
		);

	}
	/* For tuple insertion */
	if (buf_new)
	{
		/* avg = (mv.sum + diff.sum)::aggtype / (mv.count + diff.count) */
		appendStringInfo(buf_new,
			", %s = %s OPERATOR(pg_catalog./) %s",
			quote_qualified_identifier(NULL, resname),
			get_operation_string(IVM_ADD, sum_col, "mv", "diff", count_col, aggtype),
			get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
		);
		/* sum = mv.sum + diff.sum */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, sum_col),
			get_operation_string(IVM_ADD, sum_col, "mv", "diff", count_col, NULL)
		);
		/* count = mv.count + diff.count */
		appendStringInfo(buf_new,
			", %s = %s",
			quote_qualified_identifier(NULL, count_col),
			get_operation_string(IVM_ADD, count_col, "mv", "diff", NULL, NULL)
		);
	}

	appendStringInfo(aggs_list, ", %s, %s, %s",
		quote_qualified_identifier("diff", resname),
		quote_qualified_identifier("diff", IVM_colname("sum", resname)),
		quote_qualified_identifier("diff", IVM_colname("count", resname))
	);
}

/*
 * get_operation_string
 *
 * Build a string to calculate the new aggregate values.
 */
static char *
get_operation_string(IvmOp op, const char *col, const char *arg1, const char *arg2,
					 const char* count_col, const char *castType)
{
	StringInfoData buf;
	StringInfoData castString;
	char   *col1 = quote_qualified_identifier(arg1, col);
	char   *col2 = quote_qualified_identifier(arg2, col);
	char	op_char = (op == IVM_SUB ? '-' : '+');

	initStringInfo(&buf);
	initStringInfo(&castString);

	if (castType)
		appendStringInfo(&castString, "::%s", castType);

	if (!count_col)
	{
		/*
		 * If the attributes don't have count columns then calc the result
		 * by using the operator simply.
		 */
		appendStringInfo(&buf, "(%s OPERATOR(pg_catalog.%c) %s)%s",
			col1, op_char, col2, castString.data);
	}
	else
	{
		/*
		 * If the attributes have count columns then consider the condition
		 * where the result becomes NULL.
		 */
		char *null_cond = get_null_condition_string(op, arg1, arg2, count_col);

		appendStringInfo(&buf,
			"(CASE WHEN %s THEN NULL "
				"WHEN %s IS NULL THEN %s "
				"WHEN %s IS NULL THEN %s "
				"ELSE (%s OPERATOR(pg_catalog.%c) %s)%s END)",
			null_cond,
			col1, col2,
			col2, col1,
			col1, op_char, col2, castString.data
		);
	}

	return buf.data;
}

/*
 * get_null_condition_string
 *
 * Build a predicate string for CASE clause to check if an aggregate value
 * will become NULL after the given operation is applied.
 */
static char *
get_null_condition_string(IvmOp op, const char *arg1, const char *arg2,
						  const char* count_col)
{
	StringInfoData null_cond;
	initStringInfo(&null_cond);

	switch (op)
	{
		case IVM_ADD:
			appendStringInfo(&null_cond,
				"%s OPERATOR(pg_catalog.=) 0 AND %s OPERATOR(pg_catalog.=) 0",
				quote_qualified_identifier(arg1, count_col),
				quote_qualified_identifier(arg2, count_col)
			);
			break;
		case IVM_SUB:
			appendStringInfo(&null_cond,
				"%s OPERATOR(pg_catalog.=) %s",
				quote_qualified_identifier(arg1, count_col),
				quote_qualified_identifier(arg2, count_col)
			);
			break;
		default:
			elog(ERROR,"unknown operation");
	}

	return null_cond.data;
}


/*
 * apply_old_delta_with_count
 *
 * Execute a query for applying a delta table given by deltaname_old
 * which contains tuples to be deleted from a materialized view given by
 * matviewname.  This is used when counting is required, that is, the view
 * has aggregate or distinct.
 *
 * If the view doesn't have aggregates or has GROUP BY, this requires a keys
 * list to identify a tuple in the view. If the view has aggregates, this
 * requires strings representing resnames of aggregates and SET clause for
 * updating aggregate values.
 */
static void
apply_old_delta_with_count(const char *matviewname, Oid matviewRelid, const char *deltaname_old,
				List *keys, StringInfo aggs_list, StringInfo aggs_set,
				const char *count_colname)
{
	const char * tempTableName;

	StringInfoData	querybuf;
	char   *match_cond;
	bool	agg_without_groupby = (list_length(keys) == 0);

	/* build WHERE condition for searching tuples to be deleted */
	match_cond = get_matching_condition_string(keys);

/* CBDB_IVM_FIXME CBDB does not support multiple-write CTE. Revert to original
 * query when it will be supported.
 */
#if 0
	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					"WITH t AS ("			/* collecting tid of target tuples in the view */
						"SELECT diff.%s, "			/* count column */
								"(diff.%s OPERATOR(pg_catalog.=) mv.%s AND %s) AS for_dlt, "
								"mv.ctid "
								"%s "				/* aggregate columns */
						"FROM %s AS mv, %s AS diff "
						"WHERE %s"					/* tuple matching condition */
					"), updt AS ("			/* update a tuple if this is not to be deleted */
						"UPDATE %s AS mv SET %s = mv.%s OPERATOR(pg_catalog.-) t.%s "
											"%s"	/* SET clauses for aggregates */
						"FROM t WHERE mv.ctid OPERATOR(pg_catalog.=) t.ctid AND NOT for_dlt "
					")"
					/* delete a tuple if this is to be deleted */
					"DELETE FROM %s AS mv USING t "
					"WHERE mv.ctid OPERATOR(pg_catalog.=) t.ctid AND for_dlt",
					count_colname,
					count_colname, count_colname, (agg_without_groupby ? "false" : "true"),
					(aggs_list != NULL ? aggs_list->data : ""),
					matviewname, deltaname_old,
					match_cond,
					matviewname, count_colname, count_colname, count_colname,
					(aggs_set != NULL ? aggs_set->data : ""),
					matviewname);
#else
	/* CBDB_IVM_FIXME: use tuplestore to replace temp table. */
	tempTableName = MakeDeltaName("temp_old_delta", matviewRelid, gp_command_count);

	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					"CREATE TEMP TABLE %s AS SELECT diff.%s, "			/* count column */
								"(diff.%s OPERATOR(pg_catalog.=) mv.%s AND %s) AS for_dlt, "
								"mv.ctid AS tid, mv.gp_segment_id AS gid"
								"%s "				/* aggregate columns */
						"FROM %s AS mv, %s AS diff "
						"WHERE %s DISTRIBUTED RANDOMLY",	/* tuple matching condition */
					tempTableName,
					count_colname,
					count_colname, count_colname, (agg_without_groupby ? "false" : "true"),
					(aggs_list != NULL ? aggs_list->data : ""),
					matviewname, deltaname_old,
					match_cond);

	/* Create the temporary table. */
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	elogif(Debug_print_ivm, INFO, "IVM apply_old_delta_with_count select: %s", querybuf.data);

	/* Search for matching tuples from the view and update or delete if found. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					"UPDATE %s AS mv SET %s = mv.%s OPERATOR(pg_catalog.-) t.%s "
											"%s"	/* SET clauses for aggregates */
						"FROM %s t WHERE mv.ctid OPERATOR(pg_catalog.=) t.tid"
						" AND mv.gp_segment_id OPERATOR(pg_catalog.=) t.gid"
						" AND NOT for_dlt ",
					matviewname, count_colname, count_colname, count_colname,
					(aggs_set != NULL ? aggs_set->data : ""), tempTableName);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UPDATE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	elogif(Debug_print_ivm, INFO, "IVM apply_old_delta_with_count update: %s", querybuf.data);

	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					"DELETE FROM %s AS mv USING %s t "
					"WHERE mv.ctid OPERATOR(pg_catalog.=) t.tid"
					" AND mv.gp_segment_id OPERATOR(pg_catalog.=) t.gid"
					" AND for_dlt",
					matviewname, tempTableName);
#endif
	if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	elogif(Debug_print_ivm, INFO, "IVM apply_old_delta_with_count delete: %s", querybuf.data);

	/* Clean up temp tables. */
	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf, "DROP TABLE %s", tempTableName);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UTILITY)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
}

/*
 * apply_old_delta
 *
 * Execute a query for applying a delta table given by deltaname_old
 * which contains tuples to be deleted from a materialized view given by
 * matviewname.  This is used when counting is not required.
 */
static void
apply_old_delta(const char *matviewname, const char *deltaname_old,
				List *keys)
{
	StringInfoData	querybuf;
	StringInfoData	keysbuf;
	char   *match_cond;
	ListCell *lc;

	/* build WHERE condition for searching tuples to be deleted */
	match_cond = get_matching_condition_string(keys);

	/* build string of keys list */
	initStringInfo(&keysbuf);
	foreach (lc, keys)
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
		char   *resname = NameStr(attr->attname);
		appendStringInfo(&keysbuf, "%s", quote_qualified_identifier("mv", resname));
		if (lnext(keys, lc))
			appendStringInfo(&keysbuf, ", ");
	}

	/* Search for matching tuples from the view and update or delete if found. */
	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
	"DELETE FROM %s WHERE (ctid, gp_segment_id) IN ("
		"SELECT tid, sid FROM (SELECT pg_catalog.row_number() over (partition by %s) AS \"__ivm_row_number__\","
								  "mv.ctid AS tid, mv.gp_segment_id as sid,"
								  "diff.\"__ivm_count__\""
						 "FROM %s AS mv, %s AS diff "
						 "WHERE %s) v "
					"WHERE v.\"__ivm_row_number__\" OPERATOR(pg_catalog.<=) v.\"__ivm_count__\")",
					matviewname,
					keysbuf.data,
					matviewname, deltaname_old,
					match_cond);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_DELETE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	elogif(Debug_print_ivm, INFO, "IVM apply_old_delta delete: %s", querybuf.data);
}

/*
 * apply_new_delta_with_count
 *
 * Execute a query for applying a delta table given by deltaname_new
 * which contains tuples to be inserted into a materialized view given by
 * matviewname.  This is used when counting is required, that is, the view
 * has aggregate or distinct. Also, when a table in EXISTS sub queries
 * is modified.
 *
 * If the view doesn't have aggregates or has GROUP BY, this requires a keys
 * list to identify a tuple in the view. If the view has aggregates, this
 * requires strings representing SET clause for updating aggregate values.
 */
static void
apply_new_delta_with_count(const char *matviewname, const char* deltaname_new,
				List *keys, StringInfo target_list, StringInfo aggs_set,
				const char* count_colname)
{
	StringInfoData	querybuf;
	StringInfoData	returning_keys;
	ListCell	*lc;
	char	*match_cond = "";

	/* build WHERE condition for searching tuples to be updated */
	match_cond = get_matching_condition_string(keys);

	/* build string of keys list */
	initStringInfo(&returning_keys);
	if (keys)
	{
		foreach (lc, keys)
		{
			Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
			char   *resname = NameStr(attr->attname);
			appendStringInfo(&returning_keys, "%s", quote_qualified_identifier("mv", resname));
			if (lnext(keys, lc))
				appendStringInfo(&returning_keys, ", ");
		}
	}
	else
		appendStringInfo(&returning_keys, "NULL");

	/* Search for matching tuples from the view and update if found or insert if not. */
	initStringInfo(&querybuf);
#if 0
	appendStringInfo(&querybuf,
					"WITH updt AS ("		/* update a tuple if this exists in the view */
						"UPDATE %s AS mv SET %s = mv.%s OPERATOR(pg_catalog.+) diff.%s "
											"%s "	/* SET clauses for aggregates */
						"FROM %s AS diff "
						"WHERE %s "					/* tuple matching condition */
						"RETURNING %s"				/* returning keys of updated tuples */
					") INSERT INTO %s (%s)"	/* insert a new tuple if this doesn't exist */
						"SELECT %s FROM %s AS diff "
						"WHERE NOT EXISTS (SELECT 1 FROM updt AS mv WHERE %s);",
					matviewname, count_colname, count_colname, count_colname,
					(aggs_set != NULL ? aggs_set->data : ""),
					deltaname_new,
					match_cond,
					returning_keys.data,
					matviewname, target_list->data,
					target_list->data, deltaname_new,
					match_cond);
#else
	appendStringInfo(&querybuf,
					"UPDATE %s AS mv SET %s = mv.%s OPERATOR(pg_catalog.+) diff.%s "
											"%s "	/* SET clauses for aggregates */
						"FROM %s AS diff "
						"WHERE %s ",					/* tuple matching condition */
					matviewname, count_colname, count_colname, count_colname,
					(aggs_set != NULL ? aggs_set->data : ""),
					deltaname_new,
					match_cond);
	if (SPI_exec(querybuf.data, 0) != SPI_OK_UPDATE)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	elogif(Debug_print_ivm, INFO, "IVM apply_new_delta_with_count update: %s", querybuf.data);

	resetStringInfo(&querybuf);
	appendStringInfo(&querybuf,
					"INSERT INTO %s (%s)"	/* insert a new tuple if this doesn't exist */
						"SELECT %s FROM %s AS diff "
						"WHERE NOT EXISTS (SELECT 1 FROM %s AS mv WHERE %s);",
					matviewname, target_list->data,
					target_list->data, deltaname_new,
					matviewname, match_cond);
#endif
	if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	elogif(Debug_print_ivm,INFO, "IVM apply_new_delta_with_count insert: %s", querybuf.data);
}

/*
 * apply_new_delta
 *
 * Execute a query for applying a delta table given by deltaname_new
 * which contains tuples to be inserted into a materialized view given by
 * matviewname.  This is used when counting is not required.
 */
static void
apply_new_delta(const char *matviewname, const char *deltaname_new,
				StringInfo target_list)
{
	StringInfoData	querybuf;

	/* Search for matching tuples from the view and update or delete if found. */
	initStringInfo(&querybuf);

	appendStringInfo(&querybuf,
					"INSERT INTO %s (%s) SELECT %s FROM ("
						"SELECT diff.*, pg_catalog.generate_series(1, diff.\"__ivm_count__\")"
							" AS __ivm_generate_series__ "
						"FROM %s AS diff) AS v",
					matviewname, target_list->data, target_list->data,
					deltaname_new);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_INSERT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);
	elogif(Debug_print_ivm, INFO, "IVM apply_new_delta: %s", querybuf.data);
}

/*
 * get_matching_condition_string
 *
 * Build a predicate string for looking for a tuple with given keys.
 */
static char *
get_matching_condition_string(List *keys)
{
	StringInfoData match_cond;
	ListCell	*lc;

	/* If there is no key columns, the condition is always true. */
	if (keys == NIL)
		return "true";

	initStringInfo(&match_cond);
	foreach (lc, keys)
	{
		Form_pg_attribute attr = (Form_pg_attribute) lfirst(lc);
		char   *resname = NameStr(attr->attname);
		char   *mv_resname = quote_qualified_identifier("mv", resname);
		char   *diff_resname = quote_qualified_identifier("diff", resname);
		Oid		typid = attr->atttypid;

		/* Considering NULL values, we can not use simple = operator. */
		appendStringInfo(&match_cond, "(");
		generate_equal(&match_cond, typid, mv_resname, diff_resname);
		appendStringInfo(&match_cond, " OR (%s IS NULL AND %s IS NULL))",
						 mv_resname, diff_resname);

		if (lnext(keys, lc))
			appendStringInfo(&match_cond, " AND ");
	}

	return match_cond.data;
}

/*
 * generate_equal
 *
 * Generate an equality clause using given operands' default equality
 * operator.
 */
static void
generate_equal(StringInfo querybuf, Oid opttype,
			   const char *leftop, const char *rightop)
{
	TypeCacheEntry *typentry;

	typentry = lookup_type_cache(opttype, TYPECACHE_EQ_OPR);
	if (!OidIsValid(typentry->eq_opr))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_FUNCTION),
				 errmsg("could not identify an equality operator for type %s",
						format_type_be_qualified(opttype))));

	generate_operator_clause(querybuf,
							 leftop, opttype,
							 typentry->eq_opr,
							 rightop, opttype);
}

/*
 * mv_InitHashTables
 */
void
mv_InitHashTables(void)
{
	HASHCTL		info, ctl;

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(Oid);
	info.entrysize = sizeof(MV_TriggerHashEntry);
	mv_trigger_info = hash_create("MV trigger info",
								 MV_INIT_QUERYHASHSIZE,
								 &info, HASH_ELEM | HASH_BLOBS);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = SNAPSHOT_KEYSIZE;
	ctl.entrysize = sizeof(SnapshotDumpEntry);
	ctl.hash = string_hash;
	mv_trigger_snapshot = ShmemInitHash("MV trigger snapshot",
								 MV_INIT_SNAPSHOTHASHSIZE,
								 MV_INIT_SNAPSHOTHASHSIZE,
								 &ctl, HASH_ELEM | HASH_FUNCTION);

}

Size
mv_TableShmemSize(void)
{
	return hash_estimate_size(MV_INIT_SNAPSHOTHASHSIZE, sizeof(SnapshotDumpEntry));
}


/*
 * AtAbort_IVM
 *
 * Clean up hash entries for all materialized views. This is called at
 * transaction abort.
 */
void
AtAbort_IVM()
{
	AtEOXact_IVM(false);
}

void
AtEOXact_IVM(bool isCommit)
{
	ListCell *lc;
	List	*mvlist = AfterTriggerGetMvList();

	foreach(lc, mvlist)
	{
		Oid matviewOid = lfirst_oid(lc);
		{
			MV_TriggerHashEntry *entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
													  (void *) &matviewOid,
													  HASH_FIND, NULL);
			if (entry != NULL)
			{
				entry->reference--;
				ResourceOwner resowner = entry->resowner;
				MemoryContext ctx = entry->context;
				if (entry->reference == 0)
				{
					clean_up_IVM_hash_entry(entry, isCommit);
				}
				if (resowner)
				{
					ResourceOwnerRelease(resowner,
										RESOURCE_RELEASE_BEFORE_LOCKS,
										false, /* isCommit */
										false); /* isTopLevel */
					ResourceOwnerRelease(resowner,
										RESOURCE_RELEASE_LOCKS,
										false, /* isCommit */
										false); /* isTopLevel */
					ResourceOwnerRelease(resowner,
										RESOURCE_RELEASE_AFTER_LOCKS,
										false, /* isCommit */
										false); /* isTopLevel */
					ResourceOwnerDelete(resowner);
				}
				if (ctx)
				{
					MemoryContextReset(ctx);
					MemoryContextDelete(ctx);
				}
			}
		}
	}
}

/*
 * clean_up_IVM_hash_entry
 *
 * Clean up tuple stores and hash entries for a materialized view after its
 * maintenance finished.
 */
static void
clean_up_IVM_hash_entry(MV_TriggerHashEntry *entry, bool is_abort)
{
	ListCell *lc;

	foreach(lc, entry->tables)
	{
		MV_TriggerTable *table = (MV_TriggerTable *) lfirst(lc);

		if (table->old_tuplestores)
		{
			list_free(table->old_tuplestores);
			table->old_tuplestores = NIL;
		}
		if (table->new_tuplestores)
		{
			list_free(table->new_tuplestores);
			table->new_tuplestores = NIL;
		}
		if (!is_abort)
		{
			if (CurrentResourceOwner == entry->resowner)
			{
				if (table->slot) 
				{
					ExecDropSingleTupleTableSlot(table->slot);
					table->slot = NULL;
				}
				if (table->rel)
				{
					table_close(table->rel, NoLock);
					table->rel = NULL;
				}
			}
		}
	}

	if (entry->tables)
	{
		list_free(entry->tables);
		entry->tables = NIL;
	}
	clean_up_ivm_dsm_entry(entry);

	hash_search(mv_trigger_info, (void *) &entry->matview_id, HASH_REMOVE, NULL);
}

/*
 * clean_up_ivm_dsm_entry
 *
 * This function is responsible for cleaning up the DSM entry associated with
 * a given materialized view trigger hash entry.
 */
static void
clean_up_ivm_dsm_entry(MV_TriggerHashEntry *entry)
{
	if (entry->snapname && entry->snapname[0] != '\0' && Gp_is_writer)
	{
		bool 				found;
		SnapshotDumpEntry	*pDump;
		LWLockAcquire(GPIVMResLock, LW_EXCLUSIVE);
		pDump = (SnapshotDumpEntry *) hash_search(mv_trigger_snapshot,
													(void *)entry->snapname,
													HASH_FIND, &found);
		if (found)
		{
			/* Only creater can detach the dsm segment. */
			if (pDump->handle && pDump->pid == MyProcPid)
			{
				Assert(entry->matview_id == pDump->matview_id);
				dsm_detach(pDump->segment);
				pDump->handle = DSM_HANDLE_INVALID;
			}
			hash_search(mv_trigger_snapshot, (void *)entry->snapname, HASH_REMOVE, NULL);
			entry->snapname = NULL;
		}
		LWLockRelease(GPIVMResLock);
	}
}

/*
 * isIvmName
 *
 * Check if this is an IVM hidden column from the name.
 */
bool
isIvmName(const char *s)
{
	if (s)
		return (strncmp(s, "__ivm_", 6) == 0);
	return false;
}

/*
 * This function is responsible for refreshing the materialized view.
 * It fills the memory with the result of the query execution.
 * The result is then sent to the specified destination receiver.
 */
static uint64
refresh_matview_memoryfill(DestReceiver *dest, Query *query,
						 QueryEnvironment *queryEnv,
						 TupleDesc *resultTupleDesc,
						 const char *queryString, Relation matviewRel)
{
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	Query	   *copied_query;
	uint64		processed;

	List       *saved_dispatch_oids = GetAssignedOidsForDispatch();

	/* Lock and rewrite, using a copy to preserve the original query. */
	copied_query = copyObject(query);
	AcquireRewriteLocks(copied_query, true, false);
	rewritten = QueryRewrite(copied_query);

	/* SELECT should never rewrite to more or less than one SELECT query */
	if (list_length(rewritten) != 1)
		elog(ERROR, "unexpected rewrite result for refresh_matview_memoryfill.");
	query = (Query *) linitial(rewritten);
	query->parentStmtType = PARENTSTMTTYPE_CTAS;

	query->intoPolicy = matviewRel->rd_cdbpolicy;

	/* Check for user-requested abort. */
	CHECK_FOR_INTERRUPTS();
	/* Close parallel insert to tuplestore. */
	plan = pg_plan_query(query, queryString, CURSOR_OPT_PARALLEL_OK, NULL);

	plan->intoClause = makeIvmIntoClause(queryString, matviewRel);
	plan->refreshClause = NULL;
	/*
	 * Use a snapshot with an updated command ID to ensure this query sees
	 * results of any previously executed queries.  (This could only matter if
	 * the planner executed an allegedly-stable function that changed the
	 * database contents, but let's do it anyway to be safe.)
	 */
	PushCopiedSnapshot(GetActiveSnapshot());
	UpdateActiveSnapshotCommandId();

	/* Create a QueryDesc, redirecting output to our tuple receiver */
	queryDesc = CreateQueryDesc(plan, queryString,
								GetActiveSnapshot(), InvalidSnapshot,
								dest, NULL, queryEnv ? queryEnv: NULL, 0);

	RestoreOidAssignments(saved_dispatch_oids);

	/* call ExecutorStart to prepare the plan for execution */
	ExecutorStart(queryDesc, 0);

	/* run the plan */
	ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

	if (resultTupleDesc)
		*resultTupleDesc = CreateTupleDescCopy(queryDesc->tupDesc);

	/* and clean up */
	ExecutorFinish(queryDesc);
	ExecutorEnd(queryDesc);

	processed = queryDesc->es_processed;

	FreeQueryDesc(queryDesc);

	PopActiveSnapshot();

	elogif(Debug_print_ivm, INFO, "IVM processed %s, %lu tuples.", queryString, processed);

	return processed;
}

/*
 *
 * Add a preassigned materialized view entry to the hash table.
 */
void
AddPreassignedMVEntry(Oid matview_id, Oid table_id, const char* snapname)
{
	MV_TriggerTable *table;
	bool found;
	ListCell	*lc;
	MV_TriggerHashEntry *entry;
	MemoryContext oldcxt;
	ResourceOwner oldowner;
	int len = strlen(snapname);

	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matview_id,
											  HASH_ENTER, &found);
	if (!found)
	{
		entry->context = AllocSetContextCreate(TopMemoryContext,
													"IVM Reader Session",
													ALLOCSET_DEFAULT_SIZES);
		entry->resowner = ResourceOwnerCreate(TopTransactionResourceOwner, "IVM Reader Session");
		entry->matview_id = matview_id;
		entry->reference = 1;
		entry->tables = NIL;
		entry->has_old = false;
		entry->has_new = false;
		entry->pid = MyProcPid;
		entry->snapshot = NULL;
		entry->snapname = NULL;
	}

	/* Switch to the new resource owner and memory context */
	oldowner = CurrentResourceOwner;
	CurrentResourceOwner = entry->resowner;
	oldcxt = MemoryContextSwitchTo(entry->context);

	/* Copy the snapshot name */
	if (entry->snapname)
		strncpy(entry->snapname, snapname, len);
	else
		entry->snapname = pstrdup(snapname);
	entry->snapname[len] = '\0';

	/* Import the snapshot */
	entry->snapshot = ivm_import_snapshot(snapname);

	Assert(entry->snapshot);

	found = false;
	foreach(lc, entry->tables)
	{
		table = (MV_TriggerTable *) lfirst(lc);
		if (table->table_id == table_id)
		{
			found = true;
			break;
		}
	}
	if (!found)
	{
		table = (MV_TriggerTable *) palloc0(sizeof(MV_TriggerTable));
		entry->tables = lappend(entry->tables, table);
	}
	/* Switch back to the old memory context and resource owner */
	MemoryContextSwitchTo(oldcxt);
	CurrentResourceOwner = oldowner;

	AfterTriggerAppendMvList(matview_id);
	return;
}

/*
 * ivm_immediate_cleanup
 *
 * Clean up hash entries for a materialized view after its
 * maintenance finished.
 */
Datum
ivm_immediate_cleanup(PG_FUNCTION_ARGS)
{
	Oid matview_id = PG_GETARG_OID(0);
	bool result = true;
	ResourceOwner resowner;
	MemoryContext ctx;

	MV_TriggerHashEntry *entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
													  (void *) &matview_id,
													  HASH_FIND, NULL);
	if (entry != NULL)
	{
		resowner = entry->resowner;
		ctx = entry->context;
		clean_up_IVM_hash_entry(entry, true);
		if (resowner)
		{
			ResourceOwnerRelease(resowner,
								RESOURCE_RELEASE_BEFORE_LOCKS,
								false, /* isCommit */
								false); /* isTopLevel */
			ResourceOwnerRelease(resowner,
								RESOURCE_RELEASE_LOCKS,
								false, /* isCommit */
								false); /* isTopLevel */
			ResourceOwnerRelease(resowner,
								RESOURCE_RELEASE_AFTER_LOCKS,
								false, /* isCommit */
								false); /* isTopLevel */
		}
		if (ctx)
		{
			MemoryContextReset(ctx);
			MemoryContextDelete(ctx);
		}
	}

	PG_RETURN_BOOL(result);
}

/*
 * apply_cleanup
 *
 * Apply immediate cleanup for a materialized view.
 */
static void
apply_cleanup(Oid matview_id)
{
	StringInfoData	querybuf;
	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
			"SELECT pg_catalog.ivm_immediate_cleanup(%d::pg_catalog.oid)",
				matview_id);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	elogif(Debug_print_ivm, INFO, "IVM apply_cleanup: %s", querybuf.data);

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");

	return;
}

/*
 * ivm_export_snapshot
 *
 * Export a snapshot in QEs.
 *
 * Parameters:
 * - matview_id: the OID of the materialized view
 * - snapname: the name of the snapshot to export
 */
static void
ivm_export_snapshot(Oid matview_id, char *snapname)
{
	StringInfoData	querybuf;
	/* Open SPI context. */
	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	initStringInfo(&querybuf);
	appendStringInfo(&querybuf,
			"SELECT pg_catalog.pg_export_snapshot_def(%d::pg_catalog.oid, '%s')", matview_id, snapname);

	if (SPI_exec(querybuf.data, 0) != SPI_OK_SELECT)
		elog(ERROR, "SPI_exec failed: %s", querybuf.data);

	elogif(Debug_print_ivm, INFO, "IVM ivm_export_snapshot: %s", querybuf.data);

	/* Close SPI context. */
	if (SPI_finish() != SPI_OK_FINISH)
		elog(ERROR, "SPI_finish failed");
	return;
}

/*
 * pg_export_snapshot_def
 *		SQL-callable wrapper for export snapshot.
 */
Datum
pg_export_snapshot_def(PG_FUNCTION_ARGS)
{
	bool 	found;
	Oid 	matview_id = PG_GETARG_OID(0);
	char	*snapname = text_to_cstring(PG_GETARG_TEXT_PP(1));
	Snapshot snapshot = GetActiveSnapshot();
	SnapshotDumpEntry	*pDump;
	dsm_segment *segment;
	Assert(snapshot->snapshot_type == SNAPSHOT_MVCC);
	Assert(snapname);

	LWLockAcquire(GPIVMResLock, LW_EXCLUSIVE);
	pDump = (SnapshotDumpEntry *) hash_search(mv_trigger_snapshot,
											  (void *) snapname,
											  HASH_ENTER_NULL,
											  &found);
	if (!pDump)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of shared memory"),
				 errhint("out of cross-slice trigger snapshot slots.")));

	if (!found)
	{
		Size sz = EstimateSnapshotSpace(snapshot);
		segment = dsm_create(sz, 0);

		char *ptr = dsm_segment_address(segment);
		SerializeSnapshot(snapshot, ptr);

		pDump->segment = segment;
		pDump->handle = dsm_segment_handle(segment);
		pDump->pid = MyProcPid;
		pDump->matview_id = matview_id;

		dsm_pin_mapping(segment);
	}
	LWLockRelease(GPIVMResLock);

	PG_RETURN_TEXT_P(cstring_to_text(snapname));
}


/*
 * ExecuteTruncateGuts_IVM
 *
 * This function truncates the given materialized view and its dependent
 * objects, and then regenerates the data for the materialized view.
 *
 * Parameters:
 * - matviewRel: the relation of the materialized view to truncate
 * - matviewOid: the OID of the materialized view to truncate
 * - query: the query to use to regenerate the data for the materialized view
 */
static void
ExecuteTruncateGuts_IVM(Relation matviewRel,
						Oid matviewOid,
						Query *query)
{
	Oid			OIDNewHeap;
	DestReceiver *dest;
	uint64		processed = 0;
	Query		*dataQuery = rewriteQueryForIMMV(query, NIL);
	char		relpersistence = matviewRel->rd_rel->relpersistence;
	RefreshClause *refreshClause;
	/*
	* Create the transient table that will receive the regenerated data. Lock
	* it against access by any other process until commit (by which time it
	* will be gone).
	*/
	OIDNewHeap = make_new_heap(matviewOid, matviewRel->rd_rel->reltablespace, matviewRel->rd_rel->relam,
								relpersistence, ExclusiveLock, false, true);
	LockRelationOid(OIDNewHeap, AccessExclusiveLock);
	dest = CreateTransientRelDestReceiver(OIDNewHeap, matviewOid, false,
										relpersistence, false);

	RangeVar *relation = makeRangeVar(get_namespace_name(get_rel_namespace(matviewOid)),
										get_rel_name(matviewOid), -1);
	refreshClause = MakeRefreshClause(false, false, relation);

	dataQuery->intoPolicy = matviewRel->rd_cdbpolicy;
	dataQuery->parentStmtType = PARENTSTMTTYPE_REFRESH_MATVIEW;
	/* Generate the data */
	processed = refresh_matview_datafill(dest, dataQuery, "truncate", refreshClause);
	refresh_by_heap_swap(matviewOid, OIDNewHeap, relpersistence);
}


/*
 * ivm_import_snapshot
 *
 * This function imports a snapshot from the hash table based on the given name.
 *
 * Parameters:
 * - idstr: a string representing the id of the snapshot to import
 *
 * Returns:
 * - the imported snapshot
 */
static Snapshot
ivm_import_snapshot(const char *idstr)
{
	bool found;
	SnapshotDumpEntry *pDump;
	Snapshot snapshot = NULL;

	LWLockAcquire(GPIVMResLock, LW_SHARED);
	pDump = hash_search(mv_trigger_snapshot, (void*)idstr, HASH_FIND, &found);

	Assert (found && pDump != NULL);
	if (found)
	{
		Assert(pDump);
		if (pDump->pid == MyProcPid)
		{
			char *ptr = dsm_segment_address(pDump->segment);
			snapshot = RestoreSnapshot(ptr);
		}
		else
		{
			dsm_segment* segment = dsm_attach(pDump->handle);
			char *ptr = dsm_segment_address(segment);
			snapshot = RestoreSnapshot(ptr);
			dsm_detach(segment);
		}
	}
	LWLockRelease(GPIVMResLock);

	return snapshot;
}

/*
 * makeIvmIntoClause
 *
 * This function creates an IntoClause node for IVM (Incremental View Maintenance).
 * It sets the 'ivm' field to true, 'rel' field to NULL, 'enrname' field to the input enrname,
 * 'distributedBy' field to the distribution policy of the input relation (matviewRel), and 'matviewOid' field
 * to the OID of the input relation.
 *
 * Parameters:
 * - enrname: a string representing the named tuplestore
 * - matviewRel: a relation object representing the materialized view
 *
 * Returns:
 * - a pointer to the created IntoClause node
 */
static IntoClause*
makeIvmIntoClause(const char *enrname, Relation matviewRel)
{
	IntoClause *intoClause = makeNode(IntoClause);
	intoClause->ivm = true;
	/* rel is NULL means putting tuples into memory.*/
	intoClause->rel = NULL;
	intoClause->enrname = (char*) enrname;
	intoClause->distributedBy = (Node*) make_distributedby_for_rel(matviewRel);
	intoClause->matviewOid = RelationGetRelid(matviewRel);
	return intoClause;
}

/*
 * transientenr_init
 *
 * This function initializes the transientenr.
 *
 * Parameters:
 * - queryDesc: the query descriptor
 */
void
transientenr_init(QueryDesc *queryDesc)
{
	MV_TriggerHashEntry *entry;
	bool found;
	IntoClause *into = queryDesc->plannedstmt->intoClause;
	Oid matviewOid = into->matviewOid;

	entry = (MV_TriggerHashEntry *) hash_search(mv_trigger_info,
											  (void *) &matviewOid,
											  HASH_FIND, &found);
	Assert (found && entry != NULL);
	Assert (into->ivm);

	queryDesc->dest = CreateDestReceiver(DestPersistentstore);
	SetPersistentTstoreDestReceiverParams(queryDesc->dest,
											NULL,
											entry->resowner,
											entry->context,
											true,
											into->enrname);
}

/*
 * ivm_set_ts_persitent_name
 *
 * This function sets the transition table file name for the IVM trigger.
 */
static void
ivm_set_ts_persitent_name(TriggerData *trigdata, Oid relid, Oid mvid)
{
	MemoryContext oldctx;
	CmdType cmd;

	if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		cmd = CMD_INSERT;
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		cmd = CMD_DELETE;
	else if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		cmd = CMD_UPDATE;
	else
	{
		AfterTriggerAppendMvList(mvid);
		return;
	}
	oldctx = MemoryContextSwitchTo(TopMemoryContext);
	SetTransitionTableName(relid, cmd, mvid);
	MemoryContextSwitchTo(oldctx);
}

/*-------------------------------------------------------------------------
 *
 * vacuum.c
 *	  The postgres vacuum cleaner.
 *
 * This file now includes only control and dispatch code for VACUUM and
 * ANALYZE commands.  Regular VACUUM is implemented in vacuumlazy.c,
 * ANALYZE in analyze.c, and VACUUM FULL is a variant of CLUSTER, handled
 * in cluster.c.
 *
 * Also have a look at vacuum_ao.c, which contains VACUUM related code for
 * Append-Optimized tables.
 *
 * Portions Copyright (c) 2005-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2020, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/vacuum.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/clog.h"
#include "access/commit_ts.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/multixact.h"
#include "access/tableam.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/gp_matview_aux.h"
#include "catalog/namespace.h"
#include "catalog/partition.h"
#include "catalog/pg_database.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "commands/cluster.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/vacuum.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "postmaster/bgworker_internals.h"
#include "storage/bufmgr.h"
#include "storage/lmgr.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "access/appendonlywriter.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/pg_am.h"
#include "catalog/pg_appendonly.h"
#include "catalog/oid_dispatch.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "commands/analyzeutils.h"
#include "libpq-int.h"
#include "libpq/pqformat.h"
#include "utils/faultinjector.h"
#include "utils/lsyscache.h"
#include "utils/resgroup.h"


typedef struct VacuumStatsContext
{
	List		*updated_stats;
	int			nsegs;
} VacuumStatsContext;

/*
 * GUC parameters
 */
int			vacuum_freeze_min_age;
int			vacuum_freeze_table_age;
int			vacuum_multixact_freeze_min_age;
int			vacuum_multixact_freeze_table_age;
int			vacuum_failsafe_age;
int			vacuum_multixact_failsafe_age;


/* A few variables that don't seem worth passing around as parameters */
static MemoryContext vac_context = NULL;
static BufferAccessStrategy vac_strategy;


/*
 * Variables for cost-based parallel vacuum.  See comments atop
 * compute_parallel_delay to understand how it works.
 */
pg_atomic_uint32 *VacuumSharedCostBalance = NULL;
pg_atomic_uint32 *VacuumActiveNWorkers = NULL;
int			VacuumCostBalanceLocal = 0;

/* non-export function prototypes */
static List *expand_vacuum_rel(VacuumRelation *vrel, int options);
static List *get_all_vacuum_rels(int options);
static void vac_truncate_clog(TransactionId frozenXID,
							  MultiXactId minMulti,
							  TransactionId lastSaneFrozenXid,
							  MultiXactId lastSaneMinMulti);
static bool vacuum_rel(Oid relid, RangeVar *relation, VacuumParams *params,
					   bool recursing);
static double compute_parallel_delay(void);
static VacOptValue get_vacoptval_from_boolean(DefElem *def);

static void dispatchVacuum(VacuumParams *params, Oid relid,
						   VacuumStatsContext *ctx);
static List *vacuum_params_to_options_list(VacuumParams *params);
static void vacuum_combine_stats(VacuumStatsContext *stats_context,
								 CdbPgResults *cdb_pgresults);
static void vac_update_relstats_from_list(VacuumStatsContext *stats_context);

/*
 * Primary entry point for manual VACUUM and ANALYZE commands
 *
 * This is mainly a preparation wrapper for the real operations that will
 * happen in vacuum().
 */
void
ExecVacuum(ParseState *pstate, VacuumStmt *vacstmt, bool isTopLevel, bool auto_stats)
{
	VacuumParams params;
	bool		verbose = false;
	bool		skip_locked = false;
	bool		analyze = false;
	bool		freeze = false;
	bool		ao_aux_only = false;
	bool		full = false;
	bool		disable_page_skipping = false;
	bool		rootonly = false;
	bool		fullscan = false;
	int			ao_phase = 0;
	bool		process_toast = true;
	bool		update_datfrozenxid = false;
 	ListCell   *lc;

	/* index_cleanup and truncate values unspecified for now */
	params.index_cleanup = VACOPTVALUE_UNSPECIFIED;
	params.truncate = VACOPTVALUE_UNSPECIFIED;

	/* By default parallel vacuum is enabled */
	params.nworkers = 0;

	/* Parse options list */
	foreach(lc, vacstmt->options)
	{
		DefElem    *opt = (DefElem *) lfirst(lc);

		/* Parse common options for VACUUM and ANALYZE */
		if (strcmp(opt->defname, "verbose") == 0)
			verbose = defGetBoolean(opt);
		else if (strcmp(opt->defname, "skip_locked") == 0)
			skip_locked = defGetBoolean(opt);
		else if (strcmp(opt->defname, "rootpartition") == 0)
			rootonly = defGetBoolean(opt);
		else if (strcmp(opt->defname, "fullscan") == 0)
			fullscan = defGetBoolean(opt);
		else if (!vacstmt->is_vacuumcmd)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized ANALYZE option \"%s\"", opt->defname),
					 parser_errposition(pstate, opt->location)));

		/* Parse options available on VACUUM */
		else if (strcmp(opt->defname, "analyze") == 0)
			analyze = defGetBoolean(opt);
		else if (strcmp(opt->defname, "freeze") == 0)
			freeze = defGetBoolean(opt);
		else if (strcmp(opt->defname, "full") == 0)
			full = defGetBoolean(opt);
		else if (strcmp(opt->defname, "ao_aux_only") == 0)
			ao_aux_only = defGetBoolean(opt);
		else if (strcmp(opt->defname, "disable_page_skipping") == 0)
			disable_page_skipping = defGetBoolean(opt);
		else if (strcmp(opt->defname, "index_cleanup") == 0)
		{
			/* Interpret no string as the default, which is 'auto' */
			if (!opt->arg)
				params.index_cleanup = VACOPTVALUE_AUTO;
			else
			{
				char	   *sval = defGetString(opt);

				/* Try matching on 'auto' string, or fall back on boolean */
				if (pg_strcasecmp(sval, "auto") == 0)
					params.index_cleanup = VACOPTVALUE_AUTO;
				else
					params.index_cleanup = get_vacoptval_from_boolean(opt);
			}
		}
		else if (strcmp(opt->defname, "process_toast") == 0)
			process_toast = defGetBoolean(opt);
		else if (strcmp(opt->defname, "truncate") == 0)
			params.truncate = get_vacoptval_from_boolean(opt);
		else if (Gp_role == GP_ROLE_EXECUTE && strcmp(opt->defname, "ao_phase") == 0)
		{
			ao_phase = defGetInt32(opt);
			Assert((ao_phase & VACUUM_AO_PHASE_MASK) == ao_phase);
		}
		else if (strcmp(opt->defname, "parallel") == 0)
		{
			if (opt->arg == NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("parallel option requires a value between 0 and %d",
								MAX_PARALLEL_WORKER_LIMIT),
						 parser_errposition(pstate, opt->location)));
			}
			else
			{
				int			nworkers;

				nworkers = defGetInt32(opt);
				if (nworkers < 0 || nworkers > MAX_PARALLEL_WORKER_LIMIT)
					ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("parallel workers for vacuum must be between 0 and %d",
									MAX_PARALLEL_WORKER_LIMIT),
							 parser_errposition(pstate, opt->location)));

				/*
				 * Disable parallel vacuum, if user has specified parallel
				 * degree as zero.
				 */
				if (nworkers == 0)
					params.nworkers = -1;
				else
					params.nworkers = nworkers;
			}
		}
		else if (strcmp(opt->defname, "update_datfrozenxid") == 0)
			update_datfrozenxid = defGetBoolean(opt);
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized VACUUM option \"%s\"", opt->defname),
					 parser_errposition(pstate, opt->location)));
	}

	/* GPDB: autotstats related modifications */
	if (!gp_autostats_lock_wait)
		skip_locked |= auto_stats;

	/* Set vacuum options */
	params.options =
		(vacstmt->is_vacuumcmd ? VACOPT_VACUUM : VACOPT_ANALYZE) |
		(verbose ? VACOPT_VERBOSE : 0) |
		(skip_locked ? VACOPT_SKIP_LOCKED : 0) |
		(analyze ? VACOPT_ANALYZE : 0) |
		(freeze ? VACOPT_FREEZE : 0) |
		(full ? VACOPT_FULL : 0) |
		(ao_aux_only ? VACOPT_AO_AUX_ONLY : 0) |
		(disable_page_skipping ? VACOPT_DISABLE_PAGE_SKIPPING : 0) |
		(process_toast ? VACOPT_PROCESS_TOAST : 0) |
		(update_datfrozenxid ? VACOPT_UPDATE_DATFROZENXID : 0);

	if (rootonly)
		params.options |= VACOPT_ROOTONLY;
	if (fullscan)
		params.options |= VACOPT_FULLSCAN;
	params.options |= ao_phase;

	/* sanity checks on options */
	Assert(params.options & (VACOPT_VACUUM | VACOPT_ANALYZE));
	Assert((params.options & VACOPT_VACUUM) ||
		   !(params.options & (VACOPT_FULL | VACOPT_FREEZE)));

	if ((params.options & VACOPT_FULL) && params.nworkers > 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("VACUUM FULL cannot be performed in parallel")));

	/*
	 * Make sure VACOPT_ANALYZE is specified if any column lists are present.
	 */
	if (!(params.options & VACOPT_ANALYZE))
	{
		ListCell   *lc;

		foreach(lc, vacstmt->rels)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);

			if (vrel->va_cols != NIL)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("ANALYZE option must be specified when a column list is provided")));
		}
	}

	/*
	 * All freeze ages are zero if the FREEZE option is given; otherwise pass
	 * them as -1 which means to use the default values.
	 */
	if (params.options & VACOPT_FREEZE)
	{
		params.freeze_min_age = 0;
		params.freeze_table_age = 0;
		params.multixact_freeze_min_age = 0;
		params.multixact_freeze_table_age = 0;
	}
	else
	{
		params.freeze_min_age = -1;
		params.freeze_table_age = -1;
		params.multixact_freeze_min_age = -1;
		params.multixact_freeze_table_age = -1;
	}

	/* user-invoked vacuum is never "for wraparound" */
	params.is_wraparound = false;

	/* user-invoked vacuum never uses this parameter */
	params.log_min_duration = -1;

	params.auto_stats = auto_stats;

	/* Now go through the common routine */
	vacuum(vacstmt->rels, &params, NULL, isTopLevel);
}


/*
 * Internal entry point for VACUUM and ANALYZE commands.
 *
 * relations, if not NIL, is a list of VacuumRelation to process; otherwise,
 * we process all relevant tables in the database.  For each VacuumRelation,
 * if a valid OID is supplied, the table with that OID is what to process;
 * otherwise, the VacuumRelation's RangeVar indicates what to process.
 *
 * params contains a set of parameters that can be used to customize the
 * behavior.
 *
 * bstrategy is normally given as NULL, but in autovacuum it can be passed
 * in to use the same buffer strategy object across multiple vacuum() calls.
 *
 * isTopLevel should be passed down from ProcessUtility.
 *
 * It is the caller's responsibility that all parameters are allocated in a
 * memory context that will not disappear at transaction commit.
 */
void
vacuum(List *relations, VacuumParams *params,
	   BufferAccessStrategy bstrategy, bool isTopLevel)
{
	static bool in_vacuum = false;

	const char *stmttype;
	volatile bool in_outer_xact,
				use_own_xacts;

	Assert(params != NULL);

	/*
	 * VACUUM does not support ROOTPARTITION option. Normally it's not possible
	 * that VACOPT_VACUUM and VACOPT_ROOTONLY set at same time.
	 */
	Assert(!((params->options & VACOPT_VACUUM) && (params->options & VACOPT_ROOTONLY)));

	/*
	 * We force vacuum auxiliary process in system_group, so in vacuum transaction we should
	 * not assign it to any resource group.
	 */
	AssertImply(IsAutoVacuumWorkerProcess(), GetMyResGroupId() == InvalidOid);

	stmttype = (params->options & VACOPT_VACUUM) ? "VACUUM" : "ANALYZE";

	/*
	 * We cannot run VACUUM inside a user transaction block; if we were inside
	 * a transaction, then our commit- and start-transaction-command calls
	 * would not have the intended effect!	There are numerous other subtle
	 * dependencies on this, too.
	 *
	 * GPDB: AO vacuum's compaction phase has to run in a distributed
	 * transaction though.
	 *
	 */
	if ((params->options & VACOPT_VACUUM) &&
		(params->options & VACUUM_AO_PHASE_MASK) == 0)
	{
		PreventInTransactionBlock(isTopLevel, stmttype);
		in_outer_xact = false;
	}
	else
		in_outer_xact = IsInTransactionBlock(isTopLevel);

	/*
	 * Due to static variables vac_context, anl_context and vac_strategy,
	 * vacuum() is not reentrant.  This matters when VACUUM FULL or ANALYZE
	 * calls a hostile index expression that itself calls ANALYZE.
	 */
	if (in_vacuum)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("%s cannot be executed from VACUUM or ANALYZE",
						stmttype)));

	/*
	 * Sanity check DISABLE_PAGE_SKIPPING option.
	 */
	if ((params->options & VACOPT_FULL) != 0 &&
		(params->options & VACOPT_DISABLE_PAGE_SKIPPING) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("VACUUM option DISABLE_PAGE_SKIPPING cannot be used with FULL")));

	/* sanity check for PROCESS_TOAST */
	if ((params->options & VACOPT_FULL) != 0 &&
		(params->options & VACOPT_PROCESS_TOAST) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("PROCESS_TOAST required with VACUUM FULL")));

	/*
	 * Send info about dead objects to the statistics collector, unless we are
	 * in autovacuum --- autovacuum.c does this for itself.
	 */
	if ((params->options & VACOPT_VACUUM) && !IsAutoVacuumWorkerProcess())
		pgstat_vacuum_stat();

	/*
	 * Create special memory context for cross-transaction storage.
	 *
	 * Since it is a child of PortalContext, it will go away eventually even
	 * if we suffer an error; there's no need for special abort cleanup logic.
	 */
	vac_context = AllocSetContextCreate(PortalContext,
										"Vacuum",
										ALLOCSET_DEFAULT_SIZES);

	/*
	 * If caller didn't give us a buffer strategy object, make one in the
	 * cross-transaction memory context.
	 */
	if (bstrategy == NULL)
	{
		MemoryContext old_context = MemoryContextSwitchTo(vac_context);

		bstrategy = GetAccessStrategy(BAS_VACUUM);
		MemoryContextSwitchTo(old_context);
	}
	vac_strategy = bstrategy;

	/*
	 * Build list of relation(s) to process, putting any new data in
	 * vac_context for safekeeping.
	 */
	if (relations != NIL)
	{
		List		*newrels = NIL;
		ListCell  *lc;

		foreach(lc, relations)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, lc);
			List	   *sublist;
			MemoryContext old_context;

			sublist = expand_vacuum_rel(vrel, params->options);
			old_context = MemoryContextSwitchTo(vac_context);
			newrels = list_concat(newrels, sublist);
			MemoryContextSwitchTo(old_context);
		}
		relations = newrels;
	}
	else if (!(params->options & VACOPT_UPDATE_DATFROZENXID))
		relations = get_all_vacuum_rels(params->options);

	/*
	 * Decide whether we need to start/commit our own transactions.
	 *
	 * For VACUUM (with or without ANALYZE): always do so, so that we can
	 * release locks as soon as possible.  (We could possibly use the outer
	 * transaction for a one-table VACUUM, but handling TOAST tables would be
	 * problematic.)
	 *
	 * For ANALYZE (no VACUUM): if inside a transaction block, we cannot
	 * start/commit our own transactions.  Also, there's no need to do so if
	 * only processing one relation.  For multiple relations when not within a
	 * transaction block, and also in an autovacuum worker, use own
	 * transactions so we can release locks sooner.
	 */
	if (params->options & VACOPT_AO_COMPACT_PHASE)
		use_own_xacts = false;
	else if (params->options & VACOPT_VACUUM)
		use_own_xacts = true;
	else
	{
		Assert(params->options & VACOPT_ANALYZE);
		if (IsAutoVacuumWorkerProcess())
			use_own_xacts = true;
		else if (in_outer_xact)
			use_own_xacts = false;
		else if (list_length(relations) > 1)
			use_own_xacts = true;
		else
			use_own_xacts = false;
	}

	/*
	 * vacuum_rel expects to be entered with no transaction active; it will
	 * start and commit its own transaction.  But we are called by an SQL
	 * command, and so we are executing inside a transaction already. We
	 * commit the transaction started in PostgresMain() here, and start
	 * another one before exiting to match the commit waiting for us back in
	 * PostgresMain().
	 */
	if (use_own_xacts)
	{
		Assert(!in_outer_xact);

		/* ActiveSnapshot is not set by autovacuum */
		if (ActiveSnapshotSet())
			PopActiveSnapshot();

		PreserveOidAssignmentsOnCommit();

		/* matches the StartTransaction in PostgresMain() */
		CommitTransactionCommand();
	}

	/* Turn vacuum cost accounting on or off, and set/clear in_vacuum */
	PG_TRY();
	{
		ListCell   *cur;

		in_vacuum = true;
		VacuumCostActive = (VacuumCostDelay > 0);
		VacuumCostBalance = 0;
		VacuumPageHit = 0;
		VacuumPageMiss = 0;
		VacuumPageDirty = 0;
		VacuumCostBalanceLocal = 0;
		VacuumSharedCostBalance = NULL;
		VacuumActiveNWorkers = NULL;

		/*
		 * Loop to process each selected relation.
		 */
		foreach(cur, relations)
		{
			VacuumRelation *vrel = lfirst_node(VacuumRelation, cur);

			if (params->options & VACOPT_VACUUM)
			{
				if (!vacuum_rel(vrel->oid, vrel->relation, params, false))
					continue;
			}

			if (params->options & VACOPT_ANALYZE)
			{
				/*
				 * If using separate xacts, start one for analyze. Otherwise,
				 * we can use the outer transaction.
				 */
				if (use_own_xacts)
				{
					StartTransactionCommand();
					/* functions in indexes may want a snapshot set */
					PushActiveSnapshot(GetTransactionSnapshot());
				}

				analyze_rel(vrel->oid, vrel->relation, params,
							vrel->va_cols, in_outer_xact, vac_strategy, NULL);

				if (use_own_xacts)
				{
					PopActiveSnapshot();
					CommitTransactionCommand();
				}
				else
				{
					/*
					 * If we're not using separate xacts, better separate the
					 * ANALYZE actions with CCIs.  This avoids trouble if user
					 * says "ANALYZE t, t".
					 */
					CommandCounterIncrement();
				}

#ifdef FAULT_INJECTOR
				if (IsAutoVacuumWorkerProcess())
				{
					FaultInjector_InjectFaultIfSet(
						"analyze_finished_one_relation", DDLNotSpecified,
						"", vrel->relation->relname);
				}
#endif
			}
		}
	}
	PG_FINALLY();
	{
		in_vacuum = false;
		VacuumCostActive = false;
	}
	PG_END_TRY();

	/*
	 * Finish up processing.
	 */
	if (use_own_xacts)
	{
		/* here, we are not in a transaction */

		/*
		 * This matches the CommitTransaction waiting for us in
		 * PostgresMain().
		 */
		StartTransactionCommand();
		ClearOidAssignmentsOnCommit();
	}

	if ((params->options & VACOPT_VACUUM) && !IsAutoVacuumWorkerProcess() &&
		(Gp_role != GP_ROLE_EXECUTE || (params->options & VACOPT_UPDATE_DATFROZENXID)) && ENABLE_DISPATCH())
	{
		/*
		 * Update pg_database.datfrozenxid, and truncate pg_xact if possible.
		 * (autovacuum.c does this for itself.)
		 */
		vac_update_datfrozenxid();

		if (Gp_role == GP_ROLE_DISPATCH)
		{
			VacuumStmt *vacstmt = makeNode(VacuumStmt);

			int flags = DF_CANCEL_ON_ERROR;

			vacstmt->options = vacuum_params_to_options_list(params);
			vacstmt->options = lappend(vacstmt->options, makeDefElem("update_datfrozenxid", (Node *) makeInteger(1), -1));
			vacstmt->is_vacuumcmd = true;
			vacstmt->rels = NIL;

			CdbDispatchUtilityStatement((Node *) vacstmt, flags,
										GetAssignedOidsForDispatch(),
										NULL);
		}
	}

	/*
	 * Clean up working storage --- note we must do this after
	 * StartTransactionCommand, else we might be trying to delete the active
	 * context!
	 */
	MemoryContextDelete(vac_context);
	vac_context = NULL;
}

/*
 * Check if a given relation can be safely vacuumed or analyzed.  If the
 * user is not the relation owner, issue a WARNING log message and return
 * false to let the caller decide what to do with this relation.  This
 * routine is used to decide if a relation can be processed for VACUUM or
 * ANALYZE.
 */
bool
vacuum_is_relation_owner(Oid relid, Form_pg_class reltuple, bits32 options)
{
	char	   *relname;

	Assert((options & (VACOPT_VACUUM | VACOPT_ANALYZE)) != 0);

	/*
	 * Check permissions.
	 *
	 * We allow the user to vacuum or analyze a table if he is superuser, the
	 * table owner, or the database owner (but in the latter case, only if
	 * it's not a shared relation).  pg_class_ownercheck includes the
	 * superuser case.
	 *
	 * Note we choose to treat permissions failure as a WARNING and keep
	 * trying to vacuum or analyze the rest of the DB --- is this appropriate?
	 */
	if (pg_class_ownercheck(relid, GetUserId()) ||
		(pg_database_ownercheck(MyDatabaseId, GetUserId()) && !reltuple->relisshared))
		return true;

	relname = NameStr(reltuple->relname);

	if ((options & VACOPT_VACUUM) != 0)
	{
		if (reltuple->relisshared)
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- only superuser can vacuum it",
							relname)));
		else if (reltuple->relnamespace == PG_CATALOG_NAMESPACE)
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- only superuser or database owner can vacuum it",
							relname)));
		else
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- only table or database owner can vacuum it",
							relname)));

		/*
		 * For VACUUM ANALYZE, both logs could show up, but just generate
		 * information for VACUUM as that would be the first one to be
		 * processed.
		 */
		return false;
	}

	if ((options & VACOPT_ANALYZE) != 0)
	{
		if (reltuple->relisshared)
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- only superuser can analyze it",
							relname)));
		else if (reltuple->relnamespace == PG_CATALOG_NAMESPACE)
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- only superuser or database owner can analyze it",
							relname)));
		else
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- only table or database owner can analyze it",
							relname)));
	}

	return false;
}


/*
 * vacuum_open_relation
 *
 * This routine is used for attempting to open and lock a relation which
 * is going to be vacuumed or analyzed.  If the relation cannot be opened
 * or locked, a log is emitted if possible.
 */
Relation
vacuum_open_relation(Oid relid, RangeVar *relation, bits32 options,
					 bool verbose, LOCKMODE lmode)
{
	Relation	rel;
	bool		rel_lock = true;
	int			elevel;

	Assert((options & (VACOPT_VACUUM | VACOPT_ANALYZE)) != 0);

	/*
	 * Open the relation and get the appropriate lock on it.
	 *
	 * There's a race condition here: the relation may have gone away since
	 * the last time we saw it.  If so, we don't need to vacuum or analyze it.
	 *
	 * If we've been asked not to wait for the relation lock, acquire it first
	 * in non-blocking mode, before calling try_relation_open().
	 */
	if (!(options & VACOPT_SKIP_LOCKED))
		rel = try_relation_open(relid, lmode, false);
	else if (ConditionalLockRelationOid(relid, lmode))
		rel = try_relation_open(relid, NoLock, false);
	else
	{
		rel = NULL;
		rel_lock = false;
	}

	/* if relation is opened, leave */
	if (rel)
		return rel;

	/*
	 * Relation could not be opened, hence generate if possible a log
	 * informing on the situation.
	 *
	 * If the RangeVar is not defined, we do not have enough information to
	 * provide a meaningful log statement.  Chances are that the caller has
	 * intentionally not provided this information so that this logging is
	 * skipped, anyway.
	 */
	if (relation == NULL)
		return NULL;

	/*
	 * Determine the log level.
	 *
	 * For manual VACUUM or ANALYZE, we emit a WARNING to match the log
	 * statements in the permission checks; otherwise, only log if the caller
	 * so requested.
	 */
	if (!IsAutoVacuumWorkerProcess())
		elevel = WARNING;
	else if (verbose)
		elevel = LOG;
	else
		return NULL;

	if ((options & VACOPT_VACUUM) != 0)
	{
		if (!rel_lock)
			ereport(elevel,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("skipping vacuum of \"%s\" --- lock not available",
							relation->relname)));
		else
			ereport(elevel,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("skipping vacuum of \"%s\" --- relation no longer exists",
							relation->relname)));

		/*
		 * For VACUUM ANALYZE, both logs could show up, but just generate
		 * information for VACUUM as that would be the first one to be
		 * processed.
		 */
		return NULL;
	}

	if ((options & VACOPT_ANALYZE) != 0)
	{
		if (!rel_lock)
			ereport(elevel,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("skipping analyze of \"%s\" --- lock not available",
							relation->relname)));
		else
			ereport(elevel,
					(errcode(ERRCODE_UNDEFINED_TABLE),
					 errmsg("skipping analyze of \"%s\" --- relation no longer exists",
							relation->relname)));
	}

	return NULL;
}


/*
 * Given a VacuumRelation, fill in the table OID if it wasn't specified,
 * and optionally add VacuumRelations for partitions of the table.
 *
 * If a VacuumRelation does not have an OID supplied and is a partitioned
 * table, an extra entry will be added to the output for each partition.
 * Presently, only autovacuum supplies OIDs when calling vacuum(), and
 * it does not want us to expand partitioned tables.
 *
 * We take care not to modify the input data structure, but instead build
 * new VacuumRelation(s) to return.  (But note that they will reference
 * unmodified parts of the input, eg column lists.)  New data structures
 * are made in vac_context.
 *
 * GPDB: In addition to expanding a partitioned table to include its
 * partitions, we also use this function to expand appendoptimized tables to
 * their auxiliary tables if the AO_AUX_ONLY option is passed. This is
 * something of a misnomer because we do not return the originally input AO
 * table, so it's a replacement instead of strictly an expansion.
 */
static List *
expand_vacuum_rel(VacuumRelation *vrel, int options)
{
	List	   *vacrels = NIL;
	MemoryContext oldcontext;

	/* If caller supplied OID, there's nothing we need do here. */
	if (OidIsValid(vrel->oid))
	{
		oldcontext = MemoryContextSwitchTo(vac_context);
		vacrels = lappend(vacrels, vrel);
		MemoryContextSwitchTo(oldcontext);
	}
	else
	{
		/* Process a specific relation, and possibly partitions thereof */
		Oid			relid;
		HeapTuple	tuple;
		Form_pg_class classForm;
		bool		ispartition;
		bool		include_parts;
		int			rvr_opts;
		bool		skip_this = false;
		bool		skip_children = false;
		bool		skip_midlevel = false;

		/*
		 * Since autovacuum workers supply OIDs when calling vacuum(), no
		 * autovacuum worker should reach this code.
		 */
		Assert(!IsAutoVacuumWorkerProcess());

		/*
		 * We transiently take AccessShareLock to protect the syscache lookup
		 * below, as well as find_all_inheritors's expectation that the caller
		 * holds some lock on the starting relation.
		 */
		rvr_opts = (options & VACOPT_SKIP_LOCKED) ? RVR_SKIP_LOCKED : 0;
		relid = RangeVarGetRelidExtended(vrel->relation,
										 AccessShareLock,
										 rvr_opts,
										 NULL, NULL);

		/*
		 * If the lock is unavailable, emit the same log statement that
		 * vacuum_rel() and analyze_rel() would.
		 */
		if (!OidIsValid(relid))
		{
			if (options & VACOPT_VACUUM)
				ereport(WARNING,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("skipping vacuum of \"%s\" --- lock not available",
								vrel->relation->relname)));
			else
				ereport(WARNING,
						(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
						 errmsg("skipping analyze of \"%s\" --- lock not available",
								vrel->relation->relname)));
			return vacrels;
		}

		/*
		 * To check whether the relation is a partitioned table and its
		 * ownership, fetch its syscache entry.
		 */
		tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for relation %u", relid);
		classForm = (Form_pg_class) GETSTRUCT(tuple);
		ispartition = classForm->relispartition;

		/*
		 * Handle GPDB's extra options and GUCs that affect how we recurse
		 * into partitions.
		 */
		if ((options & VACOPT_ROOTONLY) != 0)
		{
			if (classForm->relkind != RELKIND_PARTITIONED_TABLE ||
				classForm->relispartition)
			{
				ereport(WARNING,
						(errmsg("skipping \"%s\" --- cannot analyze a non-root partition using ANALYZE ROOTPARTITION",
								NameStr(classForm->relname))));
				skip_this = true;
			}
			skip_children = true;
		}
		/*
		 * disable analyzing mid-level partitions directly since the users are encouraged
		 * to work with the root partition only. To gather stats on mid-level partitions
		 * (for Orca's use), the user should run ANALYZE or ANALYZE ROOTPARTITION on the
		 * root level with optimizer_analyze_midlevel_partition GUC set to ON.
		 * Planner uses the stats on leaf partitions, so it's unnecessary to collect stats on
		 * midlevel partitions.
		 */
		else if (classForm->relkind == RELKIND_PARTITIONED_TABLE &&
				 classForm->relispartition &&
				 !optimizer_analyze_midlevel_partition)
		{
			ereport(WARNING,
					(errmsg("skipping \"%s\" --- cannot analyze a mid-level partition. "
							"Please run ANALYZE on the root partition table.",
							NameStr(classForm->relname))));
			/* do nothing at all */
			skip_this = true;
			skip_children = true;
		}
		else
		{
			/*
			 * If current table is root partition table, optimizer_analyze_root_partition
			 * is set to 'off' and ROOTPARTITION option was not explicitly specified,
			 * analyze all the children, but skip the partitioned table itself.
			 *
			 * Analyzing the children will update the root table's statistics
			 * too, by merging the stats of the children.
			 */
			if (classForm->relkind == RELKIND_PARTITIONED_TABLE &&
				!optimizer_analyze_root_partition)
				skip_this = true;

			if (!optimizer_analyze_midlevel_partition)
				skip_midlevel = true;
		}

		/*
		 * Make a returnable VacuumRelation for this rel if user is a proper
		 * owner.
		 */
		if (vacuum_is_relation_owner(relid, classForm, options) && !skip_this)
		{
			oldcontext = MemoryContextSwitchTo(vac_context);
			vacrels = lappend(vacrels, makeVacuumRelation(vrel->relation,
														  relid,
														  vrel->va_cols));
			MemoryContextSwitchTo(oldcontext);
		}

		include_parts = (classForm->relkind == RELKIND_PARTITIONED_TABLE);
		ReleaseSysCache(tuple);

		/*
		 * If it is, make relation list entries for its partitions.  Note that
		 * the list returned by find_all_inheritors() includes the passed-in
		 * OID, so we have to skip that.  There's no point in taking locks on
		 * the individual partitions yet, and doing so would just add
		 * unnecessary deadlock risk.  For this last reason we do not check
		 * yet the ownership of the partitions, which get added to the list to
		 * process.  Ownership will be checked later on anyway.
		 */
		if (include_parts && !skip_children)
		{
			List	   *part_oids = find_all_inheritors(relid, NoLock, NULL);
			ListCell   *part_lc;

			foreach(part_lc, part_oids)
			{
				Oid			part_oid = lfirst_oid(part_lc);

				if (part_oid == relid)
					continue;	/* ignore original table */

				if (skip_midlevel &&
					get_rel_relkind(part_oid) == RELKIND_PARTITIONED_TABLE)
					continue;

				/*
				 * We omit a RangeVar since it wouldn't be appropriate to
				 * complain about failure to open one of these relations
				 * later.
				 */
				oldcontext = MemoryContextSwitchTo(vac_context);
				vacrels = lappend(vacrels, makeVacuumRelation(NULL,
															  part_oid,
															  vrel->va_cols));
				MemoryContextSwitchTo(oldcontext);
			}
		}

		/*
		 * Release lock again.  This means that by the time we actually try to
		 * process the table, it might be gone or renamed.  In the former case
		 * we'll silently ignore it; in the latter case we'll process it
		 * anyway, but we must beware that the RangeVar doesn't necessarily
		 * identify it anymore.  This isn't ideal, perhaps, but there's little
		 * practical alternative, since we're typically going to commit this
		 * transaction and begin a new one between now and then.  Moreover,
		 * holding locks on multiple relations would create significant risk
		 * of deadlock.
		 */
		UnlockRelationOid(relid, AccessShareLock);

		/*
		 * GPDB: The above code builds the list so that the partitions of a table
		 * come after the parent. In GPDB, we have code to build the stats of a parent
		 * table by merge the stats of leaf partitions, but that obviously won't work
		 * if the leaf partition stats haven't been built yet. Reverse the list
		 * so that the partitions are always analyzed before the parent table, so
		 * the partition stats merging code can kick in.
		 */
		{
			ListCell   *lc;
			List	   *reverse_vacrels = NIL;

			foreach (lc, vacrels)
			{
				reverse_vacrels = lcons(lfirst(lc), reverse_vacrels);
			}

			vacrels = reverse_vacrels;
		}

		/*
		 * GPDB: If you explicitly ANALYZE a partition, also update the
		 * parent's stats after the partition has been ANALYZEd. (Thanks to
		 * the code to merge leaf statistics, it should be fast.)
		 *
		 * If ROOTPARTITION is specified, that means we only analyze on root
		 * partition table. The root table's ispartition is false. And the root
		 * table doesn't have parent to merge stats.
		 * If current table is skipped, no need to merge stats for it's parent
		 * since current table's stats is not get updated.
		 */
		if ((options & VACOPT_ANALYZE) && optimizer_analyze_root_partition && !skip_this)
		{
			Oid			child_relid = relid;

			while (ispartition)
			{
				Oid			parent_relid;
				int			elevel = ((options & VACOPT_VERBOSE) ? LOG : DEBUG2);

				parent_relid = get_partition_parent(child_relid, false);
				ispartition = get_rel_relispartition(parent_relid);

				/*
				 * Only ANALYZE the parent if the stats can be updated by merging
				 * child stats.
				 */
				if (!leaf_parts_analyzed(parent_relid, child_relid, vrel->va_cols, elevel))
					break;

				/*
				 * Do not add midlevel partition unless optimizer_analyze_midlevel_partition
				 * is enabled. But always add root table.
				 * ispartition is set with relispartition flag of the parent_relid.
				 */
				if(!ispartition || optimizer_analyze_midlevel_partition)
				{
					oldcontext = MemoryContextSwitchTo(vac_context);
					vacrels = lappend(vacrels, makeVacuumRelation(vrel->relation,
											  parent_relid,
											  vrel->va_cols));
					MemoryContextSwitchTo(oldcontext);
				}

				child_relid = parent_relid;
			}
		}

		/*
		 * If AO_AUX_ONLY option is passed, replace list of relations with the
		 * auxiliary tables for each AO table in the list
		 */
		if ((options & VACOPT_AO_AUX_ONLY) != 0)
		{
			ListCell *lc;
			List		*ao_aux_vacrels = NIL;
			Oid		aoseg_relid = InvalidOid;
			Oid		aoblkdir_relid = InvalidOid;
			Oid		aovisimap_relid = InvalidOid;

			foreach (lc, vacrels)
			{
				VacuumRelation *part_vrel = lfirst_node(VacuumRelation, lc);
				tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(part_vrel->oid));
				if (!HeapTupleIsValid(tuple))
					elog(ERROR, "cache lookup failed for relation %u", relid);
				classForm = (Form_pg_class) GETSTRUCT(tuple);
				if (IsAccessMethodAO(classForm->relam))
				{
					/* no aux tables for a parent AO table */
					if (classForm->relkind == RELKIND_PARTITIONED_TABLE)
					{
						ReleaseSysCache(tuple);
						continue;
					}

					Relation aorel = table_open(classForm->oid, AccessShareLock);
					oldcontext = MemoryContextSwitchTo(vac_context);

					GetAppendOnlyEntryAuxOids(aorel,
											  &aoseg_relid,
											  &aoblkdir_relid, NULL,
											  &aovisimap_relid, NULL);

					/* make new VacuumRelations for each valid member of the 3 auxiliary tables */
					if (OidIsValid(aoseg_relid))
						ao_aux_vacrels = lappend(ao_aux_vacrels, makeVacuumRelation(NULL, aoseg_relid, part_vrel->va_cols));
					if (OidIsValid(aoblkdir_relid))
						ao_aux_vacrels = lappend(ao_aux_vacrels, makeVacuumRelation(NULL, aoblkdir_relid, part_vrel->va_cols));
					if (OidIsValid(aovisimap_relid))
						ao_aux_vacrels = lappend(ao_aux_vacrels, makeVacuumRelation(NULL, aovisimap_relid, part_vrel->va_cols));

					MemoryContextSwitchTo(oldcontext);
					table_close(aorel, AccessShareLock);
				}
				else
				{
					ereport(classForm->relispartition ? LOG : WARNING,
							(errmsg("skipping \"%s\" for VACUUM AO_AUX_ONLY --- it is not an append-optimized table",
									NameStr(classForm->relname))),
							errdetail("it will not have auxiliary tables to vacuum"));
				}
				ReleaseSysCache(tuple);
			}
			/* return only the AO AUX vacrels, dropping all heap and AO tables from the to-vacuum list */
			vacrels = ao_aux_vacrels;
		}
	}
	return vacrels;
}

/*
 * Construct a list of VacuumRelations for all vacuumable rels in
 * the current database.  The list is built in vac_context.
 */
static List *
get_all_vacuum_rels(int options)
{
	List	   *vacrels = NIL;
	Relation	pgclass;
	TableScanDesc scan;
	HeapTuple	tuple;

	pgclass = table_open(RelationRelationId, AccessShareLock);
	scan = table_beginscan_catalog(pgclass, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(tuple);
		MemoryContext oldcontext;
		Oid			relid = classForm->oid;

		/* check permissions of relation */
		if (!vacuum_is_relation_owner(relid, classForm, options))
			continue;

		/*
		 * We include partitioned tables here; depending on which operation is
		 * to be performed, caller will decide whether to process or ignore
		 * them.
		 */
		if ((options & VACOPT_AO_AUX_ONLY) == 0 &&
			classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_DIRECTORY_TABLE &&
			classForm->relkind != RELKIND_MATVIEW &&
			classForm->relkind != RELKIND_PARTITIONED_TABLE)
			continue;

		/*
		 * If ao_aux_only option is given without a tablename, we only want all
		 * AO AUX tables
		 */
		if ((options & VACOPT_AO_AUX_ONLY) != 0 &&
			classForm->relkind != RELKIND_AOSEGMENTS &&
			classForm->relkind != RELKIND_AOBLOCKDIR &&
			classForm->relkind != RELKIND_AOVISIMAP)
			continue;

		/* skip mid-level partition tables if we have disabled collecting statistics for them */
		if (!optimizer_analyze_midlevel_partition &&
			classForm->relkind == RELKIND_PARTITIONED_TABLE &&
			classForm->relispartition)
		{
			continue;
		}

		/* Likewise, skip root partition, if disabled. */
		if (!optimizer_analyze_root_partition &&
			(options & VACOPT_ROOTONLY) == 0 &&
			classForm->relkind == RELKIND_PARTITIONED_TABLE &&
			!classForm->relispartition)
		{
			continue;
		}

		/*
		 * Build VacuumRelation(s) specifying the table OIDs to be processed.
		 * We omit a RangeVar since it wouldn't be appropriate to complain
		 * about failure to open one of these relations later.
		 */
		oldcontext = MemoryContextSwitchTo(vac_context);
		vacrels = lappend(vacrels, makeVacuumRelation(NULL,
													  relid,
													  NIL));
		MemoryContextSwitchTo(oldcontext);
	}

	table_endscan(scan);
	table_close(pgclass, AccessShareLock);

	return vacrels;
}

/*
 * vacuum_set_xid_limits() -- compute oldestXmin and freeze cutoff points
 *
 * Input parameters are the target relation, applicable freeze age settings.
 *
 * The output parameters are:
 * - oldestXmin is the cutoff value used to distinguish whether tuples are
 *	 DEAD or RECENTLY_DEAD (see HeapTupleSatisfiesVacuum).
 * - freezeLimit is the Xid below which all Xids are replaced by
 *	 FrozenTransactionId during vacuum.
 * - xidFullScanLimit (computed from freeze_table_age parameter)
 *	 represents a minimum Xid value; a table whose relfrozenxid is older than
 *	 this will have a full-table vacuum applied to it, to freeze tuples across
 *	 the whole table.  Vacuuming a table younger than this value can use a
 *	 partial scan.
 * - multiXactCutoff is the value below which all MultiXactIds are removed from
 *	 Xmax.
 * - mxactFullScanLimit is a value against which a table's relminmxid value is
 *	 compared to produce a full-table vacuum, as with xidFullScanLimit.
 *
 * xidFullScanLimit and mxactFullScanLimit can be passed as NULL if caller is
 * not interested.
 */
void
vacuum_set_xid_limits(Relation rel,
					  int freeze_min_age,
					  int freeze_table_age,
					  int multixact_freeze_min_age,
					  int multixact_freeze_table_age,
					  TransactionId *oldestXmin,
					  TransactionId *freezeLimit,
					  TransactionId *xidFullScanLimit,
					  MultiXactId *multiXactCutoff,
					  MultiXactId *mxactFullScanLimit)
{
	int			freezemin;
	int			mxid_freezemin;
	int			effective_multixact_freeze_max_age;
	TransactionId limit;
	TransactionId safeLimit;
	MultiXactId oldestMxact;
	MultiXactId mxactLimit;
	MultiXactId safeMxactLimit;

	/*
	 * We can always ignore processes running lazy vacuum.  This is because we
	 * use these values only for deciding which tuples we must keep in the
	 * tables.  Since lazy vacuum doesn't write its XID anywhere (usually no
	 * XID assigned), it's safe to ignore it.  In theory it could be
	 * problematic to ignore lazy vacuums in a full vacuum, but keep in mind
	 * that only one vacuum process can be working on a particular table at
	 * any time, and that each vacuum is always an independent transaction.
	 */
	*oldestXmin = GetOldestNonRemovableTransactionId(rel);

	if (OldSnapshotThresholdActive())
	{
		TransactionId limit_xmin;
		TimestampTz limit_ts;

		if (TransactionIdLimitedForOldSnapshots(*oldestXmin, rel,
												&limit_xmin, &limit_ts))
		{
			/*
			 * TODO: We should only set the threshold if we are pruning on the
			 * basis of the increased limits.  Not as crucial here as it is
			 * for opportunistic pruning (which often happens at a much higher
			 * frequency), but would still be a significant improvement.
			 */
			SetOldSnapshotThresholdTimestamp(limit_ts, limit_xmin);
			*oldestXmin = limit_xmin;
		}
	}

	Assert(TransactionIdIsNormal(*oldestXmin));

	/*
	 * Determine the minimum freeze age to use: as specified by the caller, or
	 * vacuum_freeze_min_age, but in any case not more than half
	 * autovacuum_freeze_max_age, so that autovacuums to prevent XID
	 * wraparound won't occur too frequently.
	 */
	freezemin = freeze_min_age;
	if (freezemin < 0)
		freezemin = vacuum_freeze_min_age;
	freezemin = Min(freezemin, autovacuum_freeze_max_age / 2);
	Assert(freezemin >= 0);

	/*
	 * Compute the cutoff XID, being careful not to generate a "permanent" XID
	 */
	limit = *oldestXmin - freezemin;
	if (!TransactionIdIsNormal(limit))
		limit = FirstNormalTransactionId;

	/*
	 * If oldestXmin is very far back (in practice, more than
	 * autovacuum_freeze_max_age / 2 XIDs old), complain and force a minimum
	 * freeze age of zero.
	 */
	safeLimit = ReadNextTransactionId() - autovacuum_freeze_max_age;
	if (!TransactionIdIsNormal(safeLimit))
		safeLimit = FirstNormalTransactionId;

	if (TransactionIdPrecedes(limit, safeLimit))
	{
		ereport(WARNING,
				(errmsg("oldest xmin is far in the past"),
				 errhint("Close open transactions soon to avoid wraparound problems.\n"
						 "You might also need to commit or roll back old prepared transactions, or drop stale replication slots.")));
		limit = *oldestXmin;
	}

	*freezeLimit = limit;

	/*
	 * Compute the multixact age for which freezing is urgent.  This is
	 * normally autovacuum_multixact_freeze_max_age, but may be less if we are
	 * short of multixact member space.
	 */
	effective_multixact_freeze_max_age = MultiXactMemberFreezeThreshold();

	/*
	 * Determine the minimum multixact freeze age to use: as specified by
	 * caller, or vacuum_multixact_freeze_min_age, but in any case not more
	 * than half effective_multixact_freeze_max_age, so that autovacuums to
	 * prevent MultiXact wraparound won't occur too frequently.
	 */
	mxid_freezemin = multixact_freeze_min_age;
	if (mxid_freezemin < 0)
		mxid_freezemin = vacuum_multixact_freeze_min_age;
	mxid_freezemin = Min(mxid_freezemin,
						 effective_multixact_freeze_max_age / 2);
	Assert(mxid_freezemin >= 0);

	/* compute the cutoff multi, being careful to generate a valid value */
	oldestMxact = GetOldestMultiXactId();
	mxactLimit = oldestMxact - mxid_freezemin;
	if (mxactLimit < FirstMultiXactId)
		mxactLimit = FirstMultiXactId;

	safeMxactLimit =
		ReadNextMultiXactId() - effective_multixact_freeze_max_age;
	if (safeMxactLimit < FirstMultiXactId)
		safeMxactLimit = FirstMultiXactId;

	if (MultiXactIdPrecedes(mxactLimit, safeMxactLimit))
	{
		ereport(WARNING,
				(errmsg("oldest multixact is far in the past"),
				 errhint("Close open transactions with multixacts soon to avoid wraparound problems.")));
		/* Use the safe limit, unless an older mxact is still running */
		if (MultiXactIdPrecedes(oldestMxact, safeMxactLimit))
			mxactLimit = oldestMxact;
		else
			mxactLimit = safeMxactLimit;
	}

	*multiXactCutoff = mxactLimit;

	if (xidFullScanLimit != NULL)
	{
		int			freezetable;

		Assert(mxactFullScanLimit != NULL);

		/*
		 * Determine the table freeze age to use: as specified by the caller,
		 * or vacuum_freeze_table_age, but in any case not more than
		 * autovacuum_freeze_max_age * 0.95, so that if you have e.g nightly
		 * VACUUM schedule, the nightly VACUUM gets a chance to freeze tuples
		 * before anti-wraparound autovacuum is launched.
		 */
		freezetable = freeze_table_age;
		if (freezetable < 0)
			freezetable = vacuum_freeze_table_age;
		freezetable = Min(freezetable, autovacuum_freeze_max_age * 0.95);
		Assert(freezetable >= 0);

		/*
		 * Compute XID limit causing a full-table vacuum, being careful not to
		 * generate a "permanent" XID.
		 */
		limit = ReadNextTransactionId() - freezetable;
		if (!TransactionIdIsNormal(limit))
			limit = FirstNormalTransactionId;

		*xidFullScanLimit = limit;

		/*
		 * Similar to the above, determine the table freeze age to use for
		 * multixacts: as specified by the caller, or
		 * vacuum_multixact_freeze_table_age, but in any case not more than
		 * autovacuum_multixact_freeze_table_age * 0.95, so that if you have
		 * e.g. nightly VACUUM schedule, the nightly VACUUM gets a chance to
		 * freeze multixacts before anti-wraparound autovacuum is launched.
		 */
		freezetable = multixact_freeze_table_age;
		if (freezetable < 0)
			freezetable = vacuum_multixact_freeze_table_age;
		freezetable = Min(freezetable,
						  effective_multixact_freeze_max_age * 0.95);
		Assert(freezetable >= 0);

		/*
		 * Compute MultiXact limit causing a full-table vacuum, being careful
		 * to generate a valid MultiXact value.
		 */
		mxactLimit = ReadNextMultiXactId() - freezetable;
		if (mxactLimit < FirstMultiXactId)
			mxactLimit = FirstMultiXactId;

		*mxactFullScanLimit = mxactLimit;
	}
	else
	{
		Assert(mxactFullScanLimit == NULL);
	}
}

/*
 * vacuum_xid_failsafe_check() -- Used by VACUUM's wraparound failsafe
 * mechanism to determine if its table's relfrozenxid and relminmxid are now
 * dangerously far in the past.
 *
 * Input parameters are the target relation's relfrozenxid and relminmxid.
 *
 * When we return true, VACUUM caller triggers the failsafe.
 */
bool
vacuum_xid_failsafe_check(TransactionId relfrozenxid, MultiXactId relminmxid)
{
	TransactionId xid_skip_limit;
	MultiXactId multi_skip_limit;
	int			skip_index_vacuum;

	Assert(TransactionIdIsNormal(relfrozenxid));
	Assert(MultiXactIdIsValid(relminmxid));

	/*
	 * Determine the index skipping age to use. In any case no less than
	 * autovacuum_freeze_max_age * 1.05.
	 */
	skip_index_vacuum = Max(vacuum_failsafe_age, autovacuum_freeze_max_age * 1.05);

	xid_skip_limit = ReadNextTransactionId() - skip_index_vacuum;
	if (!TransactionIdIsNormal(xid_skip_limit))
		xid_skip_limit = FirstNormalTransactionId;

	if (TransactionIdPrecedes(relfrozenxid, xid_skip_limit))
	{
		/* The table's relfrozenxid is too old */
		return true;
	}

	/*
	 * Similar to above, determine the index skipping age to use for
	 * multixact. In any case no less than autovacuum_multixact_freeze_max_age *
	 * 1.05.
	 */
	skip_index_vacuum = Max(vacuum_multixact_failsafe_age,
							autovacuum_multixact_freeze_max_age * 1.05);

	multi_skip_limit = ReadNextMultiXactId() - skip_index_vacuum;
	if (multi_skip_limit < FirstMultiXactId)
		multi_skip_limit = FirstMultiXactId;

	if (MultiXactIdPrecedes(relminmxid, multi_skip_limit))
	{
		/* The table's relminmxid is too old */
		return true;
	}

	return false;
}

/*
 * vac_estimate_reltuples() -- estimate the new value for pg_class.reltuples
 *
 *		If we scanned the whole relation then we should just use the count of
 *		live tuples seen; but if we did not, we should not blindly extrapolate
 *		from that number, since VACUUM may have scanned a quite nonrandom
 *		subset of the table.  When we have only partial information, we take
 *		the old value of pg_class.reltuples/pg_class.relpages as a measurement
 *		of the tuple density in the unscanned pages.
 *
 *		Note: scanned_tuples should count only *live* tuples, since
 *		pg_class.reltuples is defined that way.
 */
double
vac_estimate_reltuples(Relation relation,
					   BlockNumber total_pages,
					   BlockNumber scanned_pages,
					   double scanned_tuples)
{
	BlockNumber old_rel_pages = relation->rd_rel->relpages;
	double		old_rel_tuples = relation->rd_rel->reltuples;
	double		old_density;
	double		unscanned_pages;
	double		total_tuples;

	/* If we did scan the whole table, just use the count as-is */
	if (scanned_pages >= total_pages)
		return scanned_tuples;

	/*
	 * If scanned_pages is zero but total_pages isn't, keep the existing value
	 * of reltuples.  (Note: we might be returning -1 in this case.)
	 */
	if (scanned_pages == 0)
		return old_rel_tuples;

	/*
	 * When successive VACUUM commands scan the same few pages again and
	 * again, without anything from the table really changing, there is a risk
	 * that our beliefs about tuple density will gradually become distorted.
	 * It's particularly important to avoid becoming confused in this way due
	 * to vacuumlazy.c implementation details.  For example, the tendency for
	 * our caller to always scan the last heap page should not ever cause us
	 * to believe that every page in the table must be just like the last
	 * page.
	 *
	 * We apply a heuristic to avoid these problems: if the relation is
	 * exactly the same size as it was at the end of the last VACUUM, and only
	 * a few of its pages (less than a quasi-arbitrary threshold of 2%) were
	 * scanned by this VACUUM, assume that reltuples has not changed at all.
	 */
	if (old_rel_pages == total_pages &&
		scanned_pages < (double) total_pages * 0.02)
		return old_rel_tuples;

	/*
	 * If old density is unknown, we can't do much except scale up
	 * scanned_tuples to match total_pages.
	 */
	if (old_rel_tuples < 0 || old_rel_pages == 0)
		return floor((scanned_tuples / scanned_pages) * total_pages + 0.5);

	/*
	 * Okay, we've covered the corner cases.  The normal calculation is to
	 * convert the old measurement to a density (tuples per page), then
	 * estimate the number of tuples in the unscanned pages using that figure,
	 * and finally add on the number of tuples in the scanned pages.
	 */
	old_density = old_rel_tuples / old_rel_pages;
	unscanned_pages = (double) total_pages - (double) scanned_pages;
	total_tuples = old_density * unscanned_pages + scanned_tuples;
	return floor(total_tuples + 0.5);
}


/*
 *	vac_update_relstats() -- update statistics for one relation
 *
 *		Update the whole-relation statistics that are kept in its pg_class
 *		row.  There are additional stats that will be updated if we are
 *		doing ANALYZE, but we always update these stats.  This routine works
 *		for both index and heap relation entries in pg_class.
 *
 *		We violate transaction semantics here by overwriting the rel's
 *		existing pg_class tuple with the new values.  This is reasonably
 *		safe as long as we're sure that the new values are correct whether or
 *		not this transaction commits.  The reason for doing this is that if
 *		we updated these tuples in the usual way, vacuuming pg_class itself
 *		wouldn't work very well --- by the time we got done with a vacuum
 *		cycle, most of the tuples in pg_class would've been obsoleted.  Of
 *		course, this only works for fixed-size not-null columns, but these are.
 *
 *		Another reason for doing it this way is that when we are in a lazy
 *		VACUUM and have PROC_IN_VACUUM set, we mustn't do any regular updates.
 *		Somebody vacuuming pg_class might think they could delete a tuple
 *		marked with xmin = our xid.
 *
 *		In addition to fundamentally nontransactional statistics such as
 *		relpages and relallvisible, we try to maintain certain lazily-updated
 *		DDL flags such as relhasindex, by clearing them if no longer correct.
 *		It's safe to do this in VACUUM, which can't run in parallel with
 *		CREATE INDEX/RULE/TRIGGER and can't be part of a transaction block.
 *		However, it's *not* safe to do it in an ANALYZE that's within an
 *		outer transaction, because for example the current transaction might
 *		have dropped the last index; then we'd think relhasindex should be
 *		cleared, but if the transaction later rolls back this would be wrong.
 *		So we refrain from updating the DDL flags if we're inside an outer
 *		transaction.  This is OK since postponing the flag maintenance is
 *		always allowable.
 *
 *		Note: num_tuples should count only *live* tuples, since
 *		pg_class.reltuples is defined that way.
 *
 *		This routine is shared by VACUUM and ANALYZE.
 */
void
vac_update_relstats(Relation relation,
					BlockNumber num_pages, double num_tuples,
					BlockNumber num_all_visible_pages,
					bool hasindex, TransactionId frozenxid,
					MultiXactId minmulti,
					bool in_outer_xact,
					bool isvacuum)
{
	Oid			relid = RelationGetRelid(relation);
	Relation	rd;
	HeapTuple	ctup;
	Form_pg_class pgcform;
	bool		dirty;

	/*
	 * In GPDB, all the data is stored in the segments, and the
	 * relpages/reltuples in the master reflect the sum of the values in
	 * all the segments. In VACUUM, don't overwrite relpages/reltuples with
	 * the values we counted in the QD node itself. We will dispatch the
	 * VACUUM to the segments after processing the QD node, and we will
	 * update relpages/reltuples then.
	 *
	 * Update stats for system tables normally, though (it'd better say
	 * "non-distributed" tables than system relations here, but for now
	 * it's effectively the same.)
	 */
	if (!IsSystemRelation(relation) && isvacuum)
	{
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			num_pages = relation->rd_rel->relpages;
			num_tuples = relation->rd_rel->reltuples;
			num_all_visible_pages = relation->rd_rel->relallvisible;
		}
		else if (Gp_role == GP_ROLE_EXECUTE)
		{
			vac_send_relstats_to_qd(relation,
									num_pages,
									num_tuples,
									num_all_visible_pages);
		}
	}
	
	/*
	 * We need a way to distinguish these 2 cases:
	 * a) ANALYZEd/VACUUMed table is empty
	 * b) Table has never been ANALYZEd/VACUUMed
	 * To do this, in case (a), we set relPages = 1. For case (b), relPages = 0.
	 */
	if (num_pages < 1.0)
	{
		/*
		 * When running in utility mode in the QD node, we get the number of
		 * tuples of an AO table from the pg_aoseg table, but we don't know
		 * the file size, so that's always 0. Ignore the tuple count we got,
		 * and set reltuples to 0 instead, to avoid storing a confusing
		 * combination, and to avoid hitting the Assert below (which we
		 * inherited from upstream).
		 *
		 * It's perhaps not such a great idea to overwrite perfectly good
		 * relpages/reltuples estimates in utility mode, but that's what we
		 * do for heap tables, too, because we don't have even a tuple count
		 * for them. At least this is consistent.
		 * 
		 * If there is an external index(like zombodb), the file size is 0 
		 * as well and an error will occur because the Gp_role is 
		 * GP_ROLE_EXECUTE. This part is only for AO table, so we avoid
		 * executing here in this situation.
		 */
		if (num_tuples >= 1.0 && RelationIsAppendOptimized(relation))
		{
			Assert(Gp_role == GP_ROLE_UTILITY);
			Assert(!IsSystemRelation(relation));
			Assert(RelationStorageIsAO(relation));
			num_tuples = 0;
		}

		AssertImply(RelationIsAppendOptimized(relation), num_tuples < 1.0);
		num_pages = 1.0;
	}

	rd = table_open(RelationRelationId, RowExclusiveLock);

	/* Fetch a copy of the tuple to scribble on */
	ctup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relid));
	if (!HeapTupleIsValid(ctup))
		elog(ERROR, "pg_class entry for relid %u vanished during vacuuming",
			 relid);
	pgcform = (Form_pg_class) GETSTRUCT(ctup);

	/* Apply statistical updates, if any, to copied tuple */

	/* GPDB-specific not allow change relpages and reltuples when vacuum in utility mode on QD
	 * Because there's a chance that we overwrite perfectly good stats with zeros
	 */

	bool ifUpdate = ! (IS_QUERY_DISPATCHER() && Gp_role == GP_ROLE_UTILITY) || IS_SINGLENODE();

	dirty = false;
	if (pgcform->relpages != (int32) num_pages && ifUpdate)
	{
		pgcform->relpages = (int32) num_pages;
		dirty = true;
	}
	if (pgcform->reltuples != (float4) num_tuples && ifUpdate)
	{
		pgcform->reltuples = (float4) num_tuples;
		dirty = true;
	}
	if (pgcform->relallvisible != (int32) num_all_visible_pages)
	{
		pgcform->relallvisible = (int32) num_all_visible_pages;
		dirty = true;
	}

	elog(DEBUG2, "Vacuum oid=%u pages=%d tuples=%f",
		 relid, pgcform->relpages, pgcform->reltuples);

	/* Apply DDL updates, but not inside an outer transaction (see above) */

	if (!in_outer_xact)
	{
		/*
		 * If we didn't find any indexes, reset relhasindex.
		 */
		if (pgcform->relhasindex && !hasindex)
		{
			pgcform->relhasindex = false;
			dirty = true;
		}

		/* We also clear relhasrules and relhastriggers if needed */
		if (pgcform->relhasrules && relation->rd_rules == NULL)
		{
			pgcform->relhasrules = false;
			dirty = true;
		}
		if (pgcform->relhastriggers && relation->trigdesc == NULL)
		{
			pgcform->relhastriggers = false;
			dirty = true;
		}
	}

	/*
	 * Update relfrozenxid, unless caller passed InvalidTransactionId
	 * indicating it has no new data.
	 *
	 * Ordinarily, we don't let relfrozenxid go backwards: if things are
	 * working correctly, the only way the new frozenxid could be older would
	 * be if a previous VACUUM was done with a tighter freeze_min_age, in
	 * which case we don't want to forget the work it already did.  However,
	 * if the stored relfrozenxid is "in the future", then it must be corrupt
	 * and it seems best to overwrite it with the cutoff we used this time.
	 * This should match vac_update_datfrozenxid() concerning what we consider
	 * to be "in the future".
	 *
	 * GPDB: We check if pgcform->relfrozenxid is valid because AO and CO
	 * tables should have relfrozenxid as InvalidTransactionId.
	 */
	if (TransactionIdIsNormal(frozenxid) &&
		TransactionIdIsValid(pgcform->relfrozenxid) &&
		pgcform->relfrozenxid != frozenxid &&
		(TransactionIdPrecedes(pgcform->relfrozenxid, frozenxid) ||
		 TransactionIdPrecedes(ReadNextTransactionId(),
							   pgcform->relfrozenxid)))
	{
		pgcform->relfrozenxid = frozenxid;
		dirty = true;
	}

	/* Similarly for relminmxid */
	if (MultiXactIdIsValid(minmulti) &&
		pgcform->relminmxid != minmulti &&
		(MultiXactIdPrecedes(pgcform->relminmxid, minmulti) ||
		 MultiXactIdPrecedes(ReadNextMultiXactId(), pgcform->relminmxid)))
	{
		pgcform->relminmxid = minmulti;
		dirty = true;
	}

	/* If anything changed, write out the tuple. */
	if (dirty)
		heap_inplace_update(rd, ctup);

	table_close(rd, RowExclusiveLock);
}

/* GPDB_14_MERGE_FIXME: see comments in vac_update_datfrozenxid */
#if 0
/*
 * fetch_database_tuple - Fetch a copy of database tuple from pg_database.
 *
 * This using disk heap table instead of system cache.
 * relation: opened pg_database relation in vac_update_datfrozenxid().
 */
static HeapTuple
fetch_database_tuple(Relation relation, Oid dbOid)
{
	ScanKeyData skey[1];
	SysScanDesc sscan;
	HeapTuple	tuple = NULL;

	ScanKeyInit(&skey[0],
				Anum_pg_database_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(dbOid));

	sscan = systable_beginscan(relation, DatabaseOidIndexId, true,
							   NULL, 1, skey);

	tuple = systable_getnext(sscan);
	if (HeapTupleIsValid(tuple))
		tuple = heap_copytuple(tuple);

	systable_endscan(sscan);

	return tuple;
}
#endif

/*
 *	vac_update_datfrozenxid() -- update pg_database.datfrozenxid for our DB
 *
 *		Update pg_database's datfrozenxid entry for our database to be the
 *		minimum of the pg_class.relfrozenxid values.
 *
 *		Similarly, update our datminmxid to be the minimum of the
 *		pg_class.relminmxid values.
 *
 *		If we are able to advance either pg_database value, also try to
 *		truncate pg_xact and pg_multixact.
 *
 *		We violate transaction semantics here by overwriting the database's
 *		existing pg_database tuple with the new values.  This is reasonably
 *		safe since the new values are correct whether or not this transaction
 *		commits.  As with vac_update_relstats, this avoids leaving dead tuples
 *		behind after a VACUUM.
 */
void
vac_update_datfrozenxid(void)
{
	HeapTuple	tuple;
	Form_pg_database dbform;
	Relation	relation;
	SysScanDesc scan;
	HeapTuple	classTup;
	TransactionId newFrozenXid;
	MultiXactId newMinMulti;
	TransactionId lastSaneFrozenXid;
	MultiXactId lastSaneMinMulti;
	bool		bogus = false;
	bool		dirty = false;
	ScanKeyData key[1];

	/*
	 * Restrict this task to one backend per database.  This avoids race
	 * conditions that would move datfrozenxid or datminmxid backward.  It
	 * avoids calling vac_truncate_clog() with a datfrozenxid preceding a
	 * datfrozenxid passed to an earlier vac_truncate_clog() call.
	 */
	LockDatabaseFrozenIds(ExclusiveLock);

	/*
	 * Initialize the "min" calculation with
	 * GetOldestNonRemovableTransactionId(), which is a reasonable
	 * approximation to the minimum relfrozenxid for not-yet-committed
	 * pg_class entries for new tables; see AddNewRelationTuple().  So we
	 * cannot produce a wrong minimum by starting with this.
	 *
	 * GPDB: Use GetOldestNonRemovableTransactionId with updateGlobalVis false here,
	 * rather than GetOldestNonRemovableTransactionId. We don't
	 * want to include effects of distributed transactions in this. If a
	 * database's datfrozenxid is past the oldest XID as determined by
	 * distributed transactions, we will nevertheless never encounter such
	 * XIDs on disk.
	 */
	newFrozenXid = GetLocalOldestNonRemovableTransactionId(NULL, false);

	/*
	 * Similarly, initialize the MultiXact "min" with the value that would be
	 * used on pg_class for new tables.  See AddNewRelationTuple().
	 */
	newMinMulti = GetOldestMultiXactId();

	/*
	 * Identify the latest relfrozenxid and relminmxid values that we could
	 * validly see during the scan.  These are conservative values, but it's
	 * not really worth trying to be more exact.
	 */
	lastSaneFrozenXid = ReadNextTransactionId();
	lastSaneMinMulti = ReadNextMultiXactId();

	/*
	 * We must seqscan pg_class to find the minimum Xid, because there is no
	 * index that can help us here.
	 */
	relation = table_open(RelationRelationId, AccessShareLock);

	scan = systable_beginscan(relation, InvalidOid, false,
							  NULL, 0, NULL);

	while ((classTup = systable_getnext(scan)) != NULL)
	{
		Form_pg_class classForm = (Form_pg_class) GETSTRUCT(classTup);

		/*
		 * Only consider relations able to hold unfrozen XIDs (anything else
		 * should have InvalidTransactionId in relfrozenxid anyway).
		 */
		if (classForm->relkind != RELKIND_RELATION &&
			classForm->relkind != RELKIND_DIRECTORY_TABLE &&
			classForm->relkind != RELKIND_MATVIEW &&
			classForm->relkind != RELKIND_TOASTVALUE &&
			classForm->relkind != RELKIND_AOSEGMENTS &&
			classForm->relkind != RELKIND_AOVISIMAP &&
			classForm->relkind != RELKIND_AOBLOCKDIR)
		{
			Assert(!TransactionIdIsValid(classForm->relfrozenxid));
			Assert(!MultiXactIdIsValid(classForm->relminmxid));
			continue;
		}

		/*
		 * Some table AMs might not need per-relation xid / multixid horizons.
		 * It therefore seems reasonable to allow relfrozenxid and relminmxid
		 * to not be set (i.e. set to their respective Invalid*Id)
		 * independently. Thus validate and compute horizon for each only if
		 * set.
		 *
		 * If things are working properly, no relation should have a
		 * relfrozenxid or relminmxid that is "in the future".  However, such
		 * cases have been known to arise due to bugs in pg_upgrade.  If we
		 * see any entries that are "in the future", chicken out and don't do
		 * anything.  This ensures we won't truncate clog & multixact SLRUs
		 * before those relations have been scanned and cleaned up.
		 */

		if (TransactionIdIsValid(classForm->relfrozenxid))
		{
			Assert(TransactionIdIsNormal(classForm->relfrozenxid));

			/* check for values in the future */
			if (TransactionIdPrecedes(lastSaneFrozenXid, classForm->relfrozenxid))
			{
				bogus = true;
				break;
			}

			/* determine new horizon */
			if (TransactionIdPrecedes(classForm->relfrozenxid, newFrozenXid))
				newFrozenXid = classForm->relfrozenxid;
		}

		if (MultiXactIdIsValid(classForm->relminmxid))
		{
			/* check for values in the future */
			if (MultiXactIdPrecedes(lastSaneMinMulti, classForm->relminmxid))
			{
				bogus = true;
				break;
			}

			/* determine new horizon */
			if (MultiXactIdPrecedes(classForm->relminmxid, newMinMulti))
				newMinMulti = classForm->relminmxid;
		}
	}

	/* we're done with pg_class */
	systable_endscan(scan);
	table_close(relation, AccessShareLock);

	/* chicken out if bogus data found */
	if (bogus)
	{
		/*
		 * Cherry-pick from GPDB FIXME: is it right for PG14?
		 * Ignore this for error.
		 */
#if 0
		 UnLockDatabaseFrozenIds(ExclusiveLock);
#endif
		return;
	}

	Assert(TransactionIdIsNormal(newFrozenXid));
	Assert(MultiXactIdIsValid(newMinMulti));

	/* Now fetch the pg_database tuple we need to update. */
	relation = table_open(DatabaseRelationId, RowExclusiveLock);

	/*
	 * Get the pg_database tuple to scribble on.  Note that this does not
	 * directly rely on the syscache to avoid issues with flattened toast
	 * values for the in-place update.
	 */
	ScanKeyInit(&key[0],
				Anum_pg_database_oid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(MyDatabaseId));

	scan = systable_beginscan(relation, DatabaseOidIndexId, true,
							  NULL, 1, key);
	tuple = systable_getnext(scan);
	tuple = heap_copytuple(tuple);
	systable_endscan(scan);

	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "could not find tuple for database %u", MyDatabaseId);

	dbform = (Form_pg_database) GETSTRUCT(tuple);

	/*
	 * As in vac_update_relstats(), we ordinarily don't want to let
	 * datfrozenxid go backward; but if it's "in the future" then it must be
	 * corrupt and it seems best to overwrite it.
	 */
	if (dbform->datfrozenxid != newFrozenXid &&
		(TransactionIdPrecedes(dbform->datfrozenxid, newFrozenXid) ||
		 TransactionIdPrecedes(lastSaneFrozenXid, dbform->datfrozenxid)))
	{
		dbform->datfrozenxid = newFrozenXid;
		dirty = true;
	}
	else
		newFrozenXid = dbform->datfrozenxid;

	/* Ditto for datminmxid */
	if (dbform->datminmxid != newMinMulti &&
		(MultiXactIdPrecedes(dbform->datminmxid, newMinMulti) ||
		 MultiXactIdPrecedes(lastSaneMinMulti, dbform->datminmxid)))
	{
		dbform->datminmxid = newMinMulti;
		dirty = true;
	}
	else
		newMinMulti = dbform->datminmxid;

	if (dirty)
	{
		/*
		* GPDB_14_MERGE_FIXME
		* Remove some codes(fetch_database_tuple) from GPDB upstream.
		* Check if pg upstream has already fixed it or check from GPDB
		* https://github.com/greenplum-db/gpdb/commit/373e676de819fc0cdadfb59d35d9279abe3d11d9 
		*/

		heap_inplace_update(relation, tuple);
#ifdef FAULT_INJECTOR
		FaultInjector_InjectFaultIfSet(
			"vacuum_update_dat_frozen_xid", DDLNotSpecified,
			NameStr(dbform->datname), "");
#endif
	}

	heap_freetuple(tuple);
	table_close(relation, RowExclusiveLock);

	/*
	 * If we were able to advance datfrozenxid or datminmxid, see if we can
	 * truncate pg_xact and/or pg_multixact.  Also do it if the shared
	 * XID-wrap-limit info is stale, since this action will update that too.
	 */
	if (dirty || ForceTransactionIdLimitUpdate())
		vac_truncate_clog(newFrozenXid, newMinMulti,
						  lastSaneFrozenXid, lastSaneMinMulti);
}


/*
 *	vac_truncate_clog() -- attempt to truncate the commit log
 *
 *		Scan pg_database to determine the system-wide oldest datfrozenxid,
 *		and use it to truncate the transaction commit log (pg_xact).
 *		Also update the XID wrap limit info maintained by varsup.c.
 *		Likewise for datminmxid.
 *
 *		The passed frozenXID and minMulti are the updated values for my own
 *		pg_database entry. They're used to initialize the "min" calculations.
 *		The caller also passes the "last sane" XID and MXID, since it has
 *		those at hand already.
 *
 *		This routine is only invoked when we've managed to change our
 *		DB's datfrozenxid/datminmxid values, or we found that the shared
 *		XID-wrap-limit info is stale.
 */
static void
vac_truncate_clog(TransactionId frozenXID,
				  MultiXactId minMulti,
				  TransactionId lastSaneFrozenXid,
				  MultiXactId lastSaneMinMulti)
{
	TransactionId nextXID = ReadNextTransactionId();
	Relation	relation;
	TableScanDesc scan;
	HeapTuple	tuple;
	Oid			oldestxid_datoid;
	Oid			minmulti_datoid;
	bool		bogus = false;
	bool		frozenAlreadyWrapped = false;

	/* Restrict task to one backend per cluster; see SimpleLruTruncate(). */
	LWLockAcquire(WrapLimitsVacuumLock, LW_EXCLUSIVE);

	/* init oldest datoids to sync with my frozenXID/minMulti values */
	oldestxid_datoid = MyDatabaseId;
	minmulti_datoid = MyDatabaseId;

	/*
	 * Scan pg_database to compute the minimum datfrozenxid/datminmxid
	 *
	 * Since vac_update_datfrozenxid updates datfrozenxid/datminmxid in-place,
	 * the values could change while we look at them.  Fetch each one just
	 * once to ensure sane behavior of the comparison logic.  (Here, as in
	 * many other places, we assume that fetching or updating an XID in shared
	 * storage is atomic.)
	 *
	 * Note: we need not worry about a race condition with new entries being
	 * inserted by CREATE DATABASE.  Any such entry will have a copy of some
	 * existing DB's datfrozenxid, and that source DB cannot be ours because
	 * of the interlock against copying a DB containing an active backend.
	 * Hence the new entry will not reduce the minimum.  Also, if two VACUUMs
	 * concurrently modify the datfrozenxid's of different databases, the
	 * worst possible outcome is that pg_xact is not truncated as aggressively
	 * as it could be.
	 */
	relation = table_open(DatabaseRelationId, AccessShareLock);

	scan = table_beginscan_catalog(relation, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		volatile FormData_pg_database *dbform = (Form_pg_database) GETSTRUCT(tuple);
		TransactionId datfrozenxid = dbform->datfrozenxid;
		TransactionId datminmxid = dbform->datminmxid;

		Assert(TransactionIdIsNormal(datfrozenxid));
		Assert(MultiXactIdIsValid(datminmxid));

		/*
		 * If things are working properly, no database should have a
		 * datfrozenxid or datminmxid that is "in the future".  However, such
		 * cases have been known to arise due to bugs in pg_upgrade.  If we
		 * see any entries that are "in the future", chicken out and don't do
		 * anything.  This ensures we won't truncate clog before those
		 * databases have been scanned and cleaned up.  (We will issue the
		 * "already wrapped" warning if appropriate, though.)
		 */
		if (TransactionIdPrecedes(lastSaneFrozenXid, datfrozenxid) ||
			MultiXactIdPrecedes(lastSaneMinMulti, datminmxid))
			bogus = true;

		if (TransactionIdPrecedes(nextXID, datfrozenxid))
			frozenAlreadyWrapped = true;
		else if (TransactionIdPrecedes(datfrozenxid, frozenXID))
		{
			frozenXID = datfrozenxid;
			oldestxid_datoid = dbform->oid;
		}

		if (MultiXactIdPrecedes(datminmxid, minMulti))
		{
			minMulti = datminmxid;
			minmulti_datoid = dbform->oid;
		}
	}

	table_endscan(scan);

	table_close(relation, AccessShareLock);

	/*
	 * Do not truncate CLOG if we seem to have suffered wraparound already;
	 * the computed minimum XID might be bogus.  This case should now be
	 * impossible due to the defenses in GetNewTransactionId, but we keep the
	 * test anyway.
	 */
	if (frozenAlreadyWrapped)
	{
		ereport(WARNING,
				(errmsg("some databases have not been vacuumed in over 2 billion transactions"),
				 errdetail("You might have already suffered transaction-wraparound data loss.")));
		return;
	}

	/* chicken out if data is bogus in any other way */
	if (bogus)
		return;

	/*
	 * Advance the oldest value for commit timestamps before truncating, so
	 * that if a user requests a timestamp for a transaction we're truncating
	 * away right after this point, they get NULL instead of an ugly "file not
	 * found" error from slru.c.  This doesn't matter for xact/multixact
	 * because they are not subject to arbitrary lookups from users.
	 */
	AdvanceOldestCommitTsXid(frozenXID);

	/*
	 * Truncate CLOG, multixact and CommitTs to the oldest computed value.
	 */
	TruncateCLOG(frozenXID, oldestxid_datoid);
	TruncateCommitTs(frozenXID);
	TruncateMultiXact(minMulti, minmulti_datoid);

	/*
	 * Update the wrap limit for GetNewTransactionId and creation of new
	 * MultiXactIds.  Note: these functions will also signal the postmaster
	 * for an(other) autovac cycle if needed.   XXX should we avoid possibly
	 * signaling twice?
	 */
	SetTransactionIdLimit(frozenXID, oldestxid_datoid);
	SetMultiXactIdLimit(minMulti, minmulti_datoid, false);

	LWLockRelease(WrapLimitsVacuumLock);
}


/*
 *	vacuum_rel() -- vacuum one heap relation
 *
 *		relid identifies the relation to vacuum.  If relation is supplied,
 *		use the name therein for reporting any failure to open/lock the rel;
 *		do not use it once we've successfully opened the rel, since it might
 *		be stale.
 *
 *		Returns true if it's okay to proceed with a requested ANALYZE
 *		operation on this table.
 *
 *		Doing one heap at a time incurs extra overhead, since we need to
 *		check that the heap exists again just before we vacuum it.  The
 *		reason that we do this is so that vacuuming can be spread across
 *		many small transactions.  Otherwise, two-phase locking would require
 *		us to lock the entire database during one pass of the vacuum cleaner.
 *
 *		At entry and exit, we are not inside a transaction.
 */
static bool
vacuum_rel(Oid relid, RangeVar *relation, VacuumParams *params,
		   bool recursing)
{
	LOCKMODE		lmode;
	Relation		rel;
	LockRelId		lockrelid;
	Oid			toast_relid;
	Oid			aoseg_relid = InvalidOid;
	Oid			aoblkdir_relid = InvalidOid;
	Oid			aovisimap_relid = InvalidOid;
	Oid			save_userid;
	RangeVar		*this_rangevar = NULL;
	int			ao_vacuum_phase;
	int			save_sec_context;
	int			save_nestlevel;
	bool			is_appendoptimized;
	bool			is_toast;

	Assert(params != NULL);

#ifdef FAULT_INJECTOR
	char			onerelname[NAMEDATALEN];
#endif

 	ao_vacuum_phase = (params->options & VACUUM_AO_PHASE_MASK);

	/* Begin a transaction for vacuuming this relation */
	StartTransactionCommand();

	if (!(params->options & VACOPT_FULL))
	{
		/*
		 * PostgreSQL does this:
		 * In lazy vacuum, we can set the PROC_IN_VACUUM flag, which lets
		 * other concurrent VACUUMs know that they can ignore this one while
		 * determining their OldestXmin.  (The reason we don't set it during a
		 * full VACUUM is exactly that we may have to run user-defined
		 * functions for functional indexes, and we want to make sure that if
		 * they use the snapshot set above, any tuples it requires can't get
		 * removed from other tables.  An index function that depends on the
		 * contents of other tables is arguably broken, but we won't break it
		 * here by violating transaction semantics.)
		 *
		 * GPDB doesn't use PROC_IN_VACUUM, as lazy vacuum for bitmap
		 * indexed tables performs reindex causing updates to pg_class
		 * tuples for index entries.
		 *
		 * We also set the VACUUM_FOR_WRAPAROUND flag, which is passed down by
		 * autovacuum; it's used to avoid canceling a vacuum that was invoked
		 * in an emergency.
		 *
		 * Note: these flags remain set until CommitTransaction or
		 * AbortTransaction.  We don't want to clear them until we reset
		 * MyProc->xid/xmin, otherwise GetOldestNonRemovableTransactionId()
		 * might appear to go backwards, which is probably Not Good.  (We also
		 * set PROC_IN_VACUUM *before* taking our own snapshot, so that our
		 * xmin doesn't become visible ahead of setting the flag.)
		 */
		LWLockAcquire(ProcArrayLock, LW_EXCLUSIVE);
#if 0 /* Upstream code not applicable to GPDB */
		MyProc->statusFlags |= PROC_IN_VACUUM;
#endif
		if (params->is_wraparound)
			MyProc->statusFlags |= PROC_VACUUM_FOR_WRAPAROUND;
		ProcGlobal->statusFlags[MyProc->pgxactoff] = MyProc->statusFlags;
		LWLockRelease(ProcArrayLock);
	}

	/*
	 * Need to acquire a snapshot to prevent pg_subtrans from being truncated,
	 * cutoff xids in local memory wrapping around, and to have updated xmin
	 * horizons.
	 */
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * Check for user-requested abort.  Note we want this to be inside a
	 * transaction, so xact.c doesn't issue useless WARNING.
	 */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Determine the type of lock we want --- hard exclusive lock for a FULL
	 * vacuum, but just ShareUpdateExclusiveLock for concurrent vacuum. Either
	 * way, we can be sure that no other backend is vacuuming the same table.
	 */
	// FIXME: This fault point was roughly here before. It's kept here to keep
	// the regression tests from hanging, but need to check that the tests
	// still make sense. And "drop phase" isn't a term we use anymore.
	if (ao_vacuum_phase == VACOPT_AO_POST_CLEANUP_PHASE)
	{
		SIMPLE_FAULT_INJECTOR("vacuum_relation_open_relation_during_drop_phase");
	}

	// FIXME: what's the right level for AO tables?
	lmode = (params->options & VACOPT_FULL) ?
		AccessExclusiveLock : ShareUpdateExclusiveLock;

	/* open the relation and get the appropriate lock on it */
	rel = vacuum_open_relation(relid, relation, params->options,
							   params->log_min_duration >= 0, lmode);

	/* leave if relation could not be opened or locked */
	if (!rel)
	{
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}

#ifdef FAULT_INJECTOR
	// preserve relation name for us in fault tests
	strlcpy(onerelname, NameStr(rel->rd_rel->relname), NAMEDATALEN);
#endif

	/*
	 * Check if relation needs to be skipped based on ownership.  This check
	 * happens also when building the relation list to vacuum for a manual
	 * operation, and needs to be done additionally here as VACUUM could
	 * happen across multiple transactions where relation ownership could have
	 * changed in-between.  Make sure to only generate logs for VACUUM in this
	 * case.
	 */
	if (!vacuum_is_relation_owner(RelationGetRelid(rel),
								  rel->rd_rel,
								  params->options & VACOPT_VACUUM))
	{
		relation_close(rel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}

	/*
	 * Check that it's of a vacuumable relkind.
	 */
	if (rel->rd_rel->relkind != RELKIND_RELATION &&
		rel->rd_rel->relkind != RELKIND_DIRECTORY_TABLE &&
		rel->rd_rel->relkind != RELKIND_MATVIEW &&
		rel->rd_rel->relkind != RELKIND_TOASTVALUE &&
		rel->rd_rel->relkind != RELKIND_PARTITIONED_TABLE &&
		rel->rd_rel->relkind != RELKIND_AOSEGMENTS &&
		rel->rd_rel->relkind != RELKIND_AOBLOCKDIR &&
		rel->rd_rel->relkind != RELKIND_AOVISIMAP)
	{
		ereport(WARNING,
				(errmsg("skipping \"%s\" --- cannot vacuum non-tables or special system tables",
						RelationGetRelationName(rel))));
		relation_close(rel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}

	/*
	 * Silently ignore tables that are temp tables of other backends ---
	 * trying to vacuum these will lead to great unhappiness, since their
	 * contents are probably not up-to-date on disk.  (We don't throw a
	 * warning here; it would just lead to chatter during a database-wide
	 * VACUUM.)
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
	{
		relation_close(rel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}

	/*
	 * Silently ignore partitioned tables as there is no work to be done.  The
	 * useful work is on their child partitions, which have been queued up for
	 * us separately.
	 */
	if (rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE)
	{
		relation_close(rel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		/* It's OK to proceed with ANALYZE on this table */
		return true;
	}

	/*
	 * Get a session-level lock too. This will protect our access to the
	 * relation across multiple transactions, so that we can vacuum the
	 * relation's TOAST table (if any) secure in the knowledge that no one is
	 * deleting the parent relation.
	 *
	 * NOTE: this cannot block, even if someone else is waiting for access,
	 * because the lock manager knows that both lock requests are from the
	 * same process.
	 */
	lockrelid = rel->rd_lockInfo.lockRelId;
	LockRelationIdForSession(&lockrelid, lmode);

	/*
	 * Set index_cleanup option based on index_cleanup reloption if it wasn't
	 * specified in VACUUM command, or when running in an autovacuum worker
	 */
	if (params->index_cleanup == VACOPTVALUE_UNSPECIFIED)
	{
		StdRdOptIndexCleanup vacuum_index_cleanup;

		if (rel->rd_options == NULL)
			vacuum_index_cleanup = STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO;
		else
			vacuum_index_cleanup =
				((StdRdOptions *) rel->rd_options)->vacuum_index_cleanup;

		if (vacuum_index_cleanup == STDRD_OPTION_VACUUM_INDEX_CLEANUP_AUTO)
			params->index_cleanup = VACOPTVALUE_AUTO;
		else if (vacuum_index_cleanup == STDRD_OPTION_VACUUM_INDEX_CLEANUP_ON)
			params->index_cleanup = VACOPTVALUE_ENABLED;
		else
		{
			Assert(vacuum_index_cleanup ==
				   STDRD_OPTION_VACUUM_INDEX_CLEANUP_OFF);
			params->index_cleanup = VACOPTVALUE_DISABLED;
		}
	}

	/*
	 * Set truncate option based on truncate reloption if it wasn't specified
	 * in VACUUM command, or when running in an autovacuum worker
	 */
	if (params->truncate == VACOPTVALUE_UNSPECIFIED)
	{
		if (rel->rd_options == NULL ||
			((StdRdOptions *) rel->rd_options)->vacuum_truncate)
			params->truncate = VACOPTVALUE_ENABLED;
		else
			params->truncate = VACOPTVALUE_DISABLED;
	}

	/*
	 * Remember the relation's TOAST relation for later, if the caller asked
	 * us to process it.  In VACUUM FULL, though, the toast table is
	 * automatically rebuilt by cluster_rel so we shouldn't recurse to it.
	 *
	 * GPDB: Also remember the AO segment relations for later.
	 */
	if ((params->options & VACOPT_PROCESS_TOAST) != 0 &&
		(params->options & VACOPT_FULL) == 0)
		toast_relid = rel->rd_rel->reltoastrelid;
	else
		toast_relid = InvalidOid;

	if (RelationStorageIsAO(rel))
	{
		/*
		 * GPDB: AO tables should never be passed into vacuum_rel if the
		 * AO_AUX_ONLY option is specified
		 */
		Assert(!(params->options & VACOPT_AO_AUX_ONLY));
		GetAppendOnlyEntryAuxOids(rel,
								  &aoseg_relid,
								  &aoblkdir_relid, NULL,
								  &aovisimap_relid, NULL);
	}

	/*
	 * Check permissions.
	 *
	 * We allow the user to vacuum a table if he is superuser, the table
	 * owner, or the database owner (but in the latter case, only if it's not
	 * a shared relation).	pg_class_ownercheck includes the superuser case.
	 *
	 * Note we choose to treat permissions failure as a WARNING and keep
	 * trying to vacuum the rest of the DB --- is this appropriate?
	 */
	if (!(pg_class_ownercheck(RelationGetRelid(rel), GetUserId()) ||
		  (pg_database_ownercheck(MyDatabaseId, GetUserId()) && !rel->rd_rel->relisshared)))
	{
		if (Gp_role != GP_ROLE_EXECUTE)
		{
			if (rel->rd_rel->relisshared)
				ereport(WARNING,
						(errmsg("skipping \"%s\" --- only superuser can vacuum it",
								RelationGetRelationName(rel))));
			else if (rel->rd_rel->relnamespace == PG_CATALOG_NAMESPACE)
				ereport(WARNING,
						(errmsg("skipping \"%s\" --- only superuser or database owner can vacuum it",
								RelationGetRelationName(rel))));
			else
				ereport(WARNING,
						(errmsg("skipping \"%s\" --- only table or database owner can vacuum it",
								RelationGetRelationName(rel))));
		}
		relation_close(rel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}

	/*
	 * Check that it's a vacuumable relation; we used to do this in
	 * get_rel_oids() but seems safer to check after we've locked the
	 * relation.
	 */
	if ((rel->rd_rel->relkind != RELKIND_RELATION &&
		 rel->rd_rel->relkind != RELKIND_MATVIEW &&
		 rel->rd_rel->relkind != RELKIND_DIRECTORY_TABLE &&
		 rel->rd_rel->relkind != RELKIND_TOASTVALUE &&
		 rel->rd_rel->relkind != RELKIND_AOSEGMENTS &&
		 rel->rd_rel->relkind != RELKIND_AOBLOCKDIR &&
		 rel->rd_rel->relkind != RELKIND_AOVISIMAP)
		|| rel->rd_rel->relkind == RELKIND_FOREIGN_TABLE)
	{
		ereport(WARNING,
				(errmsg("skipping \"%s\" --- cannot vacuum non-tables, external tables, foreign tables or special system tables",
						RelationGetRelationName(rel))));
		relation_close(rel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}

#ifdef FAULT_INJECTOR
	if (ao_vacuum_phase == VACOPT_AO_POST_CLEANUP_PHASE)
	{
		FaultInjector_InjectFaultIfSet(
			"compaction_before_cleanup_phase",
			DDLNotSpecified,
			"",	// databaseName
			RelationGetRelationName(rel)); // tableName
	}
#endif

	/*
	 * Silently ignore tables that are temp tables of other backends ---
	 * trying to vacuum these will lead to great unhappiness, since their
	 * contents are probably not up-to-date on disk.  (We don't throw a
	 * warning here; it would just lead to chatter during a database-wide
	 * VACUUM.)
	 */
	if (RELATION_IS_OTHER_TEMP(rel))
	{
		relation_close(rel, lmode);
		PopActiveSnapshot();
		CommitTransactionCommand();
		return false;
	}

	is_appendoptimized = RelationStorageIsAO(rel);
	is_toast = (rel->rd_rel->relkind == RELKIND_TOASTVALUE);

	if (ao_vacuum_phase && !(is_appendoptimized || is_toast))
	{
		/* We were asked to some phase of AO vacuum, but it's not an AO table. Huh? */
		elog(ERROR, "AO vacuum phase was invoked on a non-AO table");
	}

	/*
	 * If it's a partitioned relation, on entry 'relation' refers to the table
	 * that the original command was issued on, and 'relid' is the actual partition
	 * we're processing. Build a rangevar representing this partition, so that we
	 * can dispatch it.
	 */
	MemoryContext oldcontext = MemoryContextSwitchTo(vac_context);
	this_rangevar = makeRangeVar(get_namespace_name(rel->rd_rel->relnamespace),
								 pstrdup(RelationGetRelationName(rel)),
								 -1);
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Switch to the table owner's userid, so that any index functions are run
	 * as that user.  Also lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command. (This is
	 * unnecessary, but harmless, for lazy VACUUM.)
	 */
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(rel->rd_rel->relowner,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

	/*
	 * If there are any bitmap indexes, we have to acquire a ShareLock for the
	 * table, since reindex is used later. Otherwise, concurrent vacuum and
	 * inserts may cause deadlock. MPP-5960
	 */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		int 		i, nindexes;
		bool 		has_bitmap = false;
		Relation   *i_rel = NULL;

		vac_open_indexes(rel, AccessShareLock, &nindexes, &i_rel);
		if (i_rel != NULL)
		{
			for (i = 0; i < nindexes; i++)
			{
				if (RelationIsBitmapIndex(i_rel[i]))
				{
					has_bitmap = true;
					break;
				}
			}
		}
		vac_close_indexes(nindexes, i_rel, AccessShareLock);

		if (has_bitmap)
			LockRelation(rel, ShareLock);
	}
	/* TODO: vacuum directory table's temp files */

	if (!is_appendoptimized && (params->options & VACOPT_FULL))
	{
		ClusterParams cluster_params = {0};

		/* close relation before vacuuming, but hold lock until commit */
		relation_close(rel, NoLock);
		rel = NULL;

		if ((params->options & VACOPT_VERBOSE) != 0)
			cluster_params.options |= CLUOPT_VERBOSE;

		/* VACUUM FULL is now a variant of CLUSTER; see cluster.c */
		cluster_rel(relid, InvalidOid, &cluster_params);
	}
	else /* Heap vacuum or AO/CO vacuum in specific phase */
		table_relation_vacuum(rel, params, vac_strategy);

	if (IS_QD_OR_SINGLENODE() && (params->options & VACOPT_FULL))
	{
		/*
		 * Update view data status:
		 * VACUUM FULL will change the physical pages of table.
		 * FIXME: for auto vacuum process on segments, it's in utility mode,
		 * we can't handle it yet. But it's not a problem for SERVERLESS.
		 */
		SetRelativeMatviewAuxStatus(relid,
									MV_DATA_STATUS_UP_REORGANIZED,
									MV_DATA_STATUS_TRANSFER_DIRECTION_ALL);
	}

	/* Roll back any GUC changes executed by index functions */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);
	
	/* all done with this class, but hold lock until commit */
	if (rel)
		relation_close(rel, NoLock);

	/*
	 * Complete the transaction and free all temporary memory used.
	 */
	PopActiveSnapshot();
	CommitTransactionCommand();

	/* entrance of Append-Optimized table vacuum */
	if (is_appendoptimized && ao_vacuum_phase == 0)
	{
		int orig_options = params->options;

		/* orchestrate the AO vacuum phases */

		/*
		 * Do cleanup first, to reclaim as much space as possible that
		 * was left behind from previous VACUUMs. This runs under local
		 * transactions.
		 */
		params->options = orig_options | VACOPT_AO_PRE_CLEANUP_PHASE;
		vacuum_rel(relid, this_rangevar, params, false);

		/* Compact. This runs in a distributed transaction.  */
		params->options = orig_options | VACOPT_AO_COMPACT_PHASE;
		vacuum_rel(relid, this_rangevar, params, false);

		/* 
		 * Do a final round of cleanup. Hopefully, this can drop the segments
		 * that were compacted in the previous phase.
		 */
		params->options = orig_options | VACOPT_AO_POST_CLEANUP_PHASE;
		vacuum_rel(relid, this_rangevar, params, false);

		params->options = orig_options;
	}

	/*
	 * In an append-only table, the auxiliary tables are cleaned up in
	 * the POST_CLEANUP phase. Ignore them in other phases.
	 */
	if (is_appendoptimized && ao_vacuum_phase != VACOPT_AO_POST_CLEANUP_PHASE)
	{
		toast_relid = InvalidOid;
		aoseg_relid = InvalidOid;
		aoblkdir_relid = InvalidOid;
		aovisimap_relid = InvalidOid;
	}

	int orig_option = params->options;
	params->options = params->options & (~VACUUM_AO_PHASE_MASK);

	/*
	 * If the relation has a secondary toast rel, vacuum that too while we
	 * still hold the session lock on the main table.  Note however that
	 * "analyze" will not get done on the toast table.  This is good, because
	 * the toaster always uses hardcoded index access and statistics are
	 * totally unimportant for toast relations.
	 * 
	 * Note, for GPDB, set recursing to true for auxilary tables to avoid
	 * being dispatched vacuum separately.
	 */
	if (toast_relid != InvalidOid)
		vacuum_rel(toast_relid, NULL, params, true);

	/* do the same for an AO segments table, if any */
	if (aoseg_relid != InvalidOid)
		vacuum_rel(aoseg_relid, NULL , params, true);

	/* do the same for an AO block directory table, if any */
	if (aoblkdir_relid != InvalidOid)
		vacuum_rel(aoblkdir_relid, NULL, params, true);

	/* do the same for an AO visimap, if any */
	if (aovisimap_relid != InvalidOid)
		vacuum_rel(aovisimap_relid, NULL, params, true);
	params->options = orig_option;

	/*
	 * Don't dispatch auto-vacuum. Each segment performs auto-vacuum as per
	 * its own need.
	 */
	if ((Gp_role == GP_ROLE_DISPATCH || IS_SINGLENODE()) && !recursing &&
		!IsAutoVacuumWorkerProcess() &&
		(!is_appendoptimized || ao_vacuum_phase))
	{
		VacuumStatsContext stats_context;
		char	   *vsubtype;

		/*
		 * SINGLENODE_FIXME:
		 * From my understanding, we don't need to open a transaction here since
		 * we don't need to dispatch.
		 * However, there is some assert below to make sure we are in a transaction.
		 * Make it more reasonable in the future.
		 */
		/*
		 * Dispatching needs a transaction. At least in some error scenarios,
		 * it uses TopTransactionContext to store stuff.
		 */
		StartTransactionCommand();
		PushActiveSnapshot(GetTransactionSnapshot());

		if (Gp_role == GP_ROLE_DISPATCH)
		{
			stats_context.updated_stats = NIL;
			dispatchVacuum(params, relid, &stats_context);
			vac_update_relstats_from_list(&stats_context);
		}

		/* Also update pg_stat_last_operation */
		if (IsAutoVacuumWorkerProcess())
			vsubtype = "AUTO";
		else
		{
			if ((params->options & VACOPT_FULL) &&
				(0 == params->freeze_min_age))
				vsubtype = "FULL FREEZE";
			else if ((params->options & VACOPT_FULL))
				vsubtype = "FULL";
			else if (0 == params->freeze_min_age)
				vsubtype = "FREEZE";
			else
				vsubtype = "";
		}
		MetaTrackUpdObject(RelationRelationId,
						   relid,
						   GetUserId(),
						   "VACUUM",
						   vsubtype);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);

		PopActiveSnapshot();
		CommitTransactionCommand();
	}

	/*
	 * Now release the session-level lock on the main table.
	 */
	UnlockRelationIdForSession(&lockrelid, lmode);

#ifdef FAULT_INJECTOR
	FaultInjector_InjectFaultIfSet(
		"vacuum_rel_finished_one_relation",
		DDLNotSpecified,
		"", /* databaseName */
		onerelname); /* tableName */
#endif

	/* Report that we really did it. */
	return true;
}


/*
 * Open all the vacuumable indexes of the given relation, obtaining the
 * specified kind of lock on each.  Return an array of Relation pointers for
 * the indexes into *Irel, and the number of indexes into *nindexes.
 *
 * We consider an index vacuumable if it is marked insertable (indisready).
 * If it isn't, probably a CREATE INDEX CONCURRENTLY command failed early in
 * execution, and what we have is too corrupt to be processable.  We will
 * vacuum even if the index isn't indisvalid; this is important because in a
 * unique index, uniqueness checks will be performed anyway and had better not
 * hit dangling index pointers.
 */
void
vac_open_indexes(Relation relation, LOCKMODE lockmode,
				 int *nindexes, Relation **Irel)
{
	List	   *indexoidlist;
	ListCell   *indexoidscan;
	int			i;

	Assert(lockmode != NoLock);

	indexoidlist = RelationGetIndexList(relation);

	/* allocate enough memory for all indexes */
	i = list_length(indexoidlist);

	if (i > 0)
		*Irel = (Relation *) palloc(i * sizeof(Relation));
	else
		*Irel = NULL;

	/* collect just the ready indexes */
	i = 0;
	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		Relation	indrel;

		indrel = index_open(indexoid, lockmode);
		if (indrel->rd_index->indisready)
			(*Irel)[i++] = indrel;
		else
			index_close(indrel, lockmode);
	}

	*nindexes = i;

	list_free(indexoidlist);
}

/*
 * Release the resources acquired by vac_open_indexes.  Optionally release
 * the locks (say NoLock to keep 'em).
 */
void
vac_close_indexes(int nindexes, Relation *Irel, LOCKMODE lockmode)
{
	if (Irel == NULL)
		return;

	while (nindexes--)
	{
		Relation	ind = Irel[nindexes];

		index_close(ind, lockmode);
	}
	pfree(Irel);
}

/*
 * vacuum_delay_point --- check for interrupts and cost-based delay.
 *
 * This should be called in each major loop of VACUUM processing,
 * typically once per page processed.
 */
void
vacuum_delay_point(void)
{
	double		msec = 0;

	/* Always check for interrupts */
	CHECK_FOR_INTERRUPTS();

	if (!VacuumCostActive || InterruptPending)
		return;

	/*
	 * For parallel vacuum, the delay is computed based on the shared cost
	 * balance.  See compute_parallel_delay.
	 */
	if (VacuumSharedCostBalance != NULL)
		msec = compute_parallel_delay();
	else if (VacuumCostBalance >= VacuumCostLimit)
		msec = VacuumCostDelay * VacuumCostBalance / VacuumCostLimit;

	/* Nap if appropriate */
	if (msec > 0)
	{
		if (msec > VacuumCostDelay * 4)
			msec = VacuumCostDelay * 4;

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 msec,
						 WAIT_EVENT_VACUUM_DELAY);
		ResetLatch(MyLatch);

		VacuumCostBalance = 0;

		/* update balance values for workers */
		AutoVacuumUpdateDelay();

		/* Might have gotten an interrupt while sleeping */
		CHECK_FOR_INTERRUPTS();
	}
}

/*
 * Computes the vacuum delay for parallel workers.
 *
 * The basic idea of a cost-based delay for parallel vacuum is to allow each
 * worker to sleep in proportion to the share of work it's done.  We achieve this
 * by allowing all parallel vacuum workers including the leader process to
 * have a shared view of cost related parameters (mainly VacuumCostBalance).
 * We allow each worker to update it as and when it has incurred any cost and
 * then based on that decide whether it needs to sleep.  We compute the time
 * to sleep for a worker based on the cost it has incurred
 * (VacuumCostBalanceLocal) and then reduce the VacuumSharedCostBalance by
 * that amount.  This avoids putting to sleep those workers which have done less
 * I/O than other workers and therefore ensure that workers
 * which are doing more I/O got throttled more.
 *
 * We allow a worker to sleep only if it has performed I/O above a certain
 * threshold, which is calculated based on the number of active workers
 * (VacuumActiveNWorkers), and the overall cost balance is more than
 * VacuumCostLimit set by the system.  Testing reveals that we achieve
 * the required throttling if we force a worker that has done more than 50%
 * of its share of work to sleep.
 */
static double
compute_parallel_delay(void)
{
	double		msec = 0;
	uint32		shared_balance;
	int			nworkers;

	/* Parallel vacuum must be active */
	Assert(VacuumSharedCostBalance);

	nworkers = pg_atomic_read_u32(VacuumActiveNWorkers);

	/* At least count itself */
	Assert(nworkers >= 1);

	/* Update the shared cost balance value atomically */
	shared_balance = pg_atomic_add_fetch_u32(VacuumSharedCostBalance, VacuumCostBalance);

	/* Compute the total local balance for the current worker */
	VacuumCostBalanceLocal += VacuumCostBalance;

	if ((shared_balance >= VacuumCostLimit) &&
		(VacuumCostBalanceLocal > 0.5 * ((double) VacuumCostLimit / nworkers)))
	{
		/* Compute sleep time based on the local cost balance */
		msec = VacuumCostDelay * VacuumCostBalanceLocal / VacuumCostLimit;
		pg_atomic_sub_fetch_u32(VacuumSharedCostBalance, VacuumCostBalanceLocal);
		VacuumCostBalanceLocal = 0;
	}

	/*
	 * Reset the local balance as we accumulated it into the shared value.
	 */
	VacuumCostBalance = 0;

	return msec;
}

/*
 * A wrapper function of defGetBoolean().
 *
 * This function returns VACOPTVALUE_ENABLED and VACOPTVALUE_DISABLED instead
 * of true and false.
 */
static VacOptValue
get_vacoptval_from_boolean(DefElem *def)
{
	return defGetBoolean(def) ? VACOPTVALUE_ENABLED : VACOPTVALUE_DISABLED;
}



/*
 * Dispatch a Vacuum command.
 */
static void
dispatchVacuum(VacuumParams *params, Oid relid, VacuumStatsContext *ctx)
{
	CdbPgResults cdb_pgresults;
	VacuumStmt *vacstmt = makeNode(VacuumStmt);
	int flags = DF_CANCEL_ON_ERROR | DF_WITH_SNAPSHOT;
	VacuumRelation *rel;

	/*
	 * The AO compaction phase needs to run in a distributed transaction,
	 * but other phases and heap VACUUM could run in local transactions. See
	 * comments in vacuum_ao.c "Overview" section. (In practice, though,
	 * this function is called with a distributed transaction open for the
	 * other phases too, so we end up using distributed transactions for
	 * all, anyway.)
	 */
	if ((params->options & VACUUM_AO_PHASE_MASK) == VACOPT_AO_COMPACT_PHASE)
		flags |= DF_NEED_TWO_PHASE;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	/* convert the VacuumParams back into an options list */

	vacstmt->options = vacuum_params_to_options_list(params);
	if ((params->options & VACOPT_VACUUM) != 0)
		vacstmt->is_vacuumcmd = true;
	else
	{
		Assert((params->options & VACOPT_ANALYZE) != 0);
		vacstmt->is_vacuumcmd = false;
	}

	rel = makeNode(VacuumRelation);
	rel->relation = NULL;
	rel->oid = relid;
	rel->va_cols = NIL;

	vacstmt->rels = list_make1(rel);

	/* XXX: Some kinds of VACUUM assign a new relfilenode. bitmap indexes maybe? */
	CdbDispatchUtilityStatement((Node *) vacstmt, flags,
								GetAssignedOidsForDispatch(),
								&cdb_pgresults);

	vacuum_combine_stats(ctx, &cdb_pgresults);

	cdbdisp_clearCdbPgResults(&cdb_pgresults);
}

/* convert VacuumParams back into an options list, for dispatch */
static List *
vacuum_params_to_options_list(VacuumParams *params)
{
	int			optmask = params->options;
	List	   *options = NIL;

	/* VACOPT_VACUUM and ANALYZE are derived from the VacuumStmt */
	optmask &= ~(VACOPT_VACUUM | VACOPT_ANALYZE);
	if (optmask & VACOPT_VERBOSE)
	{
		options = lappend(options, makeDefElem("verbose", (Node *) makeInteger(1), -1));
		optmask &= ~VACOPT_VERBOSE;
	}
	if (optmask & VACOPT_FREEZE)
	{
		options = lappend(options, makeDefElem("freeze", (Node *) makeInteger(1), -1));
		optmask &= ~VACOPT_FREEZE;
	}
	if (optmask & VACOPT_FULL)
	{
		options = lappend(options, makeDefElem("full", (Node *) makeInteger(1), -1));
		optmask &= ~VACOPT_FULL;
	}
	if (optmask & VACOPT_AO_AUX_ONLY)
	{
		options = lappend(options, makeDefElem("ao_aux_only", (Node *) makeInteger(1), -1));
		optmask &= ~VACOPT_AO_AUX_ONLY;
	}
	if (optmask & VACOPT_SKIP_LOCKED)
	{
		options = lappend(options, makeDefElem("skip_locked", (Node *) makeInteger(1), -1));
		optmask &= ~VACOPT_SKIP_LOCKED;
	}
	if (optmask & VACOPT_PROCESS_TOAST)
	{
		/* GPDB_14_MERGE_FIXME: skip_toast is replaced by process_toast, need to check */
		options = lappend(options, makeDefElem("process_toast", (Node *) makeInteger(1), -1));
		optmask &= ~VACOPT_PROCESS_TOAST;
	}
	if (optmask & VACOPT_DISABLE_PAGE_SKIPPING)
	{
		options = lappend(options, makeDefElem("disable_page_skipping", (Node *) makeInteger(1), -1));
		optmask &= ~VACOPT_DISABLE_PAGE_SKIPPING;
	}

	if (optmask & VACUUM_AO_PHASE_MASK)
	{
		options = lappend(options, makeDefElem("ao_phase",
											   (Node *) makeInteger(optmask & VACUUM_AO_PHASE_MASK),
											   -1));
		optmask &= ~VACUUM_AO_PHASE_MASK;
	}
	if (optmask != 0)
		elog(ERROR, "unrecognized vacuum option %x", optmask);

	/*
	 * NOTE:
	 *
	 * User-invoked vacuum will never have special values for VacuumParams's
	 * freeze_min_age, freeze_table_age, multixact_freeze_min_age,
	 * multixact_freeze_table_age, is_wraparound and log_min_duration. So no need
	 * to convert them back and dispatch to QEs for now.
	 * For autovacuum, it may set these values per table. Right now, only
	 * auto-ANALYZE is enabled which will dispatch analyze from QD, but these vaules
	 * are not needed for analyze.
	 * Vacuum through autovacuum is not enabled yet, and if each segment's autovacuum
	 * launcher take care it's own vacuum process, we don't need to dispatch these
	 * values as well.
	 *
	 * We should consider dispatch these values only if we do vacuum
	 * as how we do analyze through autovacuum on coordinator.
	 *
	 * GPDB has no plan to support distributed auto vacuum (do vacuum as how we do
	 * analyze, i.e. to trigger auto vacuum on QD, and QD manages to dispatch the
	 * vacuum request to QEs as distributed transaction) for GPDB7.
	 * See more details in the head comments of autovacuum.c.
	 */
	if (params->truncate == VACOPTVALUE_DISABLED)
		options = lappend(options, makeDefElem("truncate", (Node *) makeInteger(0), -1));
	else if (params->truncate == VACOPTVALUE_ENABLED)
		options = lappend(options, makeDefElem("truncate", (Node *) makeInteger(1), -1));
	else if (params->truncate != VACOPTVALUE_UNSPECIFIED)
		elog(ERROR, "unexpected VACUUM 'truncate' option '%d'", (int) params->truncate);

	if (params->index_cleanup == VACOPTVALUE_DISABLED)
		options = lappend(options, makeDefElem("index_cleanup", (Node *) makeInteger(0), -1));
	else if (params->index_cleanup == VACOPTVALUE_ENABLED)
		options = lappend(options, makeDefElem("index_cleanup", (Node *) makeInteger(1), -1));
	else if (params->index_cleanup == VACOPTVALUE_AUTO)
		options = lappend(options, makeDefElem("index_cleanup", NULL, -1));
	else if (params->index_cleanup != VACOPTVALUE_UNSPECIFIED)
		elog(ERROR, "unexpected VACUUM 'index_cleanup' option '%d'", (int) params->index_cleanup);

	return options;
}

/*
 * vacuum_combine_stats
 * This function combine the stats information sent by QEs to generate
 * the final stats for QD relations.
 *
 * Note that the mirrorResults is ignored by this function.
 */
static void
vacuum_combine_stats(VacuumStatsContext *stats_context, CdbPgResults *cdb_pgresults)
{
	int			result_no;
	MemoryContext old_context;

	Assert(Gp_role == GP_ROLE_DISPATCH);

	if (cdb_pgresults == NULL || cdb_pgresults->numResults <= 0)
		return;

	stats_context->nsegs = cdb_pgresults->numDispatches;

	/*
	 * Process the dispatch results from the primary. Note that the QE
	 * processes also send back the new stats info, such as stats on
	 * pg_class, for the relevant table and its
	 * indexes. We parse this information, and compute the final stats
	 * for the QD.
	 *
	 * For pg_class stats, we compute the sum of tuples, number of pages and
	 * allvisible pages after processing the stats from each QE.
	 *
	 */
	for(result_no = 0; result_no < cdb_pgresults->numResults; result_no++)
	{
		struct pg_result *pgresult = cdb_pgresults->pg_results[result_no];

		if (pgresult->extras == NULL || pgresult->extraType != PGExtraTypeVacuumStats)
			continue;

		Assert(pgresult->extraslen > sizeof(int));

		/*
		 * Process the stats for pg_class. We simply compute the maximum
		 * number of rel_tuples and rel_pages.
		 */
		VPgClassStatsCombo *pgclass_stats_combo = (VPgClassStatsCombo *) pgresult->extras;
		ListCell *lc = NULL;

		foreach (lc, stats_context->updated_stats)
		{
			VPgClassStatsCombo *tmp_stats_combo = (VPgClassStatsCombo *) lfirst(lc);

			if (tmp_stats_combo->relid == pgclass_stats_combo->relid)
			{
				tmp_stats_combo->rel_pages += pgclass_stats_combo->rel_pages;
				tmp_stats_combo->rel_tuples += pgclass_stats_combo->rel_tuples;
				tmp_stats_combo->relallvisible += pgclass_stats_combo->relallvisible;
				/*
				 * Accumulate the number of QEs, assuming sending only once
				 * per QE for each relid in the VACUUM scenario.
				 */
				tmp_stats_combo->count++;
				break;
			}
		}

		if (lc == NULL) /* get the first stats result of the current relid */
		{
			Assert(pgresult->extraslen == sizeof(VPgClassStats));

			old_context = MemoryContextSwitchTo(vac_context);
			pgclass_stats_combo = palloc(sizeof(VPgClassStatsCombo));
			memcpy(pgclass_stats_combo, pgresult->extras, pgresult->extraslen);
			pgclass_stats_combo->count = 1;

			stats_context->updated_stats =
				lappend(stats_context->updated_stats, pgclass_stats_combo);
			MemoryContextSwitchTo(old_context);
		}
	}
}

/*
 * Update relpages/reltuples of all the relations in the list.
 */
static void
vac_update_relstats_from_list(VacuumStatsContext *stats_context)
{
	List *updated_stats = stats_context->updated_stats;
	ListCell *lc;

	/*
	 * This function is only called in the context of the QD, so let's be
	 * explicit about that given the assumptions taken.
	 */
	Assert(Gp_role == GP_ROLE_DISPATCH);

	foreach (lc, updated_stats)
	{
		VPgClassStatsCombo *stats = (VPgClassStatsCombo *) lfirst(lc);
		Relation	rel;
		int16		ao_segfile_count = 0;

		rel = relation_open(stats->relid, AccessShareLock);

		if (GpPolicyIsReplicated(rel->rd_cdbpolicy))
		{
			Assert(stats->count == rel->rd_cdbpolicy->numsegments);

			stats->rel_pages = stats->rel_pages / rel->rd_cdbpolicy->numsegments;
			stats->rel_tuples = stats->rel_tuples / rel->rd_cdbpolicy->numsegments;
			stats->relallvisible = stats->relallvisible / rel->rd_cdbpolicy->numsegments;
		}

		if (RelationIsAppendOptimized(rel))
		{
			/*
			 * CBDB_PARALLEL_FIXME: This is very hacky!
			 * relallvisible came from vacuum AO/AOCO processes means the segment file count
			 * of AO/AOCO tables. We use it to update pg_appendonly.segfilecount.
			 * See ao_vacuum_rel_post_cleanup in vacuum_ao.c.
			 * relallvisible of AO/AOCO tables should always be 0 in pg_class, though, we need to reset
			 * it after we got the value and before updating the stats in pg_class.
			 */
			ao_segfile_count = stats->relallvisible/getgpsegmentCount(); /* Use rel->rd_cdbpolicy->numsegments instead of getgpsegmentCount()?*/
			stats->relallvisible = 0; /* Causion: relallvisible muset be set to 0 before updating pg_class */

			Relation	aorel;
			Oid			aorelid = RelationGetRelid(rel);
			HeapTuple	aotup;
			Form_pg_appendonly aoform;

			aotup = SearchSysCache1(AORELID, ObjectIdGetDatum(aorelid));
			if (!HeapTupleIsValid(aotup))
				ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_OBJECT),
					errmsg("appendonly table relid %u does not exist in pg_appendonly", aorelid)));

			aoform = (Form_pg_appendonly) GETSTRUCT(aotup);
			if (aoform->segfilecount < MAX_AOREL_CONCURRENCY && (aoform->segfilecount != ao_segfile_count))
			{
				aorel = table_open(AppendOnlyRelationId, RowExclusiveLock);
				aoform->segfilecount = ao_segfile_count;
				heap_inplace_update(aorel, aotup);
				table_close(aorel, RowExclusiveLock);
			}
			ReleaseSysCache(aotup);
		}

		/*
		 * Update QD stats only when receiving all dispatched QEs' stats, to
		 * avoid being overwritten by a partial accumulated value (i.e., index->reltuples)
		 * in case when not receiving all QEs' stats.
		 */
		if (stats_context->nsegs > 0 && stats->count == stats_context->nsegs)
		{
			/*
			 * Pass 'false' for isvacuum, so that the stats are
			 * actually updated.
			 */
			vac_update_relstats(rel,
								stats->rel_pages, stats->rel_tuples,
								stats->relallvisible,
								rel->rd_rel->relhasindex,
								InvalidTransactionId,
								InvalidMultiXactId,
								false,
								false /* isvacuum */);
		}
		else
		{
			/*
			 * We do have chance to enter this branch in the case when in compact phase.
			 * For example, in compact phase, some QEs may need to drop dead segfiles,
			 * while others may not. Only the QEs which dropping dead segfiles could go to
			 * vacuum indexes path then update and send the statistics to QD, QD just
			 * collected part of QEs' stats hence should not be as the final result to
			 * overwrite QD's stats.
			 *
			 * One may think why not having the stats update only happens in the final
			 * phase (POST_CLEANUP_PHASE), yes that's an alternative to get a final stats
			 * accurately for QD.
			 *
			 * Given the AO/CO VACUUM is a multi-phases process which may have an interval
			 * between each phase. In real circumstance, concurrent VACUUM is mostly a heavy
			 * job and this interval could get longer than normal cases, hence it seems
			 * better to collect and update QD's stats timely. So current strategy is, QD always
			 * collect QE's stats across phases, once we collected the expected number (means
			 * same as dispatched QE number) of QE's stats, we update QD's stats subsequently,
			 * instead of updating at the final phase.
			 *
			 * Set the logging level to LOG as skipping sending stats here is not considered as
			 * a real issue, displaying it in log may be helpful to hint.
			 */
			elog(LOG, "Vacuum update stats oid=%u pages=%d tuples=%f was skipped because "
				 "collected segment number %d didn't match the expected %d.", stats->relid,
				 stats->rel_pages, stats->rel_tuples, stats->count, stats_context->nsegs);
		}

		relation_close(rel, AccessShareLock);
	}
}

/*
 * CDB: Build a special message, to send the number of tuples
 * and the number of pages in pg_class located at QEs through
 * the dispatcher.
 */
void
vac_send_relstats_to_qd(Relation relation,
						BlockNumber num_pages,
						double num_tuples,
						BlockNumber num_all_visible_pages)
{

	StringInfoData buf;
	VPgClassStats stats;
	Oid			relid = RelationGetRelid(relation);
	Assert(relid != InvalidOid);

	pq_beginmessage(&buf, 'y');
	pq_sendstring(&buf, "VACUUM");
	stats.relid = relid;
	stats.rel_pages = num_pages;
	stats.rel_tuples = num_tuples;
	stats.relallvisible = num_all_visible_pages;
	pq_sendbyte(&buf, true); /* Mark the result ready when receive this message */
	pq_sendint(&buf, PGExtraTypeVacuumStats, sizeof(PGExtraType));
	pq_sendint(&buf, sizeof(VPgClassStats), sizeof(int));
	pq_sendbytes(&buf, (char *) &stats, sizeof(VPgClassStats));
	pq_endmessage(&buf);
}

bool
vacuumStatement_IsTemporary(Relation onerel)
{
	bool bTemp = false;
	/* MPP-7576: don't track internal namespace tables */
	switch (RelationGetNamespace(onerel))
	{
		case PG_CATALOG_NAMESPACE:
			/* MPP-7773: don't track objects in system namespace
			 * if modifying system tables (eg during upgrade)
			 */
			if (allowSystemTableMods)
				bTemp = true;
			break;

		case PG_TOAST_NAMESPACE:
		case PG_BITMAPINDEX_NAMESPACE:
		case PG_AOSEGMENT_NAMESPACE:
		case PG_EXTAUX_NAMESPACE:
			bTemp = true;
			break;
		default:
			break;
	}

	/* MPP-7572: Don't track metadata if table in any
	 * temporary namespace
	 */
	if (!bTemp)
		bTemp = isAnyTempNamespace(RelationGetNamespace(onerel));
	return bTemp;
}

/*
 * GPDB: Check whether needs to update or send stats from QE to QD.
 * This is GPDB specific check in vacuum-index scenario for collecting
 * QEs' stats (such as index->relpages and index->reltuples) on QD.
 * GPDB needs accumulating all QEs' stats for updating corresponding
 * statistics into QD's pg_class correctly. So if current instance is
 * acting as QE, it should scan and send its current stats to QD instead
 * of skipping them for cost saving.
 */
bool
gp_vacuum_needs_update_stats(void)
{
	return (Gp_role == GP_ROLE_EXECUTE);
}

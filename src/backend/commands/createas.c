/*-------------------------------------------------------------------------
 *
 * createas.c
 *	  Execution of CREATE TABLE ... AS, a/k/a SELECT INTO.
 *	  Since CREATE MATERIALIZED VIEW shares syntax and most behaviors,
 *	  we implement that here, too.
 *
 * We implement this by diverting the query's normal output to a
 * specialized DestReceiver type.
 *
 * Formerly, CTAS was implemented as a variant of SELECT, which led
 * to assorted legacy behaviors that we still try to preserve, notably that
 * we must return a tuples-processed count in the QueryCompletion.  (We no
 * longer do that for CTAS ... WITH NO DATA, however.)
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/createas.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/index.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_trigger.h"
#include "catalog/toasting.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/matview.h"
#include "commands/prepare.h"
#include "commands/tablecmds.h"
#include "commands/tablespace.h"
#include "commands/taskcmds.h"
#include "commands/trigger.h"
#include "commands/view.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "optimizer/prep.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parser.h"
#include "parser/parsetree.h"
#include "parser/parse_clause.h"
#include "parser/parse_func.h"
#include "parser/parse_type.h"
#include "postmaster/autostats.h"
#include "rewrite/rewriteHandler.h"
#include "rewrite/rewriteManip.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/resgroup.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

#include "catalog/gp_matview_aux.h"
#include "catalog/oid_dispatch.h"
#include "catalog/pg_task.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdboidsync.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/memquota.h"
#include "utils/metrics_utils.h"

typedef struct
{
	DestReceiver pub;			/* publicly-known function pointers */
	IntoClause *into;			/* target relation specification */
	/* These fields are filled by intorel_startup: */
	Relation	rel;			/* relation to write to */
	ObjectAddress reladdr;		/* address of rel, for ExecCreateTableAs */
	CommandId	output_cid;		/* cmin to insert in output tuples */
	int			ti_options;		/* table_tuple_insert performance options */
	BulkInsertState bistate;	/* bulk insert state */
} DR_intorel;

typedef struct
{
	bool	has_agg;
} check_ivm_restriction_context;

static void intorel_startup_dummy(DestReceiver *self, int operation, TupleDesc typeinfo);
/* utility functions for CTAS definition creation */
static ObjectAddress create_ctas_internal(List *attrList, IntoClause *into,
										  QueryDesc *queryDesc, bool dispatch);
static ObjectAddress create_ctas_nodata(List *tlist, IntoClause *into, QueryDesc *queryDesc);

/* DestReceiver routines for collecting data */
static bool intorel_receive(TupleTableSlot *slot, DestReceiver *self);
static void intorel_shutdown(DestReceiver *self);
static void intorel_destroy(DestReceiver *self);

static void CreateIvmTriggersOnBaseTablesRecurse(Query *qry, Node *node, Oid matviewOid,
									 Relids *relids, bool ex_lock);
static void CreateIvmTrigger(Oid relOid, Oid viewOid, int16 type, int16 timing, bool ex_lock);
static void check_ivm_restriction(Node *node);
static bool check_ivm_restriction_walker(Node *node, check_ivm_restriction_context *context);
static Bitmapset *get_primary_key_attnos_from_query(Query *query, List **constraintList);
static bool check_aggregate_supports_ivm(Oid aggfnoid);

static void create_dynamic_table_auto_refresh_task(ParseState *pstate, Relation DynamicTableRel, char *schedule);

/*
 * create_ctas_internal
 *
 * Internal utility used for the creation of the definition of a relation
 * created via CREATE TABLE AS or a materialized view.  Caller needs to
 * provide a list of attributes (ColumnDef nodes).
 */
static ObjectAddress
create_ctas_internal(List *attrList, IntoClause *into, QueryDesc *queryDesc, bool dispatch)
{
	CreateStmt *create = makeNode(CreateStmt);
	bool		is_matview;
	char		relkind;
	Datum		toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	ObjectAddress intoRelationAddr;

	/* Sync OIDs for into relation, if any */
	cdb_sync_oid_to_segments();

	/* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
	is_matview = (into->viewQuery != NULL);
	relkind = is_matview ? RELKIND_MATVIEW : RELKIND_RELATION;

	if (Gp_role == GP_ROLE_EXECUTE)
	{
		create = queryDesc->ddesc->intoCreateStmt;
	}
	/* funny indentation to avoid re-indenting a lot of upstream code */
	else
  {
	/*
	 * Create the target relation by faking up a CREATE TABLE parsetree and
	 * passing it to DefineRelation.
	 */
	create->relation = into->rel;
	create->tableElts = attrList;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = into->options;
	create->oncommit = into->onCommit;
	create->tablespacename = into->tableSpaceName;
	create->if_not_exists = false;
	create->origin = ORIGIN_NO_GEN;

	create->distributedBy = NULL; /* We will pass a pre-made intoPolicy instead */
	create->partitionBy = NULL; /* CTAS does not not support partition. */

	create->buildAoBlkdir = false;
	create->attr_encodings = NULL; /* filled in by DefineRelation */

	/* Save them in CreateStmt for dispatching. */
	create->relKind = relkind;
	create->ownerid = GetUserId();
	create->accessMethod = into->accessMethod;
	create->isCtas = true;

	create->intoQuery = into->viewQuery;
	create->intoPolicy = queryDesc->plannedstmt->intoPolicy;
  }
	/* end of funny indentation */

	/*
	 * Create the relation.  (This will error out if there's an existing view,
	 * so we don't need more code to complain if "replace" is false.)
	 *
	 * Don't dispatch it yet, as we haven't created the toast and other
	 * auxiliary tables yet.
	 *
	 * Pass the policy that was computed by the planner.
	 */
    intoRelationAddr = DefineRelation(create,
                                      relkind,
                                      InvalidOid,
                                      NULL,
                                      NULL,
                                      false,
                                      queryDesc->ddesc ? queryDesc->ddesc->useChangedAOOpts : true,
                                      queryDesc->plannedstmt->intoPolicy);

	if (Gp_role == GP_ROLE_DISPATCH)
	{
		queryDesc->ddesc->intoCreateStmt = create;
	}

	/*
	 * If necessary, create a TOAST table for the target table.  Note that
	 * NewRelationCreateToastTable ends with CommandCounterIncrement(), so
	 * that the TOAST table will be visible for insertion.
	 */
	CommandCounterIncrement();

	/* parse and validate reloptions for the toast table */
	toast_options = transformRelOptions((Datum) 0,
										create->options,
										"toast",
										validnsps,
										true, false);

	NewRelationCreateToastTable(intoRelationAddr.objectId, toast_options);

	/* Create the "view" part of a materialized view. */
	if (is_matview)
	{
		/* StoreViewQuery scribbles on tree, so make a copy */
		Query	   *query = (Query *) copyObject(into->viewQuery);

		StoreViewQuery(intoRelationAddr.objectId, query, false);
		CommandCounterIncrement();
	}

	if (Gp_role == GP_ROLE_DISPATCH && dispatch)
		CdbDispatchUtilityStatement((Node *) create,
									DF_CANCEL_ON_ERROR |
									DF_NEED_TWO_PHASE |
									DF_WITH_SNAPSHOT,
									GetAssignedOidsForDispatch(),
									NULL);

	return intoRelationAddr;
}


/*
 * create_ctas_nodata
 *
 * Create CTAS or materialized view when WITH NO DATA is used, starting from
 * the targetlist of the SELECT or view definition.
 */
static ObjectAddress
create_ctas_nodata(List *tlist, IntoClause *into, QueryDesc *queryDesc)
{
	List	   *attrList;
	ListCell   *t,
			   *lc;
	ObjectAddress intoRelationAddr;

	/*
	 * Build list of ColumnDefs from non-junk elements of the tlist.  If a
	 * column name list was specified in CREATE TABLE AS, override the column
	 * names in the query.  (Too few column names are OK, too many are not.)
	 */
	attrList = NIL;
	lc = list_head(into->colNames);
	foreach(t, tlist)
	{
		TargetEntry *tle = (TargetEntry *) lfirst(t);

		if (!tle->resjunk)
		{
			ColumnDef  *col;
			char	   *colname;

			if (lc)
			{
				colname = strVal(lfirst(lc));
				lc = lnext(into->colNames, lc);
			}
			else
				colname = tle->resname;

			col = makeColumnDef(colname,
								exprType((Node *) tle->expr),
								exprTypmod((Node *) tle->expr),
								exprCollation((Node *) tle->expr));

			/*
			 * It's possible that the column is of a collatable type but the
			 * collation could not be resolved, so double-check.  (We must
			 * check this here because DefineRelation would adopt the type's
			 * default collation rather than complaining.)
			 */
			if (!OidIsValid(col->collOid) &&
				type_is_collatable(col->typeName->typeOid))
				ereport(ERROR,
						(errcode(ERRCODE_INDETERMINATE_COLLATION),
						 errmsg("no collation was derived for column \"%s\" with collatable type %s",
								col->colname,
								format_type_be(col->typeName->typeOid)),
						 errhint("Use the COLLATE clause to set the collation explicitly.")));

			attrList = lappend(attrList, col);
		}
	}

	if (lc != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("too many column names were specified")));

	/* Create the relation definition using the ColumnDef list */
	intoRelationAddr = create_ctas_internal(attrList, into, queryDesc, true);

	return intoRelationAddr;
}


/*
 * ExecCreateTableAs -- execute a CREATE TABLE AS command
 */
ObjectAddress
ExecCreateTableAs(ParseState *pstate, CreateTableAsStmt *stmt,
				  ParamListInfo params, QueryEnvironment *queryEnv,
				  QueryCompletion *qc)
{
	Query	   *query = castNode(Query, stmt->query);
	IntoClause *into = stmt->into;
	bool		is_matview = (into->viewQuery != NULL);
	DestReceiver *dest;
	Oid			save_userid = InvalidOid;
	int			save_sec_context = 0;
	int			save_nestlevel = 0;
	ObjectAddress address;
	List	   *rewritten;
	PlannedStmt *plan;
	QueryDesc  *queryDesc;
	Query	   *query_immv = NULL;
	Oid         relationOid = InvalidOid;   /* relation that is modified */
	AutoStatsCmdType cmdType = AUTOSTATS_CMDTYPE_SENTINEL;  /* command type */

	Assert(Gp_role != GP_ROLE_EXECUTE);

	/* Check if the relation exists or not */
	{
		Oid			nspid;
		Oid			oldrelid;

		nspid = RangeVarGetCreationNamespace(into->rel);

		oldrelid = get_relname_relid(into->rel->relname, nspid);
		if (OidIsValid(oldrelid))
		{
			if (!stmt->if_not_exists)
				ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists",
							into->rel->relname)));

			/*
			 * The relation exists and IF NOT EXISTS has been specified.
			 *
			 * If we are in an extension script, insist that the pre-existing
			 * object be a member of the extension, to avoid security risks.
			 */
			ObjectAddressSet(address, RelationRelationId, oldrelid);
			checkMembershipInCurrentExtension(&address);

			/* OK to skip */
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists, skipping",
							into->rel->relname)));
			return InvalidObjectAddress;
		}
	}

	/*
	 * Create the tuple receiver object and insert info it will need
	 */
	dest = CreateIntoRelDestReceiver(into);

	/*
	 * The contained Query could be a SELECT, or an EXECUTE utility command.
	 * If the latter, we just pass it off to ExecuteQuery.
	 */
	if (query->commandType == CMD_UTILITY &&
		IsA(query->utilityStmt, ExecuteStmt))
	{
		ExecuteStmt *estmt = castNode(ExecuteStmt, query->utilityStmt);

		Assert(!is_matview);	/* excluded by syntax */
		ExecuteQuery(pstate, estmt, into, params, dest, qc);

		/* get object address that intorel_startup saved for us */
		address = ((DR_intorel *) dest)->reladdr;

		return address;
	}
	Assert(query->commandType == CMD_SELECT);

	/*
	 * For materialized views, lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.  This is
	 * not necessary for security, but this keeps the behavior similar to
	 * REFRESH MATERIALIZED VIEW.  Otherwise, one could create a materialized
	 * view not possible to refresh.
	 */
	if (is_matview)
	{
		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		SetUserIdAndSecContext(save_userid,
							   save_sec_context | SECURITY_RESTRICTED_OPERATION);
		save_nestlevel = NewGUCNestLevel();
	}

	if (is_matview && into->ivm)
	{
		/* check if the query is supported in IMMV definition */
		if (contain_mutable_functions((Node *) query))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("mutable function is not supported on incrementally maintainable materialized view"),
					 errhint("functions must be marked IMMUTABLE")));

		check_ivm_restriction((Node *) query);

		/* For IMMV, we need to rewrite matview query */
		query = rewriteQueryForIMMV(query, into->colNames);
		query_immv = copyObject(query);
	}

	{
		/*
		 * Parse analysis was done already, but we still have to run the rule
		 * rewriter.  We do not do AcquireRewriteLocks: we assume the query
		 * either came straight from the parser, or suitable locks were
		 * acquired by plancache.c.
		 */
		rewritten = QueryRewrite(query);

		/* SELECT should never rewrite to more or less than one SELECT query */
		if (list_length(rewritten) != 1)
			elog(ERROR, "unexpected rewrite result for %s",
				 is_matview ? "CREATE MATERIALIZED VIEW" :
				 "CREATE TABLE AS SELECT");
		query = linitial_node(Query, rewritten);
		Assert(query->commandType == CMD_SELECT);

		/* plan the query */
		plan = pg_plan_query(query, pstate->p_sourcetext,
							CURSOR_OPT_PARALLEL_OK, params);

		/*GPDB: Save the target information in PlannedStmt */
		/*
		 * GPDB_92_MERGE_FIXME: it really should be an optimizer's responsibility
		 * to correctly set the into-clause and into-policy of the PlannedStmt.
		 */
		plan->intoClause = copyObject(stmt->into);

		/*
		 * Use a snapshot with an updated command ID to ensure this query sees
		 * results of any previously executed queries.  (This could only
		 * matter if the planner executed an allegedly-stable function that
		 * changed the database contents, but let's do it anyway to be
		 * parallel to the EXPLAIN code path.)
		 */
		PushCopiedSnapshot(GetActiveSnapshot());
		UpdateActiveSnapshotCommandId();

		/* Create a QueryDesc, redirecting output to our tuple receiver */
		queryDesc = CreateQueryDesc(plan, pstate->p_sourcetext,
									GetActiveSnapshot(), InvalidSnapshot,
									dest, params, queryEnv, 0);
	}

	/* GPDB hook for collecting query info */
	if (query_info_collect_hook)
		(*query_info_collect_hook)(METRICS_QUERY_SUBMIT, queryDesc);

	if (into->skipData)
	{
		/*
		 * If WITH NO DATA was specified, do not go through the rewriter,
		 * planner and executor.  Just define the relation using a code path
		 * similar to CREATE VIEW.  This avoids dump/restore problems stemming
		 * from running the planner before all dependencies are set up.
		 */
		queryDesc->ddesc = makeNode(QueryDispatchDesc);
		address = create_ctas_nodata(query->targetList, into, queryDesc);
	}
	else
	{
		check_and_unassign_from_resgroup(queryDesc->plannedstmt);
		queryDesc->plannedstmt->query_mem = ResourceManagerGetQueryMemoryLimit(queryDesc->plannedstmt);

		/* call ExecutorStart to prepare the plan for execution */
		ExecutorStart(queryDesc, GetIntoRelEFlags(into));

		if (Gp_role == GP_ROLE_DISPATCH)
			autostats_get_cmdtype(queryDesc, &cmdType, &relationOid);

		/* run the plan to completion */
		ExecutorRun(queryDesc, ForwardScanDirection, 0L, true);

		/* and clean up */
		ExecutorFinish(queryDesc);
		ExecutorEnd(queryDesc);

		/*
		 * In GPDB, computing the row count needs to happen after ExecutorEnd()
		 * because that is where it gets the row count from dispatch. There's
		 * also some special processing if the relation was a replicated table.
		 * In upstream Postgres, the rowcount is saved before ExecutorFinish().
		 */
		if (into->distributedBy &&
			((DistributedBy *)(into->distributedBy))->ptype == POLICYTYPE_REPLICATED)
			queryDesc->es_processed /= ((DistributedBy *)(into->distributedBy))->numsegments;

		/* get object address that intorel_startup saved for us */
		address = ((DR_intorel *) dest)->reladdr;

		/* save the rowcount if we're given a qc to fill */
		if (qc)
			SetQueryCompletion(qc, CMDTAG_SELECT, queryDesc->es_processed);

		/* MPP-14001: Running auto_stats */
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			bool inFunction = already_under_executor_run() || utility_nested();
			auto_stats(cmdType, relationOid, queryDesc->es_processed, inFunction);
		}
	}

	if (is_matview)
	{
		/* Roll back any GUC changes */
		AtEOXact_GUC(false, save_nestlevel);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);

		Oid matviewOid = address.objectId;
		Relation matviewRel = table_open(matviewOid, NoLock);

		/*
		 * Record materialized view aux entry. 
		 * This is used to check if a materialized view's meta data,
		 * ex: data is up to date and etc.
		 * The info is used for expanding Incremental Appedn Agg Plan
		 * and Answer Query Using Materialized Views.
		 */
		if (IS_QD_OR_SINGLENODE())
			InsertMatviewAuxEntry(matviewOid, (Query* )into->viewQuery, into->skipData);

		if (into->ivm)
		{
			/*
			 * Mark relisivm field, if it's a matview and into->ivm is true.
			 */
			SetMatViewIVMState(matviewRel, true);

			if (!into->skipData)
			{
				Assert(query_immv != NULL);
				/* Create triggers on incremental maintainable materialized view */
				CreateIvmTriggersOnBaseTables(query_immv, matviewOid);
			}
		}

		/* Set Dynamic Tables. */
		if (into->dynamicTbl)
		{
			SetDynamicTableState(matviewRel);
			create_dynamic_table_auto_refresh_task(pstate, matviewRel, into->schedule);
		}

		table_close(matviewRel, NoLock);
	}

	{
		dest->rDestroy(dest);

		FreeQueryDesc(queryDesc);

		PopActiveSnapshot();
	}


	return address;
}

/*
 * rewriteQueryForIMMV -- rewrite view definition query for IMMV
 *
 * count(*) is added for counting distinct tuples in views.
 * Also, additional hidden columns are added for aggregate values.
 */
Query *
rewriteQueryForIMMV(Query *query, List *colNames)
{
	Query *rewritten;

	Node *node;
	ParseState *pstate = make_parsestate(NULL);
	FuncCall *fn;

	rewritten = copyObject(query);
	pstate->p_expr_kind = EXPR_KIND_SELECT_TARGET;

	/* group keys must be in targetlist */
	if (rewritten->groupClause)
	{
		ListCell *lc;
		foreach(lc, rewritten->groupClause)
		{
			SortGroupClause *scl = (SortGroupClause *) lfirst(lc);
			TargetEntry *tle = get_sortgroupclause_tle(scl, rewritten->targetList);

			if (tle->resjunk)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("GROUP BY expression not appearing in select list is not supported on incrementally maintainable materialized view")));
		}
	}
	/* Convert DISTINCT to GROUP BY.  count(*) will be added afterward. */
	else if (!rewritten->hasAggs && rewritten->distinctClause)
		rewritten->groupClause = transformDistinctClause(NULL, &rewritten->targetList, rewritten->sortClause, false);

	/* Add additional columns for aggregate values */
	if (rewritten->hasAggs)
	{
		ListCell *lc;
		List *aggs = NIL;
		AttrNumber next_resno = list_length(rewritten->targetList) + 1;

		foreach(lc, rewritten->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			char *resname = (colNames == NIL || foreach_current_index(lc) >= list_length(colNames) ?
								tle->resname : strVal(list_nth(colNames, tle->resno - 1)));

			if (IsA(tle->expr, Aggref))
				makeIvmAggColumn(pstate, (Aggref *) tle->expr, resname, &next_resno, &aggs);
		}
		rewritten->targetList = list_concat(rewritten->targetList, aggs);
	}

	/* Add count(*) for counting distinct tuples in views */
	if (rewritten->distinctClause || rewritten->hasAggs)
	{
		TargetEntry *tle;

		fn = makeFuncCall(SystemFuncName("count"), NIL, COERCE_EXPLICIT_CALL, -1);
		fn->agg_star = true;

		node = ParseFuncOrColumn(pstate, fn->funcname, NIL, NULL, fn, false, -1);

		tle = makeTargetEntry((Expr *) node,
								list_length(rewritten->targetList) + 1,
								pstrdup("__ivm_count__"),
								false);
		rewritten->targetList = lappend(rewritten->targetList, tle);
		rewritten->hasAggs = true;
	}

	return rewritten;
}

/*
 * makeIvmAggColumn -- make additional aggregate columns for IVM
 *
 * For an aggregate column specified by aggref, additional aggregate columns
 * are added, which are used to calculate the new aggregate value in IMMV.
 * An additional aggregate columns has a name based on resname
 * (ex. ivm_count_resname), and resno specified by next_resno. The created
 * columns are returned to aggs, and the resno for the next column is also
 * returned to next_resno.
 *
 * Currently, an additional count() is created for aggref other than count.
 * In addition, sum() is created for avg aggregate column.
 */
void
makeIvmAggColumn(ParseState *pstate, Aggref *aggref, char *resname, AttrNumber *next_resno, List **aggs)
{
	TargetEntry *tle_count;
	Node *node;
	FuncCall *fn;
	Const	*dmy_arg = makeConst(INT4OID,
								 -1,
								 InvalidOid,
								 sizeof(int32),
								 Int32GetDatum(1),
								 false,
								 true); /* pass by value */
	const char *aggname = get_func_name(aggref->aggfnoid);

	/*
	 * For aggregate functions except count, add count() func with the same arg parameters.
	 * This count result is used for determining if the aggregate value should be NULL or not.
	 * Also, add sum() func for avg because we need to calculate an average value as sum/count.
	 *
	 * XXX: If there are same expressions explicitly in the target list, we can use this instead
	 * of adding new duplicated one.
	 */
	if (strcmp(aggname, "count") != 0)
	{
		fn = makeFuncCall(SystemFuncName("count"), NIL, COERCE_EXPLICIT_CALL, -1);

		/* Make a Func with a dummy arg, and then override this by the original agg's args. */
		node = ParseFuncOrColumn(pstate, fn->funcname, list_make1(dmy_arg), NULL, fn, false, -1);
		((Aggref *)node)->args = aggref->args;

		tle_count = makeTargetEntry((Expr *) node,
									*next_resno,
									pstrdup(makeObjectName("__ivm_count",resname, "_")),
									false);
		*aggs = lappend(*aggs, tle_count);
		(*next_resno)++;
	}
	if (strcmp(aggname, "avg") == 0)
	{
		List *dmy_args = NIL;
		ListCell *lc;
		foreach(lc, aggref->aggargtypes)
		{
			Oid		typeid = lfirst_oid(lc);
			Type	type = typeidType(typeid);

			Const *con = makeConst(typeid,
								   -1,
								   typeTypeCollation(type),
								   typeLen(type),
								   (Datum) 0,
								   true,
								   typeByVal(type));
			dmy_args = lappend(dmy_args, con);
			ReleaseSysCache(type);
		}
		fn = makeFuncCall(SystemFuncName("sum"), NIL, COERCE_EXPLICIT_CALL, -1);

		/* Make a Func with dummy args, and then override this by the original agg's args. */
		node = ParseFuncOrColumn(pstate, fn->funcname, dmy_args, NULL, fn, false, -1);
		((Aggref *)node)->args = aggref->args;

		tle_count = makeTargetEntry((Expr *) node,
									*next_resno,
									pstrdup(makeObjectName("__ivm_sum",resname, "_")),
									false);
		*aggs = lappend(*aggs, tle_count);
		(*next_resno)++;
	}
}

/*
 * GetIntoRelEFlags --- compute executor flags needed for CREATE TABLE AS
 *
 * This is exported because EXPLAIN and PREPARE need it too.  (Note: those
 * callers still need to deal explicitly with the skipData flag; since they
 * use different methods for suppressing execution, it doesn't seem worth
 * trying to encapsulate that part.)
 */
int
GetIntoRelEFlags(IntoClause *intoClause)
{
	int			flags = 0;

	if (intoClause->skipData)
		flags |= EXEC_FLAG_WITH_NO_DATA;

	return flags;
}

/*
 * CreateTableAsRelExists --- check existence of relation for CreateTableAsStmt
 *
 * Utility wrapper checking if the relation pending for creation in this
 * CreateTableAsStmt query already exists or not.  Returns true if the
 * relation exists, otherwise false.
 */
bool
CreateTableAsRelExists(CreateTableAsStmt *ctas)
{
	Oid			nspid;
	IntoClause *into = ctas->into;

	nspid = RangeVarGetCreationNamespace(into->rel);

	if (get_relname_relid(into->rel->relname, nspid))
	{
		if (!ctas->if_not_exists)
			ereport(ERROR,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists",
							into->rel->relname)));

		/* The relation exists and IF NOT EXISTS has been specified */
		ereport(NOTICE,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("relation \"%s\" already exists, skipping",
						into->rel->relname)));
		return true;
	}

	/* Relation does not exist, it can be created */
	return false;
}

/*
 * CreateIntoRelDestReceiver -- create a suitable DestReceiver object
 *
 * intoClause will be NULL if called from CreateDestReceiver(), in which
 * case it has to be provided later.  However, it is convenient to allow
 * self->into to be filled in immediately for other callers.
 */
DestReceiver *
CreateIntoRelDestReceiver(IntoClause *intoClause)
{
	DR_intorel *self = (DR_intorel *) palloc0(sizeof(DR_intorel));

	self->pub.receiveSlot = intorel_receive;
	self->pub.rStartup = intorel_startup_dummy;
	self->pub.rShutdown = intorel_shutdown;
	self->pub.rDestroy = intorel_destroy;
	self->pub.mydest = DestIntoRel;
	self->into = intoClause;

	return (DestReceiver *) self;
}

/*
 * intorel_startup_dummy --- executor startup
 */
static void
intorel_startup_dummy(DestReceiver *self, int operation, TupleDesc typeinfo)
{
	/* See intorel_initplan() for explanation */
	table_dml_init(((DR_intorel *)self)->rel, CMD_INSERT);
}

/*
 * intorel_initplan --- Based on PG intorel_startup().
 * Parameters are different. We need to run the code earlier before the
 * executor runs since we want the relation to be created earlier else current
 * MPP framework will fail. This could be called in InitPlan() as before, but
 * we could call it just before ExecutorRun() in ExecCreateTableAs(). In the
 * future if the requirment is general we could add an interface into
 * DestReceiver but so far that is not needed (Based on PG 11 code.)
 */
void
intorel_initplan(struct QueryDesc *queryDesc, int eflags)
{
	DR_intorel *myState;
	/* Get 'into' from the dispatched plan */
	IntoClause *into = queryDesc->plannedstmt->intoClause;
	bool		is_matview;
	List	   *attrList;
	ObjectAddress intoRelationAddr;
	Relation	intoRelationDesc;
	ListCell   *lc;
	int			attnum;
	TupleDesc   typeinfo = queryDesc->tupDesc;

	/* If EXPLAIN/QE, skip creating the "into" relation. */
	if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) ||
		(Gp_role == GP_ROLE_EXECUTE && !Gp_is_writer))
		return;

	/* This code supports both CREATE TABLE AS and CREATE MATERIALIZED VIEW */
	is_matview = (into->viewQuery != NULL);

	/*
	 * Build column definitions using "pre-cooked" type and collation info. If
	 * a column name list was specified in CREATE TABLE AS, override the
	 * column names derived from the query.  (Too few column names are OK, too
	 * many are not.)
	 */
	attrList = NIL;
	lc = list_head(into->colNames);
	for (attnum = 0; attnum < typeinfo->natts; attnum++)
	{
		Form_pg_attribute attribute = TupleDescAttr(typeinfo, attnum);
		ColumnDef  *col;
		char	   *colname;

		/* Don't override hidden columns added for IVM */
		if (lc && !isIvmName(NameStr(attribute->attname)))
		{
			colname = strVal(lfirst(lc));
			lc = lnext(into->colNames, lc);
		}
		else
			colname = NameStr(attribute->attname);

		col = makeColumnDef(colname,
							attribute->atttypid,
							attribute->atttypmod,
							attribute->attcollation);

		/*
		 * It's possible that the column is of a collatable type but the
		 * collation could not be resolved, so double-check.  (We must check
		 * this here because DefineRelation would adopt the type's default
		 * collation rather than complaining.)
		 */
		if (!OidIsValid(col->collOid) &&
			type_is_collatable(col->typeName->typeOid))
			ereport(ERROR,
					(errcode(ERRCODE_INDETERMINATE_COLLATION),
					 errmsg("no collation was derived for column \"%s\" with collatable type %s",
							col->colname,
							format_type_be(col->typeName->typeOid)),
					 errhint("Use the COLLATE clause to set the collation explicitly.")));

		attrList = lappend(attrList, col);
	}

	if (lc != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("too many column names were specified")));

	/*
	 * Actually create the target table
	 * We also get here with CREATE TABLE AS EXECUTE ... WITH NO DATA. In that
	 * case, dispatch the creation of the table immediately. Normally, the table
	 * is created in the initialization of the plan in QEs, but with NO DATA, we
	 * don't need to dispatch the plan during ExecutorStart().
	 */
	intoRelationAddr = create_ctas_internal(attrList, into, queryDesc,
											into->skipData ? true : false);

	/*
	 * Finally we can open the target table
	 */
	intoRelationDesc = table_open(intoRelationAddr.objectId, AccessExclusiveLock);

	/*
	 * Make sure the constructed table does not have RLS enabled.
	 *
	 * check_enable_rls() will ereport(ERROR) itself if the user has requested
	 * something invalid, and otherwise will return RLS_ENABLED if RLS should
	 * be enabled here.  We don't actually support that currently, so throw
	 * our own ereport(ERROR) if that happens.
	 */
	if (check_enable_rls(intoRelationAddr.objectId, InvalidOid, false) == RLS_ENABLED)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("policies not yet implemented for this command")));

	/*
	 * Tentatively mark the target as populated, if it's a matview and we're
	 * going to fill it; otherwise, no change needed.
	 */
	if (is_matview && !into->skipData)
		SetMatViewPopulatedState(intoRelationDesc, true);

	/*
	 * Fill private fields of myState for use by later routines
	 */

	if (queryDesc->dest->mydest != DestIntoRel)
		queryDesc->dest = CreateIntoRelDestReceiver(into);
	myState = (DR_intorel *) queryDesc->dest;
	myState->rel = intoRelationDesc;
	myState->reladdr = intoRelationAddr;
	myState->output_cid = GetCurrentCommandId(true);
	myState->ti_options = TABLE_INSERT_SKIP_FSM;

	/*
	 * If WITH NO DATA is specified, there is no need to set up the state for
	 * bulk inserts as there are no tuples to insert.
	 */
	if (!into->skipData)
		myState->bistate = GetBulkInsertState();
	else
		myState->bistate = NULL;

	/*
	 * Valid smgr_targblock implies something already wrote to the relation.
	 * This may be harmless, but this function hasn't planned for it.
	 */
	Assert(RelationGetTargetBlock(intoRelationDesc) == InvalidBlockNumber);
}

/*
 * intorel_receive --- receive one tuple
 */
static bool
intorel_receive(TupleTableSlot *slot, DestReceiver *self)
{
	DR_intorel *myState = (DR_intorel *) self;

	/* Nothing to insert if WITH NO DATA is specified. */
	if (!myState->into->skipData)
	{
		/*
		 * Note that the input slot might not be of the type of the target
		 * relation. That's supported by table_tuple_insert(), but slightly
		 * less efficient than inserting with the right slot - but the
		 * alternative would be to copy into a slot of the right type, which
		 * would not be cheap either. This also doesn't allow accessing per-AM
		 * data (say a tuple's xmin), but since we don't do that here...
		 */
		table_tuple_insert(myState->rel,
						   slot,
						   myState->output_cid,
						   myState->ti_options,
						   myState->bistate);
	}

	/* We know this is a newly created relation, so there are no indexes */

	return true;
}

/*
 * intorel_shutdown --- executor end
 */
static void
intorel_shutdown(DestReceiver *self)
{
	DR_intorel *myState = (DR_intorel *) self;
	IntoClause *into = myState->into;
	Relation	into_rel = myState->rel;

	if (into_rel == NULL)
		return;

	if (!into->skipData)
	{
		FreeBulkInsertState(myState->bistate);
		table_finish_bulk_insert(myState->rel, myState->ti_options);
	}

	/* close rel, but keep lock until commit */
	table_close(myState->rel, NoLock);
	myState->rel = NULL;
}

/*
 * intorel_destroy --- release DestReceiver object
 */
static void
intorel_destroy(DestReceiver *self)
{
	pfree(self);
}

/*
 * Get the OID of the relation created for SELECT INTO or CREATE TABLE AS.
 *
 * To be called between ExecutorStart and ExecutorEnd.
 */
Oid
GetIntoRelOid(QueryDesc *queryDesc)
{
	DR_intorel *myState = (DR_intorel *) queryDesc->dest;
	Relation    into_rel = myState->rel;

	if (myState && myState->pub.mydest == DestIntoRel && into_rel)
		return RelationGetRelid(into_rel);
	else
		return InvalidOid;
}

/*
 * CreateIvmTriggersOnBaseTables -- create IVM triggers on all base tables
 */
void
CreateIvmTriggersOnBaseTables(Query *qry, Oid matviewOid)
{
	Relids	relids = NULL;
	bool	ex_lock = false;
	RangeTblEntry *rte;

	/* Immediately return if we don't have any base tables. */
	if (list_length(qry->rtable) < 1)
		return;

	/*
	 * If the view has more than one base tables, we need an exclusive lock
	 * on the view so that the view would be maintained serially to avoid
	 * the inconsistency that occurs when two base tables are modified in
	 * concurrent transactions. However, if the view has only one table,
	 * we can use a weaker lock.
	 *
	 * The type of lock should be determined here, because if we check the
	 * view definition at maintenance time, we need to acquire a weaker lock,
	 * and upgrading the lock level after this increases probability of
	 * deadlock.
	 */

	rte = list_nth(qry->rtable, 0);
	if (list_length(qry->rtable) > 1 || rte->rtekind != RTE_RELATION)
		ex_lock = true;

	CreateIvmTriggersOnBaseTablesRecurse(qry, (Node *)qry, matviewOid, &relids, ex_lock);

	bms_free(relids);
}

static void
CreateIvmTriggersOnBaseTablesRecurse(Query *qry, Node *node, Oid matviewOid,
									 Relids *relids, bool ex_lock)
{
	if (node == NULL)
		return;

	/* This can recurse, so check for excessive recursion */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Query:
			{
				Query *query = (Query *) node;

				CreateIvmTriggersOnBaseTablesRecurse(qry, (Node *)query->jointree, matviewOid, relids, ex_lock);
			}
			break;

		case T_RangeTblRef:
			{
				int			rti = ((RangeTblRef *) node)->rtindex;
				RangeTblEntry *rte = rt_fetch(rti, qry->rtable);

				if (rte->rtekind == RTE_RELATION && !bms_is_member(rte->relid, *relids))
				{
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_BEFORE, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_BEFORE, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_BEFORE, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_TRUNCATE, TRIGGER_TYPE_BEFORE, true);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_INSERT, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_DELETE, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_UPDATE, TRIGGER_TYPE_AFTER, ex_lock);
					CreateIvmTrigger(rte->relid, matviewOid, TRIGGER_TYPE_TRUNCATE, TRIGGER_TYPE_AFTER, true);

					*relids = bms_add_member(*relids, rte->relid);
				}
			}
			break;

		case T_FromExpr:
			{
				FromExpr   *f = (FromExpr *) node;
				ListCell   *l;

				foreach(l, f->fromlist)
					CreateIvmTriggersOnBaseTablesRecurse(qry, lfirst(l), matviewOid, relids, ex_lock);
			}
			break;

		case T_JoinExpr:
			{
				JoinExpr   *j = (JoinExpr *) node;

				CreateIvmTriggersOnBaseTablesRecurse(qry, j->larg, matviewOid, relids, ex_lock);
				CreateIvmTriggersOnBaseTablesRecurse(qry, j->rarg, matviewOid, relids, ex_lock);
			}
			break;

		default:
			elog(ERROR, "unrecognized node type: %d", (int) nodeTag(node));
	}
}

/*
 * CreateIvmTrigger -- create IVM trigger on a base table
 */
static void
CreateIvmTrigger(Oid relOid, Oid viewOid, int16 type, int16 timing, bool ex_lock)
{
	ObjectAddress	refaddr;
	ObjectAddress	address;
	CreateTrigStmt *ivm_trigger;
	List *transitionRels = NIL;
	char		internaltrigname[NAMEDATALEN];

	Assert(timing == TRIGGER_TYPE_BEFORE || timing == TRIGGER_TYPE_AFTER);

	refaddr.classId = RelationRelationId;
	refaddr.objectId = viewOid;
	refaddr.objectSubId = 0;

	ivm_trigger = makeNode(CreateTrigStmt);
	ivm_trigger->relation = makeRangeVar(get_namespace_name(get_rel_namespace(relOid)), get_rel_name(relOid), -1);
	ivm_trigger->row = false;
	ivm_trigger->matviewId = viewOid;

	ivm_trigger->timing = timing;
	ivm_trigger->events = type;

	switch (type)
	{
		case TRIGGER_TYPE_INSERT:
			ivm_trigger->trigname = (timing == TRIGGER_TYPE_BEFORE ? "IVM_trigger_ins_before" : "IVM_trigger_ins_after");
			break;
		case TRIGGER_TYPE_DELETE:
			ivm_trigger->trigname = (timing == TRIGGER_TYPE_BEFORE ? "IVM_trigger_del_before" : "IVM_trigger_del_after");
			break;
		case TRIGGER_TYPE_UPDATE:
			ivm_trigger->trigname = (timing == TRIGGER_TYPE_BEFORE ? "IVM_trigger_upd_before" : "IVM_trigger_upd_after");
			break;
		case TRIGGER_TYPE_TRUNCATE:
			ivm_trigger->trigname = (timing == TRIGGER_TYPE_BEFORE ? "IVM_trigger_truncate_before" : "IVM_trigger_truncate_after");
			break;
		default:
			elog(ERROR, "unsupported trigger type");
	}
	snprintf(internaltrigname, sizeof(internaltrigname),
				 "%s_%u", ivm_trigger->trigname, viewOid);
	ivm_trigger->trigname = pstrdup(internaltrigname);

	if (timing == TRIGGER_TYPE_AFTER)
	{
		if (type == TRIGGER_TYPE_INSERT || type == TRIGGER_TYPE_UPDATE)
		{
			TriggerTransition *n = makeNode(TriggerTransition);
			n->name = "__ivm_newtable";
			n->isNew = true;
			n->isTable = true;

			transitionRels = lappend(transitionRels, n);
		}
		if (type == TRIGGER_TYPE_DELETE || type == TRIGGER_TYPE_UPDATE)
		{
			TriggerTransition *n = makeNode(TriggerTransition);
			n->name = "__ivm_oldtable";
			n->isNew = false;
			n->isTable = true;

			transitionRels = lappend(transitionRels, n);
		}
	}

	/*
	 * XXX: When using DELETE or UPDATE, we must use exclusive lock for now
	 * because apply_old_delta(_with_count) uses ctid to identify the tuple
	 * to be deleted/deleted, but doesn't work in concurrent situations.
	 *
	 * If the view doesn't have aggregate, distinct, or tuple duplicate,
	 * then it would work even in concurrent situations. However, we don't have
	 * any way to guarantee the view has a unique key before opening the IMMV
	 * at the maintenance time because users may drop the unique index.
	 */

	if (type == TRIGGER_TYPE_DELETE || type == TRIGGER_TYPE_UPDATE)
		ex_lock = true;

	ivm_trigger->funcname =
		(timing == TRIGGER_TYPE_BEFORE ? SystemFuncName("ivm_immediate_before") : SystemFuncName("ivm_immediate_maintenance"));

	ivm_trigger->columns = NIL;
	ivm_trigger->transitionRels = transitionRels;
	ivm_trigger->whenClause = NULL;
	ivm_trigger->isconstraint = false;
	ivm_trigger->deferrable = false;
	ivm_trigger->initdeferred = false;
	ivm_trigger->constrrel = NULL;
	ivm_trigger->args = list_make2(
		makeString(DatumGetPointer(DirectFunctionCall1(oidout, ObjectIdGetDatum(viewOid)))),
		makeString(DatumGetPointer(DirectFunctionCall1(boolout, BoolGetDatum(ex_lock))))
		);

	address = CreateTrigger(ivm_trigger, NULL, relOid, InvalidOid, InvalidOid,
						 InvalidOid, InvalidOid, InvalidOid, NULL, false, false);

	recordDependencyOn(&address, &refaddr, DEPENDENCY_AUTO);

	if (Gp_role == GP_ROLE_DISPATCH && ENABLE_DISPATCH())
	{
		CdbDispatchUtilityStatement((Node *) ivm_trigger,
									DF_CANCEL_ON_ERROR|
									DF_WITH_SNAPSHOT|
									DF_NEED_TWO_PHASE,
									GetAssignedOidsForDispatch(),
									NULL);
	}
	/* Make changes-so-far visible */
	CommandCounterIncrement();
}

/*
 * check_ivm_restriction --- look for specify nodes in the query tree
 */
static void
check_ivm_restriction(Node *node)
{
	check_ivm_restriction_context context = {false};

	check_ivm_restriction_walker(node, &context);
}

static bool
check_ivm_restriction_walker(Node *node, check_ivm_restriction_context *context)
{
	if (node == NULL)
		return false;

	/*
	 * We currently don't support Sub-Query.
	 */
	if (IsA(node, SubPlan) || IsA(node, SubLink))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("subquery is not supported on incrementally maintainable materialized view")));

	/* This can recurse, so check for excessive recursion */
	check_stack_depth();

	switch (nodeTag(node))
	{
		case T_Query:
			{
				Query *qry = (Query *)node;
				ListCell   *lc;
				List       *vars;

				/* if contained CTE, return error */
				if (qry->cteList != NIL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("CTE is not supported on incrementally maintainable materialized view")));
				if (qry->groupClause != NIL && !qry->hasAggs)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("GROUP BY clause without aggregate is not supported on incrementally maintainable materialized view")));
				if (qry->havingQual != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("HAVING clause is not supported on incrementally maintainable materialized view")));
				if (qry->sortClause != NIL)	/* There is a possibility that we don't need to return an error */
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("ORDER BY clause is not supported on incrementally maintainable materialized view")));
				if (qry->limitOffset != NULL || qry->limitCount != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("LIMIT/OFFSET clause is not supported on incrementally maintainable materialized view")));
				if (qry->hasDistinctOn)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("DISTINCT ON is not supported on incrementally maintainable materialized view")));
				if (qry->hasWindowFuncs)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("window functions are not supported on incrementally maintainable materialized view")));
				if (qry->groupingSets != NIL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("GROUPING SETS, ROLLUP, or CUBE clauses is not supported on incrementally maintainable materialized view")));
				if (qry->setOperations != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("UNION/INTERSECT/EXCEPT statements are not supported on incrementally maintainable materialized view")));
				if (list_length(qry->targetList) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("empty target list is not supported on incrementally maintainable materialized view")));
				if (qry->rowMarks != NIL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("FOR UPDATE/SHARE clause is not supported on incrementally maintainable materialized view")));

				/* system column restrictions */
				vars = pull_vars_of_level((Node *) qry, 0);
				foreach(lc, vars)
				{
					if (IsA(lfirst(lc), Var))
					{
						Var *var = (Var *) lfirst(lc);
						/* if system column, return error */
						if (var->varattno < 0)
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									 errmsg("system column is not supported on incrementally maintainable materialized view")));
					}
				}

				context->has_agg |= qry->hasAggs;

				/* restrictions for rtable */
				foreach(lc, qry->rtable)
				{
					RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);

					if (rte->subquery)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("subquery is not supported on incrementally maintainable materialized view")));

					if (rte->tablesample != NULL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("TABLESAMPLE clause is not supported on incrementally maintainable materialized view")));

					if (rte->relkind == RELKIND_PARTITIONED_TABLE)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("partitioned table is not supported on incrementally maintainable materialized view")));

					if (rte->relkind == RELKIND_RELATION && has_superclass(rte->relid))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("partitions is not supported on incrementally maintainable materialized view")));

					if (rte->relkind == RELKIND_RELATION && find_inheritance_children(rte->relid, NoLock) != NIL)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("inheritance parent is not supported on incrementally maintainable materialized view")));

					if (rte->relkind == RELKIND_FOREIGN_TABLE)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("foreign table is not supported on incrementally maintainable materialized view")));

					if (rte->relkind == RELKIND_VIEW ||
						rte->relkind == RELKIND_MATVIEW)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("VIEW or MATERIALIZED VIEW is not supported on incrementally maintainable materialized view")));

					if (rte->rtekind == RTE_VALUES)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("VALUES is not supported on incrementally maintainable materialized view")));

					if (rte->relid != InvalidOid)
					{
						Relation rel = table_open(rte->relid, NoLock);
						if (RelationIsAppendOptimized(rel))
							ereport(ERROR,
									(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("append-optimized table is not supported on incrementally maintainable materialized view")));
						table_close(rel, NoLock);
					}
				}

				query_tree_walker(qry, check_ivm_restriction_walker, (void *) context, QTW_IGNORE_RANGE_TABLE);

				break;
			}
		case T_TargetEntry:
			{
				TargetEntry *tle = (TargetEntry *)node;
				if (isIvmName(tle->resname))
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("column name %s is not supported on incrementally maintainable materialized view", tle->resname)));
				if (context->has_agg && !IsA(tle->expr, Aggref) && contain_aggs_of_level((Node *) tle->expr, 0))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("expression containing an aggregate in it is not supported on incrementally maintainable materialized view")));

				expression_tree_walker(node, check_ivm_restriction_walker, (void *) context);
				break;
			}
		case T_JoinExpr:
			{
				JoinExpr *joinexpr = (JoinExpr *)node;

				if (joinexpr->jointype > JOIN_INNER)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("OUTER JOIN is not supported on incrementally maintainable materialized view")));

				expression_tree_walker(node, check_ivm_restriction_walker, (void *) context);
				break;
			}
		case T_Aggref:
			{
				/* Check if this supports IVM */
				Aggref *aggref = (Aggref *) node;
				const char *aggname = format_procedure(aggref->aggfnoid);

				if (aggref->aggfilter != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate function with FILTER clause is not supported on incrementally maintainable materialized view")));

				if (aggref->aggdistinct != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate function with DISTINCT arguments is not supported on incrementally maintainable materialized view")));

				if (aggref->aggorder != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate function with ORDER clause is not supported on incrementally maintainable materialized view")));

				if (!check_aggregate_supports_ivm(aggref->aggfnoid))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("aggregate function %s is not supported on incrementally maintainable materialized view", aggname)));
				break;
			}
		default:
			expression_tree_walker(node, check_ivm_restriction_walker, (void *) context);
			break;
	}
	return false;
}

/*
 * check_aggregate_supports_ivm
 *
 * Check if the given aggregate function is supporting IVM
 */
static bool
check_aggregate_supports_ivm(Oid aggfnoid)
{
	switch (aggfnoid)
	{
		/* count */
		case F_COUNT_ANY:
		case F_COUNT_:

		/* sum */
		case F_SUM_INT8:
		case F_SUM_INT4:
		case F_SUM_INT2:
		case F_SUM_FLOAT4:
		case F_SUM_FLOAT8:
		case F_SUM_MONEY:
		case F_SUM_INTERVAL:
		case F_SUM_NUMERIC:

		/* avg */
		case F_AVG_INT8:
		case F_AVG_INT4:
		case F_AVG_INT2:
		case F_AVG_NUMERIC:
		case F_AVG_FLOAT4:
		case F_AVG_FLOAT8:
		case F_AVG_INTERVAL:

			return true;

		default:
			return false;
	}
}

/*
 * CreateIndexOnIMMV
 *
 * Create a unique index on incremental maintainable materialized view.
 * If the view definition query has a GROUP BY clause, the index is created
 * on the columns of GROUP BY expressions. Otherwise, if the view contains
 * all primary key attritubes of its base tables in the target list, the index
 * is created on these attritubes. In other cases, no index is created.
 */
void
CreateIndexOnIMMV(Query *query, Relation matviewRel)
{
	ListCell *lc;
	IndexStmt  *index;
	ObjectAddress address;
	List *constraintList = NIL;
	char		idxname[NAMEDATALEN];
	List	   *indexoidlist = RelationGetIndexList(matviewRel);
	ListCell   *indexoidscan;

	snprintf(idxname, sizeof(idxname), "%s_index", RelationGetRelationName(matviewRel));

	index = makeNode(IndexStmt);

	index->unique = true;
	index->primary = false;
	index->isconstraint = false;
	index->deferrable = false;
	index->initdeferred = false;
	index->idxname = idxname;
	index->relation =
		makeRangeVar(get_namespace_name(RelationGetNamespace(matviewRel)),
					 pstrdup(RelationGetRelationName(matviewRel)),
					 -1);
	index->accessMethod = DEFAULT_INDEX_TYPE;
	index->options = NIL;
	index->tableSpace = get_tablespace_name(matviewRel->rd_rel->reltablespace);
	index->whereClause = NULL;
	index->indexParams = NIL;
	index->indexIncludingParams = NIL;
	index->excludeOpNames = NIL;
	index->idxcomment = NULL;
	index->indexOid = InvalidOid;
	index->oldNode = InvalidOid;
	index->oldCreateSubid = InvalidSubTransactionId;
	index->oldFirstRelfilenodeSubid = InvalidSubTransactionId;
	index->transformed = true;
	index->concurrent = false;
	index->if_not_exists = false;

	if (query->groupClause)
	{
		/* create unique constraint on GROUP BY expression columns */
		foreach(lc, query->groupClause)
		{
			SortGroupClause *scl = (SortGroupClause *) lfirst(lc);
			TargetEntry *tle = get_sortgroupclause_tle(scl, query->targetList);
			Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, tle->resno - 1);
			IndexElem  *iparam;

			iparam = makeNode(IndexElem);
			iparam->name = pstrdup(NameStr(attr->attname));
			iparam->expr = NULL;
			iparam->indexcolname = NULL;
			iparam->collation = NIL;
			iparam->opclass = NIL;
			iparam->opclassopts = NIL;
			iparam->ordering = SORTBY_DEFAULT;
			iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;
			index->indexParams = lappend(index->indexParams, iparam);
		}
	}
	else if (query->distinctClause)
	{
		/* create unique constraint on all columns */
		foreach(lc, query->targetList)
		{
			TargetEntry *tle = (TargetEntry *) lfirst(lc);
			Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, tle->resno - 1);
			IndexElem  *iparam;

			iparam = makeNode(IndexElem);
			iparam->name = pstrdup(NameStr(attr->attname));
			iparam->expr = NULL;
			iparam->indexcolname = NULL;
			iparam->collation = NIL;
			iparam->opclass = NIL;
			iparam->opclassopts = NIL;
			iparam->ordering = SORTBY_DEFAULT;
			iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;
			index->indexParams = lappend(index->indexParams, iparam);
		}
	}
	else
	{
		Bitmapset *key_attnos;

		/* create index on the base tables' primary key columns */
		key_attnos = get_primary_key_attnos_from_query(query, &constraintList);
		if (key_attnos)
		{
			foreach(lc, query->targetList)
			{
				TargetEntry *tle = (TargetEntry *) lfirst(lc);
				Form_pg_attribute attr = TupleDescAttr(matviewRel->rd_att, tle->resno - 1);

				if (bms_is_member(tle->resno - FirstLowInvalidHeapAttributeNumber, key_attnos))
				{
					IndexElem  *iparam;

					iparam = makeNode(IndexElem);
					iparam->name = pstrdup(NameStr(attr->attname));
					iparam->expr = NULL;
					iparam->indexcolname = NULL;
					iparam->collation = NIL;
					iparam->opclass = NIL;
					iparam->opclassopts = NIL;
					iparam->ordering = SORTBY_DEFAULT;
					iparam->nulls_ordering = SORTBY_NULLS_DEFAULT;
					index->indexParams = lappend(index->indexParams, iparam);
				}
			}
		}
		else
		{
			/* create no index, just notice that an appropriate index is necessary for efficient IVM */
			ereport(NOTICE,
					(errmsg("could not create an index on materialized view \"%s\" automatically",
							RelationGetRelationName(matviewRel)),
					 errdetail("This target list does not have all the primary key columns, "
							   "or this view does not contain GROUP BY or DISTINCT clause."),
					 errhint("Create an index on the materialized view for efficient incremental maintenance.")));
			return;
		}
	}

	/* If we have a compatible index, we don't need to create another. */
	foreach(indexoidscan, indexoidlist)
	{
		Oid			indexoid = lfirst_oid(indexoidscan);
		Relation	indexRel;
		bool		hasCompatibleIndex = false;

		indexRel = index_open(indexoid, AccessShareLock);

		if (CheckIndexCompatible(indexRel->rd_id,
								index->accessMethod,
								index->indexParams,
								index->excludeOpNames))
			hasCompatibleIndex = true;

		index_close(indexRel, AccessShareLock);

		if (hasCompatibleIndex)
			return;
	}

	address = DefineIndex(RelationGetRelid(matviewRel),
						  index,
						  InvalidOid,
						  InvalidOid,
						  InvalidOid,
						  false, true, false, false, true);

	ereport(NOTICE,
			(errmsg("created index \"%s\" on materialized view \"%s\"",
					idxname, RelationGetRelationName(matviewRel))));

	/*
	 * Make dependencies so that the index is dropped if any base tables's
	 * primary key is dropped.
	 */
	foreach(lc, constraintList)
	{
		Oid constraintOid = lfirst_oid(lc);
		ObjectAddress	refaddr;

		refaddr.classId = ConstraintRelationId;
		refaddr.objectId = constraintOid;
		refaddr.objectSubId = 0;

		recordDependencyOn(&address, &refaddr, DEPENDENCY_NORMAL);
	}
}


/*
 * get_primary_key_attnos_from_query
 *
 * Identify the columns in base tables' primary keys in the target list.
 *
 * Returns a Bitmapset of the column attnos of the primary key's columns of
 * tables that used in the query.  The attnos are offset by
 * FirstLowInvalidHeapAttributeNumber as same as get_primary_key_attnos.
 *
 * If any table has no primary key or any primary key's columns is not in
 * the target list, return NULL.  We also return NULL if any pkey constraint
 * is deferrable.
 *
 * constraintList is set to a list of the OIDs of the pkey constraints.
 */
static Bitmapset *
get_primary_key_attnos_from_query(Query *query, List **constraintList)
{
	List *key_attnos_list = NIL;
	ListCell *lc;
	int i;
	Bitmapset *keys = NULL;
	Relids	rels_in_from;

	/*
	 * Collect primary key attributes from all tables used in query. The key attributes
	 * sets for each table are stored in key_attnos_list in order by RTE index.
	 */
	foreach(lc, query->rtable)
	{
		RangeTblEntry *r = (RangeTblEntry*) lfirst(lc);
		Bitmapset *key_attnos;
		bool	has_pkey = true;

		/* for tables, call get_primary_key_attnos */
		if (r->rtekind == RTE_RELATION)
		{
			Oid constraintOid;
			key_attnos = get_primary_key_attnos(r->relid, false, &constraintOid);
			*constraintList = lappend_oid(*constraintList, constraintOid);
			has_pkey = (key_attnos != NULL);
		}
		/* for other RTEs, store NULL into key_attnos_list */
		else
			key_attnos = NULL;

		/*
		 * If any table or subquery has no primary key or its pkey constraint is deferrable,
		 * we cannot get key attributes for this query, so return NULL.
		 */
		if (!has_pkey)
			return NULL;

		key_attnos_list = lappend(key_attnos_list, key_attnos);
	}

	/* Collect key attributes appearing in the target list */
	i = 1;
	foreach(lc, query->targetList)
	{
		TargetEntry *tle = (TargetEntry *) flatten_join_alias_vars(query, lfirst(lc));

		if (IsA(tle->expr, Var))
		{
			Var *var = (Var*) tle->expr;
			Bitmapset *key_attnos = list_nth(key_attnos_list, var->varno - 1);

			/* check if this attribute is from a base table's primary key */
			if (bms_is_member(var->varattno - FirstLowInvalidHeapAttributeNumber, key_attnos))
			{
				/*
				 * Remove found key attributes from key_attnos_list, and add this
				 * to the result list.
				 */
				key_attnos = bms_del_member(key_attnos, var->varattno - FirstLowInvalidHeapAttributeNumber);
				if (bms_is_empty(key_attnos))
				{
					key_attnos_list = list_delete_nth_cell(key_attnos_list, var->varno - 1);
					key_attnos_list = list_insert_nth(key_attnos_list, var->varno - 1, NULL);
				}
				keys = bms_add_member(keys, i - FirstLowInvalidHeapAttributeNumber);
			}
		}
		i++;
	}

	/* Collect RTE indexes of relations appearing in the FROM clause */
	rels_in_from = get_relids_in_jointree((Node *) query->jointree, false);

	/*
	 * Check if all key attributes of relations in FROM are appearing in the target
	 * list.  If an attribute remains in key_attnos_list in spite of the table is used
	 * in FROM clause, the target is missing this key attribute, so we return NULL.
	 */
	i = 1;
	foreach(lc, key_attnos_list)
	{
		Bitmapset *bms = (Bitmapset *)lfirst(lc);
		if (!bms_is_empty(bms) && bms_is_member(i, rels_in_from))
			return NULL;
		i++;
	}

	return keys;
}

/* 
 * Create auto-refresh task for Dynamic Tables.
 */
static void
create_dynamic_table_auto_refresh_task(ParseState *pstate, Relation DynamicTableRel, char *schedule)
{
	ObjectAddress refaddr;
	ObjectAddress address;
	StringInfoData buf;
	char *dtname = NULL;

	if (schedule == NULL)
		schedule = DYNAMIC_TABLE_DEFAULT_REFRESH_INTERVAL;

	/* Create auto refresh task. */
	CreateTaskStmt *task_stmt = makeNode(CreateTaskStmt);

	initStringInfo(&buf);
	appendStringInfo(&buf, "gp_dynamic_table_refresh_%u", RelationGetRelid(DynamicTableRel));
	task_stmt->taskname = pstrdup(buf.data);
	task_stmt->schedule = pstrdup(schedule);
	task_stmt->if_not_exists = false; /* report error if failed. */
	dtname = quote_qualified_identifier(get_namespace_name(RelationGetNamespace(DynamicTableRel)),
											 RelationGetRelationName(DynamicTableRel));
	resetStringInfo(&buf);
	appendStringInfo(&buf, "REFRESH DYNAMIC TABLE %s", dtname);
	task_stmt->sql = pstrdup(buf.data);
	bool saved_allowSystemTableMods = allowSystemTableMods;
	allowSystemTableMods = true;
	address = DefineTask(pstate, task_stmt);
	allowSystemTableMods = saved_allowSystemTableMods;

	refaddr.classId = RelationRelationId;
	refaddr.objectId = RelationGetRelid(DynamicTableRel);
	refaddr.objectSubId = 0;
	recordDependencyOn(&address, &refaddr, DEPENDENCY_INTERNAL);
}

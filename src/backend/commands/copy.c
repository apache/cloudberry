/*-------------------------------------------------------------------------
 *
 * copy.c
 *		Implements the COPY utility command
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/commands/copy.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq-int.h"

#include <ctype.h>
#include <unistd.h>
#include <sys/stat.h>

#include "access/sysattr.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "commands/copy.h"
#include "commands/copyfrom_internal.h"
#include "commands/copyto_internal.h"
#include "commands/defrem.h"
#include "executor/executor.h"
#include "libpq/libpq.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_coerce.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "rewrite/rewriteHandler.h"
#include "storage/fd.h"
#include "storage/execute_pipe.h"
#include "tcop/tcopprot.h"
#include "utils/acl.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"

#include "access/external.h"
#include "access/url.h"
#include "catalog/catalog.h"
#include "catalog/gp_matview_aux.h"
#include "catalog/namespace.h"
#include "catalog/pg_extprotocol.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbaocsam.h"
#include "cdb/cdbconn.h"
#include "cdb/cdbcopy.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbsreh.h"
#include "cdb/cdbvars.h"
#include "commands/copyto_internal.h"
#include "commands/queue.h"
#include "nodes/makefuncs.h"
#include "postmaster/autostats.h"
#include "utils/metrics_utils.h"
#include "utils/resscheduler.h"
#include "utils/string_utils.h"

/* non-export function prototypes */
static uint64 CopyDispatchOnSegment(const CopyStmt *stmt);
static uint64 CopyToQueryOnSegment(CopyToState cstate);

/* Low-level communications functions */
static List *parse_joined_option_list(char *str, char *delimiter);

volatile CopyToState glob_cstate = NULL;

/* GPDB_91_MERGE_FIXME: passing through a global variable like this is ugly */
CopyStmt *glob_copystmt = NULL;

/*
 *	 DoCopy executes the SQL COPY statement
 *
 * Either unload or reload contents of table <relation>, depending on <from>.
 * (<from> = true means we are inserting into the table.)  In the "TO" case
 * we also support copying the output of an arbitrary SELECT, INSERT, UPDATE
 * or DELETE query.
 *
 * If <pipe> is false, transfer is between the table and the file named
 * <filename>.  Otherwise, transfer is between the table and our regular
 * input/output stream. The latter could be either stdin/stdout or a
 * socket, depending on whether we're running under Postmaster control.
 *
 * Do not allow a Postgres user without the 'pg_read_server_files' or
 * 'pg_write_server_files' role to read from or write to a file.
 *
 * Do not allow the copy if user doesn't have proper permission to access
 * the table or the specifically requested columns.
 */
void
DoCopy(ParseState *pstate, const CopyStmt *stmt,
	   int stmt_location, int stmt_len,
	   uint64 *processed)
{
	bool		is_from = stmt->is_from;
	bool		pipe = (stmt->filename == NULL || Gp_role == GP_ROLE_EXECUTE);
	Relation	rel = NULL;
	LOCKMODE	lockmode;
	Oid			relid;
	RawStmt    *query = NULL;
	Node	   *whereClause = NULL;
	List	   *attnamelist = stmt->attlist;
	List	   *options;

	glob_cstate = NULL;
	glob_copystmt = (CopyStmt *) stmt;

	options = stmt->options;

	if (stmt->sreh && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY single row error handling only available using COPY FROM")));

	lockmode = is_from ? RowExclusiveLock : AccessShareLock;
	if (stmt->relation)
	{
		/* Open and lock the relation, using the appropriate lock type. */
		rel = table_openrv(stmt->relation, lockmode);
	}
	
	/*
	 * Disallow COPY to/from file or program except to users with the
	 * appropriate role.
	 */
	if (!pipe)
	{
		if (stmt->is_program)
		{
			if (!has_privs_of_role(GetUserId(), ROLE_PG_EXECUTE_SERVER_PROGRAM))
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("permission denied to COPY to or from an external program"),
						 errdetail("Only roles with privileges of the \"%s\" role may COPY to or from an external program.",
								   "pg_execute_server_program"),
						 errhint("Anyone can COPY to stdout or from stdin. "
								 "psql's \\copy command also works for anyone.")));
		}
		else
		{
			if (is_from && !is_member_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES) && rel->rd_rel->relkind != RELKIND_DIRECTORY_TABLE)
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("permission denied to COPY from a file"),
						 errdetail("Only roles with privileges of the \"%s\" role may COPY from a file.",
								   "pg_read_server_files"),
						 errhint("Anyone can COPY to stdout or from stdin. "
								 "psql's \\copy command also works for anyone.")));

			if (!is_from && !is_member_of_role(GetUserId(), ROLE_PG_WRITE_SERVER_FILES) && rel->rd_rel->relkind != RELKIND_DIRECTORY_TABLE)
				ereport(ERROR,
						(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						 errmsg("permission denied to COPY to a file"),
						 errdetail("Only roles with privileges of the \"%s\" role may COPY to a file.",
								   "pg_write_server_files"),
						 errhint("Anyone can COPY to stdout or from stdin. "
								 "psql's \\copy command also works for anyone.")));
		}
	}

	if (stmt->relation)
	{
		ParseNamespaceItem *nsitem;
		RangeTblEntry *rte;
		RTEPermissionInfo *perminfo;
		TupleDesc	tupDesc;
		List	   *attnums;
		ListCell   *cur;

		Assert(!stmt->query);

		if (is_from && !allowSystemTableMods && IsUnderPostmaster && IsSystemRelation(rel))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
							errmsg("permission denied: \"%s\" is a system catalog",
								RelationGetRelationName(rel)),
								errhint("Make sure the configuration parameter allow_system_table_mods is set.")));
		}

		relid = RelationGetRelid(rel);

		nsitem = addRangeTableEntryForRelation(pstate, rel, lockmode,
											   NULL, false, false);
		rte = nsitem->p_rte;
		perminfo = nsitem->p_perminfo;
		perminfo->requiredPerms = (is_from ? ACL_INSERT : ACL_SELECT);

		if (stmt->whereClause)
		{
			/* add nsitem to query namespace */
			addNSItemToQuery(pstate, nsitem, false, true, true);

			/* Transform the raw expression tree */
			whereClause = transformExpr(pstate, stmt->whereClause, EXPR_KIND_COPY_WHERE);

			/* Make sure it yields a boolean result. */
			whereClause = coerce_to_boolean(pstate, whereClause, "WHERE");

			/* we have to fix its collations too */
			assign_expr_collations(pstate, whereClause);

			whereClause = eval_const_expressions(NULL, whereClause);

			whereClause = (Node *) canonicalize_qual((Expr *) whereClause, false);
			whereClause = (Node *) make_ands_implicit((Expr *) whereClause);
		}

		tupDesc = RelationGetDescr(rel);
		attnums = CopyGetAttnums(tupDesc, rel, attnamelist);
		foreach (cur, attnums)
		{
			int			attno;
			Bitmapset **bms;

			attno = lfirst_int(cur) - FirstLowInvalidHeapAttributeNumber;
			bms = is_from ? &perminfo->insertedCols : &perminfo->selectedCols;

			*bms = bms_add_member(*bms, attno);
		}
		ExecCheckPermissions(pstate->p_rtable, list_make1(perminfo), true);

		/*
		 * Permission check for row security policies.
		 *
		 * check_enable_rls will ereport(ERROR) if the user has requested
		 * something invalid and will otherwise indicate if we should enable
		 * RLS (returns RLS_ENABLED) or not for this COPY statement.
		 *
		 * If the relation has a row security policy and we are to apply it
		 * then perform a "query" copy and allow the normal query processing
		 * to handle the policies.
		 *
		 * If RLS is not enabled for this, then just fall through to the
		 * normal non-filtering relation handling.
		 *
		 * GPDB: Also do this for partitioned tables. In PostgreSQL, you get
		 * an error:
		 *
		 * ERROR:  cannot copy from partitioned table "foo"
		 * HINT:  Try the COPY (SELECT ...) TO variant.
		 *
		 * In GPDB 6 and before, support for COPYing partitioned table was
		 * implemented deenop in the COPY processing code. In GPDB 7,
		 * partitiong was replaced with upstream impementation, but for
		 * backwards-compatibility, we do the translation to "COPY (SELECT
		 * ...)" variant automatically, just like PostgreSQL does for RLS.
		 */
		if (check_enable_rls(rte->relid, InvalidOid, false) == RLS_ENABLED ||
			(!is_from && rel->rd_rel->relkind == RELKIND_PARTITIONED_TABLE))
		{
			SelectStmt *select;
			ColumnRef  *cr;
			ResTarget  *target;
			RangeVar   *from;
			List	   *targetList = NIL;

			if (is_from)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("COPY FROM not supported with row-level security"),
						 errhint("Use INSERT statements instead.")));

			/*
			 * Build target list
			 *
			 * If no columns are specified in the attribute list of the COPY
			 * command, then the target list is 'all' columns. Therefore, '*'
			 * should be used as the target list for the resulting SELECT
			 * statement.
			 *
			 * In the case that columns are specified in the attribute list,
			 * create a ColumnRef and ResTarget for each column and add them
			 * to the target list for the resulting SELECT statement.
			 */
			if (!stmt->attlist)
			{
				cr = makeNode(ColumnRef);
				cr->fields = list_make1(makeNode(A_Star));
				cr->location = -1;

				target = makeNode(ResTarget);
				target->name = NULL;
				target->indirection = NIL;
				target->val = (Node *) cr;
				target->location = -1;

				targetList = list_make1(target);
			}
			else
			{
				ListCell   *lc;

				foreach(lc, stmt->attlist)
				{
					/*
					 * Build the ColumnRef for each column.  The ColumnRef
					 * 'fields' property is a String node that corresponds to
					 * the column name respectively.
					 */
					cr = makeNode(ColumnRef);
					cr->fields = list_make1(lfirst(lc));
					cr->location = -1;

					/* Build the ResTarget and add the ColumnRef to it. */
					target = makeNode(ResTarget);
					target->name = NULL;
					target->indirection = NIL;
					target->val = (Node *) cr;
					target->location = -1;

					/* Add each column to the SELECT statement's target list */
					targetList = lappend(targetList, target);
				}
			}

			/*
			 * Build RangeVar for from clause, fully qualified based on the
			 * relation which we have opened and locked.  Use "ONLY" so that
			 * COPY retrieves rows from only the target table not any
			 * inheritance children, the same as when RLS doesn't apply.
			 */
			from = makeRangeVar(get_namespace_name(RelationGetNamespace(rel)),
								pstrdup(RelationGetRelationName(rel)),
								-1);

			/* Build query */
			select = makeNode(SelectStmt);
			select->targetList = targetList;
			select->fromClause = list_make1(from);

			query = makeNode(RawStmt);
			query->stmt = (Node *) select;
			query->stmt_location = stmt_location;
			query->stmt_len = stmt_len;

			/*
			 * Close the relation for now, but keep the lock on it to prevent
			 * changes between now and when we start the query-based COPY.
			 *
			 * We'll reopen it later as part of the query-based COPY.
			 */
			table_close(rel, NoLock);
			rel = NULL;
		}
	}
	else
	{
		Assert(stmt->query);

		/* MERGE is allowed by parser, but unimplemented. Reject for now */
		if (IsA(stmt->query, MergeStmt))
			ereport(ERROR,
					errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("MERGE not supported in COPY"));

		query = makeNode(RawStmt);
		query->stmt = stmt->query;
		query->stmt_location = stmt_location;
		query->stmt_len = stmt_len;

		relid = InvalidOid;
		rel = NULL;
	}

	if (is_from)
	{
		CopyFromState cstate;

		Assert(rel);

		if (stmt->sreh && Gp_role != GP_ROLE_EXECUTE && !rel->rd_cdbpolicy)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY single row error handling only available for distributed user tables")));

		/*
		 * GPDB_91_MERGE_FIXME: is it possible to get to this point in the code
		 * with a temporary relation that belongs to another session? If so, the
		 * following code doesn't function as expected.
		 */
		/* check read-only transaction and parallel mode */
		if (XactReadOnly && !rel->rd_islocaltemp)
			PreventCommandIfReadOnly("COPY FROM");

		cstate = BeginCopyFrom(pstate, rel, whereClause,
							   stmt->filename, stmt->is_program,
							   NULL, NULL, stmt->attlist, options);

		/*
		 * Error handling setup
		 */
		if (cstate->opts.sreh)
		{
			/* Single row error handling requested */
			SingleRowErrorDesc *sreh = cstate->opts.sreh;
			char		log_to_file = LOG_ERRORS_DISABLE;

			if (IS_LOG_TO_FILE(sreh->log_error_type))
			{
				cstate->errMode = SREH_LOG;
				/* LOG ERRORS PERSISTENTLY for COPY is not allowed for now. */
				log_to_file = LOG_ERRORS_ENABLE;
			}
			else
			{
				cstate->errMode = SREH_IGNORE;
			}
			cstate->cdbsreh = makeCdbSreh(sreh->rejectlimit,
										  sreh->is_limit_in_rows,
										  cstate->filename,
										  stmt->relation->relname,
										  log_to_file);
			if (rel)
				cstate->cdbsreh->relid = RelationGetRelid(rel);
		}
		else
		{
			/* No single row error handling requested. Use "all or nothing" */
			cstate->cdbsreh = NULL; /* default - no SREH */
			cstate->errMode = ALL_OR_NOTHING; /* default */
		}

		PG_TRY();
		{
			if (Gp_role == GP_ROLE_DISPATCH && cstate->opts.on_segment)
				*processed = CopyDispatchOnSegment(stmt);
			else
				*processed = CopyFrom(cstate);	/* copy from file to database */

			/* Handle copy to replicated table returns processed number */
			if (Gp_role == GP_ROLE_DISPATCH &&
				GpPolicyIsReplicated(cstate->rel->rd_cdbpolicy))
				*processed = *processed / cstate->rel->rd_cdbpolicy->numsegments;

			/*
			 * Update view info if we actualy copy data from other place.
			 */
			if (IS_QD_OR_SINGLENODE() && *processed > 0)
			{
				SetRelativeMatviewAuxStatus(relid,
											MV_DATA_STATUS_EXPIRED_INSERT_ONLY,
											MV_DATA_STATUS_TRANSFER_DIRECTION_ALL);
			}
		}
		PG_CATCH();
		{
			if (cstate->cdbCopy)
			{
				MemoryContext oldcontext = MemoryContextSwitchTo(cstate->copycontext);

				cdbCopyAbort(cstate->cdbCopy);
				cstate->cdbCopy = NULL;
				MemoryContextSwitchTo(oldcontext);
			}
			PG_RE_THROW();
		}
		PG_END_TRY();
		EndCopyFrom(cstate);
	}
	else
	{
		CopyToState cstate;

		/*
		 * GPDB_91_MERGE_FIXME: ExecutorStart() is called in BeginCopyTo,
		 * but the TRY-CATCH block only starts here. If an error is
		 * thrown in-between, we would fail to call mppExecutorCleanup. We
		 * really should be using a ResourceOwner or something else for
		 * cleanup, instead of TRY-CATCH blocks...
		 *
		 * Update: I tried to fix this using the glob_cstate hack. It's ugly,
		 * but fixes at least some cases that came up in regression tests.
		 */
		PG_TRY();
		{
			if (rel && rel->rd_rel->relkind == RELKIND_DIRECTORY_TABLE)
			{
				cstate = BeginCopyToDirectoryTable(pstate, stmt->filename, stmt->dirfilename,
												   rel, stmt->is_program, options);
			}

			else
			{
				cstate = BeginCopyTo(pstate, rel, query, relid,
									 stmt->filename, stmt->is_program,
									 NULL, stmt->attlist, options);
			}

			/*
			 * "copy t to file on segment"					CopyDispatchOnSegment
			 * "copy (select * from t) to file on segment"	CopyToQueryOnSegment
			 * "copy t/(select * from t) to file"			DoCopyTo
			 */
			if (Gp_role == GP_ROLE_DISPATCH && cstate->opts.on_segment)
			{
				if (cstate->rel)
					*processed = CopyDispatchOnSegment(stmt);
				else
					*processed = CopyToQueryOnSegment(cstate);
			}
			else
				*processed = DoCopyTo(cstate);	/* copy from database to file */
		}
		PG_CATCH();
		{
			if (glob_cstate && glob_cstate->queryDesc)
			{
				/* should shutdown the mpp stuff such as interconnect and dispatch thread */
				mppExecutorCleanup(glob_cstate->queryDesc);
			}
			PG_RE_THROW();
		}
		PG_END_TRY();

		EndCopyTo(cstate, processed);
	}
	/*
	 * Close the relation.  If reading, we can release the AccessShareLock we
	 * got; if writing, we should hold the lock until end of transaction to
	 * ensure that updates will be committed before lock is released.
	 */
	if (rel != NULL)
		table_close(rel, (is_from ? NoLock : AccessShareLock));

	/* Issue automatic ANALYZE if conditions are satisfied (MPP-4082). */
	if (Gp_role == GP_ROLE_DISPATCH && is_from)
	{
		bool inFunction = already_under_executor_run() || utility_nested();
		auto_stats(AUTOSTATS_CMDTYPE_COPY, relid, *processed, inFunction);
	}
}

/*
 * Extract a CopyHeaderChoice value from a DefElem.  This is like
 * defGetBoolean() but also accepts the special value "match".
 */
static CopyHeaderChoice
defGetCopyHeaderChoice(DefElem *def, bool is_from)
{
	/*
	 * If no parameter value given, assume "true" is meant.
	 */
	if (def->arg == NULL)
		return COPY_HEADER_TRUE;

	/*
	 * Allow 0, 1, "true", "false", "on", "off", or "match".
	 */
	switch (nodeTag(def->arg))
	{
		case T_Integer:
			switch (intVal(def->arg))
			{
				case 0:
					return COPY_HEADER_FALSE;
				case 1:
					return COPY_HEADER_TRUE;
				default:
					/* otherwise, error out below */
					break;
			}
			break;
		default:
			{
				char	   *sval = defGetString(def);

				/*
				 * The set of strings accepted here should match up with the
				 * grammar's opt_boolean_or_string production.
				 */
				if (pg_strcasecmp(sval, "true") == 0)
					return COPY_HEADER_TRUE;
				if (pg_strcasecmp(sval, "false") == 0)
					return COPY_HEADER_FALSE;
				if (pg_strcasecmp(sval, "on") == 0)
					return COPY_HEADER_TRUE;
				if (pg_strcasecmp(sval, "off") == 0)
					return COPY_HEADER_FALSE;
				if (pg_strcasecmp(sval, "match") == 0)
				{
					if (!is_from)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("cannot use \"%s\" with HEADER in COPY TO",
										sval)));
					return COPY_HEADER_MATCH;
				}
			}
			break;
	}
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("%s requires a Boolean value or \"match\"",
					def->defname)));
	return COPY_HEADER_FALSE;	/* keep compiler quiet */
}

/*
 * Process the statement option list for COPY.
 *
 * Scan the options list (a list of DefElem) and transpose the information
 * into *opts_out, applying appropriate error checking.
 *
 * If 'opts_out' is not NULL, it is assumed to be filled with zeroes initially.
 *
 * This is exported so that external users of the COPY API can sanity-check
 * a list of options.  In that usage, 'opts_out' can be passed as NULL and
 * the collected data is just leaked until CurrentMemoryContext is reset.
 *
 * Note that additional checking, such as whether column names listed in FORCE
 * QUOTE actually exist, has to be applied later.  This just checks for
 * self-consistency of the options list.
 */
void
ProcessCopyOptions(ParseState *pstate,
				   CopyFormatOptions *opts_out,
				   bool is_from,
				   List *options,
				   Oid rel_oid)
{
	bool		format_specified = false;
	bool		freeze_specified = false;
	bool		header_specified = false;
	ListCell   *option;

	/* Support external use for option sanity checking */
	if (opts_out == NULL)
		opts_out = (CopyFormatOptions *) palloc0(sizeof(CopyFormatOptions));

	opts_out->skip_foreign_partitions = false;

	opts_out->delim_off = false;
	opts_out->file_encoding = -1;

	/* Extract options from the statement node tree */
	foreach(option, options)
	{
		DefElem    *defel = lfirst_node(DefElem, option);

		if (strcmp(defel->defname, "format") == 0)
		{
			char	   *fmt = defGetString(defel);

			if (format_specified)
				errorConflictingDefElem(defel, pstate);
			format_specified = true;
			if (strcmp(fmt, "text") == 0)
				 /* default format */ ;
			else if (strcmp(fmt, "csv") == 0)
				opts_out->csv_mode = true;
			else if (strcmp(fmt, "binary") == 0)
				opts_out->binary = true;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("COPY format \"%s\" not recognized", fmt),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "freeze") == 0)
		{
			if (freeze_specified)
				errorConflictingDefElem(defel, pstate);
			freeze_specified = true;
			opts_out->freeze = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "delimiter") == 0)
		{
			if (opts_out->delim)
				errorConflictingDefElem(defel, pstate);
			opts_out->delim = defGetString(defel);

			if (opts_out->delim && pg_strcasecmp(opts_out->delim, "off") == 0)
				opts_out->delim_off = true;
		}
		else if (strcmp(defel->defname, "null") == 0)
		{
			if (opts_out->null_print)
				errorConflictingDefElem(defel, pstate);
			opts_out->null_print = defGetString(defel);

			/*
			 * MPP-2010: unfortunately serialization function doesn't
			 * distinguish between 0x0 and empty string. Therefore we
			 * must assume that if NULL AS was indicated and has no value
			 * the actual value is an empty string.
			 */
			if(!opts_out->null_print)
				opts_out->null_print = "";
		}
		else if (strcmp(defel->defname, "default") == 0)
		{
			if (opts_out->default_print)
				errorConflictingDefElem(defel, pstate);
			opts_out->default_print = defGetString(defel);
		}
		else if (strcmp(defel->defname, "header") == 0)
		{
			if (header_specified)
				errorConflictingDefElem(defel, pstate);
			header_specified = true;
			opts_out->header_line = defGetCopyHeaderChoice(defel, is_from);
		}
		else if (strcmp(defel->defname, "quote") == 0)
		{
			if (opts_out->quote)
				errorConflictingDefElem(defel, pstate);
			opts_out->quote = defGetString(defel);
		}
		else if (strcmp(defel->defname, "escape") == 0)
		{
			if (opts_out->escape)
				errorConflictingDefElem(defel, pstate);
			opts_out->escape = defGetString(defel);
		}
		else if (strcmp(defel->defname, "force_quote") == 0)
		{
			if (opts_out->force_quote || opts_out->force_quote_all)
				errorConflictingDefElem(defel, pstate);
			if (defel->arg && IsA(defel->arg, A_Star))
				opts_out->force_quote_all = true;
			else if (defel->arg && IsA(defel->arg, List))
				opts_out->force_quote = castNode(List, defel->arg);
			else if (defel->arg && IsA(defel->arg, String))
			{
				if (strcmp(strVal(defel->arg), "*") == 0)
					opts_out->force_quote_all = true;
				else
				{
					/* OPTIONS (force_quote 'c1,c2') */
					opts_out->force_quote = parse_joined_option_list(strVal(defel->arg), ",");
				}
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "force_not_null") == 0)
		{
			if (opts_out->force_notnull)
				errorConflictingDefElem(defel, pstate);
			if (defel->arg && IsA(defel->arg, List))
				opts_out->force_notnull = castNode(List, defel->arg);
			else if (defel->arg && IsA(defel->arg, String))
			{
				/* OPTIONS (force_not_null 'c1,c2') */
				opts_out->force_notnull = parse_joined_option_list(strVal(defel->arg), ",");
			}
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "force_null") == 0)
		{
			if (opts_out->force_null)
				errorConflictingDefElem(defel, pstate);
			if (defel->arg && IsA(defel->arg, List))
				opts_out->force_null = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "convert_selectively") == 0)
		{
			/*
			 * Undocumented, not-accessible-from-SQL option: convert only the
			 * named columns to binary form, storing the rest as NULLs. It's
			 * allowed for the column list to be NIL.
			 */
			if (opts_out->convert_selectively)
				errorConflictingDefElem(defel, pstate);
			opts_out->convert_selectively = true;
			if (defel->arg == NULL || IsA(defel->arg, List))
				opts_out->convert_select = castNode(List, defel->arg);
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "encoding") == 0)
		{
			if (opts_out->file_encoding >= 0)
				errorConflictingDefElem(defel, pstate);
			opts_out->file_encoding = pg_char_to_encoding(defGetString(defel));
			if (opts_out->file_encoding < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a valid encoding name",
								defel->defname),
						 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "fill_missing_fields") == 0)
		{
			if (opts_out->fill_missing)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			opts_out->fill_missing = defGetBoolean(defel);
		}
		else if (strcmp(defel->defname, "newline") == 0)
		{
			if (opts_out->eol_str)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			opts_out->eol_str = strVal(defel->arg);
		}
		else if (strcmp(defel->defname, "sreh") == 0)
		{
			if (defel->arg == NULL || !IsA(defel->arg, SingleRowErrorDesc))
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("argument to option \"%s\" must be a list of column names",
								defel->defname)));
			if (opts_out->sreh)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));

			opts_out->sreh = (SingleRowErrorDesc *) defel->arg;
		}
		else if (strcmp(defel->defname, "on_segment") == 0)
		{
			if (opts_out->on_segment)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			opts_out->on_segment = true;
		}
		else if (strcmp(defel->defname, "skip_foreign_partitions") == 0)
		{
			if (opts_out->skip_foreign_partitions)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("conflicting or redundant options")));
			opts_out->skip_foreign_partitions = true;
		}
		else if (strcmp(defel->defname, "tag") == 0)
		{
			if (opts_out->tags)
				ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
								 errmsg("conflicting or redundant options"),
								 parser_errposition(pstate, defel->location)));
			opts_out->tags = defGetString(defel);
		}
		else if (!rel_is_external_table(rel_oid))
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("option \"%s\" not recognized",
							defel->defname),
					 parser_errposition(pstate, defel->location)));
	}

	/*
	 * Check for incompatible options (must do these two before inserting
	 * defaults)
	 */
	if (opts_out->binary && opts_out->delim)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("COPY cannot specify DELIMITER in BINARY mode")));

	if (opts_out->binary && opts_out->null_print)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("COPY cannot specify NULL in BINARY mode")));

	opts_out->eol_type = EOL_UNKNOWN;

	if (opts_out->binary && opts_out->default_print)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("cannot specify DEFAULT in BINARY mode")));

	/* Set defaults for omitted options */
	if (!opts_out->delim)
		opts_out->delim = opts_out->csv_mode ? "," : "\t";

	if (!opts_out->null_print)
		opts_out->null_print = opts_out->csv_mode ? "" : "\\N";
	opts_out->null_print_len = strlen(opts_out->null_print);

	if (opts_out->csv_mode)
	{
		if (!opts_out->quote)
			opts_out->quote = "\"";
		if (!opts_out->escape)
			opts_out->escape = opts_out->quote;
	}

	if (!opts_out->csv_mode && !opts_out->escape)
		opts_out->escape = "\\";			/* default escape for text mode */

	/* Only single-byte delimiter strings are supported. */
	/* GPDB: This is checked later */
#if 0
	if (strlen(opts_out->delim) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY delimiter must be a single one-byte character")));
#endif

	/* Disallow end-of-line characters */
	if (strchr(opts_out->delim, '\r') != NULL ||
		strchr(opts_out->delim, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot be newline or carriage return")));

	if (strchr(opts_out->null_print, '\r') != NULL ||
		strchr(opts_out->null_print, '\n') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY null representation cannot use newline or carriage return")));

	if (opts_out->default_print)
	{
		opts_out->default_print_len = strlen(opts_out->default_print);

		if (strchr(opts_out->default_print, '\r') != NULL ||
			strchr(opts_out->default_print, '\n') != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("COPY default representation cannot use newline or carriage return")));
	}

	/*
	 * Disallow unsafe delimiter characters in non-CSV mode.  We can't allow
	 * backslash because it would be ambiguous.  We can't allow the other
	 * cases because data characters matching the delimiter must be
	 * backslashed, and certain backslash combinations are interpreted
	 * non-literally by COPY IN.  Disallowing all lower case ASCII letters is
	 * more than strictly necessary, but seems best for consistency and
	 * future-proofing.  Likewise we disallow all digits though only octal
	 * digits are actually dangerous.
	 */
	if (!opts_out->csv_mode && !opts_out->delim_off &&
		strchr("\\.abcdefghijklmnopqrstuvwxyz0123456789",
			   opts_out->delim[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot be \"%s\"", opts_out->delim)));

	/* Check header */
	if (opts_out->binary && opts_out->header_line)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot specify HEADER in BINARY mode")));

	/* Check quote */
	if (!opts_out->csv_mode && opts_out->quote != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY quote available only in CSV mode")));

	if (opts_out->csv_mode && strlen(opts_out->quote) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY quote must be a single one-byte character")));

	if (opts_out->csv_mode && opts_out->delim[0] == opts_out->quote[0] && !opts_out->delim_off)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter and quote must be different")));

	/* Check escape */
	if (opts_out->csv_mode && opts_out->escape != NULL && strlen(opts_out->escape) != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY escape in CSV format must be a single character")));

	if (!opts_out->csv_mode && opts_out->escape != NULL &&
		(strchr(opts_out->escape, '\r') != NULL ||
		strchr(opts_out->escape, '\n') != NULL))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY escape representation in text format cannot use newline or carriage return")));

	if (!opts_out->csv_mode && opts_out->escape != NULL && strlen(opts_out->escape) != 1)
	{
		if (pg_strcasecmp(opts_out->escape, "off") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY escape must be a single character, or [OFF/off] to disable escapes")));
	}

	/* Check force_quote */
	if (!opts_out->csv_mode && (opts_out->force_quote || opts_out->force_quote_all))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force quote available only in CSV mode")));
	if ((opts_out->force_quote || opts_out->force_quote_all) && is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force quote only available using COPY TO")));

	/* Check force_notnull */
	if (!opts_out->csv_mode && opts_out->force_notnull != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force not null available only in CSV mode")));
	if (opts_out->force_notnull != NIL && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force not null only available using COPY FROM")));

	/* Check force_null */
	if (!opts_out->csv_mode && opts_out->force_null != NIL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force null available only in CSV mode")));

	if (opts_out->force_null != NIL && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY force null only available using COPY FROM")));

	/* Don't allow the delimiter to appear in the null string. */
	if (strchr(opts_out->null_print, opts_out->delim[0]) != NULL && !opts_out->delim_off)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("COPY delimiter must not appear in the NULL specification")));

	/* Don't allow the CSV quote char to appear in the null string. */
	if (opts_out->csv_mode &&
		strchr(opts_out->null_print, opts_out->quote[0]) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("CSV quote character must not appear in the NULL specification")));

	if (opts_out->tags != NULL && !is_from)
		ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("COPY with tag only available using COPY FROM")));

	/*
	 * DELIMITER
	 *
	 * Only single-byte delimiter strings are supported. In addition, if the
	 * server encoding is a multibyte character encoding we only allow the
	 * delimiter to be an ASCII character (like postgresql. For more info
	 * on this see discussion and comments in MPP-3756).
	 */
	if (pg_database_encoding_max_length() == 1)
	{
		/* single byte encoding such as ascii, latinx and other */
		if (strlen(opts_out->delim) != 1 && !opts_out->delim_off)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY delimiter must be a single one-byte character, or \'off\'")));
	}
	else
	{
		/* multi byte encoding such as utf8 */
		if ((strlen(opts_out->delim) != 1 || IS_HIGHBIT_SET(opts_out->delim[0])) && !opts_out->delim_off )
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("COPY delimiter must be a single one-byte character, or \'off\'")));
	}

	if (!opts_out->csv_mode && strchr(opts_out->delim, '\\') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("COPY delimiter cannot be backslash")));

	if (opts_out->fill_missing && !is_from)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				errmsg("fill missing fields only available for data loading, not unloading")));

	/* Use client encoding when ENCODING option is not specified. */
	if (opts_out->file_encoding < 0)
		opts_out->file_encoding = pg_get_client_encoding();

	/*
	 * NEWLINE
	 */
	if (opts_out->eol_str)
	{
		if (!is_from)
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_FEATURE_NOT_YET),
					errmsg("newline currently available for data loading only, not unloading")));
		}
		else
		{
			if (pg_strcasecmp(opts_out->eol_str, "lf") == 0)
				opts_out->eol_type = EOL_NL;
			else if (pg_strcasecmp(opts_out->eol_str, "cr") == 0)
				opts_out->eol_type = EOL_CR;
			else if (pg_strcasecmp(opts_out->eol_str, "crlf") == 0)
				opts_out->eol_type = EOL_CRNL;
			else
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("invalid value for NEWLINE \"%s\"",
								opts_out->eol_str),
						 errhint("Valid options are: 'LF', 'CRLF' and 'CR'.")));
		}
	}

	if (opts_out->default_print)
	{
		if (!is_from)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("COPY DEFAULT only available using COPY FROM")));

		/* Don't allow the delimiter to appear in the default string. */
		if (strchr(opts_out->default_print, opts_out->delim[0]) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("COPY delimiter must not appear in the DEFAULT specification")));

		/* Don't allow the CSV quote char to appear in the default string. */
		if (opts_out->csv_mode &&
			strchr(opts_out->default_print, opts_out->quote[0]) != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("CSV quote character must not appear in the DEFAULT specification")));

		/* Don't allow the NULL and DEFAULT string to be the same */
		if (opts_out->null_print_len == opts_out->default_print_len &&
			strncmp(opts_out->null_print, opts_out->default_print,
					opts_out->null_print_len) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("NULL specification and DEFAULT specification cannot be the same")));
	}
}

void
ProcessCopyDirectoryTableOptions(ParseState *pstate,
								 CopyFormatOptions *opts_out,
								 bool is_from,
								 List *options,
								 Oid rel_oid)
{
	bool		format_specified = false;
	ListCell   *option;

	/* Support external use for option sanity checking */
	if (opts_out == NULL)
		opts_out = (CopyFormatOptions *) palloc0(sizeof(CopyFormatOptions));

	opts_out->file_encoding = -1;

	/* Extract options from the statement node tree */
	foreach(option, options)
	{
		DefElem    *defel = lfirst_node(DefElem, option);

		if (strcmp(defel->defname, "format") == 0)
		{
			char	   *fmt = defGetString(defel);

			if (format_specified)
				ereport(ERROR,
							(errcode(ERRCODE_SYNTAX_ERROR),
							 errmsg("conflicting or redundant options"),
							 parser_errposition(pstate, defel->location)));
			format_specified = true;
			if (strcmp(fmt, "binary") == 0)
				opts_out->binary = true;
			else
				ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 	 errmsg("format option is not allowed in copy binary from directory table."),
							 parser_errposition(pstate, defel->location)));
		}
		else if (strcmp(defel->defname, "tag") == 0)
		{
			if (opts_out->tags)
				ereport(ERROR,
								(errcode(ERRCODE_SYNTAX_ERROR),
						 		 errmsg("conflicting or redundant options"),
						 		 parser_errposition(pstate, defel->location)));
			opts_out->tags = defGetString(defel);
		}
		else
			ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("option \"%s\" not recognized",
							   defel->defname),
						 parser_errposition(pstate, defel->location)));
	}

	opts_out->eol_type = EOL_UNKNOWN;

	/* Set defaults for omitted options */
	if (!opts_out->delim)
		opts_out->delim = opts_out->csv_mode ? "," : "\t";

	if (!opts_out->null_print)
		opts_out->null_print = opts_out->csv_mode ? "" : "\\N";
	opts_out->null_print_len = strlen(opts_out->null_print);

	if (opts_out->csv_mode)
	{
		if (!opts_out->quote)
			opts_out->quote = "\"";
		if (!opts_out->escape)
			opts_out->escape = opts_out->quote;
	}

	if (!opts_out->csv_mode && !opts_out->escape)
		opts_out->escape = "\\";			/* default escape for text mode */
}

/*
 * Dispatch a COPY ON SEGMENT statement to QEs.
 */
static uint64
CopyDispatchOnSegment(const CopyStmt *stmt)
{
	CopyStmt   *dispatchStmt;
	CdbPgResults pgresults = {0};
	int			i;
	uint64		processed = 0;
	uint64		rejected = 0;

	dispatchStmt = copyObject((CopyStmt *) stmt);

	CdbDispatchUtilityStatement((Node *) dispatchStmt,
								DF_NEED_TWO_PHASE |
								DF_WITH_SNAPSHOT |
								DF_CANCEL_ON_ERROR,
								NIL,
								&pgresults);

	/*
	 * GPDB_91_MERGE_FIXME: SREH handling seems to be handled in a different
	 * place for every type of copy. This should be consolidated with the
	 * others.
	 */
	for (i = 0; i < pgresults.numResults; ++i)
	{
		struct pg_result *result = pgresults.pg_results[i];

		processed += result->numCompleted;
		rejected += result->numRejected;
	}

	if (rejected)
		ReportSrehResults(NULL, rejected);

	cdbdisp_clearCdbPgResults(&pgresults);
	return processed;
}

/*
 * Modify the filename in cstate->filename, and cstate->cdbsreh if any,
 * for COPY ON SEGMENT.
 *
 * Replaces the "<SEGID>" token in the filename with this segment's ID.
 */
void
MangleCopyFileName(char **filenameptr, CdbSreh *cdbsreh)
{
	Assert(filenameptr && *filenameptr);

	char	   *filename = *filenameptr;
	StringInfoData filepath;

	initStringInfo(&filepath);
	appendStringInfoString(&filepath, filename);

	replaceStringInfoString(&filepath, "<SEG_DATA_DIR>", DataDir);

	if (strstr(filename, "<SEGID>") == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("<SEGID> is required for file name")));

	char segid_buf[8];
	snprintf(segid_buf, 8, "%d", GpIdentity.segindex);
	replaceStringInfoString(&filepath, "<SEGID>", segid_buf);

	*filenameptr = filepath.data;
	/* Rename filename if error log needed */
	if (NULL != cdbsreh)
	{
		snprintf(cdbsreh->filename,
				 sizeof(cdbsreh->filename), "%s",
				 filepath.data);
	}
}

static uint64
CopyToQueryOnSegment(CopyToState cstate)
{
	Assert(Gp_role != GP_ROLE_EXECUTE);

	/* run the plan --- the dest receiver will send tuples */
	ExecutorRun(cstate->queryDesc, ForwardScanDirection, 0L, true);
	return 0;
}

/*
 * CopyGetAttnums - build an integer list of attnums to be copied
 *
 * The input attnamelist is either the user-specified column list,
 * or NIL if there was none (in which case we want all the non-dropped
 * columns).
 *
 * We don't include generated columns in the generated full list and we don't
 * allow them to be specified explicitly.  They don't make sense for COPY
 * FROM, but we could possibly allow them for COPY TO.  But this way it's at
 * least ensured that whatever we copy out can be copied back in.
 *
 * rel can be NULL ... it's only used for error reports.
 */
List *
CopyGetAttnums(TupleDesc tupDesc, Relation rel, List *attnamelist)
{
	List	   *attnums = NIL;

	if (attnamelist == NIL)
	{
		/* Generate default column list */
		int			attr_count = tupDesc->natts;
		int			i;

		for (i = 0; i < attr_count; i++)
		{
			if (TupleDescAttr(tupDesc, i)->attisdropped)
				continue;
			if (TupleDescAttr(tupDesc, i)->attgenerated)
				continue;
			attnums = lappend_int(attnums, i + 1);
		}
	}
	else
	{
		/* Validate the user-supplied list and extract attnums */
		ListCell   *l;

		foreach(l, attnamelist)
		{
			char	   *name = strVal(lfirst(l));
			int			attnum;
			int			i;

			/* Lookup column name */
			attnum = InvalidAttrNumber;
			for (i = 0; i < tupDesc->natts; i++)
			{
				Form_pg_attribute att = TupleDescAttr(tupDesc, i);

				if (att->attisdropped)
					continue;
				if (namestrcmp(&(att->attname), name) == 0)
				{
					if (att->attgenerated)
						ereport(ERROR,
								(errcode(ERRCODE_INVALID_COLUMN_REFERENCE),
								 errmsg("column \"%s\" is a generated column",
										name),
								 errdetail("Generated columns cannot be used in COPY.")));
					attnum = att->attnum;
					break;
				}
			}
			if (attnum == InvalidAttrNumber)
			{
				if (rel != NULL)
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" of relation \"%s\" does not exist",
									name, RelationGetRelationName(rel))));
				else
					ereport(ERROR,
							(errcode(ERRCODE_UNDEFINED_COLUMN),
							 errmsg("column \"%s\" does not exist",
									name)));
			}
			/* Check for duplicates */
			if (list_member_int(attnums, attnum))
				ereport(ERROR,
						(errcode(ERRCODE_DUPLICATE_COLUMN),
						 errmsg("column \"%s\" specified more than once",
								name)));
			attnums = lappend_int(attnums, attnum);
		}
	}

	return attnums;
}

/* remove end of line chars from end of a buffer */
void truncateEol(StringInfo buf, EolType eol_type)
{
	int one_back = buf->len - 1;
	int two_back = buf->len - 2;

	if(eol_type == EOL_CRNL)
	{
		if(buf->len < 2)
			return;

		if(buf->data[two_back] == '\r' &&
		   buf->data[one_back] == '\n')
		{
			buf->data[two_back] = '\0';
			buf->data[one_back] = '\0';
			buf->len -= 2;
		}
	}
	else
	{
		if(buf->len < 1)
			return;

		if(buf->data[one_back] == '\r' ||
		   buf->data[one_back] == '\n')
		{
			buf->data[one_back] = '\0';
			buf->len--;
		}
	}
}

/* wrapper for truncateEol */
void
truncateEolStr(char *str, EolType eol_type)
{
	StringInfoData buf;

	buf.data = str;
	buf.len = strlen(str);
	buf.maxlen = buf.len;
	truncateEol(&buf, eol_type);
}

ProgramPipes*
open_program_pipes(char *command, bool forwrite)
{
	int save_errno;
	pqsigfunc save_SIGPIPE;
	/* set up extvar */
	extvar_t extvar;
	memset(&extvar, 0, sizeof(extvar));

	external_set_env_vars(&extvar, command, false, NULL, NULL, false, 0);

	ProgramPipes *program_pipes = palloc(sizeof(ProgramPipes));
	program_pipes->pid = -1;
	program_pipes->pipes[0] = -1;
	program_pipes->pipes[1] = -1;
	program_pipes->shexec = make_command(command, &extvar);

	/*
	 * Preserve the SIGPIPE handler and set to default handling.  This
	 * allows "normal" SIGPIPE handling in the command pipeline.  Normal
	 * for PG is to *ignore* SIGPIPE.
	 */
	save_SIGPIPE = pqsignal(SIGPIPE, SIG_DFL);

	program_pipes->pid = popen_with_stderr(program_pipes->pipes, program_pipes->shexec, forwrite);

	save_errno = errno;

	/* Restore the SIGPIPE handler */
	pqsignal(SIGPIPE, save_SIGPIPE);

	elog(DEBUG5, "COPY ... PROGRAM command: %s", program_pipes->shexec);
	if (program_pipes->pid == -1)
	{
		errno = save_errno;
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("can not start command: %s", command)));
	}

	return program_pipes;
}

void
close_program_pipes(ProgramPipes *program_pipes, bool ifThrow)
{
	int ret = 0;
	StringInfoData sinfo;
	initStringInfo(&sinfo);

	/* just return if pipes not created, like when relation does not exist */
	if (!program_pipes)
	{
		return;
	}

	ret = pclose_with_stderr(program_pipes->pid, program_pipes->pipes, &sinfo);

	if (ret == 0 || !ifThrow)
	{
		return;
	}

	if (ret == -1)
	{
		/* pclose()/wait4() ended with an error; errno should be valid */
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("can not close pipe: %m")));
	}
	else if (!WIFSIGNALED(ret))
	{
		/*
		 * pclose() returned the process termination state.
		 */
		ereport(ERROR,
				(errcode(ERRCODE_SQL_ROUTINE_EXCEPTION),
				 errmsg("command error message: %s", sinfo.data)));
	}
}

static List *
parse_joined_option_list(char *str, char *delimiter)
{
	char	   *token;
	char	   *comma;
	const char *whitespace = " \t\n\r";
	List	   *cols = NIL;
	int			encoding = GetDatabaseEncoding();

	token = strtokx2(str, whitespace, delimiter, "\"",
					 0, false, false, encoding);

	while (token)
	{
		if (token[0] == ',')
			break;

		cols = lappend(cols, makeString(pstrdup(token)));

		/* consume the comma if any */
		comma = strtokx2(NULL, whitespace, delimiter, "\"",
						 0, false, false, encoding);
		if (!comma || comma[0] != ',')
			break;

		token = strtokx2(NULL, whitespace, delimiter, "\"",
						 0, false, false, encoding);
	}

	return cols;
}

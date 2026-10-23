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
 * lance_fdw.c
 *	  Handler and validator for the lance_fdw foreign-data wrapper.
 *
 * The callbacks here are thin: option handling lives in lance_option.c, the
 * fragment split in lance_dispatch.c and everything between BeginForeignScan
 * and EndForeignScan in lance_scan.c.  What is left is the planner side -
 * a constant row estimate, one path, and the projection the scan will read.
 *
 * IDENTIFICATION
 *	  src/lance_fdw.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

#include "lance_fdw.h"
#include "lance_deparse.h"
#include "lance_dispatch.h"
#include "lance_import.h"
#include "lance_option.h"
#include "lance_runtime.h"
#include "lance_scan.h"

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "funcapi.h"
#include "nodes/makefuncs.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

PG_FUNCTION_INFO_V1(lance_fdw_handler);
PG_FUNCTION_INFO_V1(lance_fdw_validator);
PG_FUNCTION_INFO_V1(lance_fdw_memory);
PG_FUNCTION_INFO_V1(lance_fdw_cache_stats);

static void lanceGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
								   Oid foreigntableid);
static void lanceGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
								 Oid foreigntableid);
static ForeignScan *lanceGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
										Oid foreigntableid,
										ForeignPath *best_path,
										List *tlist,
										List *scan_clauses,
										Plan *outer_plan);

void
_PG_init(void)
{
	lance_rt_define_gucs();
	lance_rt_register_callback();
}

Datum
lance_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	routine->GetForeignRelSize = lanceGetForeignRelSize;
	routine->GetForeignPaths = lanceGetForeignPaths;
	routine->GetForeignPlan = lanceGetForeignPlan;
	routine->BeginForeignScan = lance_scan_begin;
	routine->IterateForeignScan = lance_scan_next;
	routine->ReScanForeignScan = lance_scan_rescan;
	routine->EndForeignScan = lance_scan_end;
	routine->ExplainForeignScan = lance_scan_explain;

	routine->ImportForeignSchema = lanceImportForeignSchema;

	PG_RETURN_POINTER(routine);
}

Datum
lance_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);

	lance_validate_options(options_list, catalog);

	PG_RETURN_VOID();
}

/*
 * Planning does no I/O, so the row count is the rows_hint option or a
 * constant.  Nothing here reaches the object store, which is what keeps
 * EXPLAIN working against a server whose credentials are wrong.
 */
static void
lanceGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
					   Oid foreigntableid)
{
	LanceTableOptions *opts;

	opts = (LanceTableOptions *) palloc0(sizeof(LanceTableOptions));
	lance_get_table_options(foreigntableid, opts);

	baserel->fdw_private = (void *) opts;
	baserel->rows = opts->rows_hint > 0.0 ? opts->rows_hint
		: LANCE_FDW_DEFAULT_ROWS;
}

/*
 * One path.  There is no filter or ordering pushdown in this block, so there
 * is nothing to choose between; the locus that makes the scan run on every
 * segment comes from mpp_execute and is added by create_foreignscan_path().
 */
static void
lanceGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
					 Oid foreigntableid)
{
	Cost		startup_cost = 10.0;
	Cost		total_cost = startup_cost + baserel->rows * 0.01;

	add_path(baserel, (Path *)
			 create_foreignscan_path(root,
									 baserel,
									 NULL,	/* default pathtarget */
									 baserel->rows,
									 startup_cost,
									 total_cost,
									 NIL,	/* no pathkeys */
									 NULL,	/* no outer rel */
									 NULL,	/* no extra plan */
									 NIL),
			 root);
}

/*
 * Which columns does the scan have to read?
 *
 * Everything the query references, whether it is selected or only used in a
 * qual that stayed local.  A qual pushed to Lance is evaluated there and its
 * columns are not read at all.  A whole-row reference or a system
 * column (gp_segment_id, say) needs the whole tuple, so it widens to every
 * user column.  Nothing referenced at all - count(*) - leaves both lists
 * empty, and lance-c still reports the length of every batch, so the rows are
 * counted without reading a column.
 */
static void
lance_plan_projection(RelOptInfo *baserel, Oid foreigntableid,
					  List *local_quals,
					  List **retrieved_attrs, List **columns)
{
	Bitmapset  *attrs_used = NULL;
	Relation	rel;
	TupleDesc	tupdesc;
	bool		all_columns = false;
	ListCell   *lc;
	AttrNumber	attnum;
	int			member = -1;

	*retrieved_attrs = NIL;
	*columns = NIL;

	pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid,
				   &attrs_used);

	/*
	 * Only the quals that stay here.  A qual that went to Lance is evaluated
	 * there, so the columns it reads no longer have to travel - which is where
	 * the whole benefit of this block comes from.
	 */
	foreach(lc, local_quals)
		pull_varattnos((Node *) lfirst(lc), baserel->relid, &attrs_used);

	while ((member = bms_next_member(attrs_used, member)) >= 0)
	{
		if (member + FirstLowInvalidHeapAttributeNumber <= 0)
			all_columns = true;
	}

	rel = table_open(foreigntableid, NoLock);
	tupdesc = RelationGetDescr(rel);

	for (attnum = 1; attnum <= tupdesc->natts; attnum++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, attnum - 1);

		/* A dropped column has no name to ask Lance for. */
		if (att->attisdropped)
			continue;

		if (!all_columns &&
			!bms_is_member(attnum - FirstLowInvalidHeapAttributeNumber,
						   attrs_used))
			continue;

		*retrieved_attrs = lappend_int(*retrieved_attrs, attnum);
		*columns = lappend(*columns,
						   makeString(lance_get_column_name(foreigntableid,
															attnum,
															NameStr(att->attname))));
	}

	table_close(rel, NoLock);
}

static ForeignScan *
lanceGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel,
					Oid foreigntableid, ForeignPath *best_path,
					List *tlist, List *scan_clauses, Plan *outer_plan)
{
	List	   *retrieved_attrs;
	List	   *columns;
	List	   *fdw_private;
	List	   *local_quals = NIL;
	List	   *filter_attrs = NIL;
	List	   *filter_columns = NIL;
	StringInfoData filter;
	ListCell   *lc;

	scan_clauses = extract_actual_clauses(scan_clauses, false);

	/*
	 * Split the quals.  Judging and rendering happen together, per
	 * clause, right here: a clause that judges pushable but cannot be rendered
	 * has to be able to fall back, and by a later stage it would already be
	 * gone from scan_clauses.
	 *
	 * The GUC is read at planning time, so a SET takes effect on the next plan
	 * - and, because the assign hook resets the plan cache, on cached ones
	 * too.
	 */
	initStringInfo(&filter);
	foreach(lc, scan_clauses)
	{
		Expr	   *clause = (Expr *) lfirst(lc);
		LanceDeparsed deparsed;

		if (!lance_enable_filter_pushdown ||
			!lance_deparse_qual(clause, foreigntableid, baserel->relid,
								&deparsed))
		{
			local_quals = lappend(local_quals, clause);
			continue;
		}

		if (filter.len > 0)
			appendStringInfoString(&filter, " AND ");
		appendStringInfoString(&filter, deparsed.sql);
		filter_attrs = list_concat_unique_int(filter_attrs, deparsed.attnums);
	}

	lance_plan_projection(baserel, foreigntableid, local_quals,
						  &retrieved_attrs, &columns);

	/*
	 * The columns the filter reads travel separately from the projection: they
	 * are not read, but the QD still checks them against the dataset schema
	 * with the very same converter resolution the projection gets.
	 */
	if (filter_attrs != NIL)
	{
		Relation	rel = table_open(foreigntableid, NoLock);
		TupleDesc	tupdesc = RelationGetDescr(rel);

		foreach(lc, filter_attrs)
		{
			AttrNumber	attnum = (AttrNumber) lfirst_int(lc);
			Form_pg_attribute att = TupleDescAttr(tupdesc, attnum - 1);

			filter_columns = lappend(filter_columns,
									 makeString(lance_get_column_name(foreigntableid,
																	  attnum,
																	  NameStr(att->attname))));
		}
		table_close(rel, NoLock);
	}

	/*
	 * Slot 0 is the attribute list, slot 1 the Lance column names
	 * that go with it one for one, slot 2 the pushed-down filter (empty string
	 * when there is none), slots 3 and 4 the filter's columns.  The QD appends
	 * the scan units last, and a QE tells whether it heard from the QD by the
	 * length of this list.
	 */
	fdw_private = list_make5(retrieved_attrs, columns,
							 makeString(filter.data),
							 filter_attrs, filter_columns);

	return make_foreignscan(tlist,
							local_quals,
							baserel->relid,
							NIL,	/* no expressions to evaluate */
							fdw_private,
							NIL,	/* no custom tlist */
							NIL,	/* no recheck quals */
							outer_plan);
}

/*
 * Arrow bytes this backend currently has on Cloudberry's memory ledger.
 *
 * Every reserve owes exactly one release, and a miscounted pair is invisible
 * until the ledger drifts far enough to refuse an unrelated allocation.  So
 * current_bytes is worth exposing on its own: between scans it has to be zero,
 * and that is a property a test can assert.  peak_bytes is the high-water mark
 * since this backend started, which is the figure to size the bounds against -
 * the current value is nearly always zero, because a batch is held only
 * between one IterateForeignScan and the next.
 */
Datum
lance_fdw_memory(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[2];
	bool		nulls[2] = {false, false};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "lance_fdw_memory: return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	values[0] = Int64GetDatum(lance_rt_vmem_reserved());
	values[1] = Int64GetDatum(lance_rt_vmem_peak());

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * Hit and miss counters for the two per-backend lance caches.  Sizing
 * lance_fdw.index_cache_size_mb and lance_fdw.metadata_cache_size_mb is
 * guesswork without them.
 */
Datum
lance_fdw_cache_stats(PG_FUNCTION_ARGS)
{
	LanceSessionCacheStats stats;
	TupleDesc	tupdesc;
	Datum		values[8];
	bool		nulls[8] = {false};
	LanceSession *session = lance_rt_session();
	int32		rc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "lance_fdw_cache_stats: return type must be a row type");
	tupdesc = BlessTupleDesc(tupdesc);

	if (session == NULL)
		PG_RETURN_NULL();		/* this backend has not opened a dataset yet */

	memset(&stats, 0, sizeof(stats));
	LANCE_MASKED(rc = lance_session_get_cache_stats(session, &stats));
	LANCE_CHECK(rc == 0, NULL);

	values[0] = Int64GetDatum((int64) stats.index_cache_hits);
	values[1] = Int64GetDatum((int64) stats.index_cache_misses);
	values[2] = Int64GetDatum((int64) stats.index_cache_entries);
	values[3] = Int64GetDatum((int64) stats.index_cache_size_bytes);
	values[4] = Int64GetDatum((int64) stats.metadata_cache_hits);
	values[5] = Int64GetDatum((int64) stats.metadata_cache_misses);
	values[6] = Int64GetDatum((int64) stats.metadata_cache_entries);
	values[7] = Int64GetDatum((int64) stats.metadata_cache_size_bytes);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

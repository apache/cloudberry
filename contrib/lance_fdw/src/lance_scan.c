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
 * lance_scan.c
 *	  Opening a Lance dataset, reading its fragments and turning Arrow batches
 *	  into tuples.
 *
 * There is one scan path, not three.  Whether this process is the QD listing
 * fragments for everyone else, a QE reading the share it computed, or a single
 * backend running a 'coordinator' table on its own, the difference is only
 * which fragment ids end up in the state; from there the code is the same.
 * That is deliberate: the debugging modes have to be as trustworthy as the
 * parallel one.
 *
 * Lifetime is the other thing this file is about.  A dataset, a scanner and an
 * Arrow stream all live outside palloc, so they are registered with the runtime
 * handle registry and closed by it on the error path; the Arrow batch and
 * the exported schema are released by a reset callback on this scan's own
 * memory context, which fires on the same paths.
 *
 * IDENTIFICATION
 *	  src/lance_scan.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "lance_fdw.h"
#include "lance_arrow.h"
#include "lance_dispatch.h"
#include "lance_option.h"
#include "lance_runtime.h"
#include "lance_scan.h"

#include "access/table.h"
#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "utils/memutils.h"
#include "utils/rel.h"

typedef struct LanceScanState
{
	/* Projection, straight out of the plan */
	int			ncolumns;
	AttrNumber *attnums;		/* ncolumns entries, 1-based */
	const char **columns;		/* ncolumns + 1 entries, NULL terminated */

	/* Who we are and what we are reading */
	LanceTableOptions opts;
	Oid			userid;
	char	   *uri;			/* resolved; on a QE the QD's spelling */
	bool		version_pinned; /* the table names a version */
	uint64		version;		/* the version actually opened */
	uint64	   *ids;
	int			nids;
	int			total_fragments;	/* -1 unless this process listed them */

	/* lance-c handles, all registered with the runtime */
	LanceHandle *ds_handle;
	LanceDataset *dataset;
	LanceHandle *sc_handle;
	LanceScanner *scanner;
	LanceHandle *stream_handle;
	struct ArrowArrayStream *stream;
	bool		stream_failed;	/* the stream reported an error */
	bool		stream_done;

	/* Batch cursor */
	struct ArrowSchema schema;
	struct ArrowArray batch;
	int64		batch_len;
	int64		batch_row;
	int64		batch_offset;
	int64		vmem_reserved;	/* Arrow bytes on the vmem ledger for `batch` */

	LanceConverter *converters; /* ncolumns entries */

	MemoryContext scan_cxt;		/* everything above lives here */
	MemoryContext batch_cxt;	/* values of the current batch, reset per batch */
} LanceScanState;

static void lance_scan_stream_error(LanceScanState *state) pg_attribute_noreturn();

/*
 * Release what the Arrow C data interface handed us.  Registered as a reset
 * callback on scan_cxt so that it also runs when an ERROR tears the executor
 * down without EndForeignScan.
 */
static void
lance_scan_cleanup(void *arg)
{
	LanceScanState *state = (LanceScanState *) arg;
	int			i;

	if (state->batch.release != NULL)
	{
		LANCE_MASKED(state->batch.release(&state->batch));
		state->batch.release = NULL;
	}

	lance_rt_vmem_release(state->vmem_reserved);
	state->vmem_reserved = 0;

	if (state->schema.release != NULL)
	{
		LANCE_MASKED(state->schema.release(&state->schema));
		state->schema.release = NULL;
	}

	for (i = 0; state->converters != NULL && i < state->ncolumns; i++)
		lance_arrow_converter_reset(&state->converters[i]);
	state->converters = NULL;
}

/*
 * Read the projection GetForeignPlan wrote into the plan.
 */
static void
lance_scan_read_projection(LanceScanState *state, ForeignScan *fsplan)
{
	List	   *attrs;
	List	   *names;
	ListCell   *lc;
	int			i;

	if (list_length(fsplan->fdw_private) < LANCE_FDW_PRIVATE_UNITS)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("lance_fdw: the plan carries no projection"),
				 errdetail("fdw_private has %d entries, expected at least %d.",
						   list_length(fsplan->fdw_private),
						   LANCE_FDW_PRIVATE_UNITS)));

	attrs = (List *) list_nth(fsplan->fdw_private, LANCE_FDW_PRIVATE_ATTRS);
	names = (List *) list_nth(fsplan->fdw_private, LANCE_FDW_PRIVATE_COLUMNS);

	if (list_length(attrs) != list_length(names))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("lance_fdw: the plan carries %d columns for %d attributes",
						list_length(names), list_length(attrs))));

	state->ncolumns = list_length(attrs);
	state->attnums = (AttrNumber *) palloc(sizeof(AttrNumber) *
										   Max(state->ncolumns, 1));
	state->columns = (const char **) palloc(sizeof(char *) *
											(state->ncolumns + 1));

	i = 0;
	foreach(lc, attrs)
		state->attnums[i++] = (AttrNumber) lfirst_int(lc);

	i = 0;
	foreach(lc, names)
		state->columns[i++] = strVal(lfirst(lc));

	/*
	 * A count(*) projects nothing at all.  lance-c accepts the empty list and
	 * still reports the length of every batch, so the rows are counted without
	 * reading a single column.
	 */
	state->columns[state->ncolumns] = NULL;
}

/*
 * Open the dataset at `version` (0 asks for the latest) and record which
 * version that turned out to be.
 */
static void
lance_scan_open_dataset(LanceScanState *state, uint64 version)
{
	const char **storage_opts;
	LanceSession *session;

	/*
	 * Credentials are read here, on whichever process is about to do the I/O,
	 * and never travel inside the plan.
	 */
	storage_opts = lance_build_storage_options(state->opts.serverid,
											   state->userid);

	/* Brings the runtime up, and may ereport: outside the masked call. */
	session = lance_rt_session();

	LANCE_MASKED(state->dataset = lance_dataset_open_with_session(state->uri,
																  (const char *const *) storage_opts,
																  version,
																  session));
	LANCE_CHECK(state->dataset != NULL, state->uri);
	state->ds_handle = lance_rt_track_dataset(state->dataset);

	/*
	 * Interrupts are checked here rather than inside the masked region: the
	 * open may have taken a while, and the handle is registered by now, so an
	 * ERROR from this point on still closes the dataset.
	 */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Version numbering starts at 1, so a zero is lance-c's error sentinel and
	 * not a version: publishing it would send every QE off to read "latest"
	 * instead of the snapshot this statement pinned.
	 */
	LANCE_MASKED(state->version = lance_dataset_version(state->dataset));
	LANCE_CHECK(state->version > 0, state->uri);
}

static void
lance_scan_close_dataset(LanceScanState *state)
{
	lance_rt_release(state->ds_handle);
	state->ds_handle = NULL;
	state->dataset = NULL;
}

/*
 * List every fragment of the open dataset.  Fragment ids are not dense - a
 * fragment whose rows were all deleted simply disappears - so the ids are read
 * rather than counted.
 */
static void
lance_scan_list_fragments(LanceScanState *state)
{
	uint64		count;

	LANCE_MASKED(count = lance_dataset_fragment_count(state->dataset));

	/*
	 * The count is a uint64 that becomes an int, an allocation and a
	 * space-separated list inside the plan.  A dataset far beyond what any of
	 * those can hold has to say so rather than wrap around into a share that
	 * misses fragments.
	 */
	if (count > (uint64) Min(PG_INT32_MAX, MaxAllocSize / sizeof(uint64)))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("lance_fdw: dataset has " UINT64_FORMAT " fragments, more than this wrapper can plan",
						count),
				 errdetail("While reading uri %s.", state->uri)));

	state->total_fragments = (int) count;
	state->nids = (int) count;
	state->ids = count > 0 ? (uint64 *) palloc(sizeof(uint64) * count) : NULL;

	if (count > 0)
	{
		int32		rc;

		LANCE_MASKED(rc = lance_dataset_fragment_ids(state->dataset, state->ids));
		LANCE_CHECK(rc == 0, state->uri);
	}
}

static void
lance_scan_make_scanner(LanceScanState *state)
{
	int32		rc;

	LANCE_MASKED(state->scanner = lance_scanner_new(state->dataset,
													(const char *const *) state->columns,
													NULL));
	LANCE_CHECK(state->scanner != NULL, state->uri);
	state->sc_handle = lance_rt_track_scanner(state->scanner);

	/* Every one of these has to be set before a stream is taken. */
	LANCE_MASKED(rc = lance_scanner_set_fragment_ids(state->scanner, state->ids,
													 (size_t) state->nids));
	LANCE_CHECK(rc == 0, state->uri);

	if (state->opts.batch_size > 0)
	{
		LANCE_MASKED(rc = lance_scanner_set_batch_size(state->scanner,
													   state->opts.batch_size));
		LANCE_CHECK(rc == 0, state->uri);
	}

	/*
	 * A row count is a poor memory bound when the rows are wide, and Lance
	 * rows can be very wide: a thousand rows of int4 is kilobytes, a thousand
	 * 1024-dimension vectors is megabytes.  The byte limit is the one that
	 * bounds memory; the options validator refuses both at once because lance
	 * would let this one win silently.
	 */
	if (state->opts.batch_size_bytes > 0)
	{
		LANCE_MASKED(rc = lance_scanner_set_batch_size_bytes(state->scanner,
															 (uint64) state->opts.batch_size_bytes));
		LANCE_CHECK(rc == 0, state->uri);
	}

	if (lance_io_buffer_size_mb > 0)
	{
		LANCE_MASKED(rc = lance_scanner_set_io_buffer_size(state->scanner,
														   (uint64) lance_io_buffer_size_mb * 1024 * 1024));
		LANCE_CHECK(rc == 0, state->uri);
	}

	if (lance_batch_readahead > 0)
	{
		LANCE_MASKED(rc = lance_scanner_set_batch_readahead(state->scanner,
															(size_t) lance_batch_readahead));
		LANCE_CHECK(rc == 0, state->uri);
	}
}

static void
lance_scan_open_stream(LanceScanState *state)
{
	int32		rc;

	state->stream = lance_rt_track_new_stream(&state->stream_handle);
	LANCE_MASKED(rc = lance_scanner_to_arrow_stream(state->scanner,
													state->stream));
	LANCE_CHECK(rc == 0, state->uri);
	state->stream_done = false;
}

/*
 * Resolve one converter per projected column, against the schema the stream
 * reports rather than against the dataset schema: what the stream returns is
 * what the batches will contain, in the order the projection asked for.
 */
static void
lance_scan_build_converters(LanceScanState *state, Relation rel)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			i;
	int			rc;

	memset(&state->schema, 0, sizeof(state->schema));
	LANCE_MASKED(rc = state->stream->get_schema(state->stream, &state->schema));
	if (rc != 0)
		lance_scan_stream_error(state);

	if (state->schema.n_children != (int64) state->ncolumns)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_COLUMN_NUMBER),
				 errmsg("lance_fdw: Lance returned " INT64_FORMAT " columns, expected %d",
						state->schema.n_children, state->ncolumns)));

	state->converters = (LanceConverter *)
		palloc0(sizeof(LanceConverter) * Max(state->ncolumns, 1));

	for (i = 0; i < state->ncolumns; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, state->attnums[i] - 1);

		lance_arrow_resolve_converter(state->schema.children[i],
									  att->atttypid, att->atttypmod,
									  &state->converters[i]);
	}
}

/*
 * Check the projection against the dataset's own schema.
 *
 * The schema the stream reports is only checked where there is something to
 * read (lance_scan_build_converters), so a dataset with no fragments would
 * otherwise accept a foreign table that names columns Lance does not have, or
 * types it cannot convert: nothing opens a stream, and the scan quietly
 * returns no rows.  The same holds for the QD under all segments, which never
 * reads and whose QEs may all have empty shares.
 *
 * The dataset schema comes out of the manifest that opening it already read,
 * so this is no further I/O.  The converters are resolved into
 * state->converters and reset again: they are the same objects the scan would
 * build, which is what makes this the same check rather than a second opinion,
 * and putting them there means the reset callback frees their nanoarrow views
 * even when resolving one of them raises the error this exists to raise.
 */
static void
lance_scan_validate_projection(LanceScanState *state, Relation rel)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	struct ArrowSchema schema;
	int32		rc;
	int			i;

	if (state->ncolumns == 0)
		return;					/* count(*) asks Lance for no column at all */

	memset(&schema, 0, sizeof(schema));
	LANCE_MASKED(rc = lance_dataset_schema(state->dataset, &schema));
	LANCE_CHECK(rc == 0, state->uri);

	PG_TRY();
	{
		state->converters = (LanceConverter *)
			palloc0(sizeof(LanceConverter) * state->ncolumns);

		for (i = 0; i < state->ncolumns; i++)
		{
			Form_pg_attribute att = TupleDescAttr(tupdesc, state->attnums[i] - 1);
			struct ArrowSchema *field = NULL;
			int64		c;

			for (c = 0; c < schema.n_children; c++)
			{
				struct ArrowSchema *child = schema.children[c];

				if (child != NULL && child->name != NULL &&
					strcmp(child->name, state->columns[i]) == 0)
				{
					field = child;
					break;
				}
			}

			if (field == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_UNDEFINED_COLUMN),
						 errmsg("lance_fdw: column \"%s\": the dataset has no such column",
								state->columns[i]),
						 errdetail("While reading uri %s.", state->uri),
						 errhint("Use IMPORT FOREIGN SCHEMA, or the column_name "
								 "option where the two names differ.")));

			lance_arrow_resolve_converter(field, att->atttypid, att->atttypmod,
										  &state->converters[i]);
		}

		for (i = 0; i < state->ncolumns; i++)
			lance_arrow_converter_reset(&state->converters[i]);
		state->converters = NULL;
	}
	PG_FINALLY();
	{
		if (schema.release != NULL)
			LANCE_MASKED(schema.release(&schema));
	}
	PG_END_TRY();
}

static void
lance_scan_log_share(const LanceScanState *state, const char *what)
{
	/*
	 * The parallel and snapshot suites read this: it is the only place that
	 * says which fragments a given process took and which version it opened.
	 */
	elog(DEBUG1,
		 "lance_fdw: %s %d fragment(s) [%s] of \"%s\" at version " UINT64_FORMAT,
		 what, state->nids, lance_dispatch_ids_string(state->ids, state->nids),
		 state->uri, state->version);
}

static void
lance_scan_begin_internal(ForeignScanState *node, LanceScanState *state,
						  int eflags)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	Relation	rel = node->ss.ss_currentRelation;
	Oid			relid = RelationGetRelid(rel);
	ForeignTable *table = GetForeignTable(relid);
	bool		all_segments;
	LanceScanUnits units;

	lance_scan_read_projection(state, fsplan);
	lance_get_table_options(relid, &state->opts);
	state->uri = state->opts.uri;
	state->version_pinned = state->opts.version > 0;
	state->version = state->opts.version;
	state->total_fragments = -1;

	/*
	 * postgres_fdw's rule: the plan says whose credentials to use, and only a
	 * plan built without a check-as user falls back to the current one.
	 */
	state->userid = OidIsValid(fsplan->checkAsUser) ? fsplan->checkAsUser
		: GetUserId();

	/*
	 * EXPLAIN without ANALYZE stops here.  It still has everything it prints -
	 * the uri and the requested version - and it has touched no object store,
	 * which is what lets EXPLAIN work against a server whose credentials are
	 * wrong.
	 */
	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	all_segments = (table->exec_location == FTEXECLOCATION_ALL_SEGMENTS);

	if (all_segments && Gp_role == GP_ROLE_DISPATCH)
	{
		/*
		 * The QD pins the version, lists the fragments, puts both in the plan
		 * and reads nothing.  Every QE of this statement then opens that
		 * exact version, so they all see one snapshot.
		 */
		int			nsegments = table->num_segments > 0 ? table->num_segments
			: getgpsegmentCount();

		/*
		 * A width wider than the cluster cannot be divided.  Cloudberry builds
		 * such a gang by repeating contents - `i % getgpsegmentCount()` in
		 * execUtils.c - so two QEs would carry the same GpIdentity.segindex,
		 * take the same share and read those fragments twice, while the
		 * planned indexes at or above the cluster size would have no process
		 * at all.  Measured on the three-segment cluster with num_segments
		 * '4': both QEs on content 0 logged the same fragment, and count(*)
		 * over a 15-row dataset came back as 20, 15 or 10 depending on the
		 * rotation.  Nothing lets a QE tell itself apart from its twin, so
		 * this is refused rather than guessed at.
		 */
		if (nsegments > getgpsegmentCount())
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("lance_fdw: foreign table \"%s\" is planned for %d segments, but the cluster has %d",
							RelationGetRelationName(rel), nsegments,
							getgpsegmentCount()),
					 errdetail("Fragments are divided among the segments that run the scan, and a width wider than the cluster repeats a segment."),
					 errhint("Set num_segments to at most %d, or leave it unset.",
							 getgpsegmentCount())));

		lance_scan_open_dataset(state, state->opts.version);
		lance_scan_validate_projection(state, rel);
		lance_scan_list_fragments(state);
		lance_dispatch_publish(fsplan, state->uri, state->version, nsegments,
							   state->ids, state->nids);
		lance_scan_log_share(state, "dispatching");
		lance_scan_close_dataset(state);
		state->nids = 0;
		return;
	}

	if (all_segments && lance_dispatch_read_units(fsplan, &units))
	{
		/* A QE: take this segment's share of what the QD published. */
		state->uri = units.uri;
		state->version = units.version;
		state->ids = units.ids;
		state->nids = lance_dispatch_take_share(units.ids, units.nids,
												units.nsegments);
		lance_scan_log_share(state, "reading");

		/* An empty share is not an error, and it is not I/O either. */
		if (state->nids == 0)
			return;

		lance_scan_open_dataset(state, units.version);
	}
	else
	{
		/*
		 * A QE of a dispatched all-segments statement must have found units:
		 * the QD writes them into the plan before dispatch.  Reading
		 * everything here instead would return this dataset once per segment
		 * and, opening its own version, could tear the statement's snapshot -
		 * so an unpublished plan is an error, not a fallback.
		 */
		if (all_segments && Gp_role == GP_ROLE_EXECUTE)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("lance_fdw: the dispatched plan carries no scan units"),
					 errdetail("Foreign table \"%s\" runs on all segments, so the "
							   "coordinator has to publish the fragment list.",
							   RelationGetRelationName(rel))));

		/*
		 * mpp_execute 'coordinator' or 'any', or a utility-mode backend with
		 * nobody to dispatch to: this process reads every fragment itself.
		 */
		lance_scan_open_dataset(state, state->opts.version);
		lance_scan_list_fragments(state);
		lance_scan_log_share(state, "reading all");

		if (state->nids == 0)
		{
			/*
			 * Nothing to read still has a projection to check, and this
			 * process is the only one that will ever see the schema.
			 */
			lance_scan_validate_projection(state, rel);
			lance_scan_close_dataset(state);
			return;
		}
	}

	lance_scan_make_scanner(state);
	lance_scan_open_stream(state);
	lance_scan_build_converters(state, rel);
}

void
lance_scan_begin(ForeignScanState *node, int eflags)
{
	MemoryContext scan_cxt;
	MemoryContext oldcxt;
	MemoryContextCallback *cleanup;
	LanceScanState *state;

	scan_cxt = AllocSetContextCreate(CurrentMemoryContext,
									 "lance_fdw scan",
									 ALLOCSET_DEFAULT_SIZES);
	oldcxt = MemoryContextSwitchTo(scan_cxt);

	state = (LanceScanState *) palloc0(sizeof(LanceScanState));
	state->scan_cxt = scan_cxt;
	state->batch_cxt = AllocSetContextCreate(scan_cxt,
											 "lance_fdw batch",
											 ALLOCSET_DEFAULT_SIZES);
	node->fdw_state = (void *) state;

	cleanup = (MemoryContextCallback *) palloc0(sizeof(MemoryContextCallback));
	cleanup->func = lance_scan_cleanup;
	cleanup->arg = (void *) state;
	MemoryContextRegisterResetCallback(scan_cxt, cleanup);

	lance_scan_begin_internal(node, state, eflags);

	MemoryContextSwitchTo(oldcxt);
}

static void
lance_scan_stream_error(LanceScanState *state)
{
	const char *raw = NULL;
	char	   *message;

	if (state->stream != NULL && state->stream->get_last_error != NULL)
		LANCE_MASKED(raw = state->stream->get_last_error(state->stream));

	message = pstrdup(raw != NULL ? raw : "unknown error");

	ereport(ERROR,
			(errcode(ERRCODE_IO_ERROR),
			 errmsg("lance: %s", message),
			 errdetail("While reading uri %s.", state->uri)));
}

static void
lance_scan_release_batch(LanceScanState *state)
{
	if (state->batch.release != NULL)
	{
		LANCE_MASKED(state->batch.release(&state->batch));
		state->batch.release = NULL;
	}

	lance_rt_vmem_release(state->vmem_reserved);
	state->vmem_reserved = 0;

	state->batch_len = 0;
	state->batch_row = 0;
	state->batch_offset = 0;

	/* The values converted out of that batch die with it. */
	MemoryContextReset(state->batch_cxt);
}

/*
 * Pull the next batch.  Returns false at end of stream; a zero-length batch is
 * possible and is not the end, so the caller loops.
 */
static bool
lance_scan_next_batch(LanceScanState *state)
{
	int			rc;
	int			i;

	/* Cancellation and statement_timeout land on a batch boundary. */
	CHECK_FOR_INTERRUPTS();

	lance_scan_release_batch(state);

	if (state->stream_done)
		return false;

	LANCE_MASKED(rc = state->stream->get_next(state->stream, &state->batch));

	/*
	 * Signals were blocked for the whole read, so anything that arrived during
	 * it was delivered on the restore just now and is acted on here rather
	 * than one batch later.  The batch, if there is one, is freed by the
	 * scan context's reset callback on the way out.
	 */
	CHECK_FOR_INTERRUPTS();

	if (rc != 0)
	{
		/*
		 * lance-c poisons the scanner behind a stream that failed, so remember
		 * it: a ReScan after this has to build a new one.
		 */
		state->stream_failed = true;
		lance_scan_stream_error(state);
	}

	if (state->batch.release == NULL)
	{
		state->stream_done = true;
		return false;
	}

	if (state->batch.n_children != (int64) state->ncolumns)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_COLUMN_NUMBER),
				 errmsg("lance_fdw: Lance returned a batch of " INT64_FORMAT " columns, expected %d",
						state->batch.n_children, state->ncolumns)));

	for (i = 0; i < state->ncolumns; i++)
		lance_arrow_converter_set_array(&state->converters[i],
										state->batch.children[i]);

	/*
	 * Only now can the batch be measured, and only now can the database be
	 * told it exists: lance allocated it outside palloc, so until this point
	 * the resource group and gp_vmem_protect_limit see a backend holding
	 * nothing.  If the reservation fails nothing was charged, and the batch
	 * itself is freed by the scan context's reset callback on the way out.
	 */
	{
		int64		bytes = 0;

		for (i = 0; i < state->ncolumns; i++)
			bytes += lance_arrow_converter_bytes(&state->converters[i]);

		state->vmem_reserved = lance_rt_vmem_reserve(bytes, state->uri);
	}

	state->batch_len = state->batch.length;
	state->batch_row = 0;

	/*
	 * A record batch that is itself a slice pushes its offset onto its
	 * children, and nanoarrow does not propagate it, so it is added here on
	 * every access.  Deletion files make this the normal case, not a corner.
	 */
	state->batch_offset = state->batch.offset;

	return true;
}

static void
lance_scan_fill_slot(LanceScanState *state, TupleTableSlot *slot)
{
	MemoryContext oldcxt = MemoryContextSwitchTo(state->batch_cxt);
	int64		row = state->batch_row + state->batch_offset;
	int			natts = slot->tts_tupleDescriptor->natts;
	int			i;

	/* Anything the projection left out is NULL; nothing else may read it. */
	memset(slot->tts_isnull, true, sizeof(bool) * natts);

	for (i = 0; i < state->ncolumns; i++)
	{
		LanceConverter *conv = &state->converters[i];
		int			idx = state->attnums[i] - 1;

		if (lance_arrow_converter_is_null(conv, row))
			continue;

		slot->tts_values[idx] = conv->convert(conv, row);
		slot->tts_isnull[idx] = false;
	}

	MemoryContextSwitchTo(oldcxt);
}

TupleTableSlot *
lance_scan_next(ForeignScanState *node)
{
	LanceScanState *state = (LanceScanState *) node->fdw_state;
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;

	/*
	 * Clearing first is what makes it safe for the next batch to free the
	 * memory the previous tuple pointed into.
	 */
	ExecClearTuple(slot);

	/* The QD, and a QE with an empty share, have nothing open and no rows. */
	if (state == NULL || state->stream == NULL)
		return slot;

	while (state->batch_row >= state->batch_len)
	{
		if (!lance_scan_next_batch(state))
			return slot;
	}

	lance_scan_fill_slot(state, slot);
	state->batch_row++;

	return ExecStoreVirtualTuple(slot);
}

void
lance_scan_rescan(ForeignScanState *node)
{
	LanceScanState *state = (LanceScanState *) node->fdw_state;

	if (state == NULL || state->scanner == NULL)
		return;					/* nothing was opened, nothing to rewind */

	lance_scan_release_batch(state);
	lance_rt_release(state->stream_handle);
	state->stream_handle = NULL;
	state->stream = NULL;
	state->stream_done = false;

	if (state->stream_failed)
	{
		/*
		 * The scanner behind a failed stream is poisoned, so rewinding means
		 * building a new one from the same dataset.
		 */
		lance_rt_release(state->sc_handle);
		state->sc_handle = NULL;
		state->scanner = NULL;
		state->stream_failed = false;
		lance_scan_make_scanner(state);
	}

	lance_scan_open_stream(state);
}

void
lance_scan_end(ForeignScanState *node)
{
	LanceScanState *state = (LanceScanState *) node->fdw_state;

	if (state == NULL)
		return;

	/*
	 * Release inside out - batch, stream, scanner, dataset - and let deleting
	 * the context run the cleanup callback over whatever is left.
	 */
	lance_scan_release_batch(state);

	lance_rt_release(state->stream_handle);
	state->stream_handle = NULL;
	state->stream = NULL;

	lance_rt_release(state->sc_handle);
	state->sc_handle = NULL;
	state->scanner = NULL;

	lance_scan_close_dataset(state);

	node->fdw_state = NULL;
	MemoryContextDelete(state->scan_cxt);
}

void
lance_scan_explain(ForeignScanState *node, ExplainState *es)
{
	LanceScanState *state = (LanceScanState *) node->fdw_state;

	if (state == NULL)
		return;

	ExplainPropertyText("Lance URI", state->uri, es);

	/*
	 * The version as the table asks for it, not as it resolved: "latest" is
	 * the honest answer for a table that pins nothing, and a plain EXPLAIN has
	 * not looked.
	 */
	if (state->version_pinned)
		ExplainPropertyText("Lance Version",
							psprintf(UINT64_FORMAT, state->opts.version), es);
	else
		ExplainPropertyText("Lance Version", "latest", es);

	/* Only known when this process actually opened the dataset. */
	if (state->total_fragments >= 0)
		ExplainPropertyInteger("Lance Fragments", NULL,
							   state->total_fragments, es);
}

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

#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <unistd.h>

#include "lance_fdw.h"
#include "lance_arrow.h"
#include "lance_dispatch.h"
#include "lance_option.h"
#include "lance_runtime.h"
#include "lance_scan.h"
#include "lance_vector.h"

#include "access/table.h"
#include "catalog/pg_type_d.h"
#include "cdb/cdbvars.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/array.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/wait_event.h"

typedef struct LanceScanState
{
	/* Projection, straight out of the plan */
	int			ncolumns;
	char	   *filter;			/* pushed-down Lance filter, NULL if none */
	int			nfilter_cols;	/* columns the filter reads, not projected */
	AttrNumber *filter_attnums;
	const char **filter_columns;
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

	/*
	 * Vector Top-K.  is_topk says the planner
	 * chose it; searching says this process runs the nearest-neighbour
	 * search, which is true on the one QE the QD picked, or on the QD itself
	 * under 'coordinator'.  A QD that found the query vector unusable falls
	 * back to the plain fragment scan and says why in fallback.
	 */
	bool		is_topk;
	char	   *topk_column;
	LanceVectorMetric topk_metric;
	int64		topk_k;
	bool		searching;
	float4	   *vector;
	int			dim;
	int			nprobes;
	int			refine_factor;
	int			search_segment; /* -1: this process, under 'coordinator' */
	const char *fallback;		/* why the QD did not search, or NULL */

	/*
	 * Written by the statistics callback, which may run on a lance thread and
	 * so only copies three numbers; reported once the stream is done.
	 */
	struct
	{
		bool		seen;
		uint64		indices_loaded;
		uint64		index_partitions_loaded;
		uint64		index_comparisons;
	}			stats;

	/*
	 * A search is read with lance_scanner_poll_next rather than through an
	 * Arrow stream, so that a cancel does not wait for the whole search
	 * Its schema only arrives with the first batch, which
	 * is when the converters are built, against this descriptor.
	 */
	TupleDesc	tupdesc;

	MemoryContext scan_cxt;		/* everything above lives here */
	MemoryContext batch_cxt;	/* values of the current batch, reset per batch */
} LanceScanState;

static void lance_scan_stream_error(LanceScanState *state) pg_attribute_noreturn();

/* The projection, plus the _distance a nearest-neighbour search appends. */
static inline int
lance_scan_stream_columns(const LanceScanState *state)
{
	return state->ncolumns + (state->searching ? 1 : 0);
}

/*
 * The pipe a search's waker writes to.  lance-c calls the waker on one of its
 * own threads when lance_scanner_poll_next has more to give, and the backend
 * sleeps on the read end next to its latch, so it wakes for either.
 *
 * One pipe per backend, made on the first search and never closed.  A waker
 * may run until lance_scanner_close() returns, and that close can come from
 * the resource owner callback on an error path, in any order relative to
 * other cleanup: a per-scan pipe closed first would leave a waker writing to a
 * descriptor number the backend may already have reused.  Both ends are
 * non-blocking, so the waker never blocks and a full pipe only means a wake is
 * already pending.
 */
static int	lance_waker_fds[2] = {-1, -1};

static void
lance_scan_waker(void *ctx)
{
	char		c = 0;

	/* On a lance thread: no palloc, no elog, nothing but the write. */
	(void) ctx;
	(void) !write(lance_waker_fds[1], &c, 1);
}

static void
lance_scan_open_waker(void)
{
	int			fds[2];
	int			i;

	if (lance_waker_fds[0] >= 0)
		return;

	if (pipe(fds) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("lance_fdw: could not create the vector search wakeup pipe: %m")));

	for (i = 0; i < 2; i++)
	{
		if (fcntl(fds[i], F_SETFL, O_NONBLOCK) != 0 ||
			fcntl(fds[i], F_SETFD, FD_CLOEXEC) != 0)
		{
			int			save_errno = errno;

			close(fds[0]);
			close(fds[1]);
			errno = save_errno;
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("lance_fdw: could not set up the vector search wakeup pipe: %m")));
		}
	}

	lance_waker_fds[0] = fds[0];
	lance_waker_fds[1] = fds[1];
}

/* Throw away wakes that are already accounted for, before the next poll. */
static void
lance_scan_drain_waker(void)
{
	char		buf[64];

	while (read(lance_waker_fds[0], buf, sizeof(buf)) > 0)
		;
}

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

	/*
	 * The pushed-down filter and the columns it reads.  An empty
	 * string means the planner pushed nothing down, which is the normal case
	 * for a query with no WHERE clause and for every clause off the whitelist.
	 */
	{
		char	   *f = strVal(list_nth(fsplan->fdw_private,
										LANCE_FDW_PRIVATE_FILTER));
		List	   *fattrs = (List *) list_nth(fsplan->fdw_private,
											   LANCE_FDW_PRIVATE_FILTER_ATTRS);
		List	   *fnames = (List *) list_nth(fsplan->fdw_private,
											   LANCE_FDW_PRIVATE_FILTER_COLUMNS);

		state->filter = (f != NULL && f[0] != '\0') ? f : NULL;

		if (list_length(fattrs) != list_length(fnames))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("lance_fdw: the plan carries %d filter columns for %d attributes",
							list_length(fnames), list_length(fattrs))));

		state->nfilter_cols = list_length(fattrs);
		if (state->nfilter_cols > 0)
		{
			int			k = 0;

			state->filter_attnums = (AttrNumber *)
				palloc(sizeof(AttrNumber) * state->nfilter_cols);
			state->filter_columns = (const char **)
				palloc(sizeof(char *) * state->nfilter_cols);
			foreach(lc, fattrs)
				state->filter_attnums[k++] = (AttrNumber) lfirst_int(lc);
			k = 0;
			foreach(lc, fnames)
				state->filter_columns[k++] = strVal(lfirst(lc));
		}
	}

	/* Vector Top-K: NIL unless the planner chose a Top-K. */
	{
		List	   *topk = (List *) list_nth(fsplan->fdw_private,
											 LANCE_FDW_PRIVATE_TOPK);
		Node	   *metric;
		char	   *endptr;

		if (topk == NIL)
			return;

		if (list_length(topk) != LANCE_TOPK_NFIELDS ||
			list_length(fsplan->fdw_exprs) != 1)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("lance_fdw: the plan carries a malformed vector Top-K"),
					 errdetail("%d fields and %d expressions.",
							   list_length(topk),
							   list_length(fsplan->fdw_exprs))));

		metric = (Node *) list_nth(topk, LANCE_TOPK_METRIC);
		if (!IsA(metric, Integer) || intVal(metric) < 0 ||
			intVal(metric) >= LANCE_VECTOR_NMETRICS)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("lance_fdw: the plan carries an unknown vector metric")));

		state->is_topk = true;
		state->topk_column = strVal(list_nth(topk, LANCE_TOPK_COLUMN));
		state->topk_metric = (LanceVectorMetric) intVal(metric);
		state->topk_k = strtoi64(strVal(list_nth(topk, LANCE_TOPK_K)), &endptr, 10);
		if (*endptr != '\0' || state->topk_k < 1 || state->topk_k > PG_INT32_MAX)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("lance_fdw: the plan carries an invalid vector Top-K limit \"%s\"",
							strVal(list_nth(topk, LANCE_TOPK_K)))));
		state->search_segment = -1;
	}
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

	/*
	 * Every open, on every process, says so - which is what lets a test show
	 * that a segment that was not picked for a vector search never touched the
	 * object store.  DEBUG2, so that no existing output changes.
	 */
	elog(DEBUG2, "lance_fdw: open dataset \"%s\" on segment %d",
		 state->uri, GpIdentity.segindex);

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

/*
 * Lance hands over a scan's statistics once the stream has reached its end,
 * possibly on one of its own threads and before get_next() has returned
 * (lance.h, LanceScanStatisticsCallback).  So this copies three numbers into
 * memory the scan already owns and does nothing else: no palloc, no elog, no
 * lance-c call.  They are reported from the backend's own thread once
 * get_next() is back.
 */
static void
lance_scan_on_statistics(void *ctx, const LanceScanStatistics *statistics)
{
	LanceScanState *state = (LanceScanState *) ctx;

	state->stats.indices_loaded = statistics->indices_loaded;
	state->stats.index_partitions_loaded = statistics->index_partitions_loaded;
	state->stats.index_comparisons = statistics->index_comparisons;
	state->stats.seen = true;
}

/*
 * The nearest-neighbour settings of a vector Top-K search.  No fragment
 * list: the search covers the whole dataset, which is what lets Lance use
 * its index.
 */
static void
lance_scan_set_nearest(LanceScanState *state)
{
	int32		rc;

	LANCE_MASKED(rc = lance_scanner_nearest(state->scanner, state->topk_column,
											state->vector, (size_t) state->dim,
											LANCE_DTYPE_FLOAT32,
											(uint32) state->topk_k));
	LANCE_CHECK(rc == 0, state->uri);

	LANCE_MASKED(rc = lance_scanner_set_metric(state->scanner,
											   (LanceMetricType) state->topk_metric));
	LANCE_CHECK(rc == 0, state->uri);

	/*
	 * Filter first, then search among what is left: every row that comes back
	 * satisfies the WHERE clause, and a filter that leaves few rows still gets
	 * k of them rather than whatever survives of k candidates.
	 */
	LANCE_MASKED(rc = lance_scanner_set_prefilter(state->scanner, true));
	LANCE_CHECK(rc == 0, state->uri);

	/*
	 * A floor only.  lance_scanner_set_nprobes() would pin the maximum to the
	 * same value and stop a filtered search from probing further partitions to
	 * find k rows (scanner.rs NprobesRange::exact).
	 */
	if (state->nprobes > 0)
	{
		LANCE_MASKED(rc = lance_scanner_set_minimum_nprobes(state->scanner,
															(uint32) state->nprobes));
		LANCE_CHECK(rc == 0, state->uri);
	}

	if (state->refine_factor > 0)
	{
		LANCE_MASKED(rc = lance_scanner_set_refine_factor(state->scanner,
														  (uint32) state->refine_factor));
		LANCE_CHECK(rc == 0, state->uri);
	}

	memset(&state->stats, 0, sizeof(state->stats));
	LANCE_MASKED(rc = lance_scanner_set_statistics_callback(state->scanner,
															lance_scan_on_statistics,
															state));
	LANCE_CHECK(rc == 0, state->uri);
}

static void
lance_scan_make_scanner(LanceScanState *state)
{
	int32		rc;

	LANCE_MASKED(state->scanner = lance_scanner_new(state->dataset,
													(const char *const *) state->columns,
													state->filter));
	LANCE_CHECK(state->scanner != NULL, state->uri);
	state->sc_handle = lance_rt_track_scanner(state->scanner);

	/* Every one of these has to be set before a stream is taken. */
	if (state->searching)
		lance_scan_set_nearest(state);
	else
	{
		LANCE_MASKED(rc = lance_scanner_set_fragment_ids(state->scanner, state->ids,
														 (size_t) state->nids));
		LANCE_CHECK(rc == 0, state->uri);
	}

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

	state->stream_done = false;

	/* A search is polled straight off the scanner (lance_scan_poll_batch). */
	if (state->searching)
	{
		lance_scan_open_waker();
		return;
	}

	state->stream = lance_rt_track_new_stream(&state->stream_handle);
	LANCE_MASKED(rc = lance_scanner_to_arrow_stream(state->scanner,
													state->stream));
	LANCE_CHECK(rc == 0, state->uri);
}

/*
 * Resolve one converter per projected column, against the schema the stream
 * reports rather than against the dataset schema: what the stream returns is
 * what the batches will contain, in the order the projection asked for.
 */
static void
lance_scan_resolve_converters(LanceScanState *state)
{
	TupleDesc	tupdesc = state->tupdesc;
	int			i;

	if (state->schema.n_children != (int64) lance_scan_stream_columns(state))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_COLUMN_NUMBER),
				 errmsg("lance_fdw: Lance returned " INT64_FORMAT " columns, expected %d",
						state->schema.n_children, lance_scan_stream_columns(state))));

	/*
	 * A nearest-neighbour search appends its own distance after the projected
	 * columns.  PostgreSQL computes its own distance and sorts
	 * by that, so this one is never read - but it has to be the column that is
	 * expected, or the positions the converters read would be wrong.
	 */
	if (state->searching)
	{
		struct ArrowSchema *extra = state->schema.children[state->ncolumns];

		if (extra == NULL || extra->name == NULL ||
			strcmp(extra->name, "_distance") != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_COLUMN_NUMBER),
					 errmsg("lance_fdw: a vector search returned \"%s\" where \"_distance\" was expected",
							extra != NULL && extra->name != NULL ? extra->name : "")));
	}

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

static void
lance_scan_build_converters(LanceScanState *state, Relation rel)
{
	int			rc;

	state->tupdesc = RelationGetDescr(rel);

	/* A search's schema comes with its first batch (lance_scan_poll_batch). */
	if (state->searching)
		return;

	memset(&state->schema, 0, sizeof(state->schema));
	LANCE_MASKED(rc = state->stream->get_schema(state->stream, &state->schema));
	if (rc != 0)
		lance_scan_stream_error(state);

	lance_scan_resolve_converters(state);
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
/*
 * Resolve, and immediately discard, a converter for every column the pushed
 * filter reads.  Same schema, same lookup, same
 * lance_arrow_resolve_converter() as the projection: the point is that the
 * check is identical, so a declaration Lance would refuse in the projection is
 * refused here too even though the value never travels.
 */
static void
lance_scan_validate_filter_columns(LanceScanState *state, Relation rel,
								   struct ArrowSchema *schema)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	int			i;

	for (i = 0; i < state->nfilter_cols; i++)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc,
											  state->filter_attnums[i] - 1);
		struct ArrowSchema *field = NULL;
		LanceConverter conv;
		int64		c;

		for (c = 0; c < schema->n_children; c++)
		{
			struct ArrowSchema *child = schema->children[c];

			if (child != NULL && child->name != NULL &&
				strcmp(child->name, state->filter_columns[i]) == 0)
			{
				field = child;
				break;
			}
		}

		if (field == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("lance_fdw: column \"%s\": the dataset has no such column",
							state->filter_columns[i]),
					 errdetail("It is referenced by a qualifier pushed down to Lance, while reading uri %s.",
							   state->uri),
					 errhint("Use IMPORT FOREIGN SCHEMA, or the column_name "
							 "option where the two names differ.")));

		memset(&conv, 0, sizeof(conv));
		lance_arrow_resolve_converter(field, att->atttypid, att->atttypmod,
									  &conv);
		lance_arrow_converter_reset(&conv);
	}
}

static void
lance_scan_validate_projection(LanceScanState *state, Relation rel)
{
	TupleDesc	tupdesc = RelationGetDescr(rel);
	struct ArrowSchema schema;
	int32		rc;
	int			i;

	/*
	 * count(*) asks Lance for no column - but a pushed-down qual may still
	 * name one, and that one has to be checked.  Only when there is neither is
	 * there nothing to open the schema for.
	 */
	if (state->ncolumns == 0 && state->nfilter_cols == 0)
		return;

	memset(&schema, 0, sizeof(schema));
	LANCE_MASKED(rc = lance_dataset_schema(state->dataset, &schema));
	LANCE_CHECK(rc == 0, state->uri);

	PG_TRY();
	{
		state->converters = (LanceConverter *)
			palloc0(sizeof(LanceConverter) * state->ncolumns);

		/*
		 * A column that only a pushed-down qual reads is not in
		 * the projection, so without this it would meet no check at all: a
		 * B-tier column declared as something readable would slip past the
		 * refusal that "B-tier is loud, never silent" rests on, and Lance
		 * would interpret it by its physical type instead.  Checking it here
		 * means running the same resolution the projection gets - not a
		 * weaker "does the name exist" - and throwing the converter away.
		 */
		lance_scan_validate_filter_columns(state, rel, &schema);

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

/*
 * The dimension of the Top-K column in the open dataset, or 0 if it is not a
 * fixed_size_list<float32, N> - the only shape lance_scanner_nearest() is
 * asked to search here.  Reads the schema the open already loaded.
 */
static int
lance_scan_vector_column_dim(LanceScanState *state)
{
	struct ArrowSchema schema;
	int32		rc;
	int			dim = 0;
	int64		c;

	memset(&schema, 0, sizeof(schema));
	LANCE_MASKED(rc = lance_dataset_schema(state->dataset, &schema));
	LANCE_CHECK(rc == 0, state->uri);

	for (c = 0; c < schema.n_children; c++)
	{
		struct ArrowSchema *child = schema.children[c];

		if (child == NULL || child->name == NULL ||
			strcmp(child->name, state->topk_column) != 0)
			continue;

		if (child->format != NULL && strncmp(child->format, "+w:", 3) == 0 &&
			child->n_children == 1 && child->children[0] != NULL &&
			child->children[0]->format != NULL &&
			strcmp(child->children[0]->format, "f") == 0)
		{
			long		n = strtol(child->format + 3, NULL, 10);

			if (n > 0 && n <= PG_INT32_MAX)
				dim = (int) n;
		}
		break;
	}

	if (schema.release != NULL)
		LANCE_MASKED(schema.release(&schema));

	return dim;
}

/*
 * On the searching QE: the dataset is the version the QD pinned and checked,
 * so a column that no longer matches the vector it sent is a broken plan.
 */
static void
lance_scan_check_vector_column(LanceScanState *state, bool strict)
{
	int			dim = lance_scan_vector_column_dim(state);

	if (strict && dim != state->dim)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("lance_fdw: column \"%s\" has %d dimensions here, but the coordinator sent a %d-dimensional query vector",
						state->topk_column, dim, state->dim),
				 errdetail("While reading uri %s.", state->uri)));
}

/*
 * The QD's half of vector Top-K: evaluate the query vector - a
 * parameter has its value only now - and decide whether this execution can
 * search.  Anything that would make the search differ from the exact path is
 * not an error here but a reason to scan the plain way, recorded in
 * state->fallback: the operator then meets the value where the exact path
 * would have met it, and raises its error at the same row, or never if no row
 * reaches it.
 */
static bool
lance_scan_prepare_topk(ForeignScanState *node, LanceScanState *state)
{
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	ExprState  *exprstate;
	Datum		value;
	bool		isnull;
	ArrayType  *array;
	const float4 *elements;
	int			column_dim;
	int			n;
	int			i;
	bool		all_zero = true;

	exprstate = ExecInitExpr((Expr *) linitial(fsplan->fdw_exprs),
							 (PlanState *) node);
	value = ExecEvalExprSwitchContext(exprstate, econtext, &isnull);

	if (isnull)
	{
		state->fallback = "the query vector is NULL";
		return false;
	}

	array = DatumGetArrayTypeP(value);
	if (ARR_NDIM(array) != 1)
	{
		state->fallback = "the query vector is not a one-dimensional array";
		return false;
	}
	if (ARR_HASNULL(array) && array_contains_nulls(array))
	{
		state->fallback = "the query vector has NULL elements";
		return false;
	}

	column_dim = lance_scan_vector_column_dim(state);
	if (column_dim == 0)
	{
		state->fallback = "the Lance column is not a fixed_size_list of float32";
		return false;
	}

	n = ARR_DIMS(array)[0];
	if (n != column_dim)
	{
		state->fallback = psprintf("the query vector has %d elements and the column %d",
								   n, column_dim);
		return false;
	}

	elements = (const float4 *) ARR_DATA_PTR(array);
	for (i = 0; i < n; i++)
	{
		if (isnan(elements[i]) || isinf(elements[i]))
		{
			state->fallback = "the query vector has a NaN or infinite element";
			return false;
		}
		if (elements[i] != 0.0f)
			all_zero = false;
	}

	/* A zero vector has no direction, so no cosine distance to search by. */
	if (all_zero && state->topk_metric == LANCE_VECTOR_COSINE)
	{
		state->fallback = "the query vector is zero";
		return false;
	}

	/* The value lives in per-tuple memory; keep a copy in the scan's own. */
	state->dim = n;
	state->vector = (float4 *) palloc(sizeof(float4) * n);
	memcpy(state->vector, elements, sizeof(float4) * n);
	ResetExprContext(econtext);

	return true;
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
	/*
	 * The values a vector search would use, read by the QD when the statement
	 * starts.  A QE takes them from the scan unit
	 * instead and never reads these GUCs.
	 */
	if (state->is_topk)
	{
		state->nprobes = lance_vector_nprobes;
		state->refine_factor = lance_vector_refine_factor;
	}

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

		/*
		 * Vector Top-K: one segment searches the whole dataset,
		 * the others do nothing.  If the query vector turns out to be unusable
		 * the QD publishes the plain fragment units instead - the plan above
		 * this scan is the same either way, so the statement simply runs as if
		 * the pushdown had been off.
		 */
		if (state->is_topk && lance_scan_prepare_topk(node, state))
		{
			state->search_segment = lance_dispatch_search_target(nsegments);
			lance_dispatch_publish_nearest(fsplan, state->uri, state->version,
										   nsegments, state->search_segment,
										   state->vector, state->dim,
										   state->nprobes, state->refine_factor);
			elog(DEBUG1,
				 "lance_fdw: dispatching a vector search of \"%s\" at version " UINT64_FORMAT " to segment %d",
				 state->uri, state->version, state->search_segment);
			lance_scan_close_dataset(state);
			return;
		}

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
		state->uri = units.uri;
		state->version = units.version;

		if (units.kind == LANCE_UNIT_NEAREST)
		{
			/*
			 * Only the QD's chosen segment searches.  The others leave without
			 * opening anything, which is the point: the search covers the whole
			 * dataset, and a second one would return its rows twice.
			 */
			if (!state->is_topk)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("lance_fdw: the plan carries a vector search unit but no vector Top-K")));

			if (GpIdentity.segindex != units.target)
			{
				elog(DEBUG1,
					 "lance_fdw: not the search segment, skipping \"%s\" on segment %d",
					 state->uri, GpIdentity.segindex);
				return;
			}

			state->searching = true;
			state->search_segment = units.target;
			state->vector = units.vector;
			state->dim = units.dim;
			state->nprobes = units.nprobes;
			state->refine_factor = units.refine_factor;
			lance_scan_open_dataset(state, units.version);
			lance_scan_check_vector_column(state, true);
		}
		else
		{
			/* A QE: take this segment's share of what the QD published. */
			state->ids = units.ids;
			state->nids = lance_dispatch_take_share(units.ids, units.nids,
													units.nsegments);
			lance_scan_log_share(state, "reading");

			/* An empty share is not an error, and it is not I/O either. */
			if (state->nids == 0)
				return;

			lance_scan_open_dataset(state, units.version);
		}
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
		 * nobody to dispatch to: this process reads every fragment itself -
		 * or, for a vector Top-K, searches them all itself.
		 */
		lance_scan_open_dataset(state, state->opts.version);

		if (state->is_topk)
		{
			lance_scan_validate_projection(state, rel);
			if (lance_scan_prepare_topk(node, state))
			{
				state->searching = true;
				state->search_segment = -1;
				elog(DEBUG1,
					 "lance_fdw: searching \"%s\" at version " UINT64_FORMAT " in this process",
					 state->uri, state->version);
			}
		}

		if (!state->searching)
		{
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
 * One lance_scanner_poll_next, and on a batch its export into state->batch.
 * Called masked, so nothing here may ereport: the result goes back through
 * *status and the return value.  The first batch's schema is kept for the
 * converters; later ones describe the same columns and are let go.
 */
static int32
lance_scan_poll_once(LanceScanState *state, LancePollStatus *status)
{
	LanceBatch *lbatch = NULL;
	struct ArrowSchema schema;
	int32		rc;

	*status = lance_scanner_poll_next(state->scanner, lance_scan_waker, NULL,
									  &lbatch);
	if (*status != LANCE_POLL_READY)
		return 0;

	memset(&schema, 0, sizeof(schema));
	rc = lance_batch_to_arrow(lbatch, &state->batch, &schema);
	/* The export holds its own references; freeing keeps the error intact. */
	lance_batch_free(lbatch);
	if (rc != 0)
		return rc;

	if (state->schema.release == NULL)
		state->schema = schema;
	else if (schema.release != NULL)
		schema.release(&schema);
	return 0;
}

/*
 * Wait for the search's next batch, or its end, without leaving the backend
 * deaf to a cancel for the length of the search.  A poll
 * that is not ready returns at once and lance-c goes on computing on its own
 * threads; the backend sleeps on its latch and the waker pipe, and acts on any
 * interrupt each time it wakes.  An ERROR from there closes the scanner
 * through the resource owner, and closing is what stops the search.
 *
 * Leaves state->batch released at the end of the search.
 */
static void
lance_scan_poll_batch(LanceScanState *state)
{
	for (;;)
	{
		LancePollStatus status;
		int32		rc;

		lance_scan_drain_waker();
		LANCE_MASKED(rc = lance_scan_poll_once(state, &status));
		LANCE_CHECK(rc == 0, state->uri);

		switch (status)
		{
			case LANCE_POLL_READY:
				if (state->converters == NULL)
					lance_scan_resolve_converters(state);
				return;

			case LANCE_POLL_FINISHED:
				return;

			case LANCE_POLL_ERROR:
				/* Rebuilt by a ReScan, as after a failed stream. */
				state->stream_failed = true;
				lance_rt_error(state->uri);

			case LANCE_POLL_PENDING:

				/*
				 * The waker fires at most once per pending poll.  The timeout
				 * is only a safety net: polling again early costs nothing.
				 */
				(void) WaitLatchOrSocket(MyLatch,
										 WL_LATCH_SET | WL_SOCKET_READABLE |
										 WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
										 lance_waker_fds[0], 1000L,
										 PG_WAIT_EXTENSION);
				ResetLatch(MyLatch);
				CHECK_FOR_INTERRUPTS();
				break;
		}
	}
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

	if (state->searching)
		lance_scan_poll_batch(state);
	else
	{
		LANCE_MASKED(rc = state->stream->get_next(state->stream, &state->batch));

		/*
		 * Signals were blocked for the whole read, so anything that arrived
		 * during it was delivered on the restore just now and is acted on here
		 * rather than one batch later.  The batch, if there is one, is
		 * freed by the scan context's reset callback on the way out.
		 */
		CHECK_FOR_INTERRUPTS();

		if (rc != 0)
		{
			/*
			 * lance-c poisons the scanner behind a stream that failed, so
			 * remember it: a ReScan after this has to build a new one.
			 */
			state->stream_failed = true;
			lance_scan_stream_error(state);
		}
	}

	if (state->batch.release == NULL)
	{
		state->stream_done = true;

		/*
		 * Back on the backend's own thread, so the numbers the statistics
		 * callback copied can be reported now.  index_comparisons is the one
		 * that says whether an index was used: the other two count loads from
		 * storage and are zero once the index is cached.
		 */
		if (state->searching)
			elog(DEBUG1,
				 "lance_fdw: vector search of \"%s\" on segment %d: %s, "
				 "index_comparisons " UINT64_FORMAT ", indices_loaded " UINT64_FORMAT
				 ", index_partitions_loaded " UINT64_FORMAT,
				 state->uri, GpIdentity.segindex,
				 state->stats.seen ? "statistics" : "no statistics",
				 state->stats.index_comparisons, state->stats.indices_loaded,
				 state->stats.index_partitions_loaded);
		return false;
	}

	if (state->batch.n_children != (int64) lance_scan_stream_columns(state))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_COLUMN_NUMBER),
				 errmsg("lance_fdw: Lance returned a batch of " INT64_FORMAT " columns, expected %d",
						state->batch.n_children, lance_scan_stream_columns(state))));

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

	/*
	 * The QD, and a QE with an empty share or not chosen to search, have
	 * nothing open and no rows.  A search has a scanner but no stream.
	 */
	if (state == NULL || state->scanner == NULL)
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

	if (state->stream_failed || state->searching)
	{
		/*
		 * The scanner behind a failed stream is poisoned, so rewinding means
		 * building a new one from the same dataset.
		 * A search is polled off the scanner itself, which does not start
		 * over either.
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

	/*
	 * What the scan actually asks Lance for.  This block has pushed the
	 * projection down since its first commit but there has never been a way to
	 * see it from SQL; printing it is what makes the narrowing that filter
	 * pushdown buys an observable behaviour rather than a claim.
	 */
	{
		StringInfoData cols;
		int			i;

		initStringInfo(&cols);
		for (i = 0; i < state->ncolumns; i++)
			appendStringInfo(&cols, "%s%s", i ? ", " : "", state->columns[i]);
		ExplainPropertyText("Lance Columns",
							state->ncolumns > 0 ? cols.data : "(none)", es);
	}

	if (state->filter != NULL)
		ExplainPropertyText("Lance Filter", state->filter, es);

	/*
	 * Vector Top-K.  The plan above this node looks the same with
	 * and without the pushdown, so this line is the way to tell them apart -
	 * and, under ANALYZE, whether this execution actually searched or fell back
	 * to the plain scan, and which segment searched.
	 */
	if (state->is_topk)
	{
		if (state->fallback != NULL)
			ExplainPropertyText("Lance Vector Search",
								psprintf("fell back (%s)", state->fallback), es);
		else
			ExplainPropertyText("Lance Vector Search",
								psprintf("%s %s (%s), k=" INT64_FORMAT,
										 state->topk_column,
										 lance_vector_metric_operator(state->topk_metric),
										 lance_vector_metric_name(state->topk_metric),
										 state->topk_k),
								es);

		if (es->verbose)
		{
			ExplainPropertyText("Lance nprobes",
								state->nprobes > 0 ? psprintf("%d", state->nprobes) : "default",
								es);
			ExplainPropertyText("Lance refine_factor",
								state->refine_factor > 0 ? psprintf("%d", state->refine_factor) : "default",
								es);
		}

		if (es->analyze && state->fallback == NULL)
			ExplainPropertyText("Lance Search Segment",
								state->search_segment >= 0
								? psprintf("%d", state->search_segment)
								: "coordinator",
								es);
	}

	/* Only known when this process actually opened the dataset. */
	if (state->total_fragments >= 0)
		ExplainPropertyInteger("Lance Fragments", NULL,
							   state->total_fragments, es);
}

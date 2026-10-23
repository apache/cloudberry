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
 * lance_runtime.c
 *	  Per-backend lance-c runtime.
 *
 * Three jobs, all of them about keeping a Rust async runtime inside a
 * PostgreSQL backend well behaved:
 *
 *	- lance_rt_init() creates the one LanceSession this backend uses, with the
 *	  full signal mask blocked so the tokio worker threads that lance-c spawns
 *	  inherit a blocked mask and PostgreSQL's SIGINT/SIGTERM/SIGUSR1 keep going
 *	  to the main thread only.
 *
 *	- lance_rt_error() turns lance-c's thread-local error into an ereport with
 *	  a "lance: " prefix and the error code in the detail.
 *
 *	- the handle registry closes datasets, scanners and Arrow streams exactly
 *	  once, on the normal path and on the ERROR/abort path alike.
 *
 * IDENTIFICATION
 *	  src/lance_runtime.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <pthread.h>
#include <signal.h>

#include "lance_fdw.h"
#include "lance_runtime.h"

#include "utils/vmem_tracker.h"

#include "utils/plancache.h"

#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/resowner.h"

int			lance_cpu_threads = 0;
int			lance_io_threads = 0;
int			lance_index_cache_mb = 64;
int			lance_metadata_cache_mb = 32;
int			lance_io_buffer_size_mb = 0;
int			lance_batch_readahead = 0;
bool		lance_track_memory = true;
bool		lance_enable_filter_pushdown = true;

/*
 * A plan built while pushdown was on has the pushed quals removed from its
 * qual list and the filter string frozen into fdw_private.  Nothing in the
 * relcache changes when this GUC does, so a cached generic plan would go on
 * pushing down after SET ... = off.  This GUC is the escape hatch for a
 * correctness problem, so it has to bite at once.
 */
static void
lance_assign_enable_filter_pushdown(bool newval, void *extra)
{
	if (newval != lance_enable_filter_pushdown)
		ResetPlanCache();
}

bool		lance_enable_vector_pushdown = true;
int			lance_vector_max_k = 10000;
int			lance_vector_nprobes = 0;
int			lance_vector_refine_factor = 0;
int			lance_vector_search_mode = LANCE_VECTOR_SEARCH_AUTO;

static const struct config_enum_entry lance_vector_search_mode_options[] = {
	{"auto", LANCE_VECTOR_SEARCH_AUTO, false},
	{"single", LANCE_VECTOR_SEARCH_SINGLE, false},
	{"distributed", LANCE_VECTOR_SEARCH_DISTRIBUTED, false},
	{NULL, 0, false}
};

/*
 * The same reasoning as for filter pushdown: a
 * cached plan has the Top-K decision and k frozen into it, so turning the
 * pushdown off, or lowering the bound on k, has to throw cached plans away.
 * PostgreSQL calls assign hooks for SET, SET LOCAL and the restore at the end
 * of a transaction alike, so all three replan.
 */
static void
lance_assign_enable_vector_pushdown(bool newval, void *extra)
{
	if (newval != lance_enable_vector_pushdown)
		ResetPlanCache();
}

static void
lance_assign_vector_max_k(int newval, void *extra)
{
	if (newval != lance_vector_max_k)
		ResetPlanCache();
}

typedef enum LanceHandleKind
{
	LANCE_HANDLE_DATASET,
	LANCE_HANDLE_SCANNER,
	LANCE_HANDLE_STREAM
} LanceHandleKind;

struct LanceHandle
{
	LanceHandleKind kind;
	ResourceOwner owner;
	LanceDataset *dataset;
	LanceScanner *scanner;
	struct ArrowArrayStream stream;
	struct LanceHandle *prev;
	struct LanceHandle *next;
};

static LanceSession *rt_session = NULL;

/* Arrow bytes this backend has on the vmem ledger; see lance_rt_vmem_*. */
static int64 rt_vmem_reserved = 0;

/*
 * High-water mark of the above.  The instantaneous value is almost always zero
 * when anything asks for it - a batch is held only between one IterateForeignScan
 * and the next - so the peak is what a person sizing the bounds actually needs,
 * and it is the only figure a test can observe without racing the scan.
 */
static int64 rt_vmem_peak = 0;
static bool rt_callback_registered = false;
static LanceHandle *rt_handles = NULL;

static void lance_rt_release_callback(ResourceReleasePhase phase,
									  bool isCommit,
									  bool isTopLevel,
									  void *arg);

/*
 * Register the GUCs.  Deliberately no MarkGUCPrefixReserved(): nothing here
 * needs the prefix locked down, and leaving it open keeps -c lance_fdw.* usable
 * for future placeholders.
 */
void
lance_rt_define_gucs(void)
{
	DefineCustomBoolVariable("lance_fdw.enable_filter_pushdown",
							 "Push WHERE clauses down to Lance where the two "
							 "systems are known to agree exactly.",
							 "Read at planning time, so it takes effect on the "
							 "next plan; changing it also resets the plan cache "
							 "so that already-cached plans stop pushing down. "
							 "Only the coordinator's value matters.",
							 &lance_enable_filter_pushdown,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, lance_assign_enable_filter_pushdown, NULL);

	DefineCustomBoolVariable("lance_fdw.enable_vector_pushdown",
							 "Send ORDER BY distance LIMIT k queries to Lance's "
							 "nearest-neighbour search.",
							 "On a vector index the result is approximate, as "
							 "with any vector index; off restores the exact "
							 "scan-and-sort.  Read at planning time; changing "
							 "it resets the plan cache.  Only the coordinator's "
							 "value matters.",
							 &lance_enable_vector_pushdown,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, lance_assign_enable_vector_pushdown, NULL);

	DefineCustomIntVariable("lance_fdw.vector_pushdown_max_k",
							"Largest LIMIT a vector Top-K search is pushed "
							"down for.",
							"Lance's search state for k neighbours lives outside "
							"Cloudberry's memory accounting; a larger LIMIT "
							"takes the exact path instead.  Read at planning "
							"time; changing it resets the plan cache.",
							&lance_vector_max_k,
							10000,
							1, PG_INT32_MAX,
							PGC_USERSET,
							0,
							NULL, lance_assign_vector_max_k, NULL);

	DefineCustomIntVariable("lance_fdw.nprobes",
							"Minimum number of vector index partitions a Top-K "
							"search probes; 0 leaves it to Lance.",
							"A floor, not a cap: a filtered search may probe "
							"more partitions to find k rows.  Read by the "
							"coordinator when the statement starts and sent "
							"to the segment that searches.",
							&lance_vector_nprobes,
							0,
							0, PG_INT32_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lance_fdw.refine_factor",
							"Re-rank refine_factor * k candidates by their exact "
							"distance; 0 leaves it to Lance.",
							"Read by the coordinator when the statement starts "
							"and sent to the segment that searches.",
							&lance_vector_refine_factor,
							0,
							0, PG_INT32_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomEnumVariable("lance_fdw.vector_search_mode",
							 "Which segments run a vector search of a table on "
							 "all segments: auto, single or distributed.",
							 "single: one segment searches the whole dataset. "
							 "distributed: every segment searches a contiguous "
							 "range of the fragments. auto: single when the "
							 "column's index has one segment, distributed when it "
							 "has several or there is no index. Read by the "
							 "coordinator when the statement starts.",
							 &lance_vector_search_mode,
							 LANCE_VECTOR_SEARCH_AUTO,
							 lance_vector_search_mode_options,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("lance_fdw.cpu_threads",
							"Number of CPU worker threads lance-c may use.",
							"0 leaves lance-c at its own default, which scales "
							"with the number of cores. Only read once, before "
							"the first lance-c call in a backend.",
							&lance_cpu_threads,
							0, 0, 1024,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lance_fdw.io_threads",
							"Number of I/O threads lance-c may use.",
							"0 leaves lance-c at its own default. Only read "
							"once, before the first lance-c call in a backend.",
							&lance_io_threads,
							0, 0, 1024,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lance_fdw.index_cache_size_mb",
							"Size of the per-backend Lance index cache, in MB.",
							"lance's own default is 6 GiB, which is far too "
							"much for one cache per backend. This block reads "
							"no indexes, so a small number is fine.",
							&lance_index_cache_mb,
							64, 0, 1024 * 1024,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lance_fdw.metadata_cache_size_mb",
							"Size of the per-backend Lance metadata cache, in MB.",
							"lance's own default is 1 GiB. The cache is shared "
							"by every dataset this backend opens.",
							&lance_metadata_cache_mb,
							32, 0, 1024 * 1024,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lance_fdw.io_buffer_size_mb",
							"Size of the I/O buffer one scan may fill, in MB.",
							"0 leaves lance-c at its own default. This bounds "
							"buffered reads, not everything a scan allocates. "
							"Every backend reads its own value when a scan "
							"opens, and under \"all segments\" each segment "
							"buffers this much at the same time, so a "
							"cluster-wide bound belongs in postgresql.conf "
							"rather than in a session SET.",
							&lance_io_buffer_size_mb,
							0, 0, 1024 * 1024,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("lance_fdw.batch_readahead",
							"Number of batches a scan may decode concurrently.",
							"0 leaves lance-c at its own default. Decoded "
							"batches are held at once, so this multiplies "
							"whatever one batch costs - the row or byte batch "
							"size the table sets. Read per backend when a scan "
							"opens, like lance_fdw.io_buffer_size_mb.",
							&lance_batch_readahead,
							0, 0, 1024,
							PGC_SUSET,
							0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("lance_fdw.track_memory",
							 "Charge Arrow batches to Cloudberry's memory "
							 "accounting.",
							 "lance allocates batches outside palloc, so "
							 "without this the resource group, "
							 "gp_vmem_protect_limit and the runaway detector "
							 "cannot see them and the OOM killer is what "
							 "notices. Turn it off only if the accounting "
							 "itself is in the way.",
							 &lance_track_memory,
							 true,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);
}

/*
 * Why the reservation happens after the fact: the Arrow C Data Interface hands
 * over a batch lance has already allocated, with no way to ask first.  So this
 * detects rather than prevents, and what bounds the overshoot between the
 * allocation and this check is the batch_size_bytes table option.
 */
int64
lance_rt_vmem_reserve(int64 bytes, const char *uri)
{
	MemoryAllocationStatus st;
	const char *reason;

	if (!lance_track_memory || bytes <= 0 || !VmemTrackerIsActivated())
		return 0;

	st = VmemTracker_ReserveVmem(bytes);
	if (st == MemoryAllocation_Success)
	{
		rt_vmem_reserved += bytes;
		if (rt_vmem_reserved > rt_vmem_peak)
			rt_vmem_peak = rt_vmem_reserved;
		return bytes;
	}

	switch (st)
	{
		case MemoryFailure_VmemExhausted:
			reason = "the per-segment vmem limit is exhausted";
			break;
		case MemoryFailure_SystemMemoryExhausted:
			reason = "system memory is exhausted";
			break;
		case MemoryFailure_QueryMemoryExhausted:
			reason = "this query's memory is exhausted";
			break;
		case MemoryFailure_ResourceGroupMemoryExhausted:
			reason = "the resource group's memory is exhausted";
			break;
		default:
			reason = "the memory reservation was refused";
			break;
	}

	ereport(ERROR,
			(errcode(ERRCODE_OUT_OF_MEMORY),
			 errmsg("lance_fdw: cannot reserve " INT64_FORMAT " bytes for a Lance batch",
					bytes),
			 errdetail("%s While reading uri %s.", reason,
					   uri != NULL ? uri : "?"),
			 errhint("Set batch_size_bytes on the foreign table to bound one "
					 "batch, lower lance_fdw.batch_readahead, or raise the "
					 "memory limit.")));
	return 0;					/* unreachable */
}

void
lance_rt_vmem_release(int64 bytes)
{
	if (bytes <= 0)
		return;

	VmemTracker_ReleaseVmem(bytes);
	rt_vmem_reserved -= bytes;
}

int64
lance_rt_vmem_reserved(void)
{
	return rt_vmem_reserved;
}

int64
lance_rt_vmem_peak(void)
{
	return rt_vmem_peak;
}

/*
 * Map a lance-c error code onto an SQLSTATE.
 */
static int
lance_rt_errcode(LanceErrorCode code)
{
	switch (code)
	{
		case LANCE_ERR_NOT_FOUND:
			return ERRCODE_FDW_TABLE_NOT_FOUND;
		case LANCE_ERR_IO:
			return ERRCODE_IO_ERROR;
		case LANCE_ERR_INVALID_ARGUMENT:
			return ERRCODE_INVALID_PARAMETER_VALUE;
		case LANCE_ERR_NOT_SUPPORTED:
			return ERRCODE_FEATURE_NOT_SUPPORTED;
		case LANCE_ERR_DATASET_ALREADY_EXISTS:
			return ERRCODE_DUPLICATE_OBJECT;
		case LANCE_ERR_COMMIT_CONFLICT:
			return ERRCODE_T_R_SERIALIZATION_FAILURE;
		case LANCE_ERR_INDEX:
		case LANCE_ERR_INTERNAL:
		case LANCE_ERR_PANIC:
		case LANCE_OK:
		default:
			return ERRCODE_INTERNAL_ERROR;
	}
}

void
lance_rt_error(const char *uri)
{
	LanceErrorCode code;
	const char *raw;
	char	   *message;

	LANCE_MASKED({
		code = lance_last_error_code();
		raw = lance_last_error_message();
	});

	/*
	 * Copy the message out of lance-c's thread-local storage before anything
	 * can longjmp past the free.
	 */
	if (raw != NULL)
	{
		message = pstrdup(raw);
		LANCE_MASKED(lance_free_string(raw));
	}
	else
		message = pstrdup("unknown error");

	if (uri != NULL)
		ereport(ERROR,
				(errcode(lance_rt_errcode(code)),
				 errmsg("lance: %s", message),
				 errdetail("lance error code %d, uri %s", (int) code, uri)));
	else
		ereport(ERROR,
				(errcode(lance_rt_errcode(code)),
				 errmsg("lance: %s", message),
				 errdetail("lance error code %d", (int) code)));
}

/*
 * Thread count environment variables have to be in place before the first
 * lance-c call, because that is when the runtimes are built.
 */
static void
lance_rt_set_thread_limits(void)
{
	char		buf[32];

	if (lance_cpu_threads > 0)
	{
		snprintf(buf, sizeof(buf), "%d", lance_cpu_threads);
		if (setenv("LANCE_CPU_THREADS", buf, 1) != 0)
			ereport(WARNING,
					(errmsg("lance_fdw: could not set LANCE_CPU_THREADS")));
	}

	if (lance_io_threads > 0)
	{
		snprintf(buf, sizeof(buf), "%d", lance_io_threads);
		if (setenv("LANCE_IO_THREADS", buf, 1) != 0)
			ereport(WARNING,
					(errmsg("lance_fdw: could not set LANCE_IO_THREADS")));
	}
}

/*
 * Signals and lance-c's threads
 *
 * A thread inherits the signal mask of the thread that creates it, and the
 * Rust side creates threads lazily: lance-c's tokio runtime is a LazyLock
 * built on the first block_on() (third_party/lance-c/src/runtime.rs), and
 * lance's own IO and compute pools come up on the first read.  None of that
 * happens in lance_session_new(), which only builds the two caches - so a mask
 * around the first call alone left every worker thread able to take SIGINT,
 * SIGUSR1, SIGALRM and the rest, which is what test/stability/cancel_loop.sh
 * caught (16 of 17 threads on a 16-core box, tokio's default worker count).
 *
 * Hence every call into lance-c runs with all signals blocked on the calling
 * thread (LANCE_MASKED), and the mask is restored right after.  A signal that
 * arrives meanwhile stays pending and is delivered on the restore, so the
 * backend's handlers still run on the main thread and before the next
 * CHECK_FOR_INTERRUPTS() at the batch boundary: cancellation latency is
 * what it was, and no Rust thread can ever run a PostgreSQL signal handler.
 */
void
lance_rt_block_signals(sigset_t *saved)
{
	sigset_t	all;
	int			rc;

	sigfillset(&all);
	rc = pthread_sigmask(SIG_BLOCK, &all, saved);
	if (rc != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("lance_fdw: could not block signals"),
				 errdetail("pthread_sigmask() returned %d.", rc)));
}

void
lance_rt_restore_signals(const sigset_t *saved)
{
	/* Cannot fail for a mask this thread handed out; nothing to report. */
	(void) pthread_sigmask(SIG_SETMASK, saved, NULL);
}

void
lance_rt_init(void)
{
	LanceSession *session;

	if (rt_session != NULL)
		return;

	lance_rt_set_thread_limits();

	LANCE_MASKED(session = lance_session_new((uint64_t) lance_index_cache_mb * 1024 * 1024,
											 (uint64_t) lance_metadata_cache_mb * 1024 * 1024));

	/* Restored before reporting anything: ereport must not run masked. */
	LANCE_CHECK(session != NULL, NULL);

	rt_session = session;
}

LanceSession *
lance_rt_session(void)
{
	lance_rt_init();
	return rt_session;
}

/*
 * Handle registry
 */
static LanceHandle *
lance_rt_track(LanceHandleKind kind)
{
	LanceHandle *handle;

	Assert(rt_callback_registered);

	handle = (LanceHandle *) MemoryContextAllocZero(TopMemoryContext,
													sizeof(LanceHandle));
	handle->kind = kind;
	handle->owner = CurrentResourceOwner;
	handle->next = rt_handles;
	if (rt_handles != NULL)
		rt_handles->prev = handle;
	rt_handles = handle;

	return handle;
}

LanceHandle *
lance_rt_track_dataset(LanceDataset *dataset)
{
	LanceHandle *handle = lance_rt_track(LANCE_HANDLE_DATASET);

	handle->dataset = dataset;
	return handle;
}

LanceHandle *
lance_rt_track_scanner(LanceScanner *scanner)
{
	LanceHandle *handle = lance_rt_track(LANCE_HANDLE_SCANNER);

	handle->scanner = scanner;
	return handle;
}

struct ArrowArrayStream *
lance_rt_track_new_stream(LanceHandle **handle)
{
	LanceHandle *h = lance_rt_track(LANCE_HANDLE_STREAM);

	*handle = h;
	return &h->stream;
}

static void
lance_rt_unlink(LanceHandle *handle)
{
	if (handle->prev != NULL)
		handle->prev->next = handle->next;
	else
		rt_handles = handle->next;

	if (handle->next != NULL)
		handle->next->prev = handle->prev;

	handle->prev = handle->next = NULL;
}

void
lance_rt_forget(LanceHandle *handle)
{
	if (handle == NULL)
		return;

	lance_rt_unlink(handle);
	pfree(handle);
}

void
lance_rt_release(LanceHandle *handle)
{
	if (handle == NULL)
		return;

	switch (handle->kind)
	{
		case LANCE_HANDLE_DATASET:
			LANCE_MASKED(lance_dataset_close(handle->dataset));
			handle->dataset = NULL;
			break;
		case LANCE_HANDLE_SCANNER:
			LANCE_MASKED(lance_scanner_close(handle->scanner));
			handle->scanner = NULL;
			break;
		case LANCE_HANDLE_STREAM:
			/* The Arrow contract says release runs exactly once. */
			if (handle->stream.release != NULL)
			{
				LANCE_MASKED(handle->stream.release(&handle->stream));
				handle->stream.release = NULL;
			}
			break;
	}

	lance_rt_forget(handle);
}

static const char *
lance_rt_kind_name(LanceHandleKind kind)
{
	switch (kind)
	{
		case LANCE_HANDLE_DATASET:
			return "dataset";
		case LANCE_HANDLE_SCANNER:
			return "scanner";
		case LANCE_HANDLE_STREAM:
			return "array stream";
	}
	return "handle";
}

static void
lance_rt_release_callback(ResourceReleasePhase phase,
						  bool isCommit,
						  bool isTopLevel,
						  void *arg)
{
	LanceHandle *handle;

	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;

	handle = rt_handles;
	while (handle != NULL)
	{
		LanceHandle *next = handle->next;

		if (handle->owner == CurrentResourceOwner)
		{
			/*
			 * Reaching this on a successful commit means some path forgot to
			 * release; say so, then clean up anyway.
			 */
			if (isCommit)
				elog(WARNING, "lance_fdw: leaked Lance %s handle",
					 lance_rt_kind_name(handle->kind));

			lance_rt_release(handle);
		}

		handle = next;
	}
}

/*
 * Called from _PG_init().
 */
void
lance_rt_register_callback(void)
{
	if (rt_callback_registered)
		return;

	RegisterResourceReleaseCallback(lance_rt_release_callback, NULL);
	rt_callback_registered = true;
}

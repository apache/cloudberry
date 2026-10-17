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
 * lance_runtime.h
 *	  Per-backend lance-c runtime: session, error translation and the handle
 *	  registry that keeps ResourceOwner release honest.
 *
 * IDENTIFICATION
 *	  src/lance_runtime.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LANCE_RUNTIME_H
#define LANCE_RUNTIME_H

#include <signal.h>

#include "lance_fdw.h"

/* GUCs, registered by lance_rt_define_gucs() from _PG_init() */
extern int	lance_cpu_threads;
extern int	lance_io_threads;
extern int	lance_index_cache_mb;
extern int	lance_metadata_cache_mb;
extern int	lance_io_buffer_size_mb;
extern int	lance_batch_readahead;
extern bool lance_track_memory;

/*
 * Filter pushdown, on by default.  Alone among the GUCs here it is
 * PGC_USERSET, and it is read at *planning* time, on every plan - so it only
 * ever matters on the QD, and the value a segment has is irrelevant.  Its
 * assign hook resets the plan cache, because a cached generic plan has already
 * dropped the local quals and frozen the filter: without that reset a SET
 * would silently fail to take effect on a pooled or prepared statement.
 */
extern bool lance_enable_filter_pushdown;

/*
 * Arrow batches are allocated by lance on the Rust side, so palloc, the
 * resource group and gp_vmem_protect_limit all see a backend that looks nearly
 * idle while it holds them.  These put the bytes on the same ledger as the
 * rest, which is what turns being killed by the OOM killer into an ordinary
 * "out of memory" error.  Reserve returns the bytes it actually took, 0 when
 * tracking is off, and ereports when the limit is reached; every reserve needs
 * exactly one release, error paths included.
 */
extern int64 lance_rt_vmem_reserve(int64 bytes, const char *uri);
extern void lance_rt_vmem_release(int64 bytes);
extern int64 lance_rt_vmem_reserved(void);
extern int64 lance_rt_vmem_peak(void);

extern void lance_rt_define_gucs(void);

/* Hook the handle registry onto ResourceOwner release; call once, at load. */
extern void lance_rt_register_callback(void);

/*
 * Bring the lance-c runtime up.  Cheap and idempotent after the first call;
 * every entry point that is about to touch lance-c calls it first.
 */
extern void lance_rt_init(void);

/* The shared session all datasets of this backend are opened with. */
extern LanceSession *lance_rt_session(void);

/*
 * Handle registry.  Anything that lance-c hands out lives outside
 * palloc, so it is registered against the current ResourceOwner and closed
 * exactly once - by lance_rt_release() on the normal path, or by the resource
 * owner callback on the ERROR/abort path.
 */
typedef struct LanceHandle LanceHandle;

extern LanceHandle *lance_rt_track_dataset(LanceDataset *dataset);
extern LanceHandle *lance_rt_track_scanner(LanceScanner *scanner);

/*
 * Hand out a zeroed ArrowArrayStream that is already registered.  The struct
 * itself is owned by the registry so that it outlives any executor context
 * that might be torn down before the resource owner runs.
 */
extern struct ArrowArrayStream *lance_rt_track_new_stream(LanceHandle **handle);

/* Close the underlying object and forget the handle.  NULL is accepted. */
extern void lance_rt_release(LanceHandle *handle);

/* Forget the handle without closing it (ownership moved elsewhere). */
extern void lance_rt_forget(LanceHandle *handle);

/*
 * Turn the thread-local lance-c error into an ereport(ERROR).
 * uri may be NULL.
 */
extern void lance_rt_error(const char *uri) pg_attribute_noreturn();

/*
 * Every lance-c call goes through this.  `ok` is the call's success condition
 * - a non-NULL handle, or a zero int32 return.
 */
#define LANCE_CHECK(ok, uri) \
	do { \
		if (!(ok)) \
			lance_rt_error(uri); \
	} while (0)

/*
 * lance-c code never runs on this thread with signals deliverable.
 *
 * The Rust side builds its thread pools lazily - tokio's runtime on the first
 * block_on(), lance's own IO and compute pools on the first read - and a new
 * thread inherits the signal mask of the thread that creates it.  So every
 * call into lance-c, not just the first, is made with all signals blocked on
 * the backend's main thread; the mask is restored as soon as the call returns
 * and anything that arrived meanwhile is delivered right there, still on the
 * main thread.  See lance_rt_block_signals() for the longer story.
 *
 * `stmt` must be the FFI call and at most an assignment of its result: nothing
 * inside may ereport() or otherwise longjmp, because that would skip the
 * restore and leave the backend deaf to every signal for good.  Call
 * lance_rt_session(), palloc() and LANCE_CHECK() before or after, never inside.
 */
extern void lance_rt_block_signals(sigset_t *saved);
extern void lance_rt_restore_signals(const sigset_t *saved);

#define LANCE_MASKED(stmt) \
	do { \
		sigset_t	lance_saved_mask_; \
		lance_rt_block_signals(&lance_saved_mask_); \
		stmt; \
		lance_rt_restore_signals(&lance_saved_mask_); \
	} while (0)

#endif							/* LANCE_RUNTIME_H */

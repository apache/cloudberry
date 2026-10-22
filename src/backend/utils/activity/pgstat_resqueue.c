/* -------------------------------------------------------------------------
 *
 * pgstat_resqueue.c
 *	  Implementation of resource queue statistics.
 *
 * Each backend maintains a hash table (keyed by portalid) of
 * PgStat_ResQueuePortalEntry structs for in-flight portals.  When a portal
 * finishes (admitted, rejected, or completed), its timing deltas are
 * accumulated into per-queue PgStat_ResQueueCounts pending data, which is
 * eventually flushed into the shared-memory PgStatShared_ResQueue entry by
 * pgstat_report_stat().
 *
 * Time is tracked at second granularity (via time()) to keep overhead low.
 *
 * Portions Copyright (c) 2006-2010, Greenplum inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_resqueue.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include <time.h>

#include "pgstat.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"


/* ----------
 * Backend-local hash table of in-flight portal entries.
 * Keyed by portalid (uint32).
 * ----------
 */
static HTAB *pgStatResQueuePortalHash = NULL;


/* ----------
 * pgstat_resqueue_portal_hash_init
 *
 * Lazily initialise the backend-local portal tracking hash.
 * ----------
 */
static void
pgstat_resqueue_portal_hash_init(void)
{
	HASHCTL		ctl;

	if (pgStatResQueuePortalHash != NULL)
		return;

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(PgStat_ResQueuePortalEntry);
	ctl.hcxt = TopMemoryContext;

	pgStatResQueuePortalHash = hash_create("ResQueue portal stats",
										   16,
										   &ctl,
										   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/* ----------
 * pgstat_resqueue_wait_start
 *
 * Called just before ResLockAcquire() when a portal is about to enter the
 * resource queue.  Records the wait-start timestamp and resource parameters,
 * and immediately writes the submission counter to shared stats.
 *
 * We write directly to shared stats (bypassing the pending mechanism) so
 * that stats are immediately visible to other sessions without requiring an
 * explicit flush.  This also keeps the code safe regardless of the calling
 * context.
 * ----------
 */
void
pgstat_resqueue_wait_start(uint32 portalid, Oid queueid,
						   Cost query_cost, int64 query_memory_kb)
{
	PgStat_ResQueuePortalEntry *entry;
	bool		found;
	PgStat_EntryRef *entry_ref;
	PgStatShared_ResQueue *shqent;

	/* Skip stat update if pgstat shared memory is already detached. */
	if (pgStatLocal.shared_hash == NULL)
		return;

	pgstat_resqueue_portal_hash_init();

	entry = (PgStat_ResQueuePortalEntry *)
		hash_search(pgStatResQueuePortalHash, &portalid, HASH_ENTER, &found);

	/* If a stale entry exists (e.g. from a prior run), overwrite it. */
	entry->portalid = portalid;
	entry->queueid = queueid;
	entry->t_wait_start = time(NULL);
	entry->t_exec_start = 0;
	entry->query_cost = query_cost;
	entry->query_memory_kb = query_memory_kb;

	/* Write submission counters directly to shared stats. */
	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RESQUEUE,
											InvalidOid, queueid, false);
	if (entry_ref == NULL)
		return;

	shqent = (PgStatShared_ResQueue *) entry_ref->shared_stats;
	shqent->stats.queries_submitted++;
	shqent->stats.total_cost += (PgStat_Counter) query_cost;
	shqent->stats.total_memory_kb += query_memory_kb;
	pgstat_unlock_entry(entry_ref);
}

/* ----------
 * pgstat_resqueue_wait_end
 *
 * Called after ResLockAcquire() returns successfully (portal admitted).
 * Records the exec-start timestamp and counts the admission directly in
 * shared stats.
 * ----------
 */
void
pgstat_resqueue_wait_end(uint32 portalid)
{
	PgStat_ResQueuePortalEntry *entry;
	bool		found;
	time_t		now;
	time_t		wait_secs;
	PgStat_EntryRef *entry_ref;
	PgStatShared_ResQueue *shqent;

	if (pgStatResQueuePortalHash == NULL)
		return;

	entry = (PgStat_ResQueuePortalEntry *)
		hash_search(pgStatResQueuePortalHash, &portalid, HASH_FIND, &found);

	if (!found)
		return;

	now = time(NULL);
	entry->t_exec_start = now;

	wait_secs = (entry->t_wait_start > 0) ? (now - entry->t_wait_start) : 0;
	if (wait_secs < 0)
		wait_secs = 0;

	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RESQUEUE,
											InvalidOid, entry->queueid, false);
	if (entry_ref == NULL)
		return;

	shqent = (PgStatShared_ResQueue *) entry_ref->shared_stats;
	shqent->stats.queries_admitted++;
	shqent->stats.elapsed_wait_secs += (PgStat_Counter) wait_secs;
	if ((PgStat_Counter) wait_secs > shqent->stats.max_wait_secs)
		shqent->stats.max_wait_secs = (PgStat_Counter) wait_secs;
	pgstat_unlock_entry(entry_ref);
}

/* ----------
 * pgstat_resqueue_rejected
 *
 * Called when ResLockAcquire() raises an error (portal cancelled or error
 * while waiting), or when the portal is bypassed (cost below threshold).
 * Removes the portal entry without counting exec time.
 *
 * IMPORTANT: This function may be called from inside a PG_CATCH block.
 * It must NOT call pgstat_prep_pending_entry(), which modifies the global
 * pgStatPending dlist and allocates memory that may be unsafe to use during
 * error recovery.  Instead, we update shared stats directly via
 * pgstat_get_entry_ref_locked(), which is PG_CATCH-safe because it only
 * allocates from TopMemoryContext derivatives and uses LWLock operations.
 * ----------
 */
void
pgstat_resqueue_rejected(uint32 portalid)
{
	PgStat_ResQueuePortalEntry *entry;
	bool		found;
	time_t		now;
	time_t		wait_secs;
	Oid			queueid;
	PgStat_EntryRef *entry_ref;
	PgStatShared_ResQueue *shqent;

	if (pgStatResQueuePortalHash == NULL)
		return;

	entry = (PgStat_ResQueuePortalEntry *)
		hash_search(pgStatResQueuePortalHash, &portalid, HASH_FIND, &found);

	if (!found)
		return;

	now = time(NULL);
	wait_secs = (entry->t_wait_start > 0) ? (now - entry->t_wait_start) : 0;
	if (wait_secs < 0)
		wait_secs = 0;

	queueid = entry->queueid;

	/* Remove portal entry first — hash_search(HASH_REMOVE) is PG_CATCH-safe. */
	hash_search(pgStatResQueuePortalHash, &portalid, HASH_REMOVE, NULL);

	/* Skip stat update if pgstat shared memory is already detached. */
	if (pgStatLocal.shared_hash == NULL)
		return;

	/*
	 * Update the shared stats entry directly, bypassing the pending
	 * mechanism.  pgstat_get_entry_ref_locked allocates only from
	 * TopMemoryContext derivatives and takes an LWLock, both of which are
	 * safe during error recovery.
	 */
	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RESQUEUE,
											InvalidOid, queueid, false);
	if (entry_ref == NULL)
		return;

	shqent = (PgStatShared_ResQueue *) entry_ref->shared_stats;
	shqent->stats.queries_rejected++;
	shqent->stats.elapsed_wait_secs += (PgStat_Counter) wait_secs;
	if ((PgStat_Counter) wait_secs > shqent->stats.max_wait_secs)
		shqent->stats.max_wait_secs = (PgStat_Counter) wait_secs;

	pgstat_unlock_entry(entry_ref);
}

/* ----------
 * pgstat_resqueue_exec_end
 *
 * Called from ResUnLockPortal() when a portal finishes execution (normal
 * completion, error, or cancel after admission).  Writes completion counters
 * directly to shared stats.
 * ----------
 */
void
pgstat_resqueue_exec_end(uint32 portalid)
{
	PgStat_ResQueuePortalEntry *entry;
	bool		found;
	time_t		now;
	time_t		exec_secs;
	PgStat_EntryRef *entry_ref;
	PgStatShared_ResQueue *shqent;

	if (pgStatResQueuePortalHash == NULL)
		return;

	/*
	 * pgstat_shutdown_hook (before_shmem_exit) runs before ProcKill
	 * (on_shmem_exit).  If AtExitCleanup_ResPortals calls us after pgstat
	 * has detached from shared memory, skip the stat update but still clean
	 * up the local hash entry to avoid a memory leak.
	 */
	if (pgStatLocal.shared_hash == NULL)
	{
		hash_search(pgStatResQueuePortalHash, &portalid, HASH_REMOVE, NULL);
		return;
	}

	entry = (PgStat_ResQueuePortalEntry *)
		hash_search(pgStatResQueuePortalHash, &portalid, HASH_FIND, &found);

	if (!found)
		return;

	now = time(NULL);
	exec_secs = (entry->t_exec_start > 0) ? (now - entry->t_exec_start) : 0;
	if (exec_secs < 0)
		exec_secs = 0;

	entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_RESQUEUE,
											InvalidOid, entry->queueid, false);
	if (entry_ref != NULL)
	{
		shqent = (PgStatShared_ResQueue *) entry_ref->shared_stats;
		shqent->stats.queries_completed++;
		shqent->stats.elapsed_exec_secs += (PgStat_Counter) exec_secs;
		if ((PgStat_Counter) exec_secs > shqent->stats.max_exec_secs)
			shqent->stats.max_exec_secs = (PgStat_Counter) exec_secs;
		pgstat_unlock_entry(entry_ref);
	}

	hash_search(pgStatResQueuePortalHash, &portalid, HASH_REMOVE, NULL);
}

/* ----------
 * pgstat_create_resqueue
 *
 * Called when a resource queue is created via DDL.  Ensures a stats entry
 * exists and is initialised.
 * ----------
 */
void
pgstat_create_resqueue(Oid queueid)
{
	pgstat_create_transactional(PGSTAT_KIND_RESQUEUE, InvalidOid, queueid);
	pgstat_get_entry_ref(PGSTAT_KIND_RESQUEUE, InvalidOid, queueid, true, NULL);
	pgstat_reset_entry(PGSTAT_KIND_RESQUEUE, InvalidOid, queueid, 0);
}

/* ----------
 * pgstat_drop_resqueue
 *
 * Called when a resource queue is dropped via DDL.
 * ----------
 */
void
pgstat_drop_resqueue(Oid queueid)
{
	pgstat_drop_transactional(PGSTAT_KIND_RESQUEUE, InvalidOid, queueid);
}

/* ----------
 * pgstat_fetch_stat_resqueue
 *
 * Return a palloc'd snapshot of statistics for the given resource queue OID,
 * or NULL if no stats entry exists.
 * ----------
 */
PgStat_StatResQueueEntry *
pgstat_fetch_stat_resqueue(Oid queueid)
{
	return (PgStat_StatResQueueEntry *)
		pgstat_fetch_entry(PGSTAT_KIND_RESQUEUE, InvalidOid, queueid);
}

/* ----------
 * pgstat_resqueue_flush_cb
 *
 * Flush pending per-queue delta counters into shared memory.
 * Called by pgstat_report_stat() for each entry with pending data.
 *
 * max_wait_secs and max_exec_secs are merged with MAX rather than addition.
 * ----------
 */
bool
pgstat_resqueue_flush_cb(PgStat_EntryRef *entry_ref, bool nowait)
{
	PgStat_ResQueueCounts *localent;
	PgStatShared_ResQueue *shqent;

	localent = (PgStat_ResQueueCounts *) entry_ref->pending;
	shqent = (PgStatShared_ResQueue *) entry_ref->shared_stats;

	if (!pgstat_lock_entry(entry_ref, nowait))
		return false;

#define RESQUEUE_ACC(fld)	shqent->stats.fld += localent->fld
	RESQUEUE_ACC(queries_submitted);
	RESQUEUE_ACC(queries_admitted);
	RESQUEUE_ACC(queries_rejected);
	RESQUEUE_ACC(queries_completed);
	RESQUEUE_ACC(elapsed_wait_secs);
	RESQUEUE_ACC(elapsed_exec_secs);
	RESQUEUE_ACC(total_cost);
	RESQUEUE_ACC(total_memory_kb);
#undef RESQUEUE_ACC

	/* max fields: merge with MAX */
	if (localent->max_wait_secs > shqent->stats.max_wait_secs)
		shqent->stats.max_wait_secs = localent->max_wait_secs;
	if (localent->max_exec_secs > shqent->stats.max_exec_secs)
		shqent->stats.max_exec_secs = localent->max_exec_secs;

	pgstat_unlock_entry(entry_ref);

	return true;
}

/* ----------
 * pgstat_resqueue_reset_timestamp_cb
 *
 * Reset the stat_reset_timestamp in the shared entry.
 * ----------
 */
void
pgstat_resqueue_reset_timestamp_cb(PgStatShared_Common *header, TimestampTz ts)
{
	((PgStatShared_ResQueue *) header)->stats.stat_reset_timestamp = ts;
}

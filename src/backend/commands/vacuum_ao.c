/*-------------------------------------------------------------------------
 *
 * vacuum_ao.c
 *	  VACUUM support for append-only tables.
 *
 *
 * Portions Copyright (c) 2016, VMware, Inc. or its affiliates
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * Overview
 * --------
 *
 * Vacuum of AO and AOCO tables happens in three phases:
 *
 * 1. Pre-cleanup phase
 *
 *   Truncate any old AWAITING_DROP segments to zero bytes. We would do this in
 *   the post-cleanup phase, anyway, but reclaiming space as early as possible
 *   is good. We might need the space to complete the compaction phase.
 *
 * 2. Compaction phase
 *
 *   Copy tuples from segments to new segmnents, leaving out dead tuples.
 *
 * 3. Post-cleanup phase.
 *
 *   Truncate any old AWAITING_DROP segments, making them insertable again. If there
 *   are no other transactions running (TODO: or we're in "aggressive mode" and want
 *   to risk "snapshot too old" errors), this can truncate the old segments left
 *   behind in the compaction phase.
 *
 *   Vacuum indexes.
 *
 *   Vacuum auxiliary heap tables.
 *
 * The orchestration of the phases mostly happens in vacuum_rel() (vacuum.c).
 * This file contains functions implementing the phases.
 *
 * The pre-cleanup and post-cleanup phases could run in a local transaction,
 * but the compaction phase needs a distributed transaction.
 * Currently, though, we run each phase in a distributed transaction; there's
 * no harm in that.
 *
 * Both lazy and FULL vacuum work the same on AO tables.
 *
 *
 * Why does compaction have to run in a distributed transaction?
 * ---------------------------------------------------------------------
 *
 * To determine the visibility of AO segments, we rely on the auxiliary
 * pg_aoseg_* heap table. The visiblity of the rows in the pg_aoseg table
 * determines which segments are visible to a snapshot.
 *
 * That works great currently, but if we switch to updating pg_aoseg in a
 * local transaction, some anomalies become possible. A distributed
 * transaction might see an inconsistent view of the segments, because one
 * row version in pg_aoseg is visible according to the distributed snapshot,
 * while another version of the same row is visible to its local snapshot.
 *
 * In fact, you can observe this anomaly even without appendonly tables, if
 * you modify a heap table in utility mode. Here's an example (as an
 * isolationtest2 test schedule):
 *
 * --------------------
 * DROP TABLE IF EXISTS utiltest;
 * CREATE TABLE utiltest (a int, t text);
 * INSERT INTO utiltest SELECT i as a, 'initial' FROM generate_series(1, 10) AS i;
 *
 * create or replace function myfunc() returns setof utiltest as $$ begin perform pg_sleep(10); return query select * from utiltest; end; $$ stable language plpgsql;
 *
 * -- Launch one backend to query the table with a delay. It will acquire
 * -- snapshot first, but scan the table only later. (A serializable snapshot
 * -- would achieve the same thing.)
 * 1&: select * from myfunc();
 *
 * -- Update normally via QD. The delayed query does not see this update, as
 * -- it acquire the snapshot before this.
 * 2: UPDATE utiltest SET t = 'updated' WHERE a <= 3;
 *
 * -- Update again in utility mode. Does the delayed query see this or not?
 * 0U: UPDATE utiltest  SET t = 'updated in utility mode' WHERE a <= 5;
 *
 * -- Get the delayed query's results. It returns 12 rows. It sees the
 * -- initial version of each row, but it *also* sees the new tuple
 * -- versions of the utility-mode update. The utility-updated rows have
 * -- an xmin that's visible to the local snapshot that the delayed query.
 * 1<:
 * --------------------
 *
 * In summary, this test case creates a table with 10 rows, and updates
 * some rows twice. One of the updates is made in utility mode. No rows are
 * deleted or inserted after the initial load, so any query on the table
 * should see 10 rows. But the query that's performed in the function sees
 * 12 rows. It sees two versions of the rows that were updated twice.
 *
 * This is clearly not ideal, but we can perhaps shrug it off as "don't do
 * that". If you update rows in utility mode, weird things can happen. But
 * it poses a problem for the idea of using local transactions for AO
 * vacuum. We can't let that anomaly to happen as a result of a normal VACUUM!
 *
 *
 * XXX: How does index vacuum work? We never reuse TIDs, right? So we can
 * vacuum indexes independently of dropping segments.
 *
 * XXX: We could relax the requirement for AccessExclusiveLock in Vacuum drop
 * phase with a little more effort. Scan could grab a share lock on the segfile
 * it's about to scan.
 *
 * IDENTIFICATION
 *	  src/backend/commands/vacuum_ao.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "access/table.h"
#include "access/aocs_compaction.h"
#include "access/aomd.h"
#include "access/appendonlywriter.h"
#include "access/appendonly_compaction.h"
#include "access/genam.h"
#include "access/multixact.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/pg_appendonly.h"
#include "cdb/cdbappendonlyam.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "commands/progress.h"
#include "commands/vacuum.h"
#include "pgstat.h"
#include "postmaster/autovacuum.h"
#include "storage/freespace.h"
#include "storage/proc.h"
#include "storage/procarray.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/pg_rusage.h"
#include "cdb/cdbappendonlyblockdirectory.h"


static void vacuum_appendonly_index(Relation indexRelation,
									Relation aoRelation,
									Bitmapset *dead_segs,
									int elevel,
									BufferAccessStrategy bstrategy,
									AOVacuumRelStats *vacrelstats,
									IndexBulkDeleteResult *result);

static bool appendonly_tid_reaped(ItemPointer itemptr, void *state);

static void vacuum_appendonly_fill_stats(Relation aorel, Snapshot snapshot, int elevel,
										 BlockNumber *rel_pages, double *rel_tuples,
										 int64 *dead_tuples, bool *relhasindex, BlockNumber *total_file_segs);
static int vacuum_appendonly_indexes(Relation aoRelation, int options, Bitmapset *dead_segs,
									 BufferAccessStrategy bstrategy, AOVacuumRelStats *vacrelstats);
static void ao_vacuum_rel_recycle_dead_segments(Relation onerel, VacuumParams *params,
												BufferAccessStrategy bstrategy, AOVacuumRelStats *vacrelstats);
static AOVacuumRelStats *init_vacrelstats(bool instrument);
static void cleanup_vacrelstats(AOVacuumRelStats **vacrelstatsp);
static void ao_report_index_vacuum_time(Relation indrel, TimestampTz starttime,
										double startdelaytime);
static void ao_vacuum_error_callback(void *arg);
static void ao_accum_resources(PgStat_CommonCounts *dst,
							   const PgStat_CommonCounts *src, bool subtract);
static void ao_measure_index_resources(Relation indrel,
									 LVExtStatCounters *counters,
									 IndexBulkDeleteResult *result,
									 AOVacuumRelStats *vacrelstats, bool final_cleanup, bool verbose);
static void ao_measure_table_resources(Relation rel,
									 AOVacuumRelStats *vacrelstats);

static void
ao_vacuum_rel_pre_cleanup(Relation onerel, VacuumParams *params, BufferAccessStrategy bstrategy, AOVacuumRelStats *vacrelstats)
{
	char	   *relname;
	int			elevel;
	int			options = params->options;
	FileSegTotals *fstotal;
	const int	initprog_index[] = {
		PROGRESS_VACUUM_PHASE,
		PROGRESS_VACUUM_TOTAL_HEAP_BLKS,
		PROGRESS_VACUUM_MAX_DEAD_TUPLES
	};
	int64		initprog_val[3];

	Assert(RelationStorageIsAO(onerel));

	if (options & VACOPT_VERBOSE)
		elevel = INFO;
	else
		elevel = DEBUG2;

	if (Gp_role == GP_ROLE_DISPATCH)
		elevel = DEBUG2; /* vacuum and analyze messages aren't interesting from the QD */

	relname = RelationGetRelationName(onerel);
	ereport(elevel,
			(errmsg("vacuuming \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(onerel)),
					relname)));

	/* Get statistics from the pg_aoseg table for progress reporting */
	if (RelationIsAoRows(onerel))
		fstotal = GetSegFilesTotals(onerel, GetActiveSnapshot());
	else
	{
		Assert(RelationIsAoCols(onerel));
		fstotal = GetAOCSSSegFilesTotals(onerel, GetActiveSnapshot());
	}

	/*
	 * Report that we are now in pre-cleanup phase, advertising total # of
	 * heap-equivalent blocks
	 */
	initprog_val[0] = PROGRESS_VACUUM_PHASE_AO_PRE_CLEANUP;
	initprog_val[1] = RelationGuessNumberOfBlocksFromSize(
		ao_rel_get_physical_size(onerel));
	initprog_val[2] = fstotal->totaltuples;
	pgstat_progress_update_multi_param(3, initprog_index, initprog_val);

	/* 
	 * Recycle AWAITING_DROP segments that are no longer visible to anyone.
	 *
	 * This is optional. We'll drop old AWAITING_DROP segments in the
	 * post-cleanup phase, too, but doing this first helps to reclaim some
	 * space earlier. The compaction phase might need the space.
	 *
	 * This could run in a local transaction.
	 */
	ao_vacuum_rel_recycle_dead_segments(onerel, params, bstrategy, vacrelstats);

	/*
	 * Also truncate all live segments to the EOF values stored in pg_aoseg.
	 * This releases space left behind by aborted inserts.
	 */
	AppendOptimizedTruncateToEOF(onerel, vacrelstats);
}


static void
ao_vacuum_rel_post_cleanup(Relation onerel, VacuumParams *params, BufferAccessStrategy bstrategy, AOVacuumRelStats *vacrelstats)
{
	BlockNumber	relpages;
	double		reltuples;
	int64		deadtuples;
	bool		relhasindex;
	/* AO/AOCO total file segment number, use type BlockNumber to
	 * represent same type with num_all_visible_pages in libpq.
	 */
	BlockNumber	total_file_segs;
	int			elevel;
	int			options = params->options;

	if (options & VACOPT_VERBOSE)
		elevel = INFO;
	else
		elevel = DEBUG2;

	if (Gp_role == GP_ROLE_DISPATCH)
		elevel = DEBUG2; /* vacuum and analyze messages aren't interesting from the QD */

	/*
	 * This could run in a *local* transaction:
	 *
	 * 1. Recycled any dead AWAITING_DROP segments, like in the
	 *    pre-cleanup phase.
	 *
	 * 2. Vacuum indexes.
	 * 
	 * 3. Drop/Truncate dead segments.
	 * 
	 * 4. Update statistics.
	 */
	Assert(RelationStorageIsAO(onerel));
	Assert(vacrelstats != NULL);

	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_AO_POST_CLEANUP);

	ao_vacuum_rel_recycle_dead_segments(onerel, params, bstrategy, vacrelstats);

	/* Update statistics in pg_class */
	vacuum_appendonly_fill_stats(onerel, GetActiveSnapshot(),
								 elevel,
								 &relpages,
								 &reltuples,
								 &deadtuples,
								 &relhasindex,
								 &total_file_segs);

	/*
	 * AO/AOCO tables have no per-tuple xmin/xmax, so freeze limits don't
	 * apply. Pass InvalidTransactionId/InvalidMultiXactId to keep
	 * relfrozenxid and relminmxid unchanged.
	 */
	vac_update_relstats(onerel,
						relpages,
						reltuples,
						total_file_segs, /* AO/AOCO does not currently have an equivalent to
							  Heap's 'all visible pages', use this field to represent
							  AO/AOCO's total segment file count */
						0, /* relallfrozen: AO has no visibility map */
						relhasindex,
						InvalidTransactionId,
						InvalidMultiXactId,
						NULL,
						NULL,
						false,
						true /* isvacuum */);

	vacrelstats->total_file_segs = total_file_segs;

	/* Report once all phase timers have been stopped. */
	vacrelstats->live_tuples = reltuples;
	vacrelstats->dead_tuples = deadtuples;

	SIMPLE_FAULT_INJECTOR("vacuum_ao_post_cleanup_end");
}

static void
ao_vacuum_rel_compact(Relation onerel, VacuumParams *params, BufferAccessStrategy bstrategy, AOVacuumRelStats *vacrelstats)
{
	int			compaction_segno;
	int			insert_segno;
	List	   *compacted_segments = NIL;
	List	   *compacted_and_inserted_segments = NIL;
	char	   *relname;
	int			elevel;
	int			options = params->options;

	/*
	 * This should run in a distributed transaction. But also allow utility
	 * mode. This also runs in the QD, but it should have no work to do because
	 * all data resides on QEs nodes.
	 */
	Assert(Gp_role == GP_ROLE_DISPATCH ||
		   Gp_role == GP_ROLE_UTILITY ||
		   DistributedTransactionContext == DTX_CONTEXT_QE_TWO_PHASE_IMPLICIT_WRITER ||
		   DistributedTransactionContext == DTX_CONTEXT_QE_TWO_PHASE_EXPLICIT_WRITER);
	Assert(RelationStorageIsAO(onerel));
	Assert(vacrelstats != NULL);

	if (options & VACOPT_VERBOSE)
		elevel = INFO;
	else
		elevel = DEBUG2;

	if (Gp_role == GP_ROLE_DISPATCH)
		elevel = DEBUG2; /* vacuum and analyze messages aren't interesting from the QD */

	relname = RelationGetRelationName(onerel);
	ereport(elevel,
			(errmsg("compacting \"%s.%s\"",
					get_namespace_name(RelationGetNamespace(onerel)),
					relname)));

	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_AO_COMPACT);
	/*
	 * Compact all the segfiles. Repeat as many times as required.
	 *
	 * XXX: Because we compact all segfiles in one transaction, this can
	 * require up 2x the disk space. Alternatively, we could split this into
	 * multiple transactions. The problem with that is that the updates to
	 * pg_aoseg needs to happen in a distributed transaction (Problem 3), so
	 * we would need to coordinate the transactions from the QD.
	 */
	insert_segno = -1;
	while ((compaction_segno = ChooseSegnoForCompaction(onerel, compacted_and_inserted_segments)) != -1)
	{
		/*
		 * Compact this segment. (If the segment doesn't need compaction,
		 * AppendOnlyCompact() will fall through quickly).
		 */
		compacted_segments = lappend_int(compacted_segments, compaction_segno);
		compacted_and_inserted_segments = lappend_int(compacted_and_inserted_segments,
													  compaction_segno);

		/* XXX: maybe print this deeper, only if there's work to be done? */
		if (Debug_appendonly_print_compaction)
			elog(LOG, "compacting segno %d of %s", compaction_segno, relname);

		if (RelationIsAoRows(onerel))
			AppendOnlyCompact(onerel,
							  compaction_segno,
							  &insert_segno,
							  (options & VACOPT_FULL) != 0,
							  compacted_segments,
							  vacrelstats);
		else
		{
			Assert(RelationIsAoCols(onerel));
			AOCSCompact(onerel,
						compaction_segno,
						&insert_segno,
						(options & VACOPT_FULL) != 0,
						compacted_segments,
						vacrelstats);
		}

		if (insert_segno != -1)
			compacted_and_inserted_segments = list_append_unique_int(compacted_and_inserted_segments,
																	 insert_segno);

		/*
		 * AppendOnlyCompact() updates pg_aoseg. Increment the command counter, so
		 * that we can update the insertion target pg_aoseg row again.
		 */
		CommandCounterIncrement();
	}

	SIMPLE_FAULT_INJECTOR("vacuum_ao_after_compact");
}

static AOVacuumRelStats *
init_vacrelstats(bool instrument)
{
	AOVacuumRelStats *vacrelstats;
	MemoryContext old_context;

	old_context = MemoryContextSwitchTo(TopMemoryContext);
	vacrelstats = (AOVacuumRelStats *) palloc0(sizeof(AOVacuumRelStats));
	if (instrument)
		vacrelstats->extstats = palloc0(sizeof(AOVacuumExtStats));
	MemoryContextSwitchTo(old_context);

	return vacrelstats;
}

/*
 * ao_vacuum_rel()
 *
 * Common interface for vacuuming Append-Optimized table.
 */
void
ao_vacuum_rel(Relation rel, VacuumParams *params, BufferAccessStrategy bstrategy)
{
	static AOVacuumRelStats *vacrelstats = NULL;
	ErrorContextCallback errcallback;
	instr_time	phase_start;
	instr_time	phase_end;
	double		phase_start_delay;
	LVExtStatCounters *extcounters;
	bool		verbose = (params->options & VACOPT_VERBOSE) != 0;
	bool		extstats = verbose || (set_report_vacuum_hook != NULL);
	PgStat_CommonCounts index_start = {0};
	Assert(RelationStorageIsAO(rel));
	Assert(params != NULL);

	int ao_vacuum_phase = (params->options & VACUUM_AO_PHASE_MASK);

	/*
	 * The phases of one vacuum share these stats and free them at the end of
	 * the last phase, so a vacuum that failed in between leaves them behind.
	 * Drop such leftovers: otherwise the counters of the failed vacuum are
	 * taken for the next one, whose progress is never reported because the
	 * progress command is only
	 * started along with the stats.
	 */
	if (vacrelstats != NULL &&
		(ao_vacuum_phase == VACOPT_AO_PRE_CLEANUP_PHASE ||
		 vacrelstats->relid != RelationGetRelid(rel)))
		cleanup_vacrelstats(&vacrelstats);

	if (!vacrelstats)
	{
		if (ao_vacuum_phase != VACOPT_AO_PRE_CLEANUP_PHASE && Gp_role == GP_ROLE_EXECUTE)
		{
			/*
			 * If we enter here, it indicates the previous vacuum worker exited
			 * and we are in a new worker, previous collected data in vacrelstats
			 * will be lost.
			 */
			SIMPLE_FAULT_INJECTOR("vacuum_worker_changed");

			elog(LOG, "Vacuum worker process is changed, progressing status is reset, current state is %d.",
				 ao_vacuum_phase);
		}

		pgstat_progress_start_command(PROGRESS_COMMAND_VACUUM, RelationGetRelid(rel));
		vacrelstats = init_vacrelstats(extstats);
		vacrelstats->relid = RelationGetRelid(rel);
	}

	/* Count the vacuums of the relation that an error interrupts */
	errcallback.callback = ao_vacuum_error_callback;
	errcallback.arg = rel;
	errcallback.previous = error_context_stack;
	error_context_stack = &errcallback;

	/* Sample the resource usage of the phase for the extended statistics */
	extcounters = extvac_stats_start(rel, verbose);
	if (verbose)
		index_start = vacrelstats->extstats->indexes;
	INSTR_TIME_SET_CURRENT(phase_start);
	phase_start_delay = VacuumDelayTime;

	/*
	 * Do the actual work --- either FULL or "lazy" vacuum
	 */
	if (ao_vacuum_phase == VACOPT_AO_PRE_CLEANUP_PHASE)
		ao_vacuum_rel_pre_cleanup(rel, params, bstrategy, vacrelstats);
	else if (ao_vacuum_phase == VACOPT_AO_COMPACT_PHASE)
		ao_vacuum_rel_compact(rel, params, bstrategy, vacrelstats);
	else if (ao_vacuum_phase == VACOPT_AO_POST_CLEANUP_PHASE)
		ao_vacuum_rel_post_cleanup(rel, params, bstrategy, vacrelstats);
	else
		/* Do nothing here, we will launch the stages later */
		Assert(ao_vacuum_phase == 0);

	if (ao_vacuum_phase != 0)
	{
		INSTR_TIME_SET_CURRENT(phase_end);
		INSTR_TIME_SUBTRACT(phase_end, phase_start);
		vacrelstats->vacuum_time += INSTR_TIME_GET_MILLISEC(phase_end);
		vacrelstats->delay_time += VacuumDelayTime - phase_start_delay;
	}

	if (extstats && ao_vacuum_phase != 0)
	{
		extvac_stats_end(rel, extcounters, &extcounters->report.common);
		if (verbose && Gp_role != GP_ROLE_DISPATCH)
		{
			PgStat_CommonCounts phase_usage = extcounters->report.common;
			PgStat_CommonCounts index_usage = vacrelstats->extstats->indexes;
			const char *phase_name =
				ao_vacuum_phase == VACOPT_AO_PRE_CLEANUP_PHASE ? _("AO pre-cleanup (excluding indexes)") :
				ao_vacuum_phase == VACOPT_AO_COMPACT_PHASE ? _("AO compaction (excluding indexes)") :
				_("AO post-cleanup (excluding indexes)");

			/* Only index passes executed during this phase are excluded. */
			ao_accum_resources(&index_usage, &index_start, true);
			ao_accum_resources(&phase_usage, &index_usage, true);
			extvac_stats_log(rel, phase_name, &phase_usage);
		}
		ao_accum_resources(&vacrelstats->extstats->phases,
						   &extcounters->report.common, false);
	}

	if (ao_vacuum_phase == VACOPT_AO_POST_CLEANUP_PHASE)
	{
		PgStat_VacuumStats stats = {0};

		stats.tuples_deleted = vacrelstats->num_dead_tuples;
		stats.dead_tuples = vacrelstats->dead_tuples;
		stats.recently_dead_tuples = vacrelstats->dead_tuples;
		stats.pages_scanned = vacrelstats->pages_scanned;
		stats.total_file_segs = vacrelstats->total_file_segs;
		stats.compacted_segments = vacrelstats->compacted_segments;
		stats.tuples_moved = vacrelstats->tuples_moved;
		stats.pages_removed = vacrelstats->nbytes_truncated / BLCKSZ +
			(vacrelstats->nbytes_truncated % BLCKSZ != 0);
		pgstat_report_vacuum_stats(rel, &stats);

		/* The last phase: report only the phases executed by this worker. */
		pgstat_report_vacuum_elapsed(RelationGetRelid(rel),
									 rel->rd_rel->relisshared,
									 vacrelstats->live_tuples, vacrelstats->dead_tuples,
									 (PgStat_Counter) rint(vacrelstats->vacuum_time),
									 (PgStat_Counter) rint(vacrelstats->delay_time),
									 false); /* AO has no failsafe mode. */
		if (extstats)
			ao_measure_table_resources(rel, vacrelstats);
		pgstat_progress_end_command();
		cleanup_vacrelstats(&vacrelstats);
	}

	if (extcounters != NULL)
		pfree(extcounters);
	error_context_stack = errcallback.previous;
}

/*
 * Error context callback of an append-optimized vacuum phase.
 *
 * Like vacuum_error_callback() of the heap, count a vacuum interrupted by an
 * actual ERROR, not by a lower-severity report that merely carries this
 * context, in vacuum_interrupt_count of the database.  We are inside the
 * error handler, so pgstat_count_vacuum_error() only bumps a counter.  The
 * callback adds no context line: the phases report their own progress.
 */
static void
ao_vacuum_error_callback(void *arg)
{
	Relation	rel = (Relation) arg;

	if (geterrlevel() == ERROR)
		pgstat_count_vacuum_error(rel->rd_rel->relisshared);
}

/*
 * Accumulate one vacuum pass over an index of an append-optimized table
 * into the index's cumulative vacuum and delay times, as the heap does in
 * lazy_vacuum_one_index() and lazy_cleanup_one_index().
 */
static void
ao_report_index_vacuum_time(Relation indrel, TimestampTz starttime,
							double startdelaytime)
{
	pgstat_report_index_vacuum_time(indrel,
									TimestampDifferenceMilliseconds(starttime,
																	GetCurrentTimestamp()),
									(PgStat_Counter) rint(VacuumDelayTime -
														  startdelaytime),
									IsAutoVacuumWorkerProcess());
}

/*
 * Add the resource usage counters of src to dst, or subtract them from it.
 * The tuple counter is left alone.
 */
static void
ao_accum_resources(PgStat_CommonCounts *dst, const PgStat_CommonCounts *src,
				   bool subtract)
{
	if (subtract)
	{
		dst->total_blks_read -= src->total_blks_read;
		dst->total_blks_hit -= src->total_blks_hit;
		dst->total_blks_dirtied -= src->total_blks_dirtied;
		dst->total_blks_written -= src->total_blks_written;
		/* Per-relation block counts do not include the indexes. */
		dst->blk_read_time -= src->blk_read_time;
		dst->blk_write_time -= src->blk_write_time;
		dst->wal_records -= src->wal_records;
		dst->wal_fpi -= src->wal_fpi;
		dst->wal_bytes -= src->wal_bytes;
	}
	else
	{
		dst->total_blks_read += src->total_blks_read;
		dst->total_blks_hit += src->total_blks_hit;
		dst->total_blks_dirtied += src->total_blks_dirtied;
		dst->total_blks_written += src->total_blks_written;
		dst->blks_fetched += src->blks_fetched;
		dst->blks_hit += src->blks_hit;
		dst->blk_read_time += src->blk_read_time;
		dst->blk_write_time += src->blk_write_time;
		dst->wal_records += src->wal_records;
		dst->wal_fpi += src->wal_fpi;
		dst->wal_bytes += src->wal_bytes;
	}
}

/*
 * Report one pass over an index of an append-optimized table to
 * set_report_vacuum_hook, as lazy_vacuum_one_index() does for the heap, and
 * remember its resource usage, which the table's report must not count
 * again.
 */
static void
ao_measure_index_resources(Relation indrel, LVExtStatCounters *counters,
						 IndexBulkDeleteResult *result,
						 AOVacuumRelStats *vacrelstats, bool final_cleanup, bool verbose)
{
	PgStat_VacuumRelationCounts *report = &counters->report;

	extvac_stats_end(indrel, counters, &report->common);
	if (verbose)
		extvac_stats_log(indrel, _("AO index vacuum"), &report->common);
	report->type = PGSTAT_EXTVAC_INDEX;
	report->common.tuples_deleted = (int64) result->tuples_removed;
	report->pages_deleted = result->pages_newly_deleted;
	if (final_cleanup && result->pages_deleted > result->pages_free)
		report->dead_pages = result->pages_deleted - result->pages_free;


	ao_accum_resources(&vacrelstats->extstats->indexes, &report->common, false);
	pfree(counters);
}

/*
 * Report the vacuum of an append-optimized table, all of its phases, to
 * set_report_vacuum_hook.
 *
 * The counters that mean something for append-optimized storage are set:
 * the tuples discarded, moved or left hidden, the segments actually
 * compacted, the segment count at completion, and the space scanned or
 * released in heap-equivalent pages.  Per-tuple freezing, heap pruning and
 * heap visibility-map counters stay zero.
 */
static void
ao_measure_table_resources(Relation rel, AOVacuumRelStats *vacrelstats)
{
	PgStat_VacuumRelationCounts report;

	memset(&report, 0, sizeof(report));
	report.type = PGSTAT_EXTVAC_TABLE;
	report.common = vacrelstats->extstats->phases;
	ao_accum_resources(&report.common, &vacrelstats->extstats->indexes, true);
	report.common.tuples_deleted = vacrelstats->num_dead_tuples;
	report.table.recently_dead_tuples = vacrelstats->dead_tuples;
	report.table.pages_scanned = vacrelstats->pages_scanned;
	report.table.total_file_segs = vacrelstats->total_file_segs;
	report.table.compacted_segments = vacrelstats->compacted_segments;
	report.table.tuples_moved = vacrelstats->tuples_moved;
	report.table.pages_removed =
		vacrelstats->nbytes_truncated / BLCKSZ +
		(vacrelstats->nbytes_truncated % BLCKSZ != 0);

}

/*
 * Recycling AWAITING_DROP segments.
 */
static void
ao_vacuum_rel_recycle_dead_segments(Relation onerel, VacuumParams *params,
									BufferAccessStrategy bstrategy, AOVacuumRelStats *vacrelstats)
{
	Bitmapset	*dead_segs;
	int			options = params->options;
	bool		need_drop;

	dead_segs = AppendOptimizedCollectDeadSegments(onerel);
	need_drop = !bms_is_empty(dead_segs);
	if (need_drop)
	{
		/*
		 * Vacuum indexes only when we do find AWAITING_DROP segments.
		 *
		 * Do index vacuuming before dropping dead segments for data
		 * consistency and crash safety. If dropping dead segments before
		 * cleaning up index tuples, the following issues may occur:
		 * 
		 * a) The dead segment file becomes available as soon as dropping
		 * complete. Concurrent inserts may fill it with new tuples hence
		 * might be deleted soon in the following index vacuuming;
		 * 
		 * b) Crash happens in-between ao_vacuum_rel_recycle_dead_segments()
		 * and vacuum_appendonly_indexes() result in losing the opportunity
		 * to clean index entries fully as a state for which index tuples
		 * to delete will be lost in this case.
		 * 
		 * So make sure to vacuum indexs to be based on persistent information
		 * (AWAITING_DROP state in pg_aoseg) to cleanup dead index tuples
		 * effectively.
		 */
		vacuum_appendonly_indexes(onerel, options, dead_segs, bstrategy, vacrelstats);
		/*
		 * Truncate above collected AWAITING_DROP segments to 0 byte.
		 * AppendOptimizedCollectDeadSegments() should guarantee that
		 * no transaction is able to access the dead segments for being
		 * marked as AWAITING_DROP as well as cutoff xid screening.
		 * ExclusiveLock will be held in case of concurrent VACUUM being
		 * on the same file.
		 */
		AppendOptimizedDropDeadSegments(onerel, dead_segs, vacrelstats);
	}
	else
	{
		/*
		 * If no AWAITING_DROP segments were found, we called
		 * vacuum_appendonly_indexes() in post_cleanup phase
		 * for updating statistics.
		 */
		if ((options & VACUUM_AO_PHASE_MASK) == VACOPT_AO_POST_CLEANUP_PHASE)
			vacuum_appendonly_indexes(onerel, options, dead_segs, bstrategy, vacrelstats);
	}

	bms_free(dead_segs);
}

/*
 * vacuum_appendonly_indexes()
 *
 * Perform a vacuum on all indexes of an append-only relation.
 *
 * It returns the number of indexes on the relation.
 */
static int
vacuum_appendonly_indexes(Relation aoRelation, int options, Bitmapset *dead_segs,
						  BufferAccessStrategy bstrategy, AOVacuumRelStats *vacrelstats)
{
	int			i;
	Relation   *Irel;
	int			nindexes;
	bool		final_cleanup =
		(options & VACUUM_AO_PHASE_MASK) == VACOPT_AO_POST_CLEANUP_PHASE;

	Assert(RelationStorageIsAO(aoRelation));

	if (Debug_appendonly_print_compaction)
		elog(LOG, "Vacuum indexes for append-only relation %s",
			 RelationGetRelationName(aoRelation));
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 PROGRESS_VACUUM_PHASE_VACUUM_INDEX);

	/* Now open all indexes of the relation */
	if ((options & VACOPT_FULL))
		vac_open_indexes(aoRelation, AccessExclusiveLock, &nindexes, &Irel);
	else
		vac_open_indexes(aoRelation, RowExclusiveLock, &nindexes, &Irel);

	/* Clean/scan index relation(s) */
	if (Irel != NULL)
	{
		int elevel;

		if (options & VACOPT_VERBOSE)
			elevel = INFO;
		else
			elevel = DEBUG2;

		/* just scan indexes to update statistic */
		if (Gp_role == GP_ROLE_DISPATCH || bms_is_empty(dead_segs))
		{
			for (i = 0; i < nindexes; i++)
			{
				TimestampTz istarttime = GetCurrentTimestamp();
				double		startdelaytime = VacuumDelayTime;
				LVExtStatCounters *extcounters;
				IndexBulkDeleteResult result = {0};

				extcounters = extvac_stats_start(Irel[i], elevel == INFO);
				scan_index(Irel[i],
						   aoRelation,
						   elevel,
						   bstrategy,
						   &result);
				vacuum_report_index_stats(Irel[i], &result, 0, 0, final_cleanup);
				ao_report_index_vacuum_time(Irel[i], istarttime, startdelaytime);
				if (extcounters != NULL)
					ao_measure_index_resources(Irel[i], extcounters, &result,
											 vacrelstats, final_cleanup,
											 elevel == INFO && Gp_role != GP_ROLE_DISPATCH);
			}
		}
		else
		{
			for (i = 0; i < nindexes; i++)
			{
				TimestampTz istarttime = GetCurrentTimestamp();
				double		startdelaytime = VacuumDelayTime;
				LVExtStatCounters *extcounters;
				IndexBulkDeleteResult result = {0};

				extcounters = extvac_stats_start(Irel[i], elevel == INFO);
				vacuum_appendonly_index(Irel[i],
										aoRelation,
										dead_segs,
										elevel,
										bstrategy,
										vacrelstats,
										&result);
				vacuum_report_index_stats(Irel[i], &result, 0, 0, final_cleanup);
				ao_report_index_vacuum_time(Irel[i], istarttime, startdelaytime);
				if (extcounters != NULL)
					ao_measure_index_resources(Irel[i], extcounters, &result,
											 vacrelstats, final_cleanup,
											 elevel == INFO && Gp_role != GP_ROLE_DISPATCH);
			}
		}
	}

	vac_close_indexes(nindexes, Irel, NoLock);
	pgstat_progress_update_param(PROGRESS_VACUUM_PHASE,
								 final_cleanup ?
								 PROGRESS_VACUUM_PHASE_AO_POST_CLEANUP : PROGRESS_VACUUM_PHASE_AO_PRE_CLEANUP);
	return nindexes;
}

/*
 * Vacuums an index on an append-only table.
 *
 * This is called after an append-only segment file compaction to move
 * all tuples from the compacted segment files.
 */
static void
vacuum_appendonly_index(Relation indexRelation,
						Relation aoRelation,
						Bitmapset *dead_segs,
						int elevel,
						BufferAccessStrategy bstrategy,
						AOVacuumRelStats *vacrelstats,
						IndexBulkDeleteResult *result)
{
	IndexBulkDeleteResult *stats;
	IndexVacuumInfo ivinfo = {0};
	PGRUsage	ru0;

	Assert(RelationIsValid(indexRelation));

	pg_rusage_init(&ru0);

	ivinfo.index = indexRelation;
	ivinfo.heaprel = aoRelation;
	ivinfo.analyze_only = false;
	ivinfo.message_level = elevel;
	/* 
	 * We can only provide the AO rel's reltuples as an estimate
	 * (similar to heapam. See: lazy_vacuum_index()).
	 */
	ivinfo.num_heap_tuples = aoRelation->rd_rel->reltuples;
	ivinfo.estimated_count = true;
	ivinfo.strategy = bstrategy;
	ivinfo.heaprel = aoRelation;

	/* Do bulk deletion */
	stats = index_bulk_delete(&ivinfo, NULL, appendonly_tid_reaped,
							  (void *) dead_segs);
	vacrelstats->num_index_vacuumed++;
	pgstat_progress_update_param(PROGRESS_VACUUM_NUM_INDEX_VACUUMS,
								 vacrelstats->num_index_vacuumed);

	SIMPLE_FAULT_INJECTOR("vacuum_ao_after_index_delete");

	/* Do post-VACUUM cleanup */
	stats = index_vacuum_cleanup(&ivinfo, stats);

	if (!stats)
		return;

	/* Hand the counts of the pass to the caller, for the statistics */
	if (result)
		*result = *stats;

	/*
	 * Now update statistics in pg_class, but only if the index says the count
	 * is accurate.
	 */
	if (!stats->estimated_count)
		vac_update_relstats(indexRelation,
							stats->num_pages, stats->num_index_tuples,
							0, /* relallvisible */
							0, /* relallfrozen */
							false,
							InvalidTransactionId,
							InvalidMultiXactId,
							NULL,
							NULL,
							false,
							true /* isvacuum */);

	ereport(elevel,
			(errmsg("index \"%s\" now contains %.0f row versions in %u pages",
					RelationGetRelationName(indexRelation),
					stats->num_index_tuples,
					stats->num_pages),
			 errdetail("%.0f index row versions were removed.\n"
			 "%u index pages have been deleted, %u are currently reusable.\n"
					   "%s.",
					   stats->tuples_removed,
					   stats->pages_deleted, stats->pages_free,
					   pg_rusage_show(&ru0))));

	pfree(stats);
}

/*
 * appendonly_tid_reaped()
 *
 * Is a particular tid for an appendonly reaped? the inputed state
 * is a bitmap of dropped segno. The index entry is reaped only
 * because of the segment no is a member of dead_segs. In this
 * way, no need to scan visibility map so the performance would be
 * good.
 *
 * This has the right signature to be an IndexBulkDeleteCallback.
 */
static bool
appendonly_tid_reaped(ItemPointer itemptr, void *state)
{
	Bitmapset *dead_segs = (Bitmapset *) state;
	int segno = AOTupleIdGet_segmentFileNum((AOTupleId *)itemptr);

	return bms_is_member(segno, dead_segs);
}

/*
 * Fills in the relation statistics for an append-only relation.
 *
 *	This information is used to update the reltuples and relpages information
 *	in pg_class. reltuples is the same as "pg_aoseg_<oid>:tupcount"
 *	column and we simulate relpages by subdividing the eof value
 *	("pg_aoseg_<oid>:eof") over the defined page size.
 *  total_field_segs will be set only for AO/AOCO relation.
 */
static void
vacuum_appendonly_fill_stats(Relation aorel, Snapshot snapshot, int elevel,
							 BlockNumber *rel_pages, double *rel_tuples,
							 int64 *dead_tuples, bool *relhasindex, BlockNumber *total_file_segs)
{
	FileSegTotals *fstotal;
	BlockNumber nblocks;
	char	   *relname;
	double		num_tuples;
	int64       hidden_tupcount;
	AppendOnlyVisimap visimap;
	Oid			visimaprelid;
	Oid			visimapidxid;

	Assert(RelationStorageIsAO(aorel));

	relname = RelationGetRelationName(aorel);

	/* get updated statistics from the pg_aoseg table */
	if (RelationIsAoRows(aorel))
	{
		fstotal = GetSegFilesTotals(aorel, snapshot);
	}
	else
	{
		Assert(RelationIsAoCols(aorel));
		fstotal = GetAOCSSSegFilesTotals(aorel, snapshot);
	}

	/* calculate the values we care about */
	num_tuples = (double)fstotal->totaltuples;
	nblocks = (uint32)RelationGetNumberOfBlocks(aorel);

	GetAppendOnlyEntryAuxOids(aorel,
							  NULL, NULL, NULL,
							  &visimaprelid, &visimapidxid);

	AppendOnlyVisimap_Init(&visimap,
						   visimaprelid,
						   visimapidxid,
						   AccessShareLock,
						   snapshot);
	hidden_tupcount = AppendOnlyVisimap_GetRelationHiddenTupleCount(&visimap);
	num_tuples -= hidden_tupcount;
	Assert(num_tuples > -1.0);
	AppendOnlyVisimap_Finish(&visimap, AccessShareLock);

	if (Debug_appendonly_print_compaction)
		elog(LOG,
			 "Gather statistics after vacuum for append-only relation %s: "
			 "page count %d, tuple count %f",
			 relname,
			 nblocks, num_tuples);

	*rel_pages = nblocks;
	*rel_tuples = num_tuples;
	*dead_tuples = hidden_tupcount;
	*relhasindex = aorel->rd_rel->relhasindex;
	*total_file_segs = fstotal->totalfilesegs;

	ereport(elevel,
			(errmsg("\"%s\": found %.0f rows in %u pages.",
					relname, num_tuples, nblocks)));
	pfree(fstotal);
}

static void
cleanup_vacrelstats(AOVacuumRelStats **vacrelstats)
{
	if ((*vacrelstats)->extstats != NULL)
		pfree((*vacrelstats)->extstats);
	pfree(*vacrelstats);
	*vacrelstats = NULL;
}

/*
 *	scan_index() -- scan one index relation to update pg_class statistics.
 *
 * We use this when we have no deletions to do.
 */
void
scan_index(Relation indrel, Relation aorel, int elevel, BufferAccessStrategy vac_strategy,
		   IndexBulkDeleteResult *result)
{
	IndexBulkDeleteResult *stats;
	IndexVacuumInfo ivinfo = {0};
	PGRUsage	ru0;

	pg_rusage_init(&ru0);

	ivinfo.index = indrel;
	ivinfo.heaprel = aorel;
	ivinfo.analyze_only = false;
	ivinfo.message_level = elevel;
	/* 
	 * We can only provide the AO rel's reltuples as an estimate
	 * (similar to heapam. See: lazy_vacuum_index()).
	 */
	ivinfo.num_heap_tuples = aorel->rd_rel->reltuples;
	ivinfo.estimated_count = true;
	ivinfo.strategy = vac_strategy;
	ivinfo.heaprel = aorel;


	/* Do post-VACUUM cleanup */
	stats = index_vacuum_cleanup(&ivinfo, NULL);

	if (!stats)
		return;

	/* Hand the counts of the pass to the caller, for the statistics */
	if (result)
		*result = *stats;

	/*
	 * Now update statistics in pg_class, but only if the index says the count
	 * is accurate.
	 */
	if (!stats->estimated_count)
		vac_update_relstats(indrel,
							stats->num_pages, stats->num_index_tuples,
							0, /* relallvisible, don't bother for indexes */
							0, /* relallfrozen */
							false,
							InvalidTransactionId,
							InvalidMultiXactId,
							NULL,
							NULL,
							false,
							true /* isvacuum */);

	ereport(elevel,
			(errmsg("index \"%s\" now contains %.0f row versions in %u pages",
					RelationGetRelationName(indrel),
					stats->num_index_tuples,
					stats->num_pages),
	errdetail("%u index pages have been deleted, %u are currently reusable.\n"
			  "%s.",
			  stats->pages_deleted, stats->pages_free,
			  pg_rusage_show(&ru0))));

	pfree(stats);
}

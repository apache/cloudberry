/*------------------------------------------------------------------------------
 *
 * appendonly_compaction
 *
 * Copyright (c) 2013-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/access/appendonly_compaction.h
 *
 *------------------------------------------------------------------------------
*/
#ifndef APPENDONLY_COMPACTION_H
#define APPENDONLY_COMPACTION_H

#include "pgstat.h"
#include "datatype/timestamp.h"
#include "nodes/pg_list.h"
#include "access/appendonly_visimap.h"
#include "utils/rel.h"
#include "access/memtup.h"
#include "executor/tuptable.h"

#define APPENDONLY_COMPACTION_SEGNO_INVALID (-1)

/* Optional resource counters for VACUUM VERBOSE. */
typedef struct AOVacuumExtStats
{
	PgStat_CommonCounts indexes;
	PgStat_CommonCounts index_inserts;
} AOVacuumExtStats;

/*
 * Stats for progress reporting.
 * This is AO/AOCO counterpart of LVRelStats for Heap. It lives throughout
 * all phases of AO VACUUM.
 */
typedef struct AOVacuumRelStats
{
	int64	nbytes_truncated;	/* current # of bytes truncated from segment file */
	int64	num_dead_tuples;	/* current # of dead tuples */
	int		num_index_vacuumed; /* current # of indexes been vacuumed */
	/* Index resources are excluded from each phase's VERBOSE report. */
	AOVacuumExtStats *extstats; /* allocated only for VERBOSE */
	/* Legacy timing-report baseline, retained until elapsed-time reporting. */
	TimestampTz starttime;
	double		startdelaytime;
	/* Active phase durations and delays in milliseconds, excluding phase gaps. */
	double		vacuum_time;
	double		delay_time;
	int64		live_tuples;
	int64		dead_tuples;

	/* the relation these stats were started for */
	Oid			relid;
} AOVacuumRelStats;

extern void ao_vacuum_insert_index_tuples(struct ResultRelInfo *resultRelInfo,
										 TupleTableSlot *slot, struct EState *estate,
										 AOVacuumRelStats *vacrelstats);
extern Bitmapset *AppendOptimizedCollectDeadSegments(Relation aorel);
extern void AppendOptimizedDropDeadSegments(Relation aorel, Bitmapset *segnos, AOVacuumRelStats *vacrelstats);
extern void AppendOnlyCompact(Relation aorel,
							  int compaction_segno,
							  int *insert_segno,
							  bool isFull,
							  List *avoid_segnos,
							  AOVacuumRelStats *vacrelstats);
extern bool AppendOnlyCompaction_ShouldCompact(
								   Relation aoRelation,
								   int segno,
								   int64 segmentTotalTupcount,
								   bool isFull,
								   Snapshot appendOnlyMetaDataSnapshot);
extern void AppendOnlyThrowAwayTuple(Relation rel, TupleTableSlot *slot, MemTupleBinding *mt_bind);
extern void AppendOptimizedTruncateToEOF(Relation aorel, AOVacuumRelStats *vacrelstats);

#endif

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * ext_vacuum_statistics - Extended vacuum statistics for PostgreSQL
 *
 * This module collects detailed vacuum statistics (I/O, WAL, timing, etc.)
 * at relation and database level by hooking into the vacuum reporting path.
 * Statistics are stored via pgstat custom statistics. Management of statistics
 * storage and output functions are implemented in this module.
 */
#include "postgres.h"

#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/guc.h"
#include "utils/pgstat_internal.h"
#include "utils/pgstat_kind.h"
#include "utils/tuplestore.h"
#include "utils/timestamp.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* Two kinds: relations (tables/indexes) and database aggregates */
#define PGSTAT_KIND_EXTVAC_RELATION	24
#define PGSTAT_KIND_EXTVAC_DB		25

#define SJ_NODENAME		"vacuum_statistics"

/*  GUCs  */
static bool evs_enabled = true;

/*  Hooks  */
static set_report_vacuum_hook_type prev_report_vacuum_hook = NULL;
static object_access_hook_type prev_object_access_hook = NULL;

/*  Forward declarations  */
static void pgstat_report_vacuum_extstats(Oid tableoid, bool shared,
										  PgStat_VacuumRelationCounts * params);
static void extvac_object_access(ObjectAccessType access, Oid classId,
								 Oid objectId, int subId, void *arg);

/* Shared memory entry for vacuum stats; one per relation or database. */
typedef struct PgStatShared_ExtVacEntry
{
	PgStatShared_Common header;
	PgStat_VacuumRelationCounts stats;
}			PgStatShared_ExtVacEntry;

/* A reset clears counters, not the table/index/database discriminator. */
static void
extvac_reset_data(PgStatShared_Common *header)
{
	PgStatShared_ExtVacEntry *entry = (PgStatShared_ExtVacEntry *) header;
	ExtVacReportType type = entry->stats.type;

	memset(&entry->stats, 0, sizeof(entry->stats));
	entry->stats.type = type;
}

/* PgStat kind for per-relation vacuum statistics (tables/indexes) */
static const PgStat_KindInfo extvac_relation_kind_info = {
	.name = "ext_vacuum_statistics_relation",
	.fixed_amount = false,
	.accessed_across_databases = true,
	.write_to_file = true,
	.shared_size = sizeof(PgStatShared_ExtVacEntry),
	.shared_data_off = offsetof(PgStatShared_ExtVacEntry, stats),
	.shared_data_len = sizeof(PgStat_VacuumRelationCounts),
	.pending_size = 0,
	.flush_pending_cb = NULL,
	.reset_data_cb = extvac_reset_data,
};

/* PgStat kind for per-database aggregated vacuum statistics */
static const PgStat_KindInfo extvac_db_kind_info = {
	.name = "ext_vacuum_statistics_db",
	.fixed_amount = false,
	.accessed_across_databases = true,
	.write_to_file = true,
	.shared_size = sizeof(PgStatShared_ExtVacEntry),
	.shared_data_off = offsetof(PgStatShared_ExtVacEntry, stats),
	.shared_data_len = sizeof(PgStat_VacuumRelationCounts),
	.pending_size = 0,
	.flush_pending_cb = NULL,
	.reset_data_cb = extvac_reset_data,
};

static inline void
pgstat_accumulate_common(PgStat_CommonCounts * dst, const PgStat_CommonCounts * src)
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

static inline void
pgstat_accumulate_extvac_stats(PgStat_VacuumRelationCounts * dst,
							   const PgStat_VacuumRelationCounts * src)
{
	if (dst->type == PGSTAT_EXTVAC_INVALID)
		dst->type = src->type;

	Assert(src->type != PGSTAT_EXTVAC_INVALID && src->type != PGSTAT_EXTVAC_DB);
	Assert(src->type == dst->type);

	pgstat_accumulate_common(&dst->common, &src->common);
	if (dst->type == PGSTAT_EXTVAC_TABLE)
	{
		/* A snapshot of the most recent post-cleanup phase. */
		dst->table.awaiting_drop_segments = src->table.awaiting_drop_segments;
		for (int phase = 0; phase < PGSTAT_NUM_AO_PHASES; phase++)
			pgstat_accumulate_common(&dst->table.ao_phases[phase],
									 &src->table.ao_phases[phase]);
	}
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ext_vacuum_statistics module could be loaded only on startup."),
				 errdetail("Add 'ext_vacuum_statistics' into the shared_preload_libraries list.")));

	DefineCustomBoolVariable("vacuum_statistics.enabled",
							 "Collect VACUUM resources and extension-only metrics; work counters obey track_counts.",
							 NULL, &evs_enabled, true,
							 PGC_SUSET, GUC_GPDB_NEED_SYNC,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved(SJ_NODENAME);

	pgstat_register_kind(PGSTAT_KIND_EXTVAC_RELATION, &extvac_relation_kind_info);
	pgstat_register_kind(PGSTAT_KIND_EXTVAC_DB, &extvac_db_kind_info);

	prev_report_vacuum_hook = set_report_vacuum_hook;
	set_report_vacuum_hook = pgstat_report_vacuum_extstats;

	prev_object_access_hook = object_access_hook;
	object_access_hook = extvac_object_access;
}

/*
 * Object access hook: drop the statistics of a dropped relation, and reset
 * old statistics when a new relation is created.
 *
 * Otherwise the statistics of a dropped relation stay in memory and in the
 * statistics file, and a new relation with the same OID gets them.  Both
 * actions are transactional, as for the built-in relation statistics.
 *
 * Statistics are keyed by the relation OID, and shared catalogs are never
 * created or dropped after initdb, so we do not need to read pg_class.
 *
 * Dropped databases need no handling: dropping a database drops all its
 * statistics entries, ours included.
 */
static void
extvac_object_access(ObjectAccessType access, Oid classId, Oid objectId,
					 int subId, void *arg)
{
	if (prev_object_access_hook)
		prev_object_access_hook(access, classId, objectId, subId, arg);

	/* only whole relations are of interest, not their columns */
	if (classId != RelationRelationId || subId != 0)
		return;

	if (access == OAT_DROP)
		pgstat_drop_transactional(PGSTAT_KIND_EXTVAC_RELATION, MyDatabaseId,
								  objectId);
	else if (access == OAT_POST_CREATE)
	{
		PgStat_EntryRef *entry_ref;

		/* An OID can now identify a different kind of relation. */
		entry_ref = pgstat_get_entry_ref(PGSTAT_KIND_EXTVAC_RELATION,
									   MyDatabaseId, objectId, false, NULL);
		if (entry_ref && !entry_ref->shared_entry->dropped)
		{
			PgStatShared_ExtVacEntry *entry;

			(void) pgstat_lock_entry(entry_ref, false);
			entry = (PgStatShared_ExtVacEntry *) entry_ref->shared_stats;
			entry->stats.type = PGSTAT_EXTVAC_INVALID;
			pgstat_unlock_entry(entry_ref);
		}

		/*
		 * Discard whatever an earlier owner of this OID left behind, and
		 * arrange for the entry to be dropped again should the creating
		 * transaction roll back.  This mirrors what pgstat_create_relation()
		 * does for the built-in relation statistics.
		 */
		pgstat_create_transactional(PGSTAT_KIND_EXTVAC_RELATION, MyDatabaseId,
									objectId);
	}
}

/* Accumulate common counts for database-level stats. */
static inline void
pgstat_accumulate_common_for_db(PgStat_CommonCounts * dst,
								const PgStat_CommonCounts * src)
{
	pgstat_accumulate_common(dst, src);
}

/*
 * Store incoming vacuum stats into pgstat custom statistics.
 * store_relation: create/update per-relation entry
 * store_db: accumulate into database-level entry (dboid, objid=0).
 * Uses pgstat_get_entry_ref_locked and pgstat_accumulate_* for atomic updates.
 */
static void
extvac_store(Oid dboid, Oid relid, PgStat_VacuumRelationCounts * params,
			 bool store_relation, bool store_db)
{
	PgStat_EntryRef *entry_ref;
	PgStatShared_ExtVacEntry *shared;

	if (!evs_enabled)
		return;

	if (store_relation)
	{
		entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_EXTVAC_RELATION, dboid, relid, false);
		if (entry_ref)
		{
			shared = (PgStatShared_ExtVacEntry *) entry_ref->shared_stats;
			if (shared->stats.type == PGSTAT_EXTVAC_INVALID)
			{
				memset(&shared->stats, 0, sizeof(shared->stats));
				shared->stats.type = params->type;
			}
			pgstat_accumulate_extvac_stats(&shared->stats, params);
			pgstat_unlock_entry(entry_ref);
		}
	}

	if (store_db)
	{
		entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_EXTVAC_DB, dboid, InvalidOid, false);
		if (entry_ref)
		{
			shared = (PgStatShared_ExtVacEntry *) entry_ref->shared_stats;
			if (shared->stats.type == PGSTAT_EXTVAC_INVALID)
			{
				memset(&shared->stats, 0, sizeof(shared->stats));
				shared->stats.type = PGSTAT_EXTVAC_DB;
			}
			pgstat_accumulate_common_for_db(&shared->stats.common, &params->common);
			pgstat_unlock_entry(entry_ref);
		}
	}
}

/*
 * Vacuum report hook: called when vacuum finishes. Stores stats per-relation
 * and per-database, then chains to previous hook.
 */
static void
pgstat_report_vacuum_extstats(Oid tableoid, bool shared,
							  PgStat_VacuumRelationCounts * params)
{
	Oid			dboid = shared ? InvalidOid : MyDatabaseId;

	if (evs_enabled)
		extvac_store(dboid, tableoid, params, true, true);
	if (prev_report_vacuum_hook)
		prev_report_vacuum_hook(tableoid, shared, params);
}

/* Reset statistics for a single relation entry. */
static void
extvac_reset_by_relid(Oid dboid, Oid relid)
{
	pgstat_reset_entry(PGSTAT_KIND_EXTVAC_RELATION, dboid, relid,
					   GetCurrentTimestamp());
}

/* Callback for pgstat_reset_matching_entries: match relation entries for given db */
static bool
match_extvac_relations_for_db(PgStatShared_HashEntry *entry, Datum match_data)
{
	return entry->key.kind == PGSTAT_KIND_EXTVAC_RELATION &&
		entry->key.dboid == DatumGetObjectId(match_data);
}

/*
 * Reset statistics for a database (aggregate entry) and all its relations.
 */
static void
extvac_database_reset(Oid dboid)
{
	TimestampTz ts = GetCurrentTimestamp();

	pgstat_reset_matching_entries(match_extvac_relations_for_db,
								  ObjectIdGetDatum(dboid), ts);
	pgstat_reset_entry(PGSTAT_KIND_EXTVAC_DB, dboid, InvalidOid, ts);
}

/* Reset all vacuum statistics (both relation and database entries). */
static void
extvac_stat_reset(void)
{
	pgstat_reset_of_kind(PGSTAT_KIND_EXTVAC_RELATION);
	pgstat_reset_of_kind(PGSTAT_KIND_EXTVAC_DB);
}

PG_FUNCTION_INFO_V1(vacuum_statistics_reset);
PG_FUNCTION_INFO_V1(extvac_reset_entry);
PG_FUNCTION_INFO_V1(extvac_reset_db_entry);

Datum
vacuum_statistics_reset(PG_FUNCTION_ARGS)
{
	extvac_stat_reset();
	PG_RETURN_VOID();
}

Datum
extvac_reset_entry(PG_FUNCTION_ARGS)
{
	Oid			dboid = PG_GETARG_OID(0);
	Oid			relid = PG_GETARG_OID(1);

	extvac_reset_by_relid(dboid, relid);
	PG_RETURN_VOID();
}

Datum
extvac_reset_db_entry(PG_FUNCTION_ARGS)
{
	Oid			dboid = PG_GETARG_OID(0);

	extvac_database_reset(dboid);
	PG_RETURN_VOID();
}

/*
 * Output vacuum statistics (tables, indexes, or per-database aggregates).
 */
#define EXTVAC_COMMON_STAT_COLS 9

static void
tuplestore_put_common(PgStat_CommonCounts * vacuum_ext,
					  Datum *values, bool *nulls, int *i)
{
	char		buf[256];
	const int	base PG_USED_FOR_ASSERTS_ONLY = *i;

	values[(*i)++] = Int64GetDatum(vacuum_ext->total_blks_read);
	values[(*i)++] = Int64GetDatum(vacuum_ext->total_blks_hit);
	values[(*i)++] = Int64GetDatum(vacuum_ext->total_blks_dirtied);
	values[(*i)++] = Int64GetDatum(vacuum_ext->total_blks_written);
	values[(*i)++] = Int64GetDatum(vacuum_ext->wal_records);
	values[(*i)++] = Int64GetDatum(vacuum_ext->wal_fpi);
	snprintf(buf, sizeof buf, UINT64_FORMAT, vacuum_ext->wal_bytes);
	values[(*i)++] = DirectFunctionCall3(numeric_in,
										 CStringGetDatum(buf),
										 ObjectIdGetDatum(0),
										 Int32GetDatum(-1));
	values[(*i)++] = Float8GetDatum(vacuum_ext->blk_read_time);
	values[(*i)++] = Float8GetDatum(vacuum_ext->blk_write_time);
	Assert((*i - base) == EXTVAC_COMMON_STAT_COLS);
}

#define EXTVAC_HEAP_STAT_COLS	13
#define EXTVAC_AO_STAT_COLS	(13 + PGSTAT_NUM_AO_PHASES * EXTVAC_COMMON_STAT_COLS)
#define EXTVAC_IDX_STAT_COLS	12
#define EXTVAC_MAX_STAT_COLS	Max(EXTVAC_HEAP_STAT_COLS, EXTVAC_IDX_STAT_COLS)

static void
tuplestore_put_for_relation(Oid relid, Tuplestorestate *tupstore,
							TupleDesc tupdesc, PgStat_VacuumRelationCounts * vacuum_ext)
{
	Datum		values[EXTVAC_MAX_STAT_COLS];
	bool		nulls[EXTVAC_MAX_STAT_COLS];
	int			i = 0;

	memset(nulls, 0, sizeof(nulls));
	values[i++] = ObjectIdGetDatum(relid);

	tuplestore_put_common(&vacuum_ext->common, values, nulls, &i);
	values[i++] = Int64GetDatum(vacuum_ext->common.blks_fetched - vacuum_ext->common.blks_hit);
	values[i++] = Int64GetDatum(vacuum_ext->common.blks_hit);

	if (vacuum_ext->type == PGSTAT_EXTVAC_TABLE)
	{
		values[i++] = Int64GetDatum(vacuum_ext->table.awaiting_drop_segments);
	}

	Assert(i == ((vacuum_ext->type == PGSTAT_EXTVAC_TABLE) ? EXTVAC_HEAP_STAT_COLS : EXTVAC_IDX_STAT_COLS));
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
}

static void
tuplestore_put_for_ao_table(Oid relid, Tuplestorestate *tupstore,
						   TupleDesc tupdesc, PgStat_VacuumRelationCounts *vacuum_ext)
{
	Datum		values[EXTVAC_AO_STAT_COLS];
	bool		nulls[EXTVAC_AO_STAT_COLS] = {false};
	int			i = 0;

	/* Totals and phases must come from the same fetched version of the entry. */
	values[i++] = ObjectIdGetDatum(relid);
	tuplestore_put_common(&vacuum_ext->common, values, nulls, &i);
	values[i++] = Int64GetDatum(vacuum_ext->common.blks_fetched - vacuum_ext->common.blks_hit);
	values[i++] = Int64GetDatum(vacuum_ext->common.blks_hit);
	values[i++] = Int64GetDatum(vacuum_ext->table.awaiting_drop_segments);
	for (int phase = 0; phase < PGSTAT_NUM_AO_PHASES; phase++)
		tuplestore_put_common(&vacuum_ext->table.ao_phases[phase],
							  values, nulls, &i);
	Assert(i == EXTVAC_AO_STAT_COLS);
	tuplestore_putvalues(tupstore, tupdesc, values, nulls);
}

static Datum
pg_stats_vacuum(FunctionCallInfo fcinfo, int type, bool ao_table)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext per_query_ctx;
	MemoryContext oldcontext;
	Tuplestorestate *tupstore;
	TupleDesc	tupdesc;
	Oid			dbid = PG_GETARG_OID(0);

	Assert(!ao_table || type == PGSTAT_EXTVAC_TABLE);

	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ext_vacuum_statistics: set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ext_vacuum_statistics: materialize mode required")));

	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "ext_vacuum_statistics: return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	if (type == PGSTAT_EXTVAC_INDEX || type == PGSTAT_EXTVAC_TABLE)
	{
		Oid			relid = PG_GETARG_OID(1);
		PgStat_VacuumRelationCounts *stats;

		if (!OidIsValid(relid))
			return (Datum) 0;

		stats = (PgStat_VacuumRelationCounts *)
			pgstat_fetch_entry(PGSTAT_KIND_EXTVAC_RELATION, dbid, relid);

		if (!stats)
			stats = (PgStat_VacuumRelationCounts *)
				pgstat_fetch_entry(PGSTAT_KIND_EXTVAC_RELATION, InvalidOid,
								   relid);

		if (stats && stats->type == type)
		{
			if (ao_table)
				tuplestore_put_for_ao_table(relid, tupstore, tupdesc, stats);
			else
				tuplestore_put_for_relation(relid, tupstore, tupdesc, stats);
		}
	}
	else if (type == PGSTAT_EXTVAC_DB)
	{
		if (OidIsValid(dbid))
		{
#define EXTVAC_DB_STAT_COLS 10
			Datum		values[EXTVAC_DB_STAT_COLS];
			bool		nulls[EXTVAC_DB_STAT_COLS];
			int			i = 0;
			PgStat_VacuumRelationCounts *stats;

			stats = (PgStat_VacuumRelationCounts *)
				pgstat_fetch_entry(PGSTAT_KIND_EXTVAC_DB, dbid,
								   InvalidOid);
			if (stats && stats->type == PGSTAT_EXTVAC_DB)
			{
				memset(nulls, 0, sizeof(nulls));
				values[i++] = ObjectIdGetDatum(dbid);
				tuplestore_put_common(&stats->common, values, nulls, &i);
				Assert(i == EXTVAC_DB_STAT_COLS);
				tuplestore_putvalues(tupstore, tupdesc, values, nulls);
			}
		}
		/* invalid dbid: return empty set */
	}
	else
		elog(PANIC, "ext_vacuum_statistics: invalid type %d", type);

	return (Datum) 0;
}

PG_FUNCTION_INFO_V1(pg_stats_get_vacuum_tables);
PG_FUNCTION_INFO_V1(pg_stats_get_vacuum_ao_tables);
PG_FUNCTION_INFO_V1(pg_stats_get_vacuum_indexes);
PG_FUNCTION_INFO_V1(pg_stats_get_vacuum_database);

Datum
pg_stats_get_vacuum_tables(PG_FUNCTION_ARGS)
{
	return pg_stats_vacuum(fcinfo, PGSTAT_EXTVAC_TABLE, false);
}

Datum
pg_stats_get_vacuum_ao_tables(PG_FUNCTION_ARGS)
{
	return pg_stats_vacuum(fcinfo, PGSTAT_EXTVAC_TABLE, true);
}

Datum
pg_stats_get_vacuum_indexes(PG_FUNCTION_ARGS)
{
	return pg_stats_vacuum(fcinfo, PGSTAT_EXTVAC_INDEX, false);
}

Datum
pg_stats_get_vacuum_database(PG_FUNCTION_ARGS)
{
	return pg_stats_vacuum(fcinfo, PGSTAT_EXTVAC_DB, false);
}

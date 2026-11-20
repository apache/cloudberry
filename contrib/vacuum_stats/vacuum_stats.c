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
 * vacuum_stats.c
 *		Expose the vacuum counters accumulated by the statistics collector
 *		for relations (tables and indexes) and databases.
 *
 * The backend collects these counters in ordinary statistics entries and,
 * when enabled, extended vacuum statistics. SQL access belongs to this
 * extension; read both groups through the regular pgstat fetch API.
 *
 * contrib/vacuum_stats/vacuum_stats.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "pgstat.h"

PG_MODULE_MAGIC;

/* Ordinary counters follow track_counts, independently of extended tracking. */
#define DEFINE_REL_COUNTER_FUNC(funcname, field) \
PG_FUNCTION_INFO_V1(funcname); \
Datum \
funcname(PG_FUNCTION_ARGS) \
{ \
	PgStat_StatTabEntry *entry = pgstat_fetch_stat_tabentry(PG_GETARG_OID(0)); \
	PG_RETURN_INT64(entry ? (int64) entry->field : 0); \
}

#define DEFINE_DB_COUNTER_FUNC(funcname, field) \
PG_FUNCTION_INFO_V1(funcname); \
Datum \
funcname(PG_FUNCTION_ARGS) \
{ \
	PgStat_StatDBEntry *entry = pgstat_fetch_stat_dbentry(PG_GETARG_OID(0)); \
	PG_RETURN_INT64(entry ? (int64) entry->field : 0); \
}

/* Store microseconds in the collector and expose milliseconds without rounding. */
#define DEFINE_REL_TIME_FUNC(funcname, field) \
PG_FUNCTION_INFO_V1(funcname); \
Datum \
funcname(PG_FUNCTION_ARGS) \
{ \
	PgStat_StatTabEntry *entry = pgstat_fetch_stat_tabentry(PG_GETARG_OID(0)); \
	PG_RETURN_FLOAT8(entry ? (double) entry->field / 1000.0 : 0); \
}

#define DEFINE_DB_TIME_FUNC(funcname, field) \
PG_FUNCTION_INFO_V1(funcname); \
Datum \
funcname(PG_FUNCTION_ARGS) \
{ \
	PgStat_StatDBEntry *entry = pgstat_fetch_stat_dbentry(PG_GETARG_OID(0)); \
	PG_RETURN_FLOAT8(entry ? (double) entry->field / 1000.0 : 0); \
}

DEFINE_REL_COUNTER_FUNC(pg_stat_get_frozen_page_marks_cleared, frozen_page_marks_cleared)
DEFINE_REL_COUNTER_FUNC(pg_stat_get_visible_page_marks_cleared, visible_page_marks_cleared)
DEFINE_REL_TIME_FUNC(pg_stat_get_total_vacuum_time, total_vacuum_time)
DEFINE_REL_TIME_FUNC(pg_stat_get_total_autovacuum_time, total_autovacuum_time)
DEFINE_REL_TIME_FUNC(pg_stat_get_total_vacuum_delay_time, total_vacuum_delay_time)
DEFINE_REL_TIME_FUNC(pg_stat_get_total_autovacuum_delay_time, total_autovacuum_delay_time)
DEFINE_REL_TIME_FUNC(pg_stat_get_total_analyze_time, total_analyze_time)
DEFINE_REL_TIME_FUNC(pg_stat_get_total_autoanalyze_time, total_autoanalyze_time)
DEFINE_REL_COUNTER_FUNC(pg_stat_get_vacuum_failsafe_count, vacuum_failsafe_count)
DEFINE_DB_COUNTER_FUNC(pg_stat_get_db_frozen_page_marks_cleared, n_frozen_page_marks_cleared)
DEFINE_DB_COUNTER_FUNC(pg_stat_get_db_visible_page_marks_cleared, n_visible_page_marks_cleared)
DEFINE_DB_TIME_FUNC(pg_stat_get_db_total_vacuum_time, total_vacuum_time)
DEFINE_DB_TIME_FUNC(pg_stat_get_db_total_autovacuum_time, total_autovacuum_time)
DEFINE_DB_TIME_FUNC(pg_stat_get_db_total_vacuum_delay_time, total_vacuum_delay_time)
DEFINE_DB_TIME_FUNC(pg_stat_get_db_total_autovacuum_delay_time, total_autovacuum_delay_time)
DEFINE_DB_COUNTER_FUNC(pg_stat_get_db_vacuum_failsafe_count, vacuum_failsafe_count)
DEFINE_DB_COUNTER_FUNC(pg_stat_get_db_vacuum_interrupt_count, vacuum_interrupt_count)

/* Fetch the relation's vacuum counters, or NULL when unavailable. */
static PgStat_VacuumStats *
fetch_rel_vacuum_stats(Oid relid)
{
	return pgstat_fetch_stat_vacuum_stats(relid);
}

/*
 * Fetch the per-database vacuum counters, or NULL if the statistics
 * collector has no entry for the database.
 */
static PgStat_VacuumStats *
fetch_db_vacuum_stats(Oid dbid)
{
	PgStat_StatDBEntry *dbentry;

	if (!pgstat_track_vacuum_statistics)
		return NULL;

	dbentry = pgstat_fetch_stat_dbentry(dbid);
	if (dbentry == NULL)
		return NULL;

	return &dbentry->n_vacuum_stats;
}

#define DEFINE_REL_VACSTAT_FUNC(funcname, field) \
PG_FUNCTION_INFO_V1(funcname); \
Datum \
funcname(PG_FUNCTION_ARGS) \
{ \
	Oid			relid = PG_GETARG_OID(0); \
	PgStat_VacuumStats *stats = fetch_rel_vacuum_stats(relid); \
\
	PG_RETURN_INT64(stats ? (int64) stats->field : 0); \
}

#define DEFINE_DB_VACSTAT_FUNC(funcname, field) \
PG_FUNCTION_INFO_V1(funcname); \
Datum \
funcname(PG_FUNCTION_ARGS) \
{ \
	Oid			dbid = PG_GETARG_OID(0); \
	PgStat_VacuumStats *stats = fetch_db_vacuum_stats(dbid); \
\
	PG_RETURN_INT64(stats ? (int64) stats->field : 0); \
}

DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_tuples_deleted, tuples_deleted)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_dead_tuples, dead_tuples)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_pages_deleted, pages_deleted)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_bytes_removed, bytes_removed)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_total_file_segs, total_file_segs)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_dead_pages, dead_pages)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_pages_frozen, pages_frozen)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_pages_all_visible, pages_all_visible)
DEFINE_REL_VACSTAT_FUNC(pg_stat_get_vacuum_freeze_age_count, freeze_age_vacuum_count)

DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_tuples_deleted, tuples_deleted)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_dead_tuples, dead_tuples)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_pages_deleted, pages_deleted)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_bytes_removed, bytes_removed)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_dead_pages, dead_pages)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_pages_frozen, pages_frozen)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_pages_all_visible, pages_all_visible)
DEFINE_DB_VACSTAT_FUNC(pg_stat_get_db_vacuum_freeze_age_count, freeze_age_vacuum_count)

/*
 * Throw away the vacuum counters of one relation, or of the whole database,
 * without touching the rest of the statistics -- which is what
 * pg_stat_reset() and pg_stat_reset_single_table_counters() would do.
 *
 * Like the other resetting functions this acts on the node it runs on, so on
 * a cluster it has to be dispatched to the segments as well; see
 * gp_vacuum_stats_reset() in the extension script.
 */
PG_FUNCTION_INFO_V1(vacuum_stats_reset);
Datum
vacuum_stats_reset(PG_FUNCTION_ARGS)
{
	pgstat_reset_vacuum_stats(InvalidOid, true);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(vacuum_stats_reset_relation);
Datum
vacuum_stats_reset_relation(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

	pgstat_reset_vacuum_stats(relid, false);

	PG_RETURN_VOID();
}

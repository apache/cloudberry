/*
 * Test custom statistics allocation and restart without the defining module.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/pgstat_internal.h"

PG_MODULE_MAGIC;

#define TEST_VARIABLE_KIND PGSTAT_KIND_EXPERIMENTAL
#define TEST_FIXED_KIND (PGSTAT_KIND_EXPERIMENTAL + 1)
#define TEST_NAMED_KIND (PGSTAT_KIND_EXPERIMENTAL + 2)

static char *test_kind_name;

typedef struct TestVariableStats
{
	PgStatShared_Common header;
	uint64		value;
} TestVariableStats;

static void
test_fixed_init(void *ptr)
{
	*((uint64 *) ptr) = 42;
}

static void
test_fixed_reset(TimestampTz ts)
{
	*((uint64 *) pgstat_get_custom_shmem_data(TEST_FIXED_KIND)) = 0;
}

static void
test_fixed_snapshot(void)
{
	memcpy(pgstat_get_custom_snapshot_data(TEST_FIXED_KIND),
		   pgstat_get_custom_shmem_data(TEST_FIXED_KIND), sizeof(uint64));
}

static const PgStat_KindInfo test_fixed = {
	.name = "test_custom_fixed",
	.fixed_amount = true,
	.write_to_file = true,
	/* Exceed spare shared memory, exposing an unaccounted second allocation. */
	.shared_size = 16 * 1024 * 1024,
	.shared_data_len = sizeof(uint64),
	.init_shmem_cb = test_fixed_init,
	.reset_all_cb = test_fixed_reset,
	.snapshot_cb = test_fixed_snapshot,
};

/* A global named entry exercises the other variable-entry disk format. */
static void
test_to_name(const PgStat_HashKey *key, const PgStatShared_Common *header,
			 NameData *name)
{
	namestrcpy(name, "test_entry");
}

static bool
test_from_name(const NameData *name, PgStat_HashKey *key)
{
	if (strcmp(NameStr(*name), "test_entry") != 0)
		return false;
	memset(key, 0, sizeof(*key));
	key->kind = TEST_NAMED_KIND;
	key->dboid = InvalidOid;
	key->objoid = 1;
	return true;
}

static const PgStat_KindInfo test_named = {
	.name = "test_custom_named",
	.write_to_file = true,
	.shared_size = sizeof(TestVariableStats),
	.shared_data_off = offsetof(TestVariableStats, value),
	.shared_data_len = sizeof(uint64),
	.to_serialized_name = test_to_name,
	.from_serialized_name = test_from_name,
};

void _PG_init(void);
PG_FUNCTION_INFO_V1(test_custom_stats_create);
PG_FUNCTION_INFO_V1(test_custom_stats_get);

void
_PG_init(void)
{
	DefineCustomStringVariable("test_custom_stats.name", NULL, NULL,
							   &test_kind_name, "test_custom_variable",
							   PGC_POSTMASTER, 0, NULL, NULL, NULL);
	pgstat_register_kind(TEST_FIXED_KIND, &test_fixed);
	{
		const PgStat_KindInfo info = {
			.name = test_kind_name,
			.write_to_file = true,
			.shared_size = sizeof(TestVariableStats),
			.shared_data_off = offsetof(TestVariableStats, value),
			.shared_data_len = sizeof(uint64),
		};
		PgStat_KindInfo *saved = MemoryContextAlloc(TopMemoryContext, sizeof(info));

		memcpy(saved, &info, sizeof(info));
		pgstat_register_kind(TEST_VARIABLE_KIND, saved);
	}
	pgstat_register_kind(TEST_NAMED_KIND, &test_named);
}

Datum
test_custom_stats_create(PG_FUNCTION_ARGS)
{
	PgStat_EntryRef *entry;

	entry = pgstat_get_entry_ref_locked(TEST_VARIABLE_KIND, MyDatabaseId, 1, false);
	((TestVariableStats *) entry->shared_stats)->value = 123;
	pgstat_unlock_entry(entry);
	entry = pgstat_get_entry_ref_locked(TEST_NAMED_KIND, InvalidOid, 1, false);
	((TestVariableStats *) entry->shared_stats)->value = 456;
	pgstat_unlock_entry(entry);
	/* The test has a single writer, with no concurrent snapshots. */
	*((uint64 *) pgstat_get_custom_shmem_data(TEST_FIXED_KIND)) = 789;
	PG_RETURN_VOID();
}

Datum
test_custom_stats_get(PG_FUNCTION_ARGS)
{
	int32		which = PG_GETARG_INT32(0);
	uint64	   *value;

	if (which == 0)
	{
		pgstat_snapshot_fixed(TEST_FIXED_KIND);
		value = pgstat_get_custom_snapshot_data(TEST_FIXED_KIND);
	}
	else if (which == 1)
		value = pgstat_fetch_entry(TEST_VARIABLE_KIND, MyDatabaseId, 1);
	else
		value = pgstat_fetch_entry(TEST_NAMED_KIND, InvalidOid, 1);
	if (value == NULL)
		PG_RETURN_NULL();
	PG_RETURN_INT64(*value);
}

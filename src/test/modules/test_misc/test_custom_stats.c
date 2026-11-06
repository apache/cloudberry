/*
 * Test custom statistics allocation and restart without the defining module.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "utils/pgstat_internal.h"

PG_MODULE_MAGIC;

#define TEST_VARIABLE_KIND PGSTAT_KIND_EXPERIMENTAL
#define TEST_FIXED_KIND (PGSTAT_KIND_EXPERIMENTAL + 1)

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

static const PgStat_KindInfo test_variable = {
	.name = "test_custom_variable",
	.write_to_file = true,
	.shared_size = sizeof(TestVariableStats),
	.shared_data_off = offsetof(TestVariableStats, value),
	.shared_data_len = sizeof(uint64),
};

static const PgStat_KindInfo test_fixed = {
	.name = "test_custom_fixed",
	.fixed_amount = true,
	/* Exceed spare shared memory, exposing an unaccounted second allocation. */
	.shared_size = 16 * 1024 * 1024,
	.shared_data_len = sizeof(uint64),
	.init_shmem_cb = test_fixed_init,
	.reset_all_cb = test_fixed_reset,
	.snapshot_cb = test_fixed_snapshot,
};

void _PG_init(void);
PG_FUNCTION_INFO_V1(test_custom_stats_create);

void
_PG_init(void)
{
	pgstat_register_kind(TEST_VARIABLE_KIND, &test_variable);
	pgstat_register_kind(TEST_FIXED_KIND, &test_fixed);
}

Datum
test_custom_stats_create(PG_FUNCTION_ARGS)
{
	(void) pgstat_get_entry_ref(TEST_VARIABLE_KIND, MyDatabaseId, 1, true, NULL);
	PG_RETURN_VOID();
}

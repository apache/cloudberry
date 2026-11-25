/*--------------------------------------------------------------------------
 *
 * test_bloomfilter.c
 *		Test false positive rate of Bloom filter.
 *
 * Copyright (c) 2018-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_bloomfilter/test_bloomfilter.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/pg_prng.h"
#include "fmgr.h"
#include "lib/bloomfilter.h"
#include "miscadmin.h"

PG_MODULE_MAGIC;

/* Fits decimal representation of PG_INT64_MIN + 2 bytes: */
#define MAX_ELEMENT_BYTES		21
/* False positive rate WARNING threshold (1%): */
#define FPOSITIVE_THRESHOLD		0.01


/*
 * Populate an empty Bloom filter with "nelements" dummy strings.
 */
static void
populate_with_dummy_strings(bloom_filter *filter, int64 nelements)
{
	char		element[MAX_ELEMENT_BYTES];
	int64		i;

	for (i = 0; i < nelements; i++)
	{
		CHECK_FOR_INTERRUPTS();

		snprintf(element, sizeof(element), "i" INT64_FORMAT, i);
		bloom_add_element(filter, (unsigned char *) element, strlen(element));
	}
}

/*
 * Returns number of strings that are indicated as probably appearing in Bloom
 * filter that were in fact never added by populate_with_dummy_strings().
 * These are false positives.
 */
static int64
nfalsepos_for_missing_strings(bloom_filter *filter, int64 nelements)
{
	char		element[MAX_ELEMENT_BYTES];
	int64		nfalsepos = 0;
	int64		i;

	for (i = 0; i < nelements; i++)
	{
		CHECK_FOR_INTERRUPTS();

		snprintf(element, sizeof(element), "M" INT64_FORMAT, i);
		if (!bloom_lacks_element(filter, (unsigned char *) element,
								 strlen(element)))
			nfalsepos++;
	}

	return nfalsepos;
}

static void
create_and_test_bloom(int power, int64 nelements, int callerseed)
{
	int			bloom_work_mem;
	uint64		seed;
	int64		nfalsepos;
	bloom_filter *filter;

	bloom_work_mem = (1L << power) / 8L / 1024L;

	elog(DEBUG1, "bloom_work_mem (KB): %d", bloom_work_mem);

	/*
	 * Generate random seed, or use caller's.  Seed should always be a
	 * positive value less than or equal to PG_INT32_MAX, to ensure that any
	 * random seed can be recreated through callerseed if the need arises.
	 */
	seed = callerseed < 0 ? pg_prng_int32p(&pg_global_prng_state) : callerseed;

	/* Create Bloom filter, populate it, and report on false positive rate */
	filter = bloom_create(nelements, bloom_work_mem, seed);
	populate_with_dummy_strings(filter, nelements);
	nfalsepos = nfalsepos_for_missing_strings(filter, nelements);

	ereport((nfalsepos > nelements * FPOSITIVE_THRESHOLD) ? WARNING : DEBUG1,
			(errmsg_internal("seed: " UINT64_FORMAT " false positives: " INT64_FORMAT " (%.6f%%) bitset %.2f%% set",
							 seed, nfalsepos, (double) nfalsepos / nelements,
							 100.0 * bloom_prop_bits_set(filter))));

	bloom_free(filter);
}

PG_FUNCTION_INFO_V1(test_bloomfilter);

/*
 * SQL-callable entry point to perform all tests.
 *
 * If a 1% false positive threshold is not met, emits WARNINGs.
 *
 * See README for details of arguments.
 */
Datum
test_bloomfilter(PG_FUNCTION_ARGS)
{
	int			power = PG_GETARG_INT32(0);
	int64		nelements = PG_GETARG_INT64(1);
	int			seed = PG_GETARG_INT32(2);
	int			tests = PG_GETARG_INT32(3);
	int			i;

	if (power < 23 || power > 32)
		elog(ERROR, "power argument must be between 23 and 32 inclusive");

	if (tests <= 0)
		elog(ERROR, "invalid number of tests: %d", tests);

	if (nelements < 0)
		elog(ERROR, "invalid number of elements: %d", tests);

	for (i = 0; i < tests; i++)
	{
		elog(DEBUG1, "beginning test #%d...", i + 1);

		create_and_test_bloom(power, nelements, seed);
	}

	PG_RETURN_VOID();
}

/*
 * Runtime filters hash and compare a narrow key in its canonical form, see
 * bloom_normalize_datum(): the same value must give the same Datum whether
 * it was sign-extended (heap) or zero-extended (AOCO), and equal integers of
 * different widths must give the same Datum.
 */
PG_FUNCTION_INFO_V1(test_bloomfilter_normalize);
Datum
test_bloomfilter_normalize(PG_FUNCTION_ARGS)
{
	bloom_filter *filter = bloom_create(32, 8, 123);
	Datum		key;

	key = bloom_normalize_datum(Int32GetDatum(-5), sizeof(int32));
	bloom_add_element(filter, (unsigned char *) &key, sizeof(Datum));

	key = bloom_normalize_datum((Datum) (uint32) -5, sizeof(int32));
	if (bloom_lacks_element(filter, (unsigned char *) &key, sizeof(Datum)))
		elog(ERROR, "zero-extended key not found after a sign-extended insert");
	key = bloom_normalize_datum(Int64GetDatum(-5), sizeof(int64));
	if (bloom_lacks_element(filter, (unsigned char *) &key, sizeof(Datum)))
		elog(ERROR, "int8 key not found after an equal int4 insert");

	if (DatumGetInt64(bloom_normalize_datum((Datum) (uint16) -1, sizeof(int16))) != -1 ||
		DatumGetInt64(bloom_normalize_datum((Datum) (uint32) -7, sizeof(int32))) != -7 ||
		DatumGetInt64(bloom_normalize_datum((Datum) (uint8) -3, 1)) != -3 ||
		DatumGetInt64(bloom_normalize_datum(Int32GetDatum(PG_INT32_MAX), sizeof(int32))) != PG_INT32_MAX ||
		bloom_normalize_datum(Int64GetDatum(-1), sizeof(int64)) != Int64GetDatum(-1))
		elog(ERROR, "unexpected normalized Datum");

	bloom_free(filter);
	PG_RETURN_VOID();
}

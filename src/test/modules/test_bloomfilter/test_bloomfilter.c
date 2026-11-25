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

#include "access/skey.h"
#include "catalog/pg_type.h"
#include "executor/nodeDynamicSeqscan.h"
#include "common/pg_prng.h"
#include "executor/nodeSeqscan.h"
#include "fmgr.h"
#include "lib/bloomfilter.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

static void
check_runtime_probing(void)
{
	bloom_filter *weak = bloom_create(32, 8, 123);
	bloom_filter *strong = bloom_create(32, 8, 123);
	Datum		present = Int64GetDatum(1);
	Datum		absent = Int64GetDatum(2);
	int			i;
	ScanKeyData weak_key = {0};
	ScanKeyData strong_key = {0};
	SeqScanState scan = {0};
	TupleTableSlot slot = {0};
	Datum		values[2] = {present, absent};
	bool		nulls[2] = {false, false};
	List	   *keys;
	TupleDesc	desc = CreateTemplateTupleDesc(2);

	bloom_add_element(weak, (unsigned char *) &present, sizeof(Datum));
	for (i = 0; i < BLOOM_PROBE_SAMPLE_SIZE; i++)
		bloom_runtime_probe(weak, (unsigned char *) &present, sizeof(Datum));
	if (!bloom_runtime_probe_disabled(weak) ||
		bloom_runtime_probe(weak, (unsigned char *) &absent, sizeof(Datum)) ||
		!bloom_lacks_element(weak, (unsigned char *) &absent, sizeof(Datum)))
		elog(ERROR, "ineffective runtime filter did not stop probing");

	bloom_free(weak);
	weak = bloom_create(32, 8, 123);
	bloom_add_element(weak, (unsigned char *) &present, sizeof(Datum));
	bloom_add_element(strong, (unsigned char *) &present, sizeof(Datum));
	for (i = 0; i < BLOOM_PROBE_SAMPLE_SIZE; i++)
	{
		Datum key = i < 128 ? absent : present;

		bloom_runtime_probe(weak, (unsigned char *) &key, sizeof(Datum));
	}
	for (i = 0; i < BLOOM_PROBE_SAMPLE_SIZE - 1; i++)
	{
		Datum key = i < 768 ? absent : present;

		bloom_runtime_probe(strong, (unsigned char *) &key, sizeof(Datum));
	}

	weak_key.sk_flags = strong_key.sk_flags = SK_BLOOM_FILTER;
	weak_key.sk_attno = 1;
	strong_key.sk_attno = 2;
	weak_key.sk_argument = PointerGetDatum(weak);
	strong_key.sk_argument = PointerGetDatum(strong);
	scan.filters = keys = list_make2(&weak_key, &strong_key);
	TupleDescInitBuiltinEntry(desc, 1, "weak", INT8OID, -1, 0);
	TupleDescInitBuiltinEntry(desc, 2, "strong", INT8OID, -1, 0);
	slot.tts_tupleDescriptor = desc;
	slot.tts_values = values;
	slot.tts_isnull = nulls;
	slot.tts_nvalid = 2;

	for (i = 0; i < BLOOM_PROBE_SAMPLE_SIZE - 1; i++)
	{
		if (PassByBloomFilter(&scan, &slot) || linitial(keys) != &weak_key)
			elog(ERROR, "runtime filters reordered during sampling");
	}
	if (PassByBloomFilter(&scan, &slot) ||
		linitial(keys) != &strong_key || lsecond(keys) != &weak_key)
		elog(ERROR, "runtime filters did not move the more selective probe first");
	values[1] = present;
	if (!PassByBloomFilter(&scan, &slot) ||
		scan.bloom_sample_rows != BLOOM_PROBE_SAMPLE_SIZE)
		elog(ERROR, "reordered runtime filters rejected a matching tuple");

	list_free(keys);
	bloom_free(strong);
	bloom_free(weak);
	FreeTupleDesc(desc);
}

/* Probe "n" times with "key"; return how many probes rejected it. */
static int
probe_n(bloom_filter *filter, Datum key, int n)
{
	int			rejected = 0;

	for (int i = 0; i < n; i++)
		rejected += bloom_runtime_probe(filter, (unsigned char *) &key,
										sizeof(Datum));
	return rejected;
}

/*
 * Skip until the filter samples again.  Return the number of rows skipped, or
 * -1 if it is still skipping after "limit" rows.
 */
static int
rows_until_resample(bloom_filter *filter, int limit)
{
	for (int i = 0; i < limit; i++)
	{
		if (!bloom_runtime_probe_skip(filter))
			return i;
	}
	return -1;
}

/*
 * A disabled filter samples again after a gap of skipped rows.  The gap
 * doubles while the filter stays ineffective, and goes back to its start once
 * a sample shows it rejects rows again.
 */
static void
check_runtime_recheck(void)
{
	bloom_filter *filter = bloom_create(32, 8, 123);
	Datum		present = Int64GetDatum(1);
	Datum		absent = Int64GetDatum(2);
	int			save_pct = gp_runtime_filter_min_reject_pct;

	bloom_add_element(filter, (unsigned char *) &present, sizeof(Datum));

	if (bloom_runtime_probe_skip(filter))
		elog(ERROR, "a new runtime filter skipped a row");
	probe_n(filter, present, BLOOM_PROBE_SAMPLE_SIZE);
	if (!bloom_runtime_probe_disabled(filter))
		elog(ERROR, "ineffective runtime filter stayed enabled");
	if (rows_until_resample(filter, 10 * BLOOM_PROBE_SAMPLE_SIZE) !=
		BLOOM_PROBE_SAMPLE_SIZE - 1)
		elog(ERROR, "first resample did not come after %d rows",
			 BLOOM_PROBE_SAMPLE_SIZE);

	/* The data changed: the filter rejects again and stays on. */
	if (probe_n(filter, absent, BLOOM_PROBE_SAMPLE_SIZE) != BLOOM_PROBE_SAMPLE_SIZE ||
		bloom_runtime_probe_disabled(filter) || bloom_runtime_probe_skip(filter))
		elog(ERROR, "effective runtime filter was not re-enabled");

	/* Ineffective again: the gap starts over, then doubles. */
	probe_n(filter, present, BLOOM_PROBE_SAMPLE_SIZE);
	if (rows_until_resample(filter, 10 * BLOOM_PROBE_SAMPLE_SIZE) !=
		BLOOM_PROBE_SAMPLE_SIZE - 1)
		elog(ERROR, "resample gap did not start over");
	probe_n(filter, present, BLOOM_PROBE_SAMPLE_SIZE);
	if (rows_until_resample(filter, 10 * BLOOM_PROBE_SAMPLE_SIZE) !=
		2 * BLOOM_PROBE_SAMPLE_SIZE - 1)
		elog(ERROR, "resample gap did not double");

	/* A threshold of 0 never disables a filter. */
	gp_runtime_filter_min_reject_pct = 0;
	probe_n(filter, present, 2 * BLOOM_PROBE_SAMPLE_SIZE);
	gp_runtime_filter_min_reject_pct = save_pct;
	if (bloom_runtime_probe_disabled(filter))
		elog(ERROR, "runtime filter disabled with a threshold of 0");

	bloom_free(filter);
}

static void
check_partition_runtime_keys(void)
{
	DynamicSeqScanState parent = {0};
	SeqScanState child = {0};
	TupleTableSlot parent_slot = {0};
	RelationData partition = {0};
	EState		estate = {0};
	ScanKeyData bloom_key = {0};
	ScanKeyData range_key = {0};
	TupleDesc	parent_desc = CreateTemplateTupleDesc(2);
	TupleDesc	child_desc = CreateTemplateTupleDesc(2);
	ScanKey		first;
	ScanKey		second;

	TupleDescInitEntry(parent_desc, 1, "a", INT8OID, -1, 0);
	TupleDescInitEntry(parent_desc, 2, "b", INT8OID, -1, 0);
	TupleDescInitEntry(child_desc, 1, "b", INT8OID, -1, 0);
	TupleDescInitEntry(child_desc, 2, "a", INT8OID, -1, 0);
	parent_slot.tts_tupleDescriptor = parent_desc;
	partition.rd_att = child_desc;
	parent.ss.ss_ScanTupleSlot = &parent_slot;
	parent.ss.ps.state = &estate;
	parent.partitionMemoryContext = AllocSetContextCreate(CurrentMemoryContext,
														   "runtime key test",
														   ALLOCSET_DEFAULT_SIZES);
	parent.seqScanState = &child;
	child.ss.ss_currentRelation = &partition;
	/* An already-open partition whose AM took ownership of the old keys. */
	child.filters = list_make1(&bloom_key);
	bloom_key.sk_flags = SK_BLOOM_FILTER;
	bloom_key.sk_attno = 1;
	range_key.sk_attno = 2;

	ExecDynamicSeqScanAddRuntimeFilters(&parent, list_make2(&bloom_key, &range_key));
	first = linitial(child.filters);
	second = lsecond(child.filters);
	if (!child.filter_in_seqscan || list_length(child.filters) != 2 ||
		first == &bloom_key || first->sk_attno != 2 ||
		second == &range_key || second->sk_attno != 1 ||
		list_length(parent.filters) != 2 ||
		bloom_key.sk_attno != 1 || range_key.sk_attno != 2)
		elog(ERROR, "late runtime keys were not copied to the partition layout");

	list_free(parent.filters);
	MemoryContextDelete(parent.partitionMemoryContext);
	FreeTupleDesc(child_desc);
	FreeTupleDesc(parent_desc);
}

PG_FUNCTION_INFO_V1(test_bloomfilter_runtime_probe);
Datum
test_bloomfilter_runtime_probe(PG_FUNCTION_ARGS)
{
	check_runtime_probing();
	check_runtime_recheck();
	check_partition_runtime_keys();
	PG_RETURN_VOID();
}

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

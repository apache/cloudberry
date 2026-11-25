/*-------------------------------------------------------------------------
 *
 * bloomfilter.h
 *	  Space-efficient set membership testing
 *
 * Copyright (c) 2018-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *    src/include/lib/bloomfilter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

typedef struct bloom_filter bloom_filter;

extern bloom_filter *bloom_create(int64 total_elems, int bloom_work_mem,
								  uint64 seed);
/* minimum_bytes must be a power of two, at most 512 MiB */
extern bloom_filter *bloom_create_with_minimum(int64 total_elems,
											   int bloom_work_mem, uint64 seed,
											   Size minimum_bytes);
extern bloom_filter *bloom_create_from_bitset_with_minimum(int64 total_elems,
														   int bloom_work_mem,
														   uint64 seed,
														   const unsigned char *bitset,
														   Size bitset_len,
														   Size minimum_bytes);
extern void bloom_free(bloom_filter *filter);
extern void bloom_add_element(bloom_filter *filter, unsigned char *elem,
							  size_t len);
extern bool bloom_lacks_element(bloom_filter *filter, unsigned char *elem,
								size_t len);
/* Optional runtime probes may stop rejecting rows after sampling. */
#define BLOOM_PROBE_SAMPLE_SIZE 1024
extern bool bloom_runtime_probe_disabled(const bloom_filter *filter);
extern bool bloom_runtime_probe_skip(bloom_filter *filter);
extern bool bloom_runtime_probe(bloom_filter *filter, unsigned char *elem,
								size_t len);
extern int bloom_runtime_probe_rank(const bloom_filter *filter);
extern double bloom_prop_bits_set(bloom_filter *filter);
extern double bloom_false_positive_rate(bloom_filter *filter);
extern uint64 bloom_total_bits(bloom_filter *filter);
extern bloom_filter *bloom_create_aggresive(int64 total_elems,
											int work_mem, uint64 seed);
extern Size bloom_bitset_bytes(const bloom_filter *filter);
extern const unsigned char *bloom_bitset_data(const bloom_filter *filter);
extern bloom_filter *bloom_create_from_bitset(int64 total_elems,
											  int bloom_work_mem, uint64 seed,
											  const unsigned char *bitset,
											  Size bitset_len);

/*
 * Runtime filters hash a by-value key as the bytes of its Datum, so both sides
 * of a filter must give equal keys the same bytes.  Only the low typlen bytes
 * of a narrow Datum carry the value, and the rest depends on where it came
 * from: heap tuples sign-extend, AOCO zero-extends.  Sign-extending the low
 * bytes gives one canonical Datum per value -- for an integer, its int64 value
 * -- so the same value hashes the same whatever produced it, and equal
 * integers of different widths (int4 = int8) hash the same too.  Range keys use
 * the same canonical value as an int64.
 */
static inline Datum
bloom_normalize_datum(Datum value, int16 typlen)
{
	switch (typlen)
	{
		case 1:
			return Int64GetDatum((int64) (int8) value);
		case 2:
			return Int64GetDatum((int64) (int16) value);
		case 4:
			return Int64GetDatum((int64) (int32) value);
		default:
			return value;
	}
}

#endif							/* BLOOMFILTER_H */

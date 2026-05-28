/*-------------------------------------------------------------------------
 *
 * bloomfilter.h
 *	  Space-efficient set membership testing
 *
 * Copyright (c) 2018-2021, PostgreSQL Global Development Group
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
extern void bloom_free(bloom_filter *filter);
extern void bloom_add_element(bloom_filter *filter, unsigned char *elem,
							  size_t len);
extern bool bloom_lacks_element(bloom_filter *filter, unsigned char *elem,
								size_t len);
extern double bloom_prop_bits_set(bloom_filter *filter);
extern double bloom_false_positive_rate(bloom_filter *filter);
extern uint64 bloom_total_bits(bloom_filter *filter);
extern bloom_filter *bloom_create_aggresive(int64 total_elems,
											int work_mem, uint64 seed);
extern Size bloom_bitset_bytes(bloom_filter *filter);
extern unsigned char *bloom_get_bitset(bloom_filter *filter);
extern uint64 bloom_get_seed(bloom_filter *filter);
extern int bloom_get_k(bloom_filter *filter);

#endif							/* BLOOMFILTER_H */

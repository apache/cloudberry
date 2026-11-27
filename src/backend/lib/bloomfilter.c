/*-------------------------------------------------------------------------
 *
 * bloomfilter.c
 *		Space-efficient set membership testing
 *
 * A Bloom filter is a probabilistic data structure that is used to test an
 * element's membership of a set.  False positives are possible, but false
 * negatives are not; a test of membership of the set returns either "possibly
 * in set" or "definitely not in set".  This is typically very space efficient,
 * which can be a decisive advantage.
 *
 * Elements can be added to the set, but not removed.  The more elements that
 * are added, the larger the probability of false positives.  Caller must hint
 * an estimated total size of the set when the Bloom filter is initialized.
 * This is used to balance the use of memory against the final false positive
 * rate.
 *
 * The implementation is well suited to data synchronization problems between
 * unordered sets, especially where predictable performance is important and
 * some false positives are acceptable.  It's also well suited to cache
 * filtering problems where a relatively small and/or low cardinality set is
 * fingerprinted, especially when many subsequent membership tests end up
 * indicating that values of interest are not present.  That should save the
 * caller many authoritative lookups, such as expensive probes of a much larger
 * on-disk structure.
 *
 * Copyright (c) 2018-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/lib/bloomfilter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "common/hashfn.h"
#include "lib/bloomfilter.h"
#include "port/pg_bitutils.h"
#include "utils/guc.h"

#define MAX_HASH_FUNCS		10

/*
 * A disabled runtime probe samples again after this many skipped rows, doubling
 * the gap every time the sample confirms the filter is still ineffective, up to
 * BLOOM_PROBE_MAX_SKIP rows between samples.
 */
#define BLOOM_PROBE_MIN_SKIP	((uint64) BLOOM_PROBE_SAMPLE_SIZE)
#define BLOOM_PROBE_MAX_SKIP	(UINT64CONST(64) * 1024 * 1024)

struct bloom_filter
{
	/* K hash functions are used, seeded by caller's seed */
	int			k_hash_funcs;
	uint64		seed;
	/* m is bitset size, in bits.  Must be a power of two <= 2^32.  */
	uint64		m;
	/* Runtime probe sampling, see bloom_runtime_probe() */
	int			probe_sample_count;		/* probes in the current window */
	int			probe_sample_rejected;	/* rejections in the current window */
	int			probe_last_rejected;	/* rejections in the last full window,
										 * or -1 if no window completed yet */
	bool		probe_disabled;
	uint64		probe_skip_left;		/* rows to skip before sampling again */
	uint64		probe_skip_gap;			/* current gap between samples */
	unsigned char bitset[FLEXIBLE_ARRAY_MEMBER];
};

static int	my_bloom_power(uint64 target_bitset_bits);
static int	optimal_k(uint64 bitset_bits, int64 total_elems);
static void k_hashes(bloom_filter *filter, uint32 *hashes, unsigned char *elem,
					 size_t len);
static inline uint32 mod_m(uint32 val, uint64 m);

/*
 * Create Bloom filter in caller's memory context.  We aim for a false positive
 * rate of between 1% and 2% when bitset size is not constrained by memory
 * availability.
 *
 * total_elems is an estimate of the final size of the set.  It should be
 * approximately correct, but the implementation can cope well with it being
 * off by perhaps a factor of five or more.  See "Bloom Filters in
 * Probabilistic Verification" (Dillinger & Manolios, 2004) for details of why
 * this is the case.
 *
 * bloom_work_mem is sized in KB, in line with the general work_mem convention.
 * This determines the size of the underlying bitset (trivial bookkeeping space
 * isn't counted).  The bitset is always sized as a power of two number of
 * bits, and the largest possible bitset is 512MB (2^32 bits).  The
 * implementation allocates only enough memory to target its standard false
 * positive rate, using a simple formula with caller's total_elems estimate as
 * an input.  The bitset might be as small as 1MB, even when bloom_work_mem is
 * much higher.
 *
 * The Bloom filter is seeded using a value provided by the caller.  Using a
 * distinct seed value on every call makes it unlikely that the same false
 * positives will reoccur when the same set is fingerprinted a second time.
 * Callers that don't care about this pass a constant as their seed, typically
 * 0.  Callers can also use a pseudo-random seed, eg from pg_prng_uint64().
 */
bloom_filter *
bloom_create(int64 total_elems, int bloom_work_mem, uint64 seed)
{
	return bloom_create_with_minimum(total_elems, bloom_work_mem, seed, 1024 * 1024);
}

/*
 * Like bloom_create, with a caller-selected power-of-two minimum bitset size
 * in bytes.  bloom_create keeps its historical 1 MiB floor.
 */
bloom_filter *
bloom_create_with_minimum(int64 total_elems, int bloom_work_mem, uint64 seed,
						  Size minimum_bytes)
{
	bloom_filter *filter;
	int			bloom_power;
	uint64		bitset_bytes;
	uint64		bitset_bits;

	/*
	 * Aim for two bytes per element; this is sufficient to get a false
	 * positive rate below 1%, independent of the size of the bitset or total
	 * number of elements.  Also, if rounding down the size of the bitset to
	 * the next lowest power of two turns out to be a significant drop, the
	 * false positive rate still won't exceed 2% in almost all cases.
	 */
	bitset_bytes = Min(bloom_work_mem * UINT64CONST(1024), total_elems * 2);
	Assert(minimum_bytes > 0 && minimum_bytes <= UINT64CONST(512) * 1024 * 1024);
	Assert((minimum_bytes & (minimum_bytes - 1)) == 0);
	bitset_bytes = Max(minimum_bytes, bitset_bytes);

	/*
	 * Size in bits should be the highest power of two <= target.  bitset_bits
	 * is uint64 because PG_UINT32_MAX is 2^32 - 1, not 2^32
	 */
	bloom_power = my_bloom_power(bitset_bytes * BITS_PER_BYTE);
	bitset_bits = UINT64CONST(1) << bloom_power;
	bitset_bytes = bitset_bits / BITS_PER_BYTE;

	/* Allocate bloom filter with unset bitset */
	filter = palloc0(offsetof(bloom_filter, bitset) +
					 sizeof(unsigned char) * bitset_bytes);
	filter->k_hash_funcs = optimal_k(bitset_bits, total_elems);
	filter->seed = seed;
	filter->m = bitset_bits;
	filter->probe_last_rejected = -1;

	return filter;
}

/*
 * Free Bloom filter
 */
void
bloom_free(bloom_filter *filter)
{
	pfree(filter);
}

/*
 * Add element to Bloom filter
 */
void
bloom_add_element(bloom_filter *filter, unsigned char *elem, size_t len)
{
	uint32		hashes[MAX_HASH_FUNCS];
	int			i;

	k_hashes(filter, hashes, elem, len);

	/* Map a bit-wise address to a byte-wise address + bit offset */
	for (i = 0; i < filter->k_hash_funcs; i++)
	{
		filter->bitset[hashes[i] >> 3] |= 1 << (hashes[i] & 7);
	}
}

/*
 * Test if Bloom filter definitely lacks element.
 *
 * Returns true if the element is definitely not in the set of elements
 * observed by bloom_add_element().  Otherwise, returns false, indicating that
 * element is probably present in set.
 */
bool
bloom_lacks_element(bloom_filter *filter, unsigned char *elem, size_t len)
{
	uint32		hashes[MAX_HASH_FUNCS];
	int			i;

	k_hashes(filter, hashes, elem, len);

	/* Map a bit-wise address to a byte-wise address + bit offset */
	for (i = 0; i < filter->k_hash_funcs; i++)
	{
		if (!(filter->bitset[hashes[i] >> 3] & (1 << (hashes[i] & 7))))
			return true;
	}

	return false;
}

bool
bloom_runtime_probe_disabled(const bloom_filter *filter)
{
	return filter->probe_disabled;
}

/*
 * Should the caller skip probing for this row?  Call once per row, before
 * reading the key and calling bloom_runtime_probe().  A disabled filter is
 * skipped, but it counts
 * the skipped rows and starts a new sample window once its gap is used up, so
 * that a filter switched off by an unrepresentative first sample (rows loaded
 * in key order, for instance) gets switched on again.
 */
bool
bloom_runtime_probe_skip(bloom_filter *filter)
{
	if (!filter->probe_disabled)
		return false;
	if (filter->probe_skip_left > 1)
	{
		filter->probe_skip_left--;
		return true;
	}

	/* Gap used up: sample a new window with this row. */
	filter->probe_disabled = false;
	filter->probe_skip_left = 0;
	filter->probe_sample_count = 0;
	filter->probe_sample_rejected = 0;
	return false;
}

/* Rank by rejection rate, including partial samples. */
int
bloom_runtime_probe_rank(const bloom_filter *filter)
{
	if (filter->probe_disabled)
		return -1;
	if (filter->probe_last_rejected >= 0)
		return filter->probe_last_rejected;
	if (filter->probe_sample_count == 0)
		return 0;
	return filter->probe_sample_rejected * BLOOM_PROBE_SAMPLE_SIZE /
		filter->probe_sample_count;
}

/*
 * Return true for a definite miss.
 *
 * Probes are counted in windows of BLOOM_PROBE_SAMPLE_SIZE.  When a window
 * rejects fewer than gp_runtime_filter_min_reject_pct percent of its keys, the
 * filter is disabled: bloom_runtime_probe_skip() then tells the caller to pass
 * rows through, and after a gap of skipped rows a new window is sampled.  The
 * gap starts at BLOOM_PROBE_MIN_SKIP and doubles each time a window confirms
 * the filter is still ineffective, up to BLOOM_PROBE_MAX_SKIP; a window that
 * rejects enough keys resets it.  The last probe of a window still returns its
 * actual result.  The state belongs to this filter and lasts across partitions
 * and rescans that reuse it.  A threshold of 0 never disables the filter.
 */
bool
bloom_runtime_probe(bloom_filter *filter, unsigned char *elem, size_t len)
{
	bool		lacks;

	if (filter->probe_disabled)
		return false;

	lacks = bloom_lacks_element(filter, elem, len);
	filter->probe_sample_rejected += lacks;
	if (++filter->probe_sample_count == BLOOM_PROBE_SAMPLE_SIZE)
	{
		filter->probe_last_rejected = filter->probe_sample_rejected;
		if (filter->probe_sample_rejected * 100 <
			BLOOM_PROBE_SAMPLE_SIZE * gp_runtime_filter_min_reject_pct)
		{
			filter->probe_skip_gap = filter->probe_skip_gap == 0 ?
				BLOOM_PROBE_MIN_SKIP :
				Min(filter->probe_skip_gap * 2, BLOOM_PROBE_MAX_SKIP);
			filter->probe_skip_left = filter->probe_skip_gap;
			filter->probe_disabled = true;
		}
		else
			filter->probe_skip_gap = 0;
		filter->probe_sample_count = 0;
		filter->probe_sample_rejected = 0;
	}
	return lacks;
}

/*
 * What proportion of bits are currently set?
 *
 * Returns proportion, expressed as a multiplier of filter size.  That should
 * generally be close to 0.5, even when we have more than enough memory to
 * ensure a false positive rate within target 1% to 2% band, since more hash
 * functions are used as more memory is available per element.
 *
 * This is the only instrumentation that is low overhead enough to appear in
 * debug traces.  When debugging Bloom filter code, it's likely to be far more
 * interesting to directly test the false positive rate.
 */
double
bloom_prop_bits_set(bloom_filter *filter)
{
	int			bitset_bytes = filter->m / BITS_PER_BYTE;
	uint64		bits_set = pg_popcount((char *) filter->bitset, bitset_bytes);

	return bits_set / (double) filter->m;
}

/*
 * Which element in the sequence of powers of two is less than or equal to
 * target_bitset_bits?
 *
 * Value returned here must be generally safe as the basis for actual bitset
 * size.
 *
 * Bitset is never allowed to exceed 2 ^ 32 bits (512MB).  This is sufficient
 * for the needs of all current callers, and allows us to use 32-bit hash
 * functions.  It also makes it easy to stay under the MaxAllocSize restriction
 * (caller needs to leave room for non-bitset fields that appear before
 * flexible array member, so a 1GB bitset would use an allocation that just
 * exceeds MaxAllocSize).
 */
static int
my_bloom_power(uint64 target_bitset_bits)
{
	int			bloom_power = -1;

	while (target_bitset_bits > 0 && bloom_power < 32)
	{
		bloom_power++;
		target_bitset_bits >>= 1;
	}

	return bloom_power;
}

/*
 * Determine optimal number of hash functions based on size of filter in bits,
 * and projected total number of elements.  The optimal number is the number
 * that minimizes the false positive rate.
 */
static int
optimal_k(uint64 bitset_bits, int64 total_elems)
{
	int			k = rint(log(2.0) * bitset_bits / total_elems);

	return Max(1, Min(k, MAX_HASH_FUNCS));
}

/*
 * Generate k hash values for element.
 *
 * Caller passes array, which is filled-in with k values determined by hashing
 * caller's element.
 *
 * Only 2 real independent hash functions are actually used to support an
 * interface of up to MAX_HASH_FUNCS hash functions; enhanced double hashing is
 * used to make this work.  The main reason we prefer enhanced double hashing
 * to classic double hashing is that the latter has an issue with collisions
 * when using power of two sized bitsets.  See Dillinger & Manolios for full
 * details.
 */
static void
k_hashes(bloom_filter *filter, uint32 *hashes, unsigned char *elem, size_t len)
{
	uint64		hash;
	uint32		x,
				y;
	uint64		m;
	int			i;

	/* Use 64-bit hashing to get two independent 32-bit hashes */
	hash = DatumGetUInt64(hash_any_extended(elem, len, filter->seed));
	x = (uint32) hash;
	y = (uint32) (hash >> 32);
	m = filter->m;

	x = mod_m(x, m);
	y = mod_m(y, m);

	/* Accumulate hashes */
	hashes[0] = x;
	for (i = 1; i < filter->k_hash_funcs; i++)
	{
		x = mod_m(x + y, m);
		y = mod_m(y + i, m);

		hashes[i] = x;
	}
}

/*
 * Calculate "val MOD m" inexpensively.
 *
 * Assumes that m (which is bitset size) is a power of two.
 *
 * Using a power of two number of bits for bitset size allows us to use bitwise
 * AND operations to calculate the modulo of a hash value.  It's also a simple
 * way of avoiding the modulo bias effect.
 */
static inline uint32
mod_m(uint32 val, uint64 m)
{
	Assert(m <= PG_UINT32_MAX + UINT64CONST(1));
	Assert(((m - 1) & m) == 0);

	return val & (m - 1);
}

Size
bloom_bitset_bytes(const bloom_filter *filter)
{
	return filter != NULL ? (Size) (filter->m / BITS_PER_BYTE) : 0;
}

const unsigned char *
bloom_bitset_data(const bloom_filter *filter)
{
	return filter != NULL ? filter->bitset : NULL;
}

/*
 * Create a filter sized exactly as bloom_create(total_elems, work_mem, seed) and
 * initialize its bitset from the supplied bytes in one step.  Returns NULL if the
 * supplied length is not the filter's bitset size, so a wrongly-sized bitset is
 * rejected rather than partially loaded.  This is the only supported way to
 * populate a filter from serialized bytes; afterwards a filter is only ever grown
 * by bloom_add_element (or a raw bitwise OR of two equal-sized serialized parts,
 * as Anser's coordinator fold does).
 */
bloom_filter *
bloom_create_from_bitset(int64 total_elems, int bloom_work_mem, uint64 seed,
						 const unsigned char *bitset, Size bitset_len)
{
	return bloom_create_from_bitset_with_minimum(total_elems, bloom_work_mem,
												   seed, bitset, bitset_len, 1024 * 1024);
}

/*
 * Like bloom_create_from_bitset, for a filter created with
 * bloom_create_with_minimum: pass the same minimum the producer used.
 */
bloom_filter *
bloom_create_from_bitset_with_minimum(int64 total_elems, int bloom_work_mem,
									 uint64 seed, const unsigned char *bitset,
									 Size bitset_len, Size minimum_bytes)
{
	bloom_filter *filter;

	if (bitset == NULL)
		return NULL;

	filter = bloom_create_with_minimum(total_elems, bloom_work_mem, seed, minimum_bytes);
	if (bitset_len != (Size) (filter->m / BITS_PER_BYTE))
	{
		bloom_free(filter);
		return NULL;
	}

	memcpy(filter->bitset, bitset, bitset_len);
	return filter;
}

double
bloom_false_positive_rate(bloom_filter *filter)
{
	if (filter == NULL)
		return 0;

	double prop = bloom_prop_bits_set(filter);
	return pow(prop, filter->k_hash_funcs);
}

uint64
bloom_total_bits(bloom_filter *filter)
{
	if (filter == NULL)
		return 0;
	return filter->m;
}

/*
 * Create Bloom filter in caller's memory context.
 *
 * Our goal is to limit the size of the filter to 2MB, and if possible, use
 * memory as little as possible with false positive rate around 10%.
 * 
 * If there are too many elements that cause us to fail to guarantee an
 * appropriate false positive rate, we will give up this strategy and return
 * a NULL value.
 */
bloom_filter *
bloom_create_aggresive(int64 total_elems, int work_mem, uint64 seed)
{
	bloom_filter *filter;
	int		bloom_power;
	uint64	bitset_bytes;
	uint64	bitset_bits;
	double	bits_per_elem;

	bitset_bytes = Min(work_mem * UINT64CONST(1024), total_elems * 9 / 8);
	bitset_bytes = Max(128 * 1024, bitset_bytes); /* at least 1M bits */
	bitset_bytes = Min(2 * 1024 * 1024, bitset_bytes); /* up to 2MB */

	bloom_power = my_bloom_power(bitset_bytes * BITS_PER_BYTE);
	bitset_bits = UINT64CONST(1) << bloom_power;
	bitset_bytes = bitset_bits / BITS_PER_BYTE;
	bits_per_elem = 1.0 * bitset_bits / total_elems;

	if (bits_per_elem < 1.6)
		return NULL; /* too many elements */

	/* Allocate bloom filter with unset bitset */
	filter = palloc0(offsetof(bloom_filter, bitset) +
					 sizeof(unsigned char) * bitset_bytes);
	/* balance false positive rate and calculation costs */
	filter->k_hash_funcs = bits_per_elem >= 3.5 ? 3 : 2;
	filter->seed = seed;
	filter->m = bitset_bits;
	filter->probe_last_rejected = -1;

	return filter;
}
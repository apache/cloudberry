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
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * anserfilter.c
 *	  Bloom-filter payload helpers for Anser channels.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/src/anserfilter.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "anser.h"
#include "anserfilter.h"
#include "common/hashfn.h"
#include "port/pg_bitutils.h"

static bool AnserBloomValidateHeader(const AnserBloomPartHeader *header,
									 Size payload_len);

/*
 * Map Anser's payload budget to bloom_create's work_mem (KB): the space left for
 * the bitset after the part header.  Callers must have checked that
 * max_payload_bytes leaves room for a header.
 */
static int
AnserBloomWorkMemKb(Size max_payload_bytes)
{
	return (int) ((max_payload_bytes - sizeof(AnserBloomPartHeader)) / 1024);
}

uint64
AnserBloomSeed(const char *condition_key)
{
	if (condition_key == NULL)
		return 0;

	return hash_bytes_extended((const unsigned char *) condition_key,
						   strlen(condition_key), 0);
}

/*
 * Build an empty bloom filter for a channel from the caller's parameters.
 *
 * The producer builds its filter here; the consumer rebuilds an identical one
 * from the received bitset (AnserBloomDeserializePart -> bloom_create_from_bitset)
 * using the SAME (total_elems, max_payload_bytes, seed) -- carried in the plan
 * node's custom_private and derived from the shared condition key -- so every
 * segment and the consumer realize a byte-for-byte identical filter shape.  This
 * is why the serialized part header does not need to carry the bitset parameters:
 * the reconstructing side already knows them.
 *
 * Sizing is bloom_create's, but the decision to keep the result is ours.  Two
 * ways it can come back unusable:
 *
 *   - Too large to send.  bloom_create ends with Max(1 MB, bitset), a floor
 *     that overrides the work_mem cap, so a payload cap below 1 MB yields a
 *     bitset that cannot be shipped.
 *   - Too thin to help.  bloom_create has no minimum density (unlike
 *     bloom_create_aggresive, which refuses below 1.6 bits/key), so a large
 *     enough total_elems against a fixed cap gets you a filter that matches
 *     almost everything.
 *
 * Both are judged from the filter's own accessors rather than by re-deriving
 * bloom_create's arithmetic here: my_bloom_power() and the floor are private to
 * lib/bloomfilter.c, and a copy of them would go quietly out of date on a
 * kernel rebase.  The cost of learning the answer this way is one palloc0 that
 * is immediately freed -- against a build-side scan, which is what returning
 * NULL here avoids, that is nothing.
 */
bloom_filter *
AnserBloomCreate(int64 total_elems, Size max_payload_bytes, uint64 seed)
{
	bloom_filter *filter;
	Size		serialized;
	double		bits_per_key;

	/*
	 * These were an Assert on the grounds that internal callers always pass a
	 * payload cap with room for a header.  They are checks because the caller
	 * is a plan node, and its parameters arrived from the coordinator in
	 * custom_private -- not somewhere an assertion belongs.  The cap also
	 * cannot merely be >= the header: AnserBloomWorkMemKb subtracts the header
	 * from a Size, so a smaller cap would underflow to an enormous work_mem.
	 */
	if (total_elems <= 0 || max_payload_bytes <= sizeof(AnserBloomPartHeader))
		return NULL;

	filter = bloom_create(total_elems, AnserBloomWorkMemKb(max_payload_bytes),
						  seed);
	serialized = AnserBloomSerializedSize(filter);
	bits_per_key = (double) bloom_total_bits(filter) / (double) total_elems;

	if (serialized > max_payload_bytes)
	{
		ANSER_DEBUG("anser: not building a filter: smallest bitset serializes to %zu bytes, cap is %zu",
					serialized, max_payload_bytes);
		bloom_free(filter);
		return NULL;
	}

	if (bits_per_key < ANSER_BLOOM_MIN_BITS_PER_KEY)
	{
		ANSER_DEBUG("anser: not building a filter for %ld key(s) in %zu bytes: %.2f bits/key, below the %.1f floor",
					(long) total_elems, max_payload_bytes, bits_per_key,
					ANSER_BLOOM_MIN_BITS_PER_KEY);
		bloom_free(filter);
		return NULL;
	}

	return filter;
}

/*
 * Is this serialized payload still selective enough to be worth delivering?
 *
 * Counts the set bits in the wire form, so the coordinator can ask it of a
 * merged accumulator without rebuilding a filter -- which is the point, because
 * the union of N parts is denser than any one of them.  Three parts at 60% fill
 * OR together to as much as 94%, so the producers' own checks do not protect
 * consumers from the merged result; this is the check that does.
 *
 * Fill, not false positive rate: the serialized part carries no hash count, so
 * fill is all there is to go on here.  ANSER_BLOOM_MERGED_MAX_FILL is set
 * accordingly -- high enough that it cannot reject a filter that is still good
 * at a large k, which makes it a backstop rather than a tuning knob.
 */
bool
AnserBloomPartWorthSending(const void *payload, Size payload_len)
{
	const char *bits;
	Size		bitset_bytes;
	uint64		bits_set;
	double		fill;

	if (!AnserBloomLooksLikePart(payload, payload_len))
		return false;

	bitset_bytes = payload_len - sizeof(AnserBloomPartHeader);
	if (bitset_bytes == 0)
		return false;

	bits = (const char *) payload + sizeof(AnserBloomPartHeader);
	bits_set = pg_popcount(bits, (int) bitset_bytes);
	fill = (double) bits_set / (double) (bitset_bytes * BITS_PER_BYTE);

	if (fill > ANSER_BLOOM_MERGED_MAX_FILL)
	{
		ANSER_DEBUG("anser: merged filter is %.1f%% full, above the %.0f%% limit; not worth delivering",
					fill * 100.0, ANSER_BLOOM_MERGED_MAX_FILL * 100.0);
		return false;
	}

	return true;
}

Size
AnserBloomSerializedSize(const bloom_filter *filter)
{
	if (filter == NULL)
		return 0;

	return sizeof(AnserBloomPartHeader) + bloom_bitset_bytes(filter);
}

/*
 * Does this payload look like a serialized bloom part?  Used by the in-place fold
 * to confirm both the accumulator and the incoming payload are well-formed parts
 * before OR-ing their bitsets.  A false positive is effectively impossible: a
 * part must carry the ABF1 magic, a known version, and sane part counts.
 */
bool
AnserBloomLooksLikePart(const void *payload, Size payload_len)
{
	if (payload == NULL || payload_len < sizeof(AnserBloomPartHeader))
		return false;

	return AnserBloomValidateHeader((const AnserBloomPartHeader *) payload,
									payload_len);
}

/*
 * Fold an incoming part into an accumulator part IN PLACE.
 *
 * When a merged part and the incoming part are the same serialized size (they
 * share bitset params derived from the condition key), the union is a pure
 * bitwise OR of the two bitsets plus a bump of the merged header's fold count --
 * no reallocation.  This mutates `acc` directly, so the caller must hold whatever
 * lock guards the buffer (AnserChannelLock, for the channel payload).
 *
 * Returns true only when the in-place union applied.  It returns false -- and
 * leaves `acc` untouched (all checks run before any write) -- when the union
 * cannot be done by raw OR: sizes differ, either side is not a valid part, or
 * the filter parameters disagree.  Since every part on a channel shares the same
 * (condition-key-derived) parameters and therefore the same serialized size, the
 * first part is stored verbatim and every later part folds in here; a false
 * return means a malformed/mismatched payload and the caller cancels the channel.
 */
bool
AnserBloomFoldPartInPlace(void *acc, Size acc_len,
						  const void *part, Size part_len)
{
	AnserBloomPartHeader *ah;
	unsigned char *abits;
	const unsigned char *pbits;
	Size		bitset_bytes;
	Size		i;

	/* Only a same-sized, valid part-vs-part union can be done by raw OR. */
	if (acc_len != part_len ||
		!AnserBloomLooksLikePart(acc, acc_len) ||
		!AnserBloomLooksLikePart(part, part_len))
		return false;

	ah = (AnserBloomPartHeader *) acc;

	/*
	 * Equal serialized size is sufficient: every part on a channel is built by
	 * bloom_create from the same (condition-key-derived) parameters, so equal
	 * length implies an identical bitset shape.  The bitset is whatever follows
	 * the header, so its length is the payload length minus the header.
	 */
	bitset_bytes = acc_len - sizeof(AnserBloomPartHeader);
	abits = (unsigned char *) acc + sizeof(AnserBloomPartHeader);
	pbits = (const unsigned char *) part + sizeof(AnserBloomPartHeader);

	for (i = 0; i < bitset_bytes; i++)
		abits[i] |= pbits[i];

	/* One more segment part folded into the running merged part. */
	ah->total_parts += 1;

	return true;
}

/*
 * Serialize one filter as a wire part: an AnserBloomPartHeader followed by the
 * raw bitset.  Fails (returns false) on bogus arguments or a too-small buffer.
 */
bool
AnserBloomSerializePart(const bloom_filter *filter, uint32 part_index,
						uint32 total_parts, void *buffer, Size buffer_size,
						Size *payload_len)
{
	AnserBloomPartHeader header;
	Size		bitset_bytes;
	Size		total_len;

	if (payload_len != NULL)
		*payload_len = 0;

	if (filter == NULL || buffer == NULL || total_parts == 0 ||
		part_index >= total_parts)
		return false;

	bitset_bytes = bloom_bitset_bytes(filter);
	total_len = sizeof(AnserBloomPartHeader) + bitset_bytes;
	if (buffer_size < total_len)
		return false;

	MemSet(&header, 0, sizeof(header));
	header.magic = ANSER_BLOOM_PART_MAGIC;
	header.version = ANSER_BLOOM_PART_VERSION;
	header.part_index = part_index;
	header.total_parts = total_parts;

	memcpy(buffer, &header, sizeof(header));
	memcpy((char *) buffer + sizeof(header), bloom_bitset_data(filter),
		   bitset_bytes);

	if (payload_len != NULL)
		*payload_len = total_len;
	return true;
}

/*
 * Rebuild the filter from a received merged part.
 *
 * The bitset parameters are NOT taken from the wire header: the caller passes
 * the same (total_elems, max_payload_bytes, seed) used to produce the filter --
 * it holds them in the plan node, so both ends agree by construction.  We rebuild
 * the empty filter from those, then load the received bitset into it.  The wire
 * header is still validated (magic/version/counts) and, crucially, the received
 * bitset length must exactly match the size the local parameters imply; any
 * mismatch (version/parameter skew, truncation) returns NULL so the consumer
 * fails open rather than loading a wrongly-shaped bitset.  part_index/total_parts
 * are surfaced from the header for diagnostics.
 */
bloom_filter *
AnserBloomDeserializePart(const void *payload, Size payload_len,
						  int64 total_elems, Size max_payload_bytes, uint64 seed,
						  uint32 *part_index, uint32 *total_parts)
{
	const AnserBloomPartHeader *header;
	bloom_filter *filter;

	if (part_index != NULL)
		*part_index = 0;
	if (total_parts != NULL)
		*total_parts = 0;

	if (payload == NULL || payload_len < sizeof(AnserBloomPartHeader))
		return NULL;

	header = (const AnserBloomPartHeader *) payload;
	if (!AnserBloomValidateHeader(header, payload_len))
		return NULL;

	if (max_payload_bytes <= sizeof(AnserBloomPartHeader))
		return NULL;
	if (total_elems < 1)
		total_elems = 1;

	/*
	 * Build the filter straight from the received bitset, sized by our own
	 * parameters.  bloom_create_from_bitset returns NULL unless the received
	 * length is exactly the size those parameters imply, so the fail-open
	 * described above happens here.  A filter is thus only ever populated at
	 * construction and, from then on, only grown by add/union -- never re-set.
	 */
	filter = bloom_create_from_bitset(total_elems,
									  AnserBloomWorkMemKb(max_payload_bytes),
									  seed,
									  (const unsigned char *) payload +
									  sizeof(AnserBloomPartHeader),
									  payload_len - sizeof(AnserBloomPartHeader));
	if (filter == NULL)
		return NULL;

	if (part_index != NULL)
		*part_index = header->part_index;
	if (total_parts != NULL)
		*total_parts = header->total_parts;
	return filter;
}

/*
 * Validate the wire framing of a part header.  The bitset parameters are not
 * carried on the wire (both ends rebuild the filter from the shared plan
 * parameters), so this only checks the framing: magic/version, a sane fold
 * count, and that the payload carries a header plus at least some bitset.  The
 * authoritative size check -- that the received bitset matches the size the local
 * parameters imply -- is done in AnserBloomDeserializePart.
 */
static bool
AnserBloomValidateHeader(const AnserBloomPartHeader *header, Size payload_len)
{
	if (header == NULL)
		return false;

	if (header->magic != ANSER_BLOOM_PART_MAGIC ||
		header->version != ANSER_BLOOM_PART_VERSION)
		return false;

	if (header->total_parts == 0 || header->part_index >= header->total_parts)
		return false;

	return payload_len > sizeof(AnserBloomPartHeader);
}

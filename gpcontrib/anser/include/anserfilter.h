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
 * anserfilter.h
 *	  Bloom-filter payload helpers for Anser channels.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/include/anserfilter.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANSERFILTER_H
#define ANSERFILTER_H

#include "postgres.h"

#include "lib/bloomfilter.h"

#define ANSER_BLOOM_PART_MAGIC		0x41424631U /* ABF1 */
#define ANSER_BLOOM_PART_VERSION		1U

/*
 * On-wire framing for a serialized bloom part.  It deliberately does NOT carry
 * the bitset parameters (size / seed / hash count): both the producer and the
 * consumer build the filter with bloom_create from the same plan parameters, so
 * the shape is agreed by construction and never reconstructed from the wire.
 * magic/version guard the framing; part_index/total_parts track the coordinator
 * fold count (surfaced as diagnostics).  Kept as a struct for forward
 * extensibility.
 */
typedef struct AnserBloomPartHeader
{
	uint32		magic;
	uint32		version;
	uint32		part_index;
	uint32		total_parts;
} AnserBloomPartHeader;

/*
 * When a bloom filter stops being worth building, or sending once built.
 * Three checks, at the three points where new information arrives.
 *
 * ANSER_BLOOM_MIN_BITS_PER_KEY -- a floor on the *planned* density: bitset bits
 * divided by the number of distinct keys we expect.  Below it no amount of care
 * in building the filter helps, so the filter is not built and, at plan time,
 * the nodes are not even injected.  At 4 bits/key the optimal hash count is 3
 * and the false positive rate is already ~15%; below that it collapses (2
 * bits/key is ~40%).
 *
 * ANSER_BLOOM_MAX_FPR -- the check on the *realized* filter, at publish time.
 * It catches what a row estimate cannot: the estimate was too low, so the
 * filter saturated anyway.  This one is expressed as a false positive rate
 * rather than a fill fraction on purpose.  FPR is fill^k, so a single fill
 * limit is not a single quality bar: 80% full is a 51% FPR at k=3 but only
 * 10.7% at k=10, and a rule that cancelled the latter would be throwing away a
 * filter that eliminates nine probe rows in ten.  The producer has the filter
 * and therefore its k, so it can just ask.
 *
 * ANSER_BLOOM_MERGED_MAX_FILL -- the same question asked by the coordinator of
 * the merged payload, which is the only place it can be asked about what
 * consumers will actually receive.  It has to fall back on fill, because k is
 * derived from plan parameters and is not carried in the serialized part.  It
 * is therefore set where even k=10 is past saving (0.95^10 = 60% FPR), so that
 * it only ever rejects the hopeless.  Putting k in AnserBloomPartHeader would
 * let this use the FPR too.
 *
 * All three numbers are first cuts.  Calibrating them is the "stop building
 * filters that add nothing" part of the bloom-performance work; the debug trace
 * logs fill and FPR at every decision point so that study has data.
 */
#define ANSER_BLOOM_MIN_BITS_PER_KEY	4.0
#define ANSER_BLOOM_MAX_FPR				0.50
#define ANSER_BLOOM_MERGED_MAX_FILL		0.95

/*
 * Is a serialized part (or a merged accumulator of them) still worth
 * delivering?  False when too many of its bits are set for it to reject
 * anything useful.  Works on the wire form, so the coordinator can ask this of
 * a merged payload without rebuilding a filter.
 */
extern bool AnserBloomPartWorthSending(const void *payload, Size payload_len);

extern uint64 AnserBloomSeed(const char *condition_key);

/*
 * Build an empty filter, or return NULL when AnserBloomShapeFor says it is not
 * worth building.  A NULL return is not an error: the producer turns it into an
 * immediate cancel, so consumers stop waiting instead of timing out.
 */
extern bloom_filter *AnserBloomCreate(int64 total_elems,
								  Size max_payload_bytes,
								  uint64 seed);
extern Size AnserBloomSerializedSize(const bloom_filter *filter);
extern bool AnserBloomSerializePart(const bloom_filter *filter,
									uint32 part_index,
									uint32 total_parts,
									void *buffer,
									Size buffer_size,
									Size *payload_len);
extern bloom_filter *AnserBloomDeserializePart(const void *payload,
										  Size payload_len,
										  int64 total_elems,
										  Size max_payload_bytes,
										  uint64 seed,
										  uint32 *part_index,
										  uint32 *total_parts);
extern bool AnserBloomLooksLikePart(const void *payload, Size payload_len);
extern bool AnserBloomFoldPartInPlace(void *acc, Size acc_len,
									  const void *part, Size part_len);

#endif							/* ANSERFILTER_H */

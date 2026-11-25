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
 * anserbloom.h
 *	  Standalone Anser Bloom filter producer/consumer executor helpers.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/include/anserbloom.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ANSERBLOOM_H
#define ANSERBLOOM_H

#include "postgres.h"

#include "anser.h"
#include "lib/bloomfilter.h"
#include "executor/executor.h"

/*
 * Only the low attlen bytes of a by-value Datum belong to the value. Heap
 * access may sign-extend them while AOCO zero-extends them. Hash the value in
 * core's canonical form, sign-extended to 64 bits (bloom_normalize_datum),
 * independent of storage and projection paths: a filter handed to a scan as
 * an SK_BLOOM_FILTER key is probed by core (PassByBloomFilter) and by table
 * AMs such as PAX in that form.  Callers validate by-value key types before
 * reaching the executor.
 */
static inline Datum
AnserNormalizeBloomDatum(Datum value, int16 key_length)
{
	Assert(key_length == 1 || key_length == 2 || key_length == 4 ||
		   key_length == sizeof(Datum));
	return bloom_normalize_datum(value, key_length);
}

/* ERROR-free for a valid by-value key width; NULL handling is the caller's. */
static inline void
AnserBloomAddKey(bloom_filter *filter, Datum value, int16 key_length)
{
	value = AnserNormalizeBloomDatum(value, key_length);
	bloom_add_element(filter, (unsigned char *) &value, sizeof(Datum));
}

static inline bool
AnserBloomLacksKey(bloom_filter *filter, Datum value, int16 key_length)
{
	value = AnserNormalizeBloomDatum(value, key_length);
	return bloom_lacks_element(filter, (unsigned char *) &value, sizeof(Datum));
}

/*
 * AnserBloomLacksKey with adaptive probing: a filter that rejects too few keys
 * turns itself off and samples again later (bloom_runtime_probe).  Ask
 * bloom_runtime_probe_skip() first, once per row; this only probes.
 */
static inline bool
AnserBloomProbe(bloom_filter *filter, Datum value, int16 key_length)
{
	value = AnserNormalizeBloomDatum(value, key_length);
	return bloom_runtime_probe(filter, (unsigned char *) &value, sizeof(Datum));
}

/* Output position, not table attno. No allocation or catalog access. */
static inline int16
AnserKeyLength(PlanState *child, AttrNumber attno)
{
	TupleDesc desc;

	Assert(child != NULL);
	desc = ExecGetResultType(child);
	Assert(desc != NULL);
	Assert(attno >= 1 && attno <= desc->natts);
	Assert(TupleDescAttr(desc, attno - 1)->attbyval);
	return TupleDescAttr(desc, attno - 1)->attlen;
}

/*
 * Opaque executor state handles for the Bloom filter producer and
 * consumer; the struct definitions are private to
 * anserbloomproduce.c and anserbloomconsume.c.
 */
typedef struct AnserBloomFilterProduceState AnserBloomFilterProduceState;
typedef struct AnserBloomFilterConsumeState AnserBloomFilterConsumeState;

/* Producer side: build one Bloom filter part and publish it to the channel. */
extern PGDLLEXPORT AnserBloomFilterProduceState *ExecInitAnserBloomFilterProduce(
										const AnserChannelKey *channel_key,
										int64 total_elems,
										Size max_payload_bytes,
										uint32 part_index,
										uint32 total_parts);
extern PGDLLEXPORT void ExecAnserBloomFilterProduceAddDatum(AnserBloomFilterProduceState *state,
										 Datum value, bool isnull, int16 key_length);
extern PGDLLEXPORT bool ExecAnserBloomFilterProduceHasFilter(AnserBloomFilterProduceState *state);
extern PGDLLEXPORT bool ExecAnserBloomFilterProducePublish(AnserBloomFilterProduceState *state);
extern PGDLLEXPORT bool ExecAnserBloomFilterProduceCancel(AnserBloomFilterProduceState *state);
extern PGDLLEXPORT void ExecEndAnserBloomFilterProduce(AnserBloomFilterProduceState *state);

/* Consumer side: gather all parts of a channel into one Bloom filter. */
extern PGDLLEXPORT AnserBloomFilterConsumeState *ExecInitAnserBloomFilterConsume(
										const AnserChannelKey *channel_key,
										int64 total_elems,
										Size max_payload_bytes,
										uint32 expected_parts);
extern PGDLLEXPORT bool ExecAnserBloomFilterConsume(AnserBloomFilterConsumeState *state,
									long registration_timeout_ms);
extern PGDLLEXPORT bloom_filter *ExecAnserBloomFilterConsumerGetFilter(
									AnserBloomFilterConsumeState *state);
/*
 * The producer count the delivered payload declares, or 0 if no filter was
 * built.  Every part of a channel carries that count and folding does not
 * change it, so this is a cross-check on the payload, not a tally of arrivals:
 * the coordinator delivers only once it holds every part, so a filter that
 * reaches a consumer at all is a complete one.
 */
extern PGDLLEXPORT uint32 ExecAnserBloomFilterConsumerReceivedParts(
									AnserBloomFilterConsumeState *state);
extern PGDLLEXPORT bool ExecAnserBloomFilterConsumerWasCancelled(
								  AnserBloomFilterConsumeState *state);
extern PGDLLEXPORT void ExecEndAnserBloomFilterConsume(AnserBloomFilterConsumeState *state);

#endif							/* ANSERBLOOM_H */

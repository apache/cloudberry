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
 * anserbloomproduce.c
 *	  Standalone Anser Bloom filter producer executor helper.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/src/anserbloomproduce.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "anser.h"
#include "anserbloom.h"
#include "anserfilter.h"
#include "anserpayload.h"
#include "ansersideband.h"
#include "cdb/cdbvars.h"

/*
 * State for a single bloom filter producer: the target channel, the filter
 * being built, and this producer's identity within total_parts.  published
 * and cancelled guard against double publication and drive teardown.
 */
struct AnserBloomFilterProduceState
{
	AnserChannelKey channel_key;
	bloom_filter *filter;
	uint32		part_index;
	uint32		total_parts;
	bool		published;
	bool		cancelled;
};

/*
 * Publish one part.
 *
 * A segment sends it to the coordinator over the dispatch connection; a
 * coordinator-local producer hands it to the same per-query channel table the
 * coordinator merges into, since it has no connection to itself.  total_parts
 * doubles as expected_producers: each producer contributes exactly one part.
 */
static bool
AnserProducePublishPart(AnserBloomFilterProduceState *state,
						const void *payload, Size payload_len, bool cancelled)
{
	if (Gp_role == GP_ROLE_EXECUTE)
		return AnserSidebandPublish(&state->channel_key, ANSER_PAYLOAD_BLOOM,
									state->part_index, state->total_parts,
									payload, payload_len, cancelled);

	return AnserDispatchLocalPublish(&state->channel_key, ANSER_PAYLOAD_BLOOM,
									 state->part_index, state->total_parts,
									 payload, payload_len, cancelled);
}

AnserBloomFilterProduceState *
ExecInitAnserBloomFilterProduce(const AnserChannelKey *channel_key,
								int64 total_elems,
								Size max_payload_bytes,
								uint32 part_index,
								uint32 total_parts)
{
	AnserBloomFilterProduceState *state;
	uint64		seed;

	if (channel_key == NULL || total_parts == 0 || part_index >= total_parts)
		return NULL;

	state = palloc0(sizeof(AnserBloomFilterProduceState));
	state->channel_key = *channel_key;
	state->part_index = part_index;
	state->total_parts = total_parts;
	seed = AnserBloomSeed(channel_key->condition_key);
	state->filter = AnserBloomCreate(total_elems, max_payload_bytes, seed);

	return state;
}

void
ExecAnserBloomFilterProduceAddDatum(AnserBloomFilterProduceState *state,
									Datum value, bool isnull)
{
	/*
	 * A NULL filter means AnserBloomCreate declined to build one -- too many
	 * keys for the payload cap.  There is nothing to add to, and the node has
	 * already published the cancel.
	 */
	if (state == NULL || state->filter == NULL || state->published || isnull)
		return;

	bloom_add_element(state->filter, (unsigned char *) &value, sizeof(Datum));
}

/* Does this producer have a filter to fill?  False means "already hopeless". */
bool
ExecAnserBloomFilterProduceHasFilter(AnserBloomFilterProduceState *state)
{
	return state != NULL && state->filter != NULL;
}

bool
ExecAnserBloomFilterProducePublish(AnserBloomFilterProduceState *state)
{
	Size		payload_size;
	Size		payload_len = 0;
	void	   *payload;
	bool		ok;

	if (state == NULL || state->published)
	{
		ANSER_DEBUG("anser: publish skipped (%s)",
					state == NULL ? "no producer state" : "already published");
		return false;
	}

	if (state->cancelled || state->filter == NULL)
	{
		ANSER_DEBUG("anser: publishing a cancel (%s)",
					state->cancelled ? "producer cancelled" : "no filter");
		state->published = true;
		return AnserProducePublishPart(state, NULL, 0, true);
	}

	/*
	 * Too saturated to be worth anything?  Cancel rather than send.
	 *
	 * This is the check the planner cannot make: it decided the filter was
	 * worth building from a row estimate, and estimates are wrong.  A filter
	 * whose bits are nearly all set matches nearly every probe row, so shipping
	 * it would buy the consumers a hash and k probes per row in exchange for
	 * almost no rows eliminated -- pure overhead on both sides.
	 */
	if (bloom_false_positive_rate(state->filter) > ANSER_BLOOM_MAX_FPR)
	{
		ANSER_DEBUG("anser: publishing a cancel: filter is %.1f%% full, est. FPR %.1f%% above the %.0f%% limit",
					bloom_prop_bits_set(state->filter) * 100.0,
					bloom_false_positive_rate(state->filter) * 100.0,
					ANSER_BLOOM_MAX_FPR * 100.0);
		state->published = true;
		return AnserProducePublishPart(state, NULL, 0, true);
	}

	ANSER_DEBUG("anser: filter is %.1f%% full, est. FPR %.2f%%",
				bloom_prop_bits_set(state->filter) * 100.0,
				bloom_false_positive_rate(state->filter) * 100.0);

	/*
	 * Serialize as a self-contained single part (index 0 of 1).  The coordinator
	 * stores the first part verbatim and OR-folds each later part, bumping the
	 * merged header's fold count, so the final count reflects how many parts were
	 * unioned.  state->part_index / state->total_parts identify this producer to
	 * the channel (expected_producers), not the on-wire part layout.
	 */
	payload_size = AnserBloomSerializedSize(state->filter);
	payload = palloc(payload_size);
	ok = AnserBloomSerializePart(state->filter,
								  0,
								  1,
								  payload,
								  payload_size,
								  &payload_len);
	if (ok)
		ok = AnserProducePublishPart(state, payload, payload_len, false);
	else
		ANSER_DEBUG("anser: publish skipped: could not serialize part (size=%zu)",
					payload_size);

	pfree(payload);
	state->published = true;
	return ok;
}

bool
ExecAnserBloomFilterProduceCancel(AnserBloomFilterProduceState *state)
{
	if (state == NULL || state->published)
		return false;

	state->cancelled = true;
	state->published = true;
	return AnserProducePublishPart(state, NULL, 0, true);
}

/*
 * Free the producer state.
 *
 * Deliberately does NOT publish a cancel for an unpublished producer: whether
 * silence means "ran and was abandoned" or "never ran at all" is knowable only
 * to the node, and the two need opposite handling (see anser_produce_end in
 * anserplanexec.c).
 */
void
ExecEndAnserBloomFilterProduce(AnserBloomFilterProduceState *state)
{
	if (state == NULL)
		return;

	if (state->filter != NULL)
		bloom_free(state->filter);
	pfree(state);
}

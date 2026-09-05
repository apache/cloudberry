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
		return AnserSidebandPublish(&state->channel_key, state->part_index,
									state->total_parts, payload, payload_len,
									cancelled);

	return AnserDispatchLocalPublish(&state->channel_key, state->part_index,
									 state->total_parts, payload, payload_len,
									 cancelled);
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
	if (state == NULL || state->published || isnull)
		return;

	bloom_add_element(state->filter, (unsigned char *) &value, sizeof(Datum));
}

bool
ExecAnserBloomFilterProducePublish(AnserBloomFilterProduceState *state)
{
	Size		payload_size;
	Size		payload_len = 0;
	void	   *payload;
	bool		ok;

	if (state == NULL || state->published)
		return false;

	if (state->cancelled)
	{
		state->published = true;
		return AnserProducePublishPart(state, NULL, 0, true);
	}

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

void
ExecEndAnserBloomFilterProduce(AnserBloomFilterProduceState *state)
{
	if (state == NULL)
		return;

	if (!state->published)
		(void) ExecAnserBloomFilterProduceCancel(state);

	if (state->filter != NULL)
		bloom_free(state->filter);
	pfree(state);
}

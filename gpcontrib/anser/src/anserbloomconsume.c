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
 * anserbloomconsume.c
 *	  Standalone Anser Bloom filter consumer executor helper.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/src/anserbloomconsume.c
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
 * State for one Bloom filter consumer.  Consumes the merged payload for a
 * channel exactly once (consumed), either from the coordinator's shared
 * coordinator.  cancelled records that the producer side aborted instead of
 * delivering the payload.
 */
struct AnserBloomFilterConsumeState
{
	AnserChannelKey channel_key;
	bloom_filter *filter;
	int64		total_elems;	/* filter sizing, shared with the producer */
	Size		max_payload_bytes;
	uint64		seed;
	uint32		expected_parts;
	uint32		received_parts;
	bool		consumed;
	bool		cancelled;
};

static bool ExecAnserBloomFilterConsumeSideband(AnserBloomFilterConsumeState *state,
												long timeout_ms);
static bool ExecAnserBloomFilterConsumeFinish(AnserBloomFilterConsumeState *state,
											  void *payload, Size payload_len);

AnserBloomFilterConsumeState *
ExecInitAnserBloomFilterConsume(const AnserChannelKey *channel_key,
								int64 total_elems, Size max_payload_bytes,
								uint32 expected_parts)
{
	AnserBloomFilterConsumeState *state;

	if (channel_key == NULL || expected_parts == 0)
		return NULL;

	state = palloc0(sizeof(AnserBloomFilterConsumeState));
	state->channel_key = *channel_key;
	state->total_elems = total_elems;
	state->max_payload_bytes = max_payload_bytes;
	state->seed = AnserBloomSeed(channel_key->condition_key);
	state->expected_parts = expected_parts;
	return state;
}

bool
ExecAnserBloomFilterConsume(AnserBloomFilterConsumeState *state,
							long registration_timeout_ms)
{
	if (state == NULL)
		return false;

	if (state->consumed)
		return state->filter != NULL;

	return ExecAnserBloomFilterConsumeSideband(state, registration_timeout_ms);
}

/*
 * Dispatch-connection consume path.
 *
 * On a segment this blocks on our own dispatch socket until the coordinator
 * pushes the merged filter; on the coordinator the merge happened in this very
 * process, so there is nothing to wait for and we just read it.
 */
static bool
ExecAnserBloomFilterConsumeSideband(AnserBloomFilterConsumeState *state,
									long timeout_ms)
{
	void	   *payload = NULL;
	Size		payload_len = 0;
	bool		cancelled = false;
	bool		got;

	if (Gp_role == GP_ROLE_EXECUTE)
		got = AnserSidebandConsumeWait(&state->channel_key, &payload,
									   &payload_len, &cancelled, timeout_ms);
	else
		got = AnserDispatchLocalConsume(&state->channel_key, &payload,
										&payload_len, &cancelled);

	if (!got || cancelled)
	{
		if (payload != NULL)
			pfree(payload);
		state->cancelled = cancelled;
		state->consumed = true;
		return false;
	}

	return ExecAnserBloomFilterConsumeFinish(state, payload, payload_len);
}

/*
 * Turn a merged payload into this consumer's filter.
 *
 * The coordinator unions every segment's part into one before delivery, so a
 * single chunk is deserialized rather than N; the merged header's total_parts
 * records how many were folded, which we surface as the received count.
 */
static bool
ExecAnserBloomFilterConsumeFinish(AnserBloomFilterConsumeState *state,
								  void *payload, Size payload_len)
{
	uint32		part_index = 0;
	uint32		folded = 0;

	state->filter = AnserBloomDeserializePart(payload, payload_len,
											  state->total_elems,
											  state->max_payload_bytes,
											  state->seed,
											  &part_index, &folded);
	state->received_parts = (state->filter != NULL) ? folded : 0;

	if (payload != NULL)
		pfree(payload);
	state->consumed = true;
	return state->filter != NULL;
}

bloom_filter *
ExecAnserBloomFilterConsumerGetFilter(AnserBloomFilterConsumeState *state)
{
	return state != NULL ? state->filter : NULL;
}

uint32
ExecAnserBloomFilterConsumerReceivedParts(AnserBloomFilterConsumeState *state)
{
	return state != NULL ? state->received_parts : 0;
}

bool
ExecAnserBloomFilterConsumerWasCancelled(AnserBloomFilterConsumeState *state)
{
	return state != NULL && state->cancelled;
}

void
ExecEndAnserBloomFilterConsume(AnserBloomFilterConsumeState *state)
{
	if (state == NULL)
		return;

	if (state->filter != NULL)
		bloom_free(state->filter);
	pfree(state);
}

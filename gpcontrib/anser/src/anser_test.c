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
 * anser_test.c
 *	  SQL-callable test helpers for the Anser subsystem.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/src/anser_test.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "anser.h"
#include "anserbloom.h"
#include "anserfilter.h"
#include "ansersideband.h"
#include "cdb/cdbvars.h"
#include "fmgr.h"
#include "lib/bloomfilter.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "varatt.h"

/*
 * Bloom sizing used by the test helpers.  bloom_create floors every filter at
 * 1 MB, so these are the smallest filters we can build; producer and consumer
 * sides must pass the identical pair (that is the whole point of carrying the
 * parameters in the node rather than on the wire).
 */
#define ANSER_TEST_ELEMS		32
#define ANSER_TEST_MAX_PAYLOAD	(1024 * 1024)

PG_FUNCTION_INFO_V1(anser_test_bloom_roundtrip);
PG_FUNCTION_INFO_V1(anser_test_bloom_fold_inplace);
PG_FUNCTION_INFO_V1(anser_test_bloom_rejects_mismatch);
PG_FUNCTION_INFO_V1(anser_test_node_roundtrip);


Datum
anser_test_bloom_roundtrip(PG_FUNCTION_ARGS)
{
	char	   *key = text_to_cstring(PG_GETARG_TEXT_PP(0));
	int32		value_arg = PG_GETARG_INT32(1);
	Datum		value = Int32GetDatum(value_arg);
	uint64		seed = AnserBloomSeed(key);
	bloom_filter *filter;
	bloom_filter *roundtrip;
	char	   *payload;
	Size		payload_size;
	Size		payload_len = 0;
	uint32		part_index = 0;
	uint32		total_parts = 0;
	bool		lacks;

	filter = AnserBloomCreate(32, 1024 * 1024, seed);
	if (filter == NULL)
		PG_RETURN_BOOL(false);

	bloom_add_element(filter, (unsigned char *) &value, sizeof(Datum));
	payload_size = AnserBloomSerializedSize(filter);
	payload = palloc(payload_size);
	if (!AnserBloomSerializePart(filter, 0, 1, payload, payload_size,
								  &payload_len))
		PG_RETURN_BOOL(false);

	roundtrip = AnserBloomDeserializePart(payload, payload_len,
									   32, 1024 * 1024, seed,
									   &part_index, &total_parts);
	if (roundtrip == NULL)
		PG_RETURN_BOOL(false);

	lacks = bloom_lacks_element(roundtrip, (unsigned char *) &value,
							 sizeof(Datum));
	bloom_free(filter);
	bloom_free(roundtrip);
	PG_RETURN_BOOL(!lacks && part_index == 0 && total_parts == 1);
}

/*
 * In-place fold: folding an equally-sized part into a merged part is a bitwise
 * OR of the bitset plus a fold-count bump, mutating the buffer without realloc.
 * This is the coordinator's only combine path: the first part is stored
 * verbatim, every later part folds in here.  A differently-sized part is
 * rejected and leaves the accumulator untouched.
 */
Datum
anser_test_bloom_fold_inplace(PG_FUNCTION_ARGS)
{
	uint64		seed = AnserBloomSeed("inplace_bloom");
	bloom_filter *left;
	bloom_filter *right;
	bloom_filter *big;
	bloom_filter *merged;
	Datum		left_value = Int32GetDatum(7);
	Datum		right_value = Int32GetDatum(9);
	char	   *acc;
	char	   *part;
	char	   *big_part;
	Size		acc_size;
	Size		part_size;
	Size		big_size;
	Size		acc_len = 0;
	Size		part_len = 0;
	Size		big_len = 0;
	uint32		part_index = 0;
	uint32		total_parts = 0;
	uint32		tp_before = 0;
	uint32		tp_after = 0;
	bool		same_ok;
	bool		mismatch_rejected;

	/* Two same-parameter parts: acc is the running merged part, part folds in. */
	left = AnserBloomCreate(32, 1024 * 1024, seed);
	right = AnserBloomCreate(32, 1024 * 1024, seed);
	if (left == NULL || right == NULL)
		PG_RETURN_BOOL(false);
	bloom_add_element(left, (unsigned char *) &left_value, sizeof(Datum));
	bloom_add_element(right, (unsigned char *) &right_value, sizeof(Datum));
	acc_size = AnserBloomSerializedSize(left);
	part_size = AnserBloomSerializedSize(right);
	acc = palloc(acc_size);
	part = palloc(part_size);
	if (!AnserBloomSerializePart(left, 0, 1, acc, acc_size, &acc_len) ||
		!AnserBloomSerializePart(right, 0, 1, part, part_size, &part_len))
	{
		bloom_free(left);
		bloom_free(right);
		PG_RETURN_BOOL(false);
	}
	bloom_free(left);
	bloom_free(right);

	same_ok = AnserBloomFoldPartInPlace(acc, acc_len, part, part_len);
	merged = same_ok ?
		AnserBloomDeserializePart(acc, acc_len, 32, 1024 * 1024, seed,
								  &part_index, &total_parts) : NULL;
	same_ok = same_ok &&
		acc_len == acc_size &&			/* size unchanged, folded in place */
		merged != NULL &&
		part_index == 0 &&
		total_parts == 2 &&				/* one more part folded */
		!bloom_lacks_element(merged, (unsigned char *) &left_value,
							 sizeof(Datum)) &&
		!bloom_lacks_element(merged, (unsigned char *) &right_value,
							 sizeof(Datum));
	if (merged != NULL)
		bloom_free(merged);

	/*
	 * A differently-sized part must be rejected and leave acc untouched.  Since
	 * bloom_create floors every filter at 1 MB, we need a genuinely larger
	 * cardinality/budget to get a bigger (2 MB) bitset than the 1 MB acc.
	 */
	big = AnserBloomCreate(1500000, 4 * 1024 * 1024, seed);
	if (big == NULL)
		PG_RETURN_BOOL(false);
	big_size = AnserBloomSerializedSize(big);
	big_part = palloc(big_size);
	if (!AnserBloomSerializePart(big, 0, 1, big_part, big_size, &big_len))
	{
		bloom_free(big);
		PG_RETURN_BOOL(false);
	}
	bloom_free(big);

	tp_before = ((const AnserBloomPartHeader *) acc)->total_parts;
	mismatch_rejected = big_len != acc_len &&
		!AnserBloomFoldPartInPlace(acc, acc_len, big_part, big_len);
	tp_after = ((const AnserBloomPartHeader *) acc)->total_parts;
	mismatch_rejected = mismatch_rejected && tp_before == tp_after;

	PG_RETURN_BOOL(same_ok && mismatch_rejected);
}

/*
 * Safety regression for the size/format check in AnserBloomDeserializePart.
 *
 * The consumer rebuilds the filter from its OWN (total_elems, max_payload, seed)
 * parameters, then requires the received bitset to be exactly the size those
 * parameters imply and the wire header to carry the expected magic.  A
 * well-formed part must load; a truncated one, an oversized one, and one with a
 * corrupted magic must all be rejected (NULL) so the consumer fails open rather
 * than loading a wrongly-shaped bitset.  Returns true iff the good part loads and
 * every bad one is rejected.
 */
Datum
anser_test_bloom_rejects_mismatch(PG_FUNCTION_ARGS)
{
	uint64		seed = AnserBloomSeed("reject_mismatch");
	bloom_filter *filter;
	char	   *good;
	Size		good_size;
	Size		good_len = 0;
	bloom_filter *ok_load;
	bloom_filter *short_load;
	bloom_filter *long_load;
	bloom_filter *magic_load;
	AnserBloomPartHeader *hdr;
	uint32		saved_magic;
	bool		ok;

	filter = AnserBloomCreate(ANSER_TEST_ELEMS, ANSER_TEST_MAX_PAYLOAD, seed);
	if (filter == NULL)
		PG_RETURN_BOOL(false);

	good_size = AnserBloomSerializedSize(filter);
	good = palloc(good_size);
	if (!AnserBloomSerializePart(filter, 0, 1, good, good_size, &good_len))
	{
		bloom_free(filter);
		PG_RETURN_BOOL(false);
	}
	bloom_free(filter);

	/* Well-formed: loads. */
	ok_load = AnserBloomDeserializePart(good, good_len, ANSER_TEST_ELEMS,
										ANSER_TEST_MAX_PAYLOAD, seed, NULL, NULL);

	/* One byte short of the expected bitset: rejected. */
	short_load = AnserBloomDeserializePart(good, good_len - 1, ANSER_TEST_ELEMS,
										   ANSER_TEST_MAX_PAYLOAD, seed, NULL, NULL);

	/* Claiming more bytes than the expected bitset: rejected. */
	long_load = AnserBloomDeserializePart(good, good_len + 1, ANSER_TEST_ELEMS,
										  ANSER_TEST_MAX_PAYLOAD, seed, NULL, NULL);

	/* Corrupted wire magic: rejected before the size check. */
	hdr = (AnserBloomPartHeader *) good;
	saved_magic = hdr->magic;
	hdr->magic = saved_magic ^ 0xFFFFFFFFU;
	magic_load = AnserBloomDeserializePart(good, good_len, ANSER_TEST_ELEMS,
										   ANSER_TEST_MAX_PAYLOAD, seed, NULL, NULL);
	hdr->magic = saved_magic;

	ok = ok_load != NULL && short_load == NULL && long_load == NULL &&
		magic_load == NULL;

	if (ok_load != NULL)
		bloom_free(ok_load);
	if (short_load != NULL)
		bloom_free(short_load);
	if (long_load != NULL)
		bloom_free(long_load);
	if (magic_load != NULL)
		bloom_free(magic_load);
	pfree(good);

	PG_RETURN_BOOL(ok);
}

/* Send a cancel request for conn's in-flight query (best effort). */
/* Printable name for a channel state ("UNKNOWN" when out of range). */
/*
 * Drive a producer and a consumer through the whole path in this one backend.
 *
 * Coordinator-local, so it exercises the merge, the channel table and the
 * lifetime rules without needing segments; the segment half (NOTIFY out,
 * sideband message in) is covered by the runtime-filter test on a cluster.
 */
Datum
anser_test_node_roundtrip(PG_FUNCTION_ARGS)
{
	AnserChannelKey key;
	AnserBloomFilterProduceState *producer;
	AnserBloomFilterConsumeState *consumer;
	int32		value_arg = PG_GETARG_INT32(0);
	Datum		value = Int32GetDatum(value_arg);
	bool		ok = false;

	MemSet(&key, 0, sizeof(key));
	key.gp_session_id = gp_session_id;
	key.gp_command_count = gp_command_count;
	key.condition_id = 77;
	strlcpy(key.condition_key, "sideband_roundtrip", ANSER_CONDITION_KEY_SIZE);

	PG_TRY();
	{
		producer = ExecInitAnserBloomFilterProduce(&key, 32, 1024 * 1024, 0, 1,
												   NULL);
		if (producer == NULL)
			ok = false;
		else
		{
			ExecAnserBloomFilterProduceAddDatum(producer, value, false);
			ok = ExecAnserBloomFilterProducePublish(producer);
			ExecEndAnserBloomFilterProduce(producer);
		}

		if (ok)
		{
			consumer = ExecInitAnserBloomFilterConsume(&key, 32, 1024 * 1024, 1,
													   NULL);
			if (consumer == NULL)
				ok = false;
			else
			{
				ok = ExecAnserBloomFilterConsume(consumer, 1000) &&
					ExecAnserBloomFilterConsumerGetFilter(consumer) != NULL &&
					ExecAnserBloomFilterConsumerReceivedParts(consumer) == 1 &&
					!ExecAnserBloomFilterConsumerWasCancelled(consumer) &&
					!bloom_lacks_element(ExecAnserBloomFilterConsumerGetFilter(consumer),
										 (unsigned char *) &value,
										 sizeof(Datum));
				ExecEndAnserBloomFilterConsume(consumer);
			}
		}
	}
	PG_FINALLY();
	{
		AnserSidebandResetAll();
	}
	PG_END_TRY();

	PG_RETURN_BOOL(ok);
}

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

#include "access/htup_details.h"
#include "anser.h"
#include "anserbloom.h"
#include "anserfilter.h"
#include "anserpayload.h"
#include "anserplan.h"
#include "ansersideband.h"
#include "cdb/cdbvars.h"
#include "common/base64.h"
#include "fmgr.h"
#include "funcapi.h"
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

/*
 * bloom_create floors every bitset at 1 MB, so a payload cap must be 1 MB
 * *plus* room for the serialized-part header -- the same allowance the planner
 * makes (ANSER_RF_HEADER_ROOM).  Passing a flat 1 MB asks AnserBloomCreate for
 * a filter that cannot fit the cap it was given, and it now declines.
 */
#define ANSER_TEST_MAX_PAYLOAD	(1024 * 1024 + 64)

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

	filter = AnserBloomCreate(ANSER_TEST_ELEMS, ANSER_TEST_MAX_PAYLOAD, seed);
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
	left = AnserBloomCreate(ANSER_TEST_ELEMS, ANSER_TEST_MAX_PAYLOAD, seed);
	right = AnserBloomCreate(ANSER_TEST_ELEMS, ANSER_TEST_MAX_PAYLOAD, seed);
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
	strlcpy(key.condition_key, "node_roundtrip", ANSER_CONDITION_KEY_SIZE);

	PG_TRY();
	{
		producer = ExecInitAnserBloomFilterProduce(&key, ANSER_TEST_ELEMS,
												   ANSER_TEST_MAX_PAYLOAD, 0, 1);
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
			consumer = ExecInitAnserBloomFilterConsume(&key, ANSER_TEST_ELEMS,
													   ANSER_TEST_MAX_PAYLOAD, 1);
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

/*
 * ---------------------------------------------------------------------------
 * Sizing and give-up decisions.
 *
 * These four take their inputs as arguments and return what Anser decided, so
 * the case tables live in sql/anser_test.sql where they are readable and the
 * expected output records real sizes rather than a bare "ok".  Between them
 * they cover each of the four points where Anser can decide not to bother:
 * the planner, filter construction, publication, and the merged result.
 * ---------------------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(anser_test_rf_size);
PG_FUNCTION_INFO_V1(anser_test_bloom_shape);
PG_FUNCTION_INFO_V1(anser_test_worth_delivering);
PG_FUNCTION_INFO_V1(anser_test_producer_decision);

/*
 * The planner gate: what AnserRuntimeFilterSize decides for an estimated build
 * cardinality.  NULL means no filter would be injected at all.
 */
Datum
anser_test_rf_size(PG_FUNCTION_ARGS)
{
	double		est_rows = PG_GETARG_FLOAT8(0);
	int64		total_elems = 0;
	int64		max_payload = 0;
	int64		planned_bytes = 0;
	bool		injected;
	Datum		values[4];
	bool		nulls[4] = {false, false, false, false};
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "anser_test_rf_size: expected a composite return type");
	tupdesc = BlessTupleDesc(tupdesc);

	injected = AnserRuntimeFilterSize(est_rows, &total_elems, &max_payload,
									  &planned_bytes);

	values[0] = BoolGetDatum(injected);
	values[1] = Int64GetDatum(total_elems);
	values[2] = Int64GetDatum(max_payload);
	values[3] = Int64GetDatum(planned_bytes);
	nulls[1] = nulls[2] = nulls[3] = !injected;

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * Filter construction: what AnserBloomCreate makes of (total_elems, cap).
 * NULL means it declined -- either the smallest possible filter does not fit
 * the cap, or there are too many keys for it to be worth building.
 *
 * Returns the realized sizes so the expected output pins down bloom_create's
 * actual behaviour (its 1 MB floor, its power-of-two rounding) and not merely
 * our verdict on it.
 */
Datum
anser_test_bloom_shape(PG_FUNCTION_ARGS)
{
	int64		total_elems = PG_GETARG_INT64(0);
	int64		cap = PG_GETARG_INT64(1);
	bloom_filter *filter;
	Datum		values[4];
	bool		nulls[4] = {false, false, false, false};
	TupleDesc	tupdesc;

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "anser_test_bloom_shape: expected a composite return type");
	tupdesc = BlessTupleDesc(tupdesc);

	/* A negative cap would be a Size wraparound; the SQL side never sends one. */
	if (cap < 0)
		elog(ERROR, "anser_test_bloom_shape: negative cap");

	filter = AnserBloomCreate(total_elems, (Size) cap, AnserBloomSeed("shape"));

	values[0] = BoolGetDatum(filter != NULL);
	nulls[1] = nulls[2] = nulls[3] = (filter == NULL);
	if (filter != NULL)
	{
		values[1] = Int64GetDatum((int64) bloom_total_bits(filter));
		values[2] = Int64GetDatum((int64) AnserBloomSerializedSize(filter));
		values[3] = Float8GetDatum((double) bloom_total_bits(filter) /
								   (double) total_elems);
		bloom_free(filter);
	}

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}

/*
 * The coordinator's gate, on a synthetic part.
 *
 * Builds a part whose bitset has 'bytes_set' of its 'bitset_bytes' bytes fully
 * set, which pins the fill fraction exactly -- reaching a given fill by
 * inserting keys would be both slow and only statistically precise.  'damage'
 * corrupts the framing instead: 'magic', 'version', 'parts' or 'null'.
 */
Datum
anser_test_worth_delivering(PG_FUNCTION_ARGS)
{
	int32		bitset_bytes = PG_GETARG_INT32(0);
	int32		bytes_set = PG_GETARG_INT32(1);
	const char *damage = text_to_cstring(PG_GETARG_TEXT_PP(2));
	AnserBloomPartHeader *header;
	char	   *payload;
	Size		payload_len;

	if (strcmp(damage, "null") == 0)
		PG_RETURN_BOOL(AnserBloomPartWorthSending(NULL, 1024));

	if (bitset_bytes < 0 || bytes_set < 0 || bytes_set > bitset_bytes)
		elog(ERROR, "anser_test_worth_delivering: bad bitset arguments");

	payload_len = sizeof(AnserBloomPartHeader) + (Size) bitset_bytes;
	payload = palloc0(payload_len);
	header = (AnserBloomPartHeader *) payload;
	header->magic = ANSER_BLOOM_PART_MAGIC;
	header->version = ANSER_BLOOM_PART_VERSION;
	header->part_index = 0;
	header->total_parts = 1;

	if (strcmp(damage, "magic") == 0)
		header->magic = ANSER_BLOOM_PART_MAGIC + 1;
	else if (strcmp(damage, "version") == 0)
		header->version = ANSER_BLOOM_PART_VERSION + 1;
	else if (strcmp(damage, "parts") == 0)
		header->total_parts = 0;
	else if (damage[0] != '\0')
		elog(ERROR, "anser_test_worth_delivering: unknown damage \"%s\"", damage);

	if (bytes_set > 0)
		memset(payload + sizeof(AnserBloomPartHeader), 0xff, (Size) bytes_set);

	PG_RETURN_BOOL(AnserBloomPartWorthSending(payload, payload_len));
}

/*
 * Producer end to end, on the coordinator-local path: build a filter for
 * (total_elems, cap), insert 'n_keys' distinct keys, publish, then consume.
 *
 * Returns "<construction>:<delivery>", where construction is built/no-filter
 * and delivery is delivered/cancelled/missing.  Every combination that can
 * occur says something different:
 *
 *   built:delivered      the normal case
 *   built:cancelled      the filter saturated, so publication became a cancel
 *   no-filter:cancelled  construction declined, cancelled before any scan
 *
 * Each call takes a fresh condition_id, since several rows of one query share a
 * session and command counter and would otherwise collide on one channel.
 */
Datum
anser_test_producer_decision(PG_FUNCTION_ARGS)
{
	static uint32 next_condition_id = 1000;

	int64		total_elems = PG_GETARG_INT64(0);
	int64		cap = PG_GETARG_INT64(1);
	int32		n_keys = PG_GETARG_INT32(2);
	AnserChannelKey key;
	AnserBloomFilterProduceState *producer;
	const char *construction;
	const char *delivery;
	void	   *payload = NULL;
	Size		payload_len = 0;
	bool		cancelled = false;
	int32		i;

	MemSet(&key, 0, sizeof(key));
	key.gp_session_id = gp_session_id;
	key.gp_command_count = gp_command_count;
	key.condition_id = next_condition_id++;
	snprintf(key.condition_key, ANSER_CONDITION_KEY_SIZE, "anser_rf_%u",
			 key.condition_id);

	producer = ExecInitAnserBloomFilterProduce(&key, total_elems, (Size) cap,
											   0, 1);
	if (producer == NULL)
		PG_RETURN_TEXT_P(cstring_to_text("no-producer:missing"));

	construction = ExecAnserBloomFilterProduceHasFilter(producer)
		? "built" : "no-filter";

	for (i = 0; i < n_keys; i++)
	{
		Datum		value = Int32GetDatum(i);

		ExecAnserBloomFilterProduceAddDatum(producer, value, false);
	}

	(void) ExecAnserBloomFilterProducePublish(producer);
	ExecEndAnserBloomFilterProduce(producer);

	if (!AnserDispatchLocalConsume(&key, ANSER_PAYLOAD_BLOOM, &payload,
								   &payload_len, &cancelled))
		delivery = cancelled ? "cancelled" : "missing";
	else
		delivery = "delivered";

	if (payload != NULL)
		pfree(payload);

	PG_RETURN_TEXT_P(cstring_to_text(psprintf("%s:%s", construction, delivery)));
}

/*
 * ---------------------------------------------------------------------------
 * The wire protocol.
 *
 * Not exhaustive -- a fuzzer would be the right tool for that, and pg_regress
 * is not one.  What these cover is the part of the format that is easy to get
 * wrong and expensive to get wrong: that the header is a fixed width with the
 * fields where they are documented, that a payload cannot be mistaken for
 * framing (the reason the newline-delimited format was replaced), that the
 * length cross-check rejects a message that does not add up, and that the
 * checksum rejects a single altered byte anywhere it is supposed to cover --
 * and does not reject one where it deliberately does not.
 * ---------------------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(anser_test_wire_format);
PG_FUNCTION_INFO_V1(anser_test_wire_roundtrip);
PG_FUNCTION_INFO_V1(anser_test_push_crc);

/* Fixed channel coordinates, so the golden headers below are stable. */
#define ANSER_TEST_WIRE_SESSION		42
#define ANSER_TEST_WIRE_COMMAND		7
#define ANSER_TEST_WIRE_CONDITION	3
#define ANSER_TEST_WIRE_PART		1
#define ANSER_TEST_WIRE_TOTAL		3

static void
anser_test_wire_key(AnserChannelKey *key, const char *condition_key)
{
	MemSet(key, 0, sizeof(*key));
	key->gp_session_id = ANSER_TEST_WIRE_SESSION;
	key->gp_command_count = ANSER_TEST_WIRE_COMMAND;
	key->condition_id = ANSER_TEST_WIRE_CONDITION;
	strlcpy(key->condition_key, condition_key, ANSER_CONDITION_KEY_SIZE);
}

/*
 * The formatted message, verbatim.
 *
 * Returning it as text is itself a check: a NOTIFY payload travels through
 * pq_sendstring and so must be free of NUL bytes, and a text Datum cannot
 * carry one -- which is why the body is base64 even though the header is not.
 * Pass a body containing NULs and the length in the expected output proves it.
 */
Datum
anser_test_wire_format(PG_FUNCTION_ARGS)
{
	char		kind = PG_GETARG_CHAR(0);
	char		payload_type = PG_GETARG_CHAR(1);
	int32		flags = PG_GETARG_INT32(2);
	char	   *condition_key = text_to_cstring(PG_GETARG_TEXT_PP(3));
	bytea	   *body = PG_GETARG_BYTEA_PP(4);
	AnserChannelKey key;
	char	   *msg;

	anser_test_wire_key(&key, condition_key);
	msg = AnserWireFormat(&key, kind, payload_type, ANSER_TEST_WIRE_PART,
						  ANSER_TEST_WIRE_TOTAL, flags,
						  VARDATA_ANY(body), VARSIZE_ANY_EXHDR(body));

	PG_RETURN_TEXT_P(cstring_to_text(msg));
}

/*
 * Format a message, optionally alter one byte of it, then parse it back and
 * report what the reader made of it.
 *
 * The offsets touched are derived from the format macros, never hardcoded, so
 * this keeps working if a field is added.  Outcomes mirror what the notify
 * handler does with each: "malformed" is rejected by framing before anything
 * is allocated, "unknown-type" has no registry entry, "checksum-mismatch" is
 * well-framed but altered, and "ok" means every field and the decoded body
 * came back exactly as they went in.
 */
Datum
anser_test_wire_roundtrip(PG_FUNCTION_ARGS)
{
	char	   *condition_key = text_to_cstring(PG_GETARG_TEXT_PP(0));
	bytea	   *body = PG_GETARG_BYTEA_PP(1);
	char	   *tamper = text_to_cstring(PG_GETARG_TEXT_PP(2));
	char		payload_type = PG_GETARG_CHAR(3);
	AnserChannelKey key;
	AnserWireMsg parsed;
	char	   *msg;
	Size		msg_len;
	const char *raw_body = VARDATA_ANY(body);
	int			raw_len = VARSIZE_ANY_EXHDR(body);
	char	   *decoded = NULL;
	int			decoded_len = 0;

	anser_test_wire_key(&key, condition_key);
	msg = AnserWireFormat(&key, ANSER_WIRE_KIND_PART, payload_type,
						  ANSER_TEST_WIRE_PART, ANSER_TEST_WIRE_TOTAL, 0,
						  raw_len > 0 ? raw_body : NULL, (Size) raw_len);
	msg_len = strlen(msg);

	/* One byte, or one length, altered -- see the case table in the test. */
	if (strcmp(tamper, "") == 0)
		 /* no damage */ ;
	else if (strcmp(tamper, "truncate") == 0)
		msg[msg_len - 1] = '\0';
	else if (strcmp(tamper, "append") == 0)
	{
		char	   *longer = palloc(msg_len + 2);

		memcpy(longer, msg, msg_len);
		longer[msg_len] = 'x';
		longer[msg_len + 1] = '\0';
		msg = longer;
	}
	else if (strcmp(tamper, "bodylen") == 0)
	{
		/* Last digit of the bodylen field: it ends one space before the CRC. */
		char	   *digit = msg + ANSER_WIRE_CRC_OFFSET - 2;

		*digit = (*digit == '9') ? '8' : (char) (*digit + 1);
	}
	else if (strcmp(tamper, "tag") == 0)
		msg[0] = 'x';
	else if (strcmp(tamper, "kind") == 0)
		msg[sizeof(ANSER_WIRE_TAG)] = ANSER_WIRE_KIND_SUBSCRIBE;
	else if (strcmp(tamper, "type") == 0)
		msg[sizeof(ANSER_WIRE_TAG) + 2] = 'Z';
	else if (strcmp(tamper, "crc") == 0)
	{
		char	   *digit = msg + ANSER_WIRE_HDR_LEN - 1;

		*digit = (*digit == '0') ? '1' : '0';
	}
	else if (strcmp(tamper, "key") == 0)
	{
		char	   *first = msg + ANSER_WIRE_HDR_LEN;

		*first = (*first == 'a') ? 'b' : 'a';
	}
	else if (strcmp(tamper, "body") == 0)
	{
		/* Stay inside the base64 alphabet so this tests the CRC, not decoding. */
		char	   *first = msg + ANSER_WIRE_HDR_LEN + strlen(condition_key);

		*first = (*first == 'A') ? 'B' : 'A';
	}
	else
		elog(ERROR, "anser_test_wire_roundtrip: unknown tamper \"%s\"", tamper);

	if (!AnserWireParse(msg, &parsed))
		PG_RETURN_TEXT_P(cstring_to_text("malformed"));

	if (AnserPayloadLookup(parsed.payload_type) == NULL)
		PG_RETURN_TEXT_P(cstring_to_text("unknown-type"));

	if (parsed.body_len > 0)
	{
		int			maxlen = pg_b64_dec_len(parsed.body_len);

		decoded = palloc(maxlen);
		decoded_len = pg_b64_decode(parsed.body, parsed.body_len, decoded,
									maxlen);
		if (decoded_len < 0)
			PG_RETURN_TEXT_P(cstring_to_text("undecodable"));
	}

	if (!AnserWireCheckCrc(&parsed, decoded, (Size) decoded_len))
		PG_RETURN_TEXT_P(cstring_to_text("checksum-mismatch"));

	/* Framed and vouched for: now every field must have survived the trip. */
	if (parsed.kind != ANSER_WIRE_KIND_PART)
		PG_RETURN_TEXT_P(cstring_to_text("mismatch: kind"));
	if (parsed.payload_type != payload_type)
		PG_RETURN_TEXT_P(cstring_to_text("mismatch: payload_type"));
	if (parsed.key.gp_session_id != ANSER_TEST_WIRE_SESSION ||
		parsed.key.gp_command_count != ANSER_TEST_WIRE_COMMAND ||
		parsed.key.condition_id != ANSER_TEST_WIRE_CONDITION)
		PG_RETURN_TEXT_P(cstring_to_text("mismatch: channel"));
	if (parsed.part_index != ANSER_TEST_WIRE_PART ||
		parsed.total_parts != ANSER_TEST_WIRE_TOTAL)
		PG_RETURN_TEXT_P(cstring_to_text("mismatch: part"));
	if (parsed.flags != 0)
		PG_RETURN_TEXT_P(cstring_to_text("mismatch: flags"));
	if (strcmp(parsed.key.condition_key, condition_key) != 0)
		PG_RETURN_TEXT_P(cstring_to_text("mismatch: condition_key"));
	if (decoded_len != raw_len ||
		(raw_len > 0 && memcmp(decoded, raw_body, raw_len) != 0))
		PG_RETURN_TEXT_P(cstring_to_text("mismatch: body"));

	PG_RETURN_TEXT_P(cstring_to_text("ok"));
}

/*
 * The QD -> QE checksum, as an integer so the test can compare two of them.
 *
 * What matters is not the value but which inputs change it: every routing field
 * and the key always, and the body only for a payload type that asks for its
 * body to be covered.
 */
Datum
anser_test_push_crc(PG_FUNCTION_ARGS)
{
	char		payload_type = PG_GETARG_CHAR(0);
	int32		condition_id = PG_GETARG_INT32(1);
	int32		flags = PG_GETARG_INT32(2);
	char	   *condition_key = text_to_cstring(PG_GETARG_TEXT_PP(3));
	bytea	   *body = PG_GETARG_BYTEA_PP(4);

	PG_RETURN_INT64((int64) (uint32)
					AnserWirePushCrc(payload_type, (uint32) condition_id,
									 (uint32) flags, condition_key,
									 (int) strlen(condition_key),
									 VARDATA_ANY(body),
									 (int) VARSIZE_ANY_EXHDR(body)));
}

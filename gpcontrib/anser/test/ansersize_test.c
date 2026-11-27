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
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * ansersize_test.c
 *    Planner/runtime sizing and real Bloom serialization at the 8 KiB floor.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/test/ansersize_test.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <stdlib.h>
#undef elog
#define elog(level, ...) do { if ((level) >= ERROR) abort(); } while (0)
#include "../src/anserplan.c"
#include "../src/anserfilter.c"
#include "../../../src/backend/lib/bloomfilter.c"
#include "anserbloom.h"

#define CHECK(c) do { if (!(c)) abort(); } while (0)

bool gp_anser_debug;
int gp_anser_max_info_size = 65 * 1024 * 1024;
int gp_anser_debug_min_filter_bytes;

void ExceptionalCondition(const char *a, const char *b, int c) { abort(); }

static void
roundtrip(double rows)
{
	int64 elems, cap, bytes;
	bloom_filter *filter, *received;
	void *payload;
	Size length;
	int i;

	CHECK(AnserRuntimeFilterSize(rows, &elems, &cap, &bytes));
	filter = AnserBloomCreate(elems, cap, 123);
	CHECK(filter && bloom_bitset_bytes(filter) == bytes);
	CHECK(bytes >= 8192);
	for (i = 1; i <= 100; ++i)
		AnserBloomAddKey(filter, Int32GetDatum(-i), sizeof(int32));
	length = AnserBloomSerializedSize(filter);
	CHECK(length <= cap);
	payload = malloc(length);
	CHECK(payload);
	CHECK(AnserBloomSerializePart(filter, 0, 1, payload, length, &length));
	received = AnserBloomDeserializePart(payload, length, elems, cap, 123, NULL, NULL);
	CHECK(received && bloom_bitset_bytes(received) == bytes);
	for (i = 1; i <= 100; ++i)
		CHECK(!AnserBloomLacksKey(received, (Datum) (uint32) -i, sizeof(int32)));
	CHECK(bloom_create_from_bitset_with_minimum(elems, (cap - 16) / 1024,
		123, bloom_bitset_data(filter), bytes - 1, ANSER_BLOOM_MIN_BYTES) == NULL);
	bloom_free(received);
	bloom_free(filter);
	free(payload);
}

static void
merged_filter_delivery(void)
{
	bloom_filter *filter = AnserBloomCreate(32, 8192 + 64, 123);
	unsigned char *first;
	unsigned char *second;
	Size len;
	Size wire_len;

	CHECK(filter != NULL);
	len = AnserBloomSerializedSize(filter);
	first = palloc(len);
	second = palloc(len);
	CHECK(AnserBloomSerializePart(filter, 0, 2, first, len, &wire_len));
	CHECK(wire_len == len);
	CHECK(AnserBloomSerializePart(filter, 1, 2, second, len, &wire_len));
	CHECK(wire_len == len);

	/* Each part is half full. Their disjoint bits saturate the union. */
	memset(first + sizeof(AnserBloomPartHeader), 0x0f, bloom_bitset_bytes(filter));
	memset(second + sizeof(AnserBloomPartHeader), 0xf0, bloom_bitset_bytes(filter));
	CHECK(AnserBloomPartWorthSending(first, len));
	CHECK(AnserBloomPartWorthSending(second, len));
	CHECK(AnserBloomFoldPartInPlace(first, len, second, len));
	CHECK(!AnserBloomPartWorthSending(first, len));

	pfree(second);
	pfree(first);
	bloom_free(filter);
}

int main(void)
{
	bloom_filter *filter, *legacy_copy;
	double rows[] = {0, 1, 32, 1000, 4096, 4097, 8192, 1000000};
	int64 elems, cap, bytes;
	int i;

	merged_filter_delivery();

	/* Existing core callers keep the historical 1 MiB minimum. */
	filter = bloom_create(32, 8, 123);
	CHECK(bloom_bitset_bytes(filter) == 1024 * 1024);
	legacy_copy = bloom_create_from_bitset(32, 8, 123,
		bloom_bitset_data(filter), bloom_bitset_bytes(filter));
	CHECK(legacy_copy && bloom_bitset_bytes(legacy_copy) == 1024 * 1024);
	bloom_free(legacy_copy);
	bloom_free(filter);
	for (i = 0; i < lengthof(rows); ++i)
		roundtrip(rows[i]);
	CHECK(AnserRuntimeFilterSize(32, &elems, &cap, &bytes) && bytes == 8192);
	CHECK(AnserBloomCreate(32, 8192, 123) == NULL);
	CHECK(AnserBloomCreate(32, 8192 + 15, 123) == NULL);
	filter = AnserBloomCreate(32, 8192 + 16, 123);
	CHECK(filter && AnserBloomSerializedSize(filter) == 8192 + 16);
	bloom_free(filter);
	filter = AnserBloomCreate(16384, 8192 + 64, 123);
	CHECK(filter && bloom_total_bits(filter) == 65536);
	bloom_free(filter);
	CHECK(AnserBloomCreate(16385, 8192 + 64, 123) == NULL);
	CHECK(!AnserRuntimeFilterSize(134217729, &elems, &cap, &bytes));
	gp_anser_debug_min_filter_bytes = 32768;
	roundtrip(32);
	return 0;
}

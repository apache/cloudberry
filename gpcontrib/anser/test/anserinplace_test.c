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
 * anserinplace_test.c
 *    In-place build/probe callback reuse across rescans.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/test/anserinplace_test.c
 *
 *-------------------------------------------------------------------------
 */
/* Exercise real in-place callbacks; the Bloom stub stores exact integer keys.
 * Core's adaptive probing and normalization have their own regression tests. */
#include "../src/anserinplace.c"
#include <stdlib.h>

#define CHECK(c) do { if (!(c)) abort(); } while (0)

static uint64 keys;
static bool skip_probes;
static int build_cursor;
static int probe_cursor;
static Datum build_value = 2;
static TupleTableSlot build_slot;
static TupleTableSlot probe_slot;
static Datum probe_value;
static bool slot_isnull;

void ExceptionalCondition(const char *a, const char *b, int c) { abort(); }
void slot_getsomeattrs_int(TupleTableSlot *slot, int attnum) { abort(); }
bool errstart(int elevel, const char *domain) { abort(); }
bool errstart_cold(int elevel, const char *domain) { abort(); }
void errfinish(const char *file, int line, const char *func) { abort(); }
void errmsg_internal(const char *fmt, ...) { abort(); }

static uint64
key_bit(unsigned char *value, size_t len)
{
	Datum key;

	CHECK(len == sizeof(Datum));
	memcpy(&key, value, len);
	CHECK(key < 64);
	return UINT64CONST(1) << key;
}

void
bloom_add_element(bloom_filter *filter, unsigned char *value, size_t len)
{
	CHECK(filter == (bloom_filter *) &keys);
	keys |= key_bit(value, len);
}

bool
bloom_runtime_probe_skip(bloom_filter *filter)
{
	CHECK(filter == (bloom_filter *) &keys);
	return skip_probes;
}

bool
bloom_runtime_probe(bloom_filter *filter, unsigned char *value, size_t len)
{
	CHECK(filter == (bloom_filter *) &keys);
	return !(keys & key_bit(value, len));
}

static TupleTableSlot *
build_next(PlanState *ps)
{
	/* Model ExecProcNodeFirst installing the underlying callback. */
	ps->ExecProcNode = build_next;
	return build_cursor++ == 0 ? &build_slot : NULL;
}

static TupleTableSlot *
probe_next(PlanState *ps)
{
	static const Datum values[] = {1, 2, 3, 2};

	if (probe_cursor == lengthof(values)) return NULL;
	probe_value = values[probe_cursor++];
	return &probe_slot;
}

int
main(void)
{
	PlanState build = {0};
	PlanState scan = {0};
	Instrumentation instr = {0};
	AnserInplaceFilter filter = {0};
	AnserInplaceProbe probe = {0};
	ListCell filter_cell = {.ptr_value = &filter};
	ListCell probe_cell = {.ptr_value = &probe};
	ListCell attno_cell = {.int_value = 1};
	List filters = {T_List, 1, 1, &filter_cell};
	List probes = {T_List, 1, 1, &probe_cell};
	List attnos = {T_IntList, 1, 1, &attno_cell};

	build_slot.tts_nvalid = probe_slot.tts_nvalid = 1;
	build_slot.tts_values = &build_value;
	probe_slot.tts_values = &probe_value;
	build_slot.tts_isnull = probe_slot.tts_isnull = &slot_isnull;
	build.ExecProcNode = anser_inplace_build_next;
	scan.instrument = &instr;
	filter.build = &build;
	filter.build_next = build_next;
	filter.build_attno = 1;
	filter.key_length = sizeof(int32);
	filter.filter = (bloom_filter *) &keys;
	probe.scan = &scan;
	probe.scan_next = probe_next;
	probe.filters = &filters;
	probe.attnos = &attnos;
	inplace_filters = &filters;
	inplace_probes = &probes;

	/* Nothing may be rejected until the build reaches EOF. */
	probe_value = 1;
	CHECK(anser_inplace_keep(&probe, &probe_slot));
	CHECK(anser_inplace_build_next(&build) == &build_slot);
	CHECK(!filter.ready);
	CHECK(build.ExecProcNode == anser_inplace_build_next);
	CHECK(filter.build_next == build_next);
	CHECK(anser_inplace_keep(&probe, &probe_slot));
	CHECK(anser_inplace_build_next(&build) == NULL && filter.ready);

	/* Rewinding only the probe must reuse the complete filter. */
	for (int pass = 0; pass < 2; ++pass)
	{
		probe_cursor = 0;
		CHECK(anser_inplace_probe_next(&scan) == &probe_slot && probe_value == 2);
		CHECK(anser_inplace_probe_next(&scan) == &probe_slot && probe_value == 2);
		CHECK(anser_inplace_probe_next(&scan) == NULL);
		CHECK(keys == (UINT64CONST(1) << 2));
	}
	CHECK(instr.nfilteredPRF == 4);

	/* A rewound build adds keys without losing earlier matches. */
	build_cursor = 0;
	build_value = 3;
	CHECK(anser_inplace_build_next(&build) == &build_slot);
	CHECK(anser_inplace_build_next(&build) == NULL && filter.ready);
	probe_cursor = 0;
	CHECK(anser_inplace_probe_next(&scan) == &probe_slot && probe_value == 2);
	CHECK(anser_inplace_probe_next(&scan) == &probe_slot && probe_value == 3);
	CHECK(anser_inplace_probe_next(&scan) == &probe_slot && probe_value == 2);
	CHECK(anser_inplace_probe_next(&scan) == NULL);
	CHECK(instr.nfilteredPRF == 5);

	probe_value = 1;
	skip_probes = true;
	CHECK(anser_inplace_keep(&probe, &probe_slot));
	skip_probes = false;
	slot_isnull = true;
	CHECK(anser_inplace_keep(&probe, &probe_slot));
	inplace_filters = inplace_probes = NIL;
	return 0;
}

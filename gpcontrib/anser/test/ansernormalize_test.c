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
 * ansernormalize_test.c
 *    Storage-independent Bloom keys and scan qualification.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/test/ansernormalize_test.c
 *
 *-------------------------------------------------------------------------
 */
/* Use the production producer and pushed-down evaluator. The Bloom stubs
 * inspect the exact key bytes, rather than accepting a possible false positive.
 * Unused executor/transport functions are discarded by the linker. */
#include "../src/anserplanexec.c"
#include "../src/anserbloomproduce.c"
#include "../src/anserplankeys.c"
#include <stdlib.h>

#define CHECK(c) do { if (!(c)) abort(); } while (0)

MemoryContext CurrentMemoryContext;
static Datum expected;
static bool reject;
static int lookups;
static int original_calls;
static bool original_result;

void ExceptionalCondition(const char *a, const char *b, int c) { abort(); }
void slot_getsomeattrs_int(TupleTableSlot *slot, int attnum) { abort(); }
void bloom_add_element(bloom_filter *filter, unsigned char *value, size_t len)
{
	CHECK(len == sizeof(Datum));
	CHECK(memcmp(value, &expected, len) == 0);
}
bool bloom_lacks_element(bloom_filter *filter, unsigned char *value, size_t len)
{
	CHECK(len == sizeof(Datum));
	CHECK(memcmp(value, &expected, len) == 0);
	++lookups;
	return reject;
}

/* Adaptive probing is core's (bloomfilter.c); here it is on or off by hand. */
static bool skip_probes;
bool bloom_runtime_probe_skip(bloom_filter *filter) { return skip_probes; }
bool bloom_runtime_probe(bloom_filter *filter, unsigned char *value, size_t len)
{
	return bloom_lacks_element(filter, value, len);
}

TupleDesc ExecGetResultType(PlanState *state) { return state->ps_ResultTupleDesc; }

static void
key_length_bounds(void)
{
	PlanState child = {0};
	TupleDesc desc = calloc(1, sizeof(TupleDescData) + 2 * sizeof(FormData_pg_attribute));

	CHECK(desc != NULL);
	desc->natts = 2;
	TupleDescAttr(desc, 0)->attlen = 2;
	TupleDescAttr(desc, 0)->attbyval = true;
	TupleDescAttr(desc, 1)->attlen = 4;
	TupleDescAttr(desc, 1)->attbyval = true;
	child.ps_ResultTupleDesc = desc;
	CHECK(AnserKeyLength(&child, 1) == 2);
	CHECK(AnserKeyLength(&child, 2) == 4);
	free(desc);
}

static Datum
original_qual(ExprState *state, ExprContext *context, bool *isnull)
{
	++original_calls;
	*isnull = false;
	return BoolGetDatum(original_result);
}

/* Match ExecScan's one generic filter increment per rejected scan tuple. */
static Datum
scan_qual(ExprState *expr, ExprContext *context, bool *isnull)
{
	Datum result = anser_bloom_scan_qual(expr, context, isnull);

	if (!DatumGetBool(result))
		InstrCountFiltered1(((AnserBloomScanQual *) expr)->scan, 1);
	return result;
}

static uint32
runtime_filter_scan_flags(Relation rel)
{
	return SCAN_SUPPORT_RUNTIME_FILTER;
}

static void
scan_qual_eligibility(void)
{
	SeqScan scan = {0};
	SeqScanState state = {0};
	RelationData relation = {0};
	TableAmRoutine am = {0};
	Var column = {0};
	TargetEntry target = {0};
	List tlist = {0};
	ListCell cell = {0};
	AttrNumber table_attno;

	scan.scan.plan.type = T_SeqScan;
	scan.scan.scanrelid = 1;
	column.xpr.type = T_Var;
	column.varno = 1;
	column.varattno = 7;
	target.expr = (Expr *) &column;
	target.resno = 1;
	tlist.type = T_List;
	tlist.length = tlist.max_length = 1;
	tlist.elements = &cell;
	cell.ptr_value = &target;
	scan.scan.plan.targetlist = &tlist;
	state.ss.ps.type = T_SeqScanState;
	state.ss.ps.plan = &scan.scan.plan;
	/* Core scan-key eligibility stays conservative. Scan quals work without
	 * filter_in_seqscan, including parallel-aware scans. */
	CHECK(!AnserPushdownAccepts(&state.ss.ps, 1, &table_attno));
	CHECK(table_attno == InvalidAttrNumber);
	CHECK(anser_scan_qual_accepts(&state.ss.ps, 1, &table_attno));
	CHECK(table_attno == 7);
	scan.scan.plan.parallel_aware = true;
	CHECK(anser_scan_qual_accepts(&state.ss.ps, 1, &table_attno));
	CHECK(!AnserPushdownAccepts(&state.ss.ps, 1, &table_attno));
	state.filter_in_seqscan = true;
	CHECK(AnserPushdownAccepts(&state.ss.ps, 1, &table_attno));
	CHECK(table_attno == 7);
	state.ss.ss_currentRelation = &relation;
	relation.rd_tableam = &am;
	relation.rd_amhandler = F_HEAP_TABLEAM_HANDLER;
	CHECK(anser_runtime_qual_accepts(&state.ss.ps, 1, &table_attno));
	relation.rd_amhandler = F_AO_COLUMN_TABLEAM_HANDLER;
	CHECK(!anser_runtime_qual_accepts(&state.ss.ps, 1, &table_attno));
	/* An AM that takes runtime filters (PAX) may replace the scan's qual. */
	relation.rd_amhandler = F_HEAP_TABLEAM_HANDLER;
	am.scan_flags = runtime_filter_scan_flags;
	CHECK(!anser_runtime_qual_accepts(&state.ss.ps, 1, &table_attno));
	am.scan_flags = NULL;
	CHECK(AnserPushdownAccepts(&state.ss.ps, 1, &table_attno));
	column.varno = 2;
	CHECK(!anser_scan_qual_accepts(&state.ss.ps, 1, &table_attno));
	column.varno = 1;
	column.varattno = 0;
	CHECK(!anser_scan_qual_accepts(&state.ss.ps, 1, &table_attno));
	CHECK(!anser_scan_qual_accepts(&state.ss.ps, 2, &table_attno));
}

int main(void)
{
	AnserBloomFilterProduceState producer = {0};
	AnserBloomScanQual qual = {0};
	ExprState original = {0};
	ExprContext context = {0};
	TupleTableSlot slot = {0};
	PlanState scan = {0};
	Instrumentation instrumentation = {0};
	Datum values[1];
	bool nulls[1] = {false};
	bool isnull;
	int lengths[] = {1, 2, 4, sizeof(Datum)};
	int i;

	key_length_bounds();
	scan_qual_eligibility();
	producer.filter = (bloom_filter *) &producer;
	slot.tts_nvalid = 1;
	slot.tts_values = values;
	slot.tts_isnull = nulls;
	context.ecxt_scantuple = &slot;
	original.flags = EEO_FLAG_IS_QUAL;
	original.evalfunc = original_qual;
	qual.state.flags = EEO_FLAG_IS_QUAL;
	qual.state.evalfunc = anser_bloom_scan_qual;
	qual.original = &original;
	qual.scan = &scan;
	qual.filter = producer.filter;
	qual.key_attno = 1;
	scan.instrument = &instrumentation;

	for (i = 0; i < lengthof(lengths); ++i)
	{
		int bits = lengths[i] * 8;
		Datum mask = bits == sizeof(Datum) * 8 ? ~(Datum) 0 :
			(((Datum) 1 << bits) - 1);
		Datum low = mask; /* -1, zero-extended by AOCO */
		Datum high = ~(Datum) 0; /* -1, sign-extended by heap */

		qual.key_length = lengths[i];
		expected = high;	/* core's canonical form, sign-extended */
		CHECK(AnserNormalizeBloomDatum(low, lengths[i]) == expected);
		CHECK(AnserNormalizeBloomDatum(high, lengths[i]) == expected);
		ExecAnserBloomFilterProduceAddDatum(&producer, low, false, lengths[i]);
		ExecAnserBloomFilterProduceAddDatum(&producer, high, false, lengths[i]);
		values[0] = high;
		original_result = true;
		reject = false;
		CHECK(DatumGetBool(scan_qual(&qual.state, &context, &isnull)));
		CHECK(!isnull);
		values[0] = low;
		CHECK(DatumGetBool(scan_qual(&qual.state, &context, &isnull)));
		original_result = false;
		CHECK(!DatumGetBool(scan_qual(&qual.state, &context, &isnull)));
		reject = true;
		original_calls = 0;
		CHECK(!DatumGetBool(scan_qual(&qual.state, &context, &isnull)));
		CHECK(original_calls == 0);
		/* Only the original SQL qual contributes to Rows Removed by Filter. */
		CHECK(instrumentation.nfiltered1 == i + 1);
	}
	/* Multiple consumers can wrap the same scan without losing its qual. */
	{
		AnserBloomScanQual stacked = qual;

		stacked.original = &qual.state;
		reject = false;
		original_result = true;
		original_calls = 0;
		i = lookups;
		CHECK(DatumGetBool(scan_qual(&stacked.state, &context, &isnull)));
		CHECK(lookups == i + 2 && original_calls == 1);
		original_result = false;
		CHECK(!DatumGetBool(scan_qual(&stacked.state, &context, &isnull)));
		/* Even a stacked Bloom rejection receives one compensation total. */
		reject = true;
		original_calls = 0;
		CHECK(!DatumGetBool(scan_qual(&stacked.state, &context, &isnull)));
		CHECK(original_calls == 0);
		CHECK(instrumentation.nfiltered1 == lengthof(lengths) + 1);
	}
	/* A filter that turned itself off neither looks up nor rejects, but the
	 * scan's own qual still runs. */
	{
		int before = lookups;

		skip_probes = true;
		reject = true;
		original_calls = 0;
		original_result = true;
		CHECK(DatumGetBool(scan_qual(&qual.state, &context, &isnull)));
		CHECK(lookups == before && original_calls == 1);
		original_result = false;
		CHECK(!DatumGetBool(scan_qual(&qual.state, &context, &isnull)));
		CHECK(lookups == before && original_calls == 2);
		skip_probes = false;
		instrumentation.nfiltered1 = lengthof(lengths) + 1;
	}
	/* Negative int4 boundary values and arbitrary unused high bytes. */
	{
		int32 keys[] = {-1, -1000, PG_INT32_MIN, 0, PG_INT32_MAX};
		int k;

		for (k = 0; k < lengthof(keys); ++k)
		{
			expected = Int64GetDatum((int64) keys[k]);
			ExecAnserBloomFilterProduceAddDatum(&producer, Int32GetDatum(keys[k]),
											  false, sizeof(int32));
		}
	}
	/* NULL keys pass to the original qual without a Bloom lookup. */
	nulls[0] = true;
	original_result = true;
	i = lookups;
	CHECK(DatumGetBool(scan_qual(&qual.state, &context, &isnull)));
	CHECK(lookups == i);
	CHECK(instrumentation.nfilteredPRF == lengthof(lengths) + 1);
	CHECK(instrumentation.nfiltered1 == lengthof(lengths) + 1);
	return 0;
}

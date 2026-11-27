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
 * local_hash_test.c
 *    Storage-independent keys in the real local producer and consumer.
 *
 * IDENTIFICATION
 *	  gpcontrib/robust_predicate_transfer/test/local_hash_test.c
 *
 *-------------------------------------------------------------------------
 */
#include "../src/rptlocal.c"
#include <stdlib.h>

#define CHECK(c) do { if (!(c)) abort(); } while (0)

MemoryContext CurrentMemoryContext;
int work_mem;
bool gp_anser_zero_copy;
static TupleTableSlot input;
static bool exhausted;
static Datum stored;
static int probes;
static bool skip_probe;
static int additions;

void ExceptionalCondition(const char *a, const char *b, int c) { abort(); }
void slot_getsomeattrs_int(TupleTableSlot *slot, int attnum) { abort(); }
void ExecReScan(PlanState *state) { abort(); }
void *palloc0(Size bytes) { abort(); }
void MemoryContextRegisterResetCallback(MemoryContext ctx, MemoryContextCallback *cb) { abort(); }
bloom_filter *bloom_create_with_minimum(int64 elems, int mem, uint64 seed, Size minimum) { abort(); }
void bloom_add_element(bloom_filter *filter, unsigned char *value, size_t len)
{
	CHECK(len == sizeof(Datum));
	memcpy(&stored, value, len);
	++additions;
}
bool bloom_runtime_probe_skip(bloom_filter *filter) { return skip_probe; }
bool bloom_runtime_probe(bloom_filter *filter, unsigned char *value, size_t len)
{
	CHECK(len == sizeof(Datum));
	++probes;
	return memcmp(value, &stored, len) != 0;
}

/* Allocation for plan construction only (the executor paths above must not
 * allocate: palloc0 still aborts).  Never freed. */
static void *
test_alloc(Size size)
{
	void	   *p = calloc(1, size);

	CHECK(p != NULL);
	return p;
}
void *MemoryContextAllocZeroAligned(MemoryContext ctx, Size size) { return test_alloc(size); }
void *MemoryContextAllocZero(MemoryContext ctx, Size size) { return test_alloc(size); }
char *
pstrdup(const char *in)
{
	return strcpy(test_alloc(strlen(in) + 1), in);
}
static List *
test_list(NodeTag t, int n, const ListCell *cells)
{
	List	   *list = test_alloc(sizeof(List));

	list->type = t;
	list->length = list->max_length = n;
	list->elements = test_alloc(sizeof(ListCell) * n);
	memcpy(list->elements, cells, sizeof(ListCell) * n);
	return list;
}
List *list_make1_impl(NodeTag t, ListCell a) { return test_list(t, 1, &a); }
List *
list_make3_impl(NodeTag t, ListCell a, ListCell b, ListCell c)
{
	ListCell	cells[3] = {a, b, c};

	return test_list(t, 3, cells);
}
List *
lappend(List *list, void *datum)
{
	ListCell	cell = {.ptr_value = datum};
	List	   *out;

	if (list == NIL)
		return test_list(T_List, 1, &cell);
	out = test_list(T_List, list->length + 1, list->elements);
	out->elements[list->length] = cell;
	return out;
}
Integer *
makeInteger(int i)
{
	Integer    *v = test_alloc(sizeof(Integer));

	v->type = T_Integer;
	v->ival = i;
	return v;
}
Var *
makeVar(int varno, AttrNumber attno, Oid type, int32 typmod, Oid collid, Index levelsup)
{
	Var		   *v = test_alloc(sizeof(Var));

	v->xpr.type = T_Var;
	v->varno = varno;
	v->varattno = attno;
	v->vartype = type;
	v->vartypmod = typmod;
	v->varcollid = collid;
	v->varlevelsup = levelsup;
	return v;
}
TargetEntry *
makeTargetEntry(Expr *expr, AttrNumber resno, char *name, bool junk)
{
	TargetEntry *t = test_alloc(sizeof(TargetEntry));

	t->xpr.type = T_TargetEntry;
	t->expr = expr;
	t->resno = resno;
	t->resname = name;
	t->resjunk = junk;
	return t;
}
Oid exprType(const Node *e) { return ((const Var *) e)->vartype; }
int32 exprTypmod(const Node *e) { return ((const Var *) e)->vartypmod; }
Oid exprCollation(const Node *e) { return ((const Var *) e)->varcollid; }
void *
copyObjectImpl(const void *from)
{
	if (from == NULL)
		return NULL;
	CHECK(IsA(from, List));
	return test_list(T_List, ((const List *) from)->length, ((const List *) from)->elements);
}
Node *expression_tree_mutator_impl(Node *node, tree_mutator_callback m, void *c) { abort(); }
TargetEntry *get_tle_by_resno(List *tlist, AttrNumber resno) { abort(); }
bool errstart(int elevel, const char *domain) { abort(); }
bool errstart_cold(int elevel, const char *domain) { abort(); }
void errfinish(const char *filename, int lineno, const char *funcname) { abort(); }
void errmsg_internal(const char *fmt,...) { abort(); }

/* Referenced by the node's method tables; the tests never run them. */
void ExecEndNode(PlanState *node) { abort(); }
TupleTableSlot *ExecScan(ScanState *node, ExecScanAccessMtd a, ExecScanRecheckMtd r) { abort(); }
void ExplainPropertyInteger(const char *l, const char *u, int64 v, ExplainState *es) { abort(); }
void ExplainPropertyFloat(const char *l, const char *u, double v, int nd, ExplainState *es) { abort(); }

static PlanState *init_result;
PlanState *
ExecInitNode(Plan *node, EState *estate, int eflags)
{
	init_result->plan = node;
	return init_result;
}
TupleDesc ExecGetResultType(PlanState *state) { return state->ps_ResultTupleDesc; }

static TupleTableSlot *
next_tuple(PlanState *state)
{
	if (exhausted) return NULL;
	exhausted = true;
	return &input;
}
static void
copy_slot(TupleTableSlot *dst, TupleTableSlot *src)
{
	dst->tts_flags = src->tts_flags;
}

static void
one_key(int16 length, Datum source, Datum target)
{
	RptLocalScanState producer = {0}, consumer = {0};
	RptLocalEntry entry = {0};
	EState estate = {0};
	PlanState child = {0};
	const TupleTableSlotOps output_ops = {.copyslot = copy_slot};
	TupleTableSlot output = {.tts_ops = &output_ops};
	List children = {0};
	ListCell child_cell = {0};
	Datum values[1];
	bool nulls[1] = {false};
	int before;

	child.ExecProcNode = next_tuple;
	children.type = T_List;
	children.length = children.max_length = 1;
	children.elements = &child_cell;
	child_cell.ptr_value = &child;
	input.tts_nvalid = 1;
	input.tts_values = values;
	input.tts_isnull = nulls;
	input.tts_flags = 0;
	entry.estate = &estate;
	entry.condition_id = 1;
	entry.filter = (bloom_filter *) &entry;
	dlist_push_head(&rpt_local_entries, &entry.node);
	producer.started = true; /* Registry setup replaces allocation only. */
	producer.entry = &entry;
	producer.key_attno = consumer.key_attno = 1;
	producer.key_length = consumer.key_length = length;
	producer.csstate.custom_ps = consumer.csstate.custom_ps = &children;
	producer.csstate.ss.ss_ScanTupleSlot = consumer.csstate.ss.ss_ScanTupleSlot = &output;
	consumer.csstate.ss.ps.state = &estate;
	consumer.condition_id = 1;

	values[0] = source;
	exhausted = false;
	before = additions;
	CHECK(rpt_local_produce_next(&producer.csstate) == &output);
	CHECK(additions == before + 1);
	CHECK(rpt_local_produce_next(&producer.csstate) == NULL && entry.complete);
	values[0] = target;
	exhausted = false;
	before = probes;
	CHECK(rpt_local_consume_next(&consumer.csstate) == &output);
	CHECK(probes == before + 1);
	CHECK(rpt_local_consume_next(&consumer.csstate) == NULL);

	/* A distinct key must be rejected by the actual consumer loop. */
	values[0] = 17;
	exhausted = false;
	CHECK(rpt_local_consume_next(&consumer.csstate) == NULL);
	/* The shared adaptive policy can skip a probe, then resume it. */
	skip_probe = true;
	exhausted = false;
	before = probes;
	CHECK(rpt_local_consume_next(&consumer.csstate) == &output);
	CHECK(probes == before);
	skip_probe = false;
	exhausted = false;
	CHECK(rpt_local_consume_next(&consumer.csstate) == NULL);
	CHECK(probes == before + 1);
	/* NULL keys pass without hashing. */
	nulls[0] = true;
	exhausted = false;
	before = probes;
	CHECK(rpt_local_consume_next(&consumer.csstate) == &output);
	CHECK(probes == before);
	before = additions;
	exhausted = false;
	CHECK(rpt_local_produce_next(&producer.csstate) == &output);
	CHECK(additions == before);
	/* No completed filter: preserve the fail-open behavior. */
	nulls[0] = false;
	consumer.entry = NULL;
	exhausted = false;
	CHECK(rpt_local_consume_next(&consumer.csstate) == &output);
	rpt_local_unregister(&entry);
	CHECK(dlist_is_empty(&rpt_local_entries));
}

/*
 * RPT's local filter nodes over the shared Anser helpers.  RPT expects:
 * AnserInitFilterScan builds the pass-through shell, after which RPT forces
 * parallel_safe off (its registry is per process) and owns custom_private;
 * and AnserKeyLength reads the key width at the node's OUTPUT position --
 * the same position the producer and consumer hash at -- not at a table
 * attribute number.
 */
static void
local_nodes_use_shared_helpers(void)
{
	SeqScan		scan = {0};
	Var			col[2] = {0};
	TargetEntry tle[2] = {0};
	ListCell	cells[2];
	CustomScan *producer;
	CustomScan *consumer;
	RptLocalScanState st = {0};
	PlanState	child = {0};
	TupleDesc	desc = calloc(1, offsetof(struct TupleDescData, attrs) +
							  2 * sizeof(FormData_pg_attribute));
	int			i;

	scan.scan.plan.type = T_SeqScan;
	scan.scan.scanrelid = 3;
	scan.scan.plan.parallel_safe = true;
	for (i = 0; i < 2; ++i)
	{
		col[i].xpr.type = T_Var;
		col[i].varno = 3;
		col[i].varattno = 9 - i;	/* table attnos 9, 8: not positions */
		col[i].vartype = i == 0 ? INT8OID : INT2OID;
		tle[i].xpr.type = T_TargetEntry;
		tle[i].expr = (Expr *) &col[i];
		tle[i].resno = i + 1;
		cells[i].ptr_value = &tle[i];
	}
	scan.scan.plan.targetlist = test_list(T_List, 2, cells);

	producer = RptBuildLocalProducerScan(&scan.scan.plan, 2, 77, 1000);
	consumer = RptBuildLocalConsumerScan(&scan.scan.plan, 2, 77, 1000);
	CHECK(RptIsLocalScan(&producer->scan.plan) && RptIsLocalScan(&consumer->scan.plan));
	CHECK(producer->methods != consumer->methods);
	CHECK(!producer->scan.plan.parallel_safe && !consumer->scan.plan.parallel_safe);
	CHECK(linitial(producer->custom_plans) == &scan.scan.plan);
	CHECK(list_length(producer->scan.plan.targetlist) == 2);
	CHECK(intVal(list_nth(producer->custom_private, RPT_LOCAL_PRIV_CONDITION)) == 77);
	CHECK(intVal(list_nth(producer->custom_private, RPT_LOCAL_PRIV_KEY_ATTNO)) == 2);
	CHECK(intVal(list_nth(producer->custom_private, RPT_LOCAL_PRIV_TOTAL_ELEMS)) == 1000);

	/* The child's result descriptor is indexed by output position. */
	CHECK(desc != NULL);
	desc->natts = 2;
	TupleDescAttr(desc, 0)->attlen = 8;
	TupleDescAttr(desc, 1)->attlen = 2;
	TupleDescAttr(desc, 0)->attbyval = TupleDescAttr(desc, 1)->attbyval = true;
	child.ps_ResultTupleDesc = desc;
	init_result = &child;
	st.csstate.ss.ps.plan = &consumer->scan.plan;
	rpt_local_begin(&st.csstate, NULL, 0);
	CHECK(st.key_attno == 2 && st.key_length == 2);
	CHECK(st.condition_id == 77 && st.total_elems == 1000);
	CHECK(linitial(st.csstate.custom_ps) == &child && !st.started && st.entry == NULL);
	free(desc);
}

int main(void)
{
	int lengths[] = {1, 2, 4, sizeof(Datum)};
	int i;

	local_nodes_use_shared_helpers();
	for (i = 0; i < lengthof(lengths); ++i)
	{
		int bits = lengths[i] * 8;
		Datum low = bits == sizeof(Datum) * 8 ? ~(Datum) 0 :
			(((Datum) 1 << bits) - 1);
		Datum high = ~(Datum) 0;

		one_key(lengths[i], low, high); /* AOCO producer, heap consumer */
		one_key(lengths[i], high, low); /* heap producer, AOCO consumer */
	}
	one_key(4, (Datum) (uint32) PG_INT32_MIN, Int32GetDatum(PG_INT32_MIN));
	one_key(4, Int32GetDatum(PG_INT32_MIN), (Datum) (uint32) PG_INT32_MIN);
	return 0;
}

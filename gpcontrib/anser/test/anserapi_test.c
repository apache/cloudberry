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
 * anserapi_test.c
 *    Contract tests for the helpers Anser shares with other extensions.
 *
 * Every public helper in anserplan.h (key checks), anserplanutils.h and
 * anserbloom.h has one test function below.  The comment above each one is
 * the helper's contract; the checks are what makes it hold.  Callers (RPT+)
 * test only that they use the helpers as intended, so a behavior change here
 * must be a deliberate edit of the contract, not a side effect.
 *
 * IDENTIFICATION
 *	  gpcontrib/anser/test/anserapi_test.c
 *
 *-------------------------------------------------------------------------
 */
#include "../src/anserplankeys.c"
#include "anserbloom.h"
#include "anserplanutils.h"
#include "catalog/pg_type.h"

#include <stdio.h>
#include <stdlib.h>

/* fputs, not fprintf: port.h maps fprintf to pgport, which is not linked. */
#define CHECK_STR(x) #x
#define CHECK_LINE(x) CHECK_STR(x)
#define CHECK(c) \
	do { \
		if (!(c)) { \
			fputs(__FILE__ ":" CHECK_LINE(__LINE__) ": check failed: " #c "\n", stderr); \
			abort(); \
		} \
	} while (0)

/* ---- backend stubs; anything a contract says is not called aborts ---- */

#define DOMAIN_OVER_INT4 900001
#define INT4_EQ_OPR 96
#define INT8_EQ_OPR 410

static const Plan *identity_filter;
static int	alloc_count;

void ExceptionalCondition(const char *a, const char *b, int c) { abort(); }

bool
AnserIsRuntimeFilterScan(const Plan *plan)
{
	return plan != NULL && plan == identity_filter;
}

Oid
getBaseType(Oid type)
{
	return type == DOMAIN_OVER_INT4 ? INT4OID : type;
}

TypeCacheEntry *
lookup_type_cache(Oid type, int flags)
{
	static TypeCacheEntry entry;

	CHECK(flags == TYPECACHE_EQ_OPR);
	memset(&entry, 0, sizeof(entry));
	entry.type_id = type;
	entry.eq_opr = type == INT4OID ? INT4_EQ_OPR :
		type == INT8OID ? INT8_EQ_OPR : InvalidOid;
	return &entry;
}

static void *
test_alloc(Size size)
{
	void	   *p = calloc(1, size);

	CHECK(p != NULL);
	++alloc_count;
	return p;
}

char *
pstrdup(const char *in)
{
	char	   *out = test_alloc(strlen(in) + 1);

	strcpy(out, in);
	return out;
}

List *
list_make1_impl(NodeTag t, ListCell datum1)
{
	List	   *list = test_alloc(sizeof(List));

	list->type = t;
	list->length = list->max_length = 1;
	list->elements = test_alloc(sizeof(ListCell));
	list->elements[0] = datum1;
	return list;
}

List *
lappend(List *list, void *datum)
{
	if (list == NIL)
		return list_make1_impl(T_List, (ListCell) {.ptr_value = datum});
	list->elements = realloc(list->elements, sizeof(ListCell) * (list->length + 1));
	CHECK(list->elements != NULL);
	list->elements[list->length++].ptr_value = datum;
	list->max_length = list->length;
	return list;
}

Var *
makeVar(int varno, AttrNumber varattno, Oid vartype, int32 vartypmod,
		Oid varcollid, Index varlevelsup)
{
	Var		   *var = test_alloc(sizeof(Var));

	var->xpr.type = T_Var;
	var->varno = varno;
	var->varattno = varattno;
	var->vartype = vartype;
	var->vartypmod = vartypmod;
	var->varcollid = varcollid;
	var->varlevelsup = varlevelsup;
	return var;
}

TargetEntry *
makeTargetEntry(Expr *expr, AttrNumber resno, char *resname, bool resjunk)
{
	TargetEntry *tle = test_alloc(sizeof(TargetEntry));

	tle->xpr.type = T_TargetEntry;
	tle->expr = expr;
	tle->resno = resno;
	tle->resname = resname;
	tle->resjunk = resjunk;
	return tle;
}

Oid exprType(const Node *expr) { CHECK(IsA(expr, Var)); return ((const Var *) expr)->vartype; }
int32 exprTypmod(const Node *expr) { CHECK(IsA(expr, Var)); return ((const Var *) expr)->vartypmod; }
Oid exprCollation(const Node *expr) { CHECK(IsA(expr, Var)); return ((const Var *) expr)->varcollid; }

/* Shallow copies are enough to tell a copy from the original. */
void *
copyObjectImpl(const void *from)
{
	if (from == NULL)
		return NULL;
	if (IsA(from, Flow))
	{
		Flow	   *flow = test_alloc(sizeof(Flow));

		memcpy(flow, from, sizeof(Flow));
		return flow;
	}
	if (IsA(from, List))
	{
		const List *src = (const List *) from;
		List	   *list = test_alloc(sizeof(List));

		*list = *src;
		list->elements = test_alloc(sizeof(ListCell) * src->length);
		memcpy(list->elements, src->elements, sizeof(ListCell) * src->length);
		return list;
	}
	abort();
}

/* AnserFilterScanTlist only mutates a CustomScan child's list. */
Node *expression_tree_mutator_impl(Node *node, tree_mutator_callback mutator, void *context) { abort(); }
TargetEntry *get_tle_by_resno(List *tlist, AttrNumber resno) { abort(); }
bool errstart(int elevel, const char *domain) { abort(); }
bool errstart_cold(int elevel, const char *domain) { abort(); }
void errfinish(const char *filename, int lineno, const char *funcname) { abort(); }
void errmsg_internal(const char *fmt,...) { abort(); }

TupleDesc
ExecGetResultType(PlanState *planstate)
{
	return planstate->ps_ResultTupleDesc;
}

/* A bloom filter that is exact over the bytes it is given. */
#define BLOOM_MAX 16
static unsigned char bloom_keys[BLOOM_MAX][sizeof(Datum)];
static int	bloom_count;

void
bloom_add_element(bloom_filter *filter, unsigned char *elem, size_t len)
{
	CHECK(len == sizeof(Datum));
	CHECK(bloom_count < BLOOM_MAX);
	memcpy(bloom_keys[bloom_count++], elem, len);
}

bool
bloom_lacks_element(bloom_filter *filter, unsigned char *elem, size_t len)
{
	int			i;

	CHECK(len == sizeof(Datum));
	for (i = 0; i < bloom_count; ++i)
		if (memcmp(bloom_keys[i], elem, len) == 0)
			return false;
	return true;
}

/* ---- fixtures: plan nodes on the heap, never freed ---- */

static Var *
var(int varno, AttrNumber attno)
{
	return makeVar(varno, attno, INT4OID, -1, InvalidOid, 0);
}

static TargetEntry *
tle(Expr *expr, AttrNumber resno)
{
	return makeTargetEntry(expr, resno, NULL, false);
}

/* A targetlist whose entries are the given Vars, in order. */
static List *
tlist2(Var *a, Var *b)
{
	List	   *list = lappend(NIL, tle((Expr *) a, 1));

	return b != NULL ? lappend(list, tle((Expr *) b, 2)) : list;
}

static Plan *
plan_node(NodeTag tag, Size size, List *tlist, Plan *left, Plan *right)
{
	Plan	   *plan = test_alloc(size);

	plan->type = tag;
	plan->targetlist = tlist;
	plan->lefttree = left;
	plan->righttree = right;
	return plan;
}

static Plan *
seqscan(Index scanrelid, List *tlist)
{
	Plan	   *plan = plan_node(T_SeqScan, sizeof(SeqScan), tlist, NULL, NULL);

	((Scan *) plan)->scanrelid = scanrelid;
	return plan;
}

static OpExpr *
equality(Oid opno, Node *left, Node *right)
{
	OpExpr	   *op = test_alloc(sizeof(OpExpr));

	op->xpr.type = T_OpExpr;
	op->opno = opno;
	op->args = lappend(lappend(NIL, left), right);
	return op;
}

/* ---- anserplan.h key checks ---- */

/*
 * AnserKeyTypeSupportsRawHash(type)
 * Contract: true exactly for by-value base types whose SQL equality is
 * bitwise equality of the value: bool, "char", int2/4/8, oid, xid, cid,
 * date, time, timestamp(tz), money, pg_lsn, xid8.  Everything else is
 * refused, including floats (0.0 = -0.0), any by-reference type, and domain
 * OIDs -- callers unwrap domains first.
 */
static void
test_key_type_supports_raw_hash(void)
{
	Oid			yes[] = {BOOLOID, CHAROID, INT2OID, INT4OID, INT8OID, OIDOID,
		XIDOID, CIDOID, DATEOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID,
	MONEYOID, PG_LSNOID, XID8OID};
	Oid			no[] = {InvalidOid, FLOAT4OID, FLOAT8OID, NUMERICOID, TEXTOID,
		BPCHAROID, VARCHAROID, UUIDOID, INTERVALOID, TIMETZOID, NAMEOID,
	DOMAIN_OVER_INT4};
	int			i;

	for (i = 0; i < lengthof(yes); ++i)
		CHECK(AnserKeyTypeSupportsRawHash(yes[i]));
	for (i = 0; i < lengthof(no); ++i)
		CHECK(!AnserKeyTypeSupportsRawHash(no[i]));
}

/*
 * AnserEqualityKeys(clause, &outer, &inner, &type)
 * Contract: true only for a two-argument OpExpr whose operands are, after
 * stripping any RelabelTypes, one OUTER_VAR and one INNER_VAR Var (either
 * order) of the same type, whose base type passes
 * AnserKeyTypeSupportsRawHash, and whose operator is that type's default
 * equality.  On true, the outer and inner outputs are the borrowed Vars by side (not by
 * operand position) and *type is the base type.  On false nothing is
 * written.  May consult the type cache.
 */
static void
test_equality_keys(void)
{
	Var		   *outer = var(OUTER_VAR, 3);
	Var		   *inner = var(INNER_VAR, 5);
	Var		   *sentinel = var(0, 0);
	Var		   *out = sentinel;
	Var		   *in = sentinel;
	Oid			type = InvalidOid;
	RelabelType *relabel = test_alloc(sizeof(RelabelType));
	RelabelType *relabel2 = test_alloc(sizeof(RelabelType));
	OpExpr	   *op;

	/* Both operand orders; sides come from varno, not position. */
	CHECK(AnserEqualityKeys((Node *) equality(INT4_EQ_OPR, (Node *) outer, (Node *) inner),
							&out, &in, &type));
	CHECK(out == outer && in == inner && type == INT4OID);
	CHECK(AnserEqualityKeys((Node *) equality(INT4_EQ_OPR, (Node *) inner, (Node *) outer),
							&out, &in, &type));
	CHECK(out == outer && in == inner);

	/* Binary-compatible relabels, nested, are looked through. */
	relabel->xpr.type = relabel2->xpr.type = T_RelabelType;
	relabel2->arg = (Expr *) outer;
	relabel->arg = (Expr *) relabel2;
	CHECK(AnserEqualityKeys((Node *) equality(INT4_EQ_OPR, (Node *) relabel, (Node *) inner),
							&out, &in, &type));
	CHECK(out == outer);

	/* A domain on both sides resolves to its base type. */
	outer->vartype = inner->vartype = DOMAIN_OVER_INT4;
	CHECK(AnserEqualityKeys((Node *) equality(INT4_EQ_OPR, (Node *) outer, (Node *) inner),
							&out, &in, &type));
	CHECK(type == INT4OID);

	/* Every refusal leaves the outputs untouched. */
#define REFUSED(clause) \
	do { \
		out = in = sentinel; type = InvalidOid; \
		CHECK(!AnserEqualityKeys((Node *) (clause), &out, &in, &type)); \
		CHECK(out == sentinel && in == sentinel && type == InvalidOid); \
	} while (0)

	/* Domain vs its base type: the Var types differ. */
	inner->vartype = INT4OID;
	REFUSED(equality(INT4_EQ_OPR, (Node *) outer, (Node *) inner));
	outer->vartype = INT4OID;
	/* Cross-type equality. */
	inner->vartype = INT8OID;
	REFUSED(equality(INT4_EQ_OPR, (Node *) outer, (Node *) inner));
	inner->vartype = INT4OID;
	/* Not the type's default equality. */
	REFUSED(equality(INT8_EQ_OPR, (Node *) outer, (Node *) inner));
	/* Not a raw-hashable type, even with its own equality operator. */
	outer->vartype = inner->vartype = FLOAT8OID;
	REFUSED(equality(670, (Node *) outer, (Node *) inner));
	outer->vartype = inner->vartype = INT4OID;
	/* Both operands from one side, or a base-relation Var. */
	REFUSED(equality(INT4_EQ_OPR, (Node *) outer, (Node *) var(OUTER_VAR, 1)));
	REFUSED(equality(INT4_EQ_OPR, (Node *) inner, (Node *) var(INNER_VAR, 1)));
	REFUSED(equality(INT4_EQ_OPR, (Node *) outer, (Node *) var(1, 1)));
	/* An operand that is not a Var. */
	{
		Param	   *param = test_alloc(sizeof(Param));

		param->xpr.type = T_Param;
		REFUSED(equality(INT4_EQ_OPR, (Node *) outer, (Node *) param));
	}
	/* Not an OpExpr, wrong arity, or no clause at all. */
	REFUSED(outer);
	op = equality(INT4_EQ_OPR, (Node *) outer, (Node *) inner);
	op->args = lappend(op->args, inner);
	REFUSED(op);
	REFUSED(NULL);
#undef REFUSED
}

/*
 * AnserPlanOutputVar(plan, position)
 * Contract: the Var that is the expression of output column `position`
 * (1-based) of plan->targetlist, borrowed, unchanged.  NULL for a NULL plan,
 * a position outside the targetlist, or a computed expression.  The Var's
 * varattno is whatever the targetlist says -- an output position is not a
 * table attribute number.
 */
static void
test_plan_output_var(void)
{
	Var		   *a = var(7, 4);
	Var		   *b = var(7, 2);
	Plan	   *scan = seqscan(7, tlist2(a, b));
	Const	   *constant = test_alloc(sizeof(Const));

	CHECK(AnserPlanOutputVar(scan, 1) == a && a->varattno == 4);
	CHECK(AnserPlanOutputVar(scan, 2) == b);
	CHECK(AnserPlanOutputVar(scan, 0) == NULL);
	CHECK(AnserPlanOutputVar(scan, 3) == NULL);
	CHECK(AnserPlanOutputVar(NULL, 1) == NULL);
	constant->xpr.type = T_Const;
	((TargetEntry *) linitial(scan->targetlist))->expr = (Expr *) constant;
	CHECK(AnserPlanOutputVar(scan, 1) == NULL);
}

/*
 * AnserKeyPassthroughNode(plan)
 * Contract: Anser's own conservative descent set -- Hash, Motion, HashJoin
 * and Anser runtime-filter scans -- and nothing else.  Nodes whose output
 * depends on the whole input (Agg, Limit, WindowAgg, ...) and other joins or
 * buffering nodes are not in it; callers extend the set themselves.
 */
static void
test_key_passthrough_node(void)
{
	NodeTag		yes[] = {T_Hash, T_Motion, T_HashJoin};
	NodeTag		no[] = {T_SeqScan, T_Result, T_Agg, T_Limit, T_WindowAgg,
		T_Sort, T_Material, T_MergeJoin, T_NestLoop, T_Append, T_CustomScan,
	T_SubqueryScan};
	Plan	   *filter = plan_node(T_CustomScan, sizeof(CustomScan), NIL, NULL, NULL);
	int			i;

	for (i = 0; i < lengthof(yes); ++i)
		CHECK(AnserKeyPassthroughNode(plan_node(yes[i], sizeof(HashJoin), NIL, NULL, NULL)));
	for (i = 0; i < lengthof(no); ++i)
		CHECK(!AnserKeyPassthroughNode(plan_node(no[i], sizeof(CustomScan), NIL, NULL, NULL)));
	identity_filter = filter;
	CHECK(AnserKeyPassthroughNode(filter));
	identity_filter = NULL;
}

/* ---- anserplanutils.h ---- */

#define ALL_JOIN_TYPES_THROUGH JOIN_DEDUP_SEMI_REVERSE

/*
 * AnserJoinSideReducible(type, inner_side)
 * Contract: may this input of the join be reduced by keys from the join's
 * other input without changing the join's result?  INNER and SEMI: both
 * sides.  LEFT: only the inner (nullable) side.  RIGHT: only the outer
 * side.  Every other type -- FULL, ANTI, LASJ_NOTIN, RIGHT_ANTI and the
 * planner-internal codes -- neither side.
 */
static void
test_join_side_reducible(void)
{
	int			t;

	for (t = JOIN_INNER; t <= ALL_JOIN_TYPES_THROUGH; ++t)
	{
		bool		outer_ok = t == JOIN_INNER || t == JOIN_SEMI || t == JOIN_RIGHT;
		bool		inner_ok = t == JOIN_INNER || t == JOIN_SEMI || t == JOIN_LEFT;

		CHECK(AnserJoinSideReducible((JoinType) t, false) == outer_ok);
		CHECK(AnserJoinSideReducible((JoinType) t, true) == inner_ok);
	}
}

/*
 * AnserJoinSidePushdownSafe(type, inner_side)
 * Contract: may a filter from ABOVE the join be pushed into this input?
 * Never into a nullable input (it would turn pruned rows into
 * NULL-extended ones) and never into a semi join's inner input (it would
 * change which inner row an outer row matches).  INNER: both sides.  LEFT
 * and SEMI: outer only.  RIGHT: inner only.  Every other type: neither.
 * This is a different question from AnserJoinSideReducible.
 */
static void
test_join_side_pushdown_safe(void)
{
	int			t;

	for (t = JOIN_INNER; t <= ALL_JOIN_TYPES_THROUGH; ++t)
	{
		bool		outer_ok = t == JOIN_INNER || t == JOIN_LEFT || t == JOIN_SEMI;
		bool		inner_ok = t == JOIN_INNER || t == JOIN_RIGHT;

		CHECK(AnserJoinSidePushdownSafe((JoinType) t, false) == outer_ok);
		CHECK(AnserJoinSidePushdownSafe((JoinType) t, true) == inner_ok);
	}
}

/*
 * AnserScanOutputVar(scan, position)
 * Contract: the base-table Var behind output column `position` of a scan:
 * a Var whose varno is the scan's own scanrelid and whose varattno is a
 * user column (>= 1).  For an IndexOnlyScan an INDEX_VAR output is first
 * mapped through indextlist.  NULL for anything else: a computed column,
 * another relation's Var, a whole-row or system column, scanrelid 0, or a
 * bad position.
 */
static void
test_scan_output_var(void)
{
	Var		   *col = var(5, 4);
	Plan	   *scan = seqscan(5, tlist2(col, var(6, 1)));
	Plan	   *ios = plan_node(T_IndexOnlyScan, sizeof(IndexOnlyScan),
								tlist2(var(INDEX_VAR, 2), var(INDEX_VAR, 3)),
								NULL, NULL);
	Var		   *indexed = var(8, 6);

	CHECK(AnserScanOutputVar((Scan *) scan, 1) == col);
	CHECK(AnserScanOutputVar((Scan *) scan, 2) == NULL);	/* other relation */
	CHECK(AnserScanOutputVar((Scan *) scan, 3) == NULL);
	col->varattno = 0;			/* whole row */
	CHECK(AnserScanOutputVar((Scan *) scan, 1) == NULL);
	col->varattno = -1;			/* system column */
	CHECK(AnserScanOutputVar((Scan *) scan, 1) == NULL);
	col->varattno = 4;
	((Scan *) scan)->scanrelid = 0;
	CHECK(AnserScanOutputVar((Scan *) scan, 1) == NULL);

	((Scan *) ios)->scanrelid = 8;
	((IndexOnlyScan *) ios)->indextlist = tlist2(var(8, 1), indexed);
	CHECK(AnserScanOutputVar((Scan *) ios, 1) == indexed);
	CHECK(AnserScanOutputVar((Scan *) ios, 2) == NULL);	/* past indextlist */
	((TargetEntry *) lsecond(((IndexOnlyScan *) ios)->indextlist))->expr =
		(Expr *) test_alloc(sizeof(Const));
	((Node *) ((TargetEntry *) lsecond(((IndexOnlyScan *) ios)->indextlist))->expr)->type = T_Const;
	CHECK(AnserScanOutputVar((Scan *) ios, 1) == NULL);
}

/* Policy for the AnserFindKeyScan tests; records every edge it is asked. */
typedef struct EdgeLog
{
	const Plan *parent[8];
	const Plan *child[8];
	bool		inner[8];
	int			n;
	bool		veto_inner;
} EdgeLog;

static bool test_terminus(const Plan *plan) { return IsA(plan, SeqScan); }
static bool test_descend(const Plan *plan)
{
	return IsA(plan, Hash) || IsA(plan, HashJoin) || IsA(plan, Motion);
}
static bool test_identity(const Plan *plan) { return plan == identity_filter; }
static bool
test_edge(const Plan *parent, const Plan *child, bool inner_side, void *arg)
{
	EdgeLog    *log = (EdgeLog *) arg;

	CHECK(log->n < 8);
	log->parent[log->n] = parent;
	log->child[log->n] = child;
	log->inner[log->n++] = inner_side;
	return !(inner_side && log->veto_inner);
}

/*
 * AnserFindKeyScan(top, position, policy, context, &result)
 * Contract: follow output column `position` of `top` down to the terminus
 * scan that produces it.  Every node on the path, `top` included, must be a
 * terminus, a may_descend node or an identity filter; the column must be a
 * plain Var at each step.  OUTER_VAR goes to the outer child, INNER_VAR to
 * the inner child of a HashJoin/MergeJoin/NestLoop only, INDEX_VAR into the
 * single custom_plans child of an identity filter.  may_cross_edge, when
 * set, is asked before each step with the inner_side flag and the caller's
 * context, and can veto it.  On true: scan, parent (NULL when top is the
 * terminus), the output position in the scan and the table attno.  On
 * false the result is untouched.
 */
static void
test_find_key_scan(void)
{
	AnserKeyScanPolicy policy = {test_terminus, test_descend, test_identity, test_edge};
	AnserKeyScanPolicy no_edge = {test_terminus, test_descend, test_identity, NULL};
	/* build: rel 5, output (table 4, table 2) */
	Plan	   *build = seqscan(5, tlist2(var(5, 4), var(5, 2)));
	/* Hash swaps the columns. */
	Plan	   *hash = plan_node(T_Hash, sizeof(Hash),
								 tlist2(var(OUTER_VAR, 2), var(OUTER_VAR, 1)), build, NULL);
	Plan	   *probe = seqscan(6, tlist2(var(6, 1), NULL));
	Plan	   *join = plan_node(T_HashJoin, sizeof(HashJoin),
								 tlist2(var(OUTER_VAR, 1), var(INNER_VAR, 2)), probe, hash);
	AnserKeyScanResult result;
	AnserKeyScanResult untouched;
	EdgeLog		log = {0};

	/* Inner side: join pos 2 -> hash pos 2 -> build pos 1 -> table attno 4. */
	CHECK(AnserFindKeyScan(join, 2, &policy, &log, &result));
	CHECK(result.scan == build && result.parent == hash);
	CHECK(result.output_attno == 1 && result.table_attno == 4);
	CHECK(log.n == 2);
	CHECK(log.parent[0] == join && log.child[0] == hash && log.inner[0]);
	CHECK(log.parent[1] == hash && log.child[1] == build && !log.inner[1]);

	/* Outer side. */
	memset(&log, 0, sizeof(log));
	CHECK(AnserFindKeyScan(join, 1, &policy, &log, &result));
	CHECK(result.scan == probe && result.parent == join && result.table_attno == 1);
	CHECK(log.n == 1 && !log.inner[0]);

	/* No edge callback is allowed. */
	CHECK(AnserFindKeyScan(join, 2, &no_edge, NULL, &result));
	CHECK(result.scan == build);

	/* top itself a terminus: no parent. */
	CHECK(AnserFindKeyScan(build, 2, &policy, &log, &result));
	CHECK(result.scan == build && result.parent == NULL);
	CHECK(result.output_attno == 2 && result.table_attno == 2);

	memset(&untouched, 0x5a, sizeof(untouched));
#define NOT_FOUND(top, pos, pol) \
	do { \
		result = untouched; \
		CHECK(!AnserFindKeyScan((top), (pos), (pol), &log, &result)); \
		CHECK(memcmp(&result, &untouched, sizeof(result)) == 0); \
	} while (0)

	/* A veto stops the descent. */
	log.veto_inner = true;
	NOT_FOUND(join, 2, &policy);
	log.veto_inner = false;

	/* The starting node is checked too: a Result is not transparent. */
	NOT_FOUND(plan_node(T_Result, sizeof(Result), tlist2(var(OUTER_VAR, 1), NULL),
						build, NULL), 1, &policy);
	/* A boundary below a descend node. */
	NOT_FOUND(plan_node(T_Hash, sizeof(Hash), tlist2(var(OUTER_VAR, 1), NULL),
						plan_node(T_Agg, sizeof(Agg), tlist2(var(OUTER_VAR, 1), NULL),
								  build, NULL), NULL), 1, &policy);
	/* INNER_VAR on a non-join, and a missing child. */
	NOT_FOUND(plan_node(T_Hash, sizeof(Hash), tlist2(var(INNER_VAR, 1), NULL),
						build, build), 1, &policy);
	NOT_FOUND(plan_node(T_Hash, sizeof(Hash), tlist2(var(OUTER_VAR, 1), NULL),
						NULL, NULL), 1, &policy);
	/* A base Var above the scan, a bad position, a computed column. */
	NOT_FOUND(plan_node(T_Hash, sizeof(Hash), tlist2(var(5, 1), NULL), build, NULL),
			  1, &policy);
	NOT_FOUND(join, 3, &policy);
	NOT_FOUND(join, 0, &policy);
	{
		Plan	   *computed = plan_node(T_Hash, sizeof(Hash),
										 tlist2(var(OUTER_VAR, 1), NULL), build, NULL);
		Const	   *constant = test_alloc(sizeof(Const));

		constant->xpr.type = T_Const;
		((TargetEntry *) linitial(computed->targetlist))->expr = (Expr *) constant;
		NOT_FOUND(computed, 1, &policy);
	}
	/* The terminus must output its own relation's column. */
	NOT_FOUND(seqscan(5, tlist2(var(6, 1), NULL)), 1, &policy);

	/* Identity filter: INDEX_VAR into its only custom_plans child. */
	{
		CustomScan *filter = (CustomScan *) plan_node(T_CustomScan, sizeof(CustomScan),
													  tlist2(var(INDEX_VAR, 1), NULL),
													  NULL, NULL);

		filter->custom_plans = lappend(NIL, build);
		identity_filter = &filter->scan.plan;
		CHECK(AnserFindKeyScan(&filter->scan.plan, 1, &policy, &log, &result));
		CHECK(result.scan == build && result.parent == &filter->scan.plan);
		CHECK(result.table_attno == 4);
		filter->custom_plans = lappend(filter->custom_plans, probe);
		NOT_FOUND(&filter->scan.plan, 1, &policy);
		identity_filter = NULL;
		/* Not recognized as an identity filter: a boundary. */
		filter->custom_plans = lappend(NIL, build);
		NOT_FOUND(&filter->scan.plan, 1, &policy);
	}
#undef NOT_FOUND
}

typedef struct VisitLog
{
	Plan	   *seen[8];
	int			n;
} VisitLog;

static void
record_visit(Plan *plan, void *arg)
{
	VisitLog   *log = (VisitLog *) arg;

	CHECK(log->n < 8);
	log->seen[log->n++] = plan;
}

/*
 * AnserForEachChildPlan(plan, visit, arg)
 * Contract: calls visit once per direct, non-NULL child plan: lefttree,
 * then righttree, then the node's child list (Append, MergeAppend,
 * Sequence, BitmapAnd/Or, CustomScan custom_plans) or SubqueryScan's
 * subplan, in list order.  It does not recurse, and does not visit
 * initPlans or SubPlans (they live in stmt->subplans).  A NULL plan is a
 * no-op.
 */
static void
test_for_each_child_plan(void)
{
	Plan	   *a = seqscan(1, NIL);
	Plan	   *b = seqscan(2, NIL);
	Plan	   *c = seqscan(3, NIL);
	VisitLog	log = {0};
	Plan	   *plan;
	const struct
	{
		NodeTag tag;
		Size size;
	} list_nodes[] = {
		{T_Append, sizeof(Append)},
		{T_MergeAppend, sizeof(MergeAppend)},
		{T_Sequence, sizeof(Sequence)},
		{T_BitmapAnd, sizeof(BitmapAnd)},
		{T_BitmapOr, sizeof(BitmapOr)},
		{T_CustomScan, sizeof(CustomScan)}
	};
	int			i;

	AnserForEachChildPlan(NULL, record_visit, &log);
	CHECK(log.n == 0);
	AnserForEachChildPlan(a, record_visit, &log);
	CHECK(log.n == 0);

	plan = plan_node(T_HashJoin, sizeof(HashJoin), NIL, a, b);
	plan->initPlan = lappend(NIL, c);	/* must not be visited */
	AnserForEachChildPlan(plan, record_visit, &log);
	CHECK(log.n == 2 && log.seen[0] == a && log.seen[1] == b);

	for (i = 0; i < lengthof(list_nodes); ++i)
	{
		List	   *children = lappend(lappend(lappend(NIL, b), NULL), c);

		/* A child-list field may lie beyond sizeof(CustomScan), notably
		 * MergeAppend.mergeplans. Allocate the actual tagged node type. */
		plan = plan_node(list_nodes[i].tag, list_nodes[i].size, NIL, a, NULL);
		switch (list_nodes[i].tag)
		{
			case T_Append: ((Append *) plan)->appendplans = children; break;
			case T_MergeAppend: ((MergeAppend *) plan)->mergeplans = children; break;
			case T_Sequence: ((Sequence *) plan)->subplans = children; break;
			case T_BitmapAnd: ((BitmapAnd *) plan)->bitmapplans = children; break;
			case T_BitmapOr: ((BitmapOr *) plan)->bitmapplans = children; break;
			default: ((CustomScan *) plan)->custom_plans = children; break;
		}
		memset(&log, 0, sizeof(log));
		AnserForEachChildPlan(plan, record_visit, &log);
		CHECK(log.n == 3);
		CHECK(log.seen[0] == a && log.seen[1] == b && log.seen[2] == c);
	}

	plan = plan_node(T_SubqueryScan, sizeof(SubqueryScan), NIL, NULL, NULL);
	((SubqueryScan *) plan)->subplan = b;
	memset(&log, 0, sizeof(log));
	AnserForEachChildPlan(plan, record_visit, &log);
	CHECK(log.n == 1 && log.seen[0] == b);
}

/*
 * AnserReplaceChildPlan(parent, child, replacement)
 * Contract: replace the one link from parent to child -- lefttree,
 * righttree or a CustomScan custom_plans cell -- with replacement and
 * return true.  If child is not linked, or linked more than once, or any
 * argument is NULL, return false and change nothing.  Replacing a child by
 * itself is the ownership check without a change.  No allocation.
 */
static void
test_replace_child_plan(void)
{
	Plan	   *a = seqscan(1, NIL);
	Plan	   *b = seqscan(2, NIL);
	Plan	   *r = seqscan(3, NIL);
	Plan	   *join = plan_node(T_HashJoin, sizeof(HashJoin), NIL, a, b);
	CustomScan *custom = (CustomScan *) plan_node(T_CustomScan, sizeof(CustomScan),
												  NIL, NULL, NULL);
	int			before = alloc_count;

	CHECK(AnserReplaceChildPlan(join, a, a));
	CHECK(join->lefttree == a && join->righttree == b);
	CHECK(AnserReplaceChildPlan(join, a, r) && join->lefttree == r && join->righttree == b);
	CHECK(AnserReplaceChildPlan(join, b, a) && join->righttree == a);
	CHECK(!AnserReplaceChildPlan(join, b, r));	/* no longer linked */
	CHECK(!AnserReplaceChildPlan(NULL, a, r));
	CHECK(!AnserReplaceChildPlan(join, NULL, r));
	CHECK(!AnserReplaceChildPlan(join, a, NULL));
	join->lefttree = a;			/* a is now both children: ambiguous */
	CHECK(!AnserReplaceChildPlan(join, a, r));
	CHECK(join->lefttree == a && join->righttree == a);
	CHECK(alloc_count == before);

	custom->custom_plans = lappend(lappend(NIL, a), b);
	CHECK(AnserReplaceChildPlan(&custom->scan.plan, b, r));
	CHECK(linitial(custom->custom_plans) == a && lsecond(custom->custom_plans) == r);
	custom->scan.plan.lefttree = a;	/* a linked twice */
	CHECK(!AnserReplaceChildPlan(&custom->scan.plan, a, r));
	CHECK(linitial(custom->custom_plans) == a && custom->scan.plan.lefttree == a);
	custom->scan.plan.lefttree = NULL;
	custom->custom_plans = lappend(lappend(NIL, a), a);
	CHECK(!AnserReplaceChildPlan(&custom->scan.plan, a, r));
}

/*
 * AnserFilterIdentityTlist(child)
 * Contract: a new targetlist with one entry per child output column, in
 * order: Var(INDEX_VAR, position) carrying the column's type, typmod and
 * collation, the same resno, a copied resname (NULL stays NULL) and the same
 * resjunk.  The child's list is not modified.  Allocates.
 */
static void
test_filter_identity_tlist(void)
{
	Var		   *a = makeVar(5, 4, INT8OID, 7, 100, 0);
	Var		   *b = var(5, 2);
	Plan	   *child = seqscan(5, tlist2(a, b));
	TargetEntry *first = linitial(child->targetlist);
	TargetEntry *second = lsecond(child->targetlist);
	List	   *tlist;
	TargetEntry *out;
	Var		   *v;

	first->resname = "k";
	second->resjunk = true;
	tlist = AnserFilterIdentityTlist(child);
	CHECK(list_length(tlist) == 2 && tlist != child->targetlist);
	out = linitial(tlist);
	v = (Var *) out->expr;
	CHECK(IsA(v, Var) && v->varno == INDEX_VAR && v->varattno == 1);
	CHECK(v->vartype == INT8OID && v->vartypmod == 7 && v->varcollid == 100);
	CHECK(out->resno == 1 && !out->resjunk);
	CHECK(out->resname != first->resname && strcmp(out->resname, "k") == 0);
	out = lsecond(tlist);
	CHECK(((Var *) out->expr)->varattno == 2 && out->resno == 2);
	CHECK(out->resname == NULL && out->resjunk);
	CHECK(linitial(child->targetlist) == first && first->expr == (Expr *) a);
	CHECK(AnserFilterIdentityTlist(seqscan(5, NIL)) == NIL);
}

/*
 * AnserInitFilterScan(cs, methods, child)
 * Contract: turn a zeroed CustomScan into a pass-through filter over child:
 * identity targetlist, no qual, no lefttree/righttree, the child's costs,
 * rows and width, not parallel_aware, parallel_safe inherited, a copy of the
 * child's Flow (NULL stays NULL), scanrelid 0, flags 0, no custom_exprs,
 * custom_plans = [child], custom_scan_tlist = AnserFilterScanTlist(child)
 * (a copy), and the given methods.  custom_private and plan_node_id are left
 * to the caller.  Allocates.
 */
static void
test_init_filter_scan(void)
{
	static const CustomScanMethods methods = {"test"};
	Plan	   *child = seqscan(5, tlist2(var(5, 4), NULL));
	CustomScan *cs = (CustomScan *) plan_node(T_CustomScan, sizeof(CustomScan),
											  NIL, NULL, NULL);
	Flow		flow = {0};
	List	   *priv = lappend(NIL, child);

	flow.type = T_Flow;
	flow.numsegments = 3;
	child->flow = &flow;
	child->startup_cost = 1;
	child->total_cost = 25;
	child->plan_rows = 1000;
	child->plan_width = 8;
	child->parallel_aware = child->parallel_safe = true;
	/* Garbage the helper must overwrite, and fields it must keep. */
	cs->scan.plan.qual = priv;
	cs->scan.plan.lefttree = child;
	cs->scan.plan.plan_node_id = 42;
	cs->custom_private = priv;

	AnserInitFilterScan(cs, &methods, child);
	CHECK(cs->methods == &methods);
	CHECK(list_length(cs->custom_plans) == 1 && linitial(cs->custom_plans) == child);
	CHECK(cs->scan.plan.qual == NIL);
	CHECK(cs->scan.plan.lefttree == NULL && cs->scan.plan.righttree == NULL);
	CHECK(cs->scan.plan.startup_cost == 1 && cs->scan.plan.total_cost == 25);
	CHECK(cs->scan.plan.plan_rows == 1000 && cs->scan.plan.plan_width == 8);
	CHECK(!cs->scan.plan.parallel_aware && cs->scan.plan.parallel_safe);
	CHECK(cs->scan.plan.flow != &flow && cs->scan.plan.flow->numsegments == 3);
	CHECK(cs->scan.scanrelid == 0 && cs->flags == 0 && cs->custom_exprs == NIL);
	CHECK(list_length(cs->scan.plan.targetlist) == 1);
	CHECK(((Var *) ((TargetEntry *) linitial(cs->scan.plan.targetlist))->expr)->varno == INDEX_VAR);
	CHECK(cs->custom_scan_tlist != child->targetlist);
	CHECK(linitial(cs->custom_scan_tlist) == linitial(child->targetlist));
	CHECK(cs->custom_private == priv && cs->scan.plan.plan_node_id == 42);

	child->flow = NULL;
	child->parallel_safe = false;
	AnserInitFilterScan(cs, &methods, child);
	CHECK(cs->scan.plan.flow == NULL && !cs->scan.plan.parallel_safe);
}

static TupleTableSlot *copied_dst;
static TupleTableSlot *copied_src;
static void
record_copy(TupleTableSlot *dst, TupleTableSlot *src)
{
	copied_dst = dst;
	copied_src = src;
}

/*
 * AnserFilterChildSlot(node, childslot)
 * Contract: copy the child's tuple into the node's scan slot and return the
 * scan slot -- positional pass-through, no projection.
 * AnserFilterRecheck(node, slot)
 * Contract: always true; a filter node adds no recheck condition.
 */
static void
test_filter_child_slot_and_recheck(void)
{
	static const TupleTableSlotOps ops = {.copyslot = record_copy};
	TupleTableSlot scan_slot = {.tts_ops = &ops};
	TupleTableSlot child_slot = {.tts_ops = &ops};
	CustomScanState node = {0};

	node.ss.ss_ScanTupleSlot = &scan_slot;
	CHECK(AnserFilterChildSlot(&node, &child_slot) == &scan_slot);
	CHECK(copied_dst == &scan_slot && copied_src == &child_slot);
	copied_dst = copied_src = NULL;
	AnserInitFilterChildSlot(&node, true);
	CHECK(AnserFilterChildSlot(&node, &child_slot) == &child_slot);
	CHECK(!node.ss.ps.resultopsfixed && node.ss.ps.resultopsset);
	CHECK(copied_dst == NULL && copied_src == NULL);
	CHECK(AnserFilterRecheck(&node, &scan_slot));
	CHECK(AnserFilterRecheck(NULL, NULL));
}

/* ---- anserbloom.h ---- */

/*
 * AnserNormalizeBloomDatum(value, key_length)
 * Contract: for key_length 1, 2 or 4 take the low key_length bytes of the
 * Datum as a signed value and sign-extend it -- core's canonical form, see
 * bloom_normalize_datum -- so a sign-extended and a zero-extended copy of one
 * value become identical; for sizeof(Datum) return it unchanged.  Distinct
 * values of the key's width stay distinct.
 */
static void
test_normalize_bloom_datum(void)
{
	Datum		all = ~(Datum) 0;

	CHECK(AnserNormalizeBloomDatum(all, 1) == all);
	CHECK(AnserNormalizeBloomDatum(all, 2) == all);
	CHECK(AnserNormalizeBloomDatum(all, 4) == all);
	CHECK(AnserNormalizeBloomDatum(all, sizeof(Datum)) == all);
	CHECK(AnserNormalizeBloomDatum((Datum) 0x7f, 1) == (Datum) 0x7f);
	/* heap sign-extends, AOCO zero-extends: one value after normalizing */
	CHECK(AnserNormalizeBloomDatum(Int32GetDatum(-5), 4) ==
		  AnserNormalizeBloomDatum((Datum) (uint32) -5, 4));
	CHECK(AnserNormalizeBloomDatum(Int16GetDatum(-5), 2) ==
		  AnserNormalizeBloomDatum((Datum) (uint16) -5, 2));
	CHECK(AnserNormalizeBloomDatum(CharGetDatum((char) 0x80), 1) ==
		  AnserNormalizeBloomDatum((Datum) 0x80, 1));
	CHECK(AnserNormalizeBloomDatum(Int32GetDatum(-5), 4) !=
		  AnserNormalizeBloomDatum(Int32GetDatum(5), 4));
	CHECK(AnserNormalizeBloomDatum(Int64GetDatum(-5), 8) == Int64GetDatum(-5));
	/* the same Datum core's scan-key path and PAX hash */
	CHECK(AnserNormalizeBloomDatum((Datum) (uint32) -5, 4) ==
		  bloom_normalize_datum((Datum) (uint32) -5, 4));
	CHECK(AnserNormalizeBloomDatum((Datum) (uint32) -5, 4) == Int64GetDatum(-5));
}

/*
 * AnserBloomAddKey(filter, value, key_length) /
 * AnserBloomLacksKey(filter, value, key_length)
 * Contract: both hash exactly sizeof(Datum) bytes of the normalized value,
 * so a key added from one storage representation is found from the other,
 * and a key never added is reported missing (up to the filter's own false
 * positives).  NULL handling is the caller's.  No allocation.
 */
static void
test_bloom_add_lacks_key(void)
{
	bloom_filter *filter = (bloom_filter *) &bloom_count;	/* opaque here */
	int16		lengths[] = {1, 2, 4};
	int			before = alloc_count;
	int			i;

	for (i = 0; i < lengthof(lengths); ++i)
	{
		Datum		sign_extended = ~(Datum) 0;		/* -1 from heap */
		Datum		zero_extended = lengths[i] == 1 ? (Datum) 0xff :
			lengths[i] == 2 ? (Datum) 0xffff : (Datum) 0xffffffff;	/* AOCO */

		bloom_count = 0;
		AnserBloomAddKey(filter, zero_extended, lengths[i]);
		CHECK(!AnserBloomLacksKey(filter, sign_extended, lengths[i]));
		CHECK(!AnserBloomLacksKey(filter, zero_extended, lengths[i]));
		CHECK(AnserBloomLacksKey(filter, (Datum) 1, lengths[i]));
		bloom_count = 0;
		AnserBloomAddKey(filter, sign_extended, lengths[i]);
		CHECK(!AnserBloomLacksKey(filter, zero_extended, lengths[i]));
	}
	bloom_count = 0;
	AnserBloomAddKey(filter, Int64GetDatum(-1), sizeof(Datum));
	CHECK(!AnserBloomLacksKey(filter, Int64GetDatum(-1), sizeof(Datum)));
	CHECK(AnserBloomLacksKey(filter, (Datum) 0xffffffff, sizeof(Datum)));
	CHECK(alloc_count == before);
}

/*
 * AnserKeyLength(child, position)
 * Contract: attlen of output column `position` (1-based, not a table
 * attno) of the child's result tuple descriptor.  The column must exist and
 * be by-value; anything else is a caller bug caught by assertions.
 */
static void
test_key_length(void)
{
	PlanState	child = {0};
	TupleDesc	desc = calloc(1, offsetof(struct TupleDescData, attrs) +
							  3 * sizeof(FormData_pg_attribute));
	int16		lens[] = {2, 4, 8};
	int			i;

	CHECK(desc != NULL);
	desc->natts = 3;
	for (i = 0; i < 3; ++i)
	{
		TupleDescAttr(desc, i)->attlen = lens[i];
		TupleDescAttr(desc, i)->attbyval = true;
	}
	child.ps_ResultTupleDesc = desc;
	for (i = 0; i < 3; ++i)
		CHECK(AnserKeyLength(&child, i + 1) == lens[i]);
	free(desc);
}

int
main(void)
{
	test_key_type_supports_raw_hash();
	test_equality_keys();
	test_plan_output_var();
	test_key_passthrough_node();
	test_join_side_reducible();
	test_join_side_pushdown_safe();
	test_scan_output_var();
	test_find_key_scan();
	test_for_each_child_plan();
	test_replace_child_plan();
	test_filter_identity_tlist();
	test_init_filter_scan();
	test_filter_child_slot_and_recheck();
	test_normalize_bloom_datum();
	test_bloom_add_lacks_key();
	test_key_length();
	return 0;
}

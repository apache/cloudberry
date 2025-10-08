#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "../explain.c"

extern bool	*gp_resmanager_print_operator_memory_limits;
extern int explain_memory_verbosity;

static void
test__explain_test(void **state)
{
	will_return(get_rel_name, "t1");
	expect_any(get_rel_name, relid);

	// Check the behavior of explain with memory counters if their values are
	// greater than 4Gb.

	// Test operatorMem
	gp_resmanager_print_operator_memory_limits = palloc(sizeof(bool));
	*gp_resmanager_print_operator_memory_limits = true;

	PlanState *planState = makeNode(PlanState);
	planState->plan = (Plan *) makeNode(SeqScan);
	planState->plan->operatorMemKB = (1l << 35);

	SeqScan* seqScan = (SeqScan *) planState->plan;
	seqScan->scanrelid = 1;

	ExplainState *es = palloc0(sizeof(ExplainState));
	ExplainInitState(es);
	RangeTblEntry *rte = makeNode(RangeTblEntry);
	rte->rtekind = RTE_RELATION;
	rte->relid = 1;
	char *tableName = "t1";
	es->rtable = list_make1(rte);
	es->rtable_names = list_make1(tableName);

	ExplainNode(planState, NIL, NULL, NULL, es);
	assert_string_equal(es->str->data,
		"Seq Scan on t1  (cost=0.00..0.00 rows=0 width=0)  operatorMem: 34359738368\n\n");

	// Test Executor Memory of slice
	explain_memory_verbosity = false;
	CdbExplain_ShowStatCtx showStatCtx = {};
	showStatCtx.nslice = 1;
	showStatCtx.slices = palloc0(sizeof(CdbExplain_SliceSummary));
	showStatCtx.slices[0].peakmemused.vcnt = 1;
	showStatCtx.slices[0].peakmemused.vmax = (1l << 35);

	EState *eState = makeNode(EState);
	es = palloc0(sizeof(ExplainState));
	ExplainInitState(es);
	es->format = EXPLAIN_FORMAT_JSON;
	es->extra->groupingstack = list_make1_int(0);
	gpexplain_formatSlicesOutput(&showStatCtx, eState, es);
	assert_string_equal(es->str->data, "\n"
									   "\"Slice statistics\": [\n"
									   "  {\n"
									   "    \"Slice\": 0,\n"
									   "    \"Executor Memory\": 34359738368\n"
									   "  }\n"
									   "]");

	// Test Executor Memory with more than one worker
	showStatCtx.nslice = 1;
	showStatCtx.slices = palloc0(sizeof(CdbExplain_SliceSummary));
	showStatCtx.slices[0].peakmemused.vcnt = 2;
	showStatCtx.slices[0].peakmemused.vsum = (1l << 35);
	showStatCtx.slices[0].peakmemused.vmax = (1l << 35);

	es = palloc0(sizeof(ExplainState));
	ExplainInitState(es);
	es->format = EXPLAIN_FORMAT_JSON;
	es->extra->groupingstack = list_make1_int(0);
	gpexplain_formatSlicesOutput(&showStatCtx, eState, es);
	assert_string_equal(es->str->data, "\n"
									   "\"Slice statistics\": [\n"
									   "  {\n"
									   "    \"Slice\": 0,\n"
									   "    \"Executor Memory\": {\n"
									   "      \"Average\": 17179869184,\n"
									   "      \"Workers\": 2,\n"
									   "      \"Maximum Memory Used\": 34359738368\n"
									   "    }\n"
									   "  }\n"
									   "]");

	// Test Global Peak Memory and Total memory used across slices
	explain_memory_verbosity = true;
	showStatCtx.nslice = 1;
	showStatCtx.slices = palloc0(sizeof(CdbExplain_SliceSummary));
	showStatCtx.slices[0].memory_accounting_global_peak.vcnt = 1;
	showStatCtx.slices[0].memory_accounting_global_peak.vmax = (1l << 50);

	es = palloc0(sizeof(ExplainState));
	ExplainInitState(es);
	es->format = EXPLAIN_FORMAT_JSON;
	es->extra->groupingstack = list_make1_int(0);
	gpexplain_formatSlicesOutput(&showStatCtx, eState, es);
	assert_string_equal(es->str->data, "\n\"Slice statistics\": [\n"
									   "  {\n"
									   "    \"Slice\": 0,\n"
									   "    \"Global Peak Memory\": 1125899906842624\n"
									   "  }\n"
									   "],\n"
									   "\"Total memory used across slices\": 1099511627776");

	// Test Global Peak Memory with more than one worker
	explain_memory_verbosity = true;
	showStatCtx.nslice = 1;
	showStatCtx.slices = palloc0(sizeof(CdbExplain_SliceSummary));
	showStatCtx.slices[0].memory_accounting_global_peak.vcnt = 2;
	showStatCtx.slices[0].memory_accounting_global_peak.vmax = (1l << 50);
	showStatCtx.slices[0].memory_accounting_global_peak.vsum = (1l << 50);

	es = palloc0(sizeof(ExplainState));
	ExplainInitState(es);
	es->format = EXPLAIN_FORMAT_JSON;
	es->extra->groupingstack = list_make1_int(0);
	gpexplain_formatSlicesOutput(&showStatCtx, eState, es);
	assert_string_equal(es->str->data, "\n"
									   "\"Slice statistics\": [\n"
									   "  {\n"
									   "    \"Slice\": 0,\n"
									   "    \"Global Peak Memory\": {\n"
									   "      \"Average\": 562949953421312,\n"
									   "      \"Workers\": 2,\n"
									   "      \"Maximum Memory Used\": 1125899906842624\n"
									   "    }\n"
									   "  }\n"
									   "],\n"
									   "\"Total memory used across slices\": 1099511627776");

	// Test Virtual Memory
	explain_memory_verbosity = true;
	showStatCtx.nslice = 1;
	showStatCtx.slices = palloc0(sizeof(CdbExplain_SliceSummary));
	showStatCtx.slices[0].vmem_reserved.vcnt = 1;
	showStatCtx.slices[0].vmem_reserved.vmax = (1l << 35);

	es = palloc0(sizeof(ExplainState));
	ExplainInitState(es);
	es->format = EXPLAIN_FORMAT_JSON;
	es->extra->groupingstack = list_make1_int(0);
	gpexplain_formatSlicesOutput(&showStatCtx, eState, es);
	assert_string_equal(es->str->data, "\n"
									   "\"Slice statistics\": [\n"
									   "  {\n"
									   "    \"Slice\": 0,\n"
									   "    \"Virtual Memory\": 34359738368\n"
									   "  }\n"
									   "]");

	// Test Virtual Memory with more than one worker
	explain_memory_verbosity = true;
	showStatCtx.nslice = 1;
	showStatCtx.slices = palloc0(sizeof(CdbExplain_SliceSummary));
	showStatCtx.slices[0].vmem_reserved.vcnt = 2;
	showStatCtx.slices[0].vmem_reserved.vmax = (1l << 35);
	showStatCtx.slices[0].vmem_reserved.vsum = (1l << 35);

	es = palloc0(sizeof(ExplainState));
	ExplainInitState(es);
	es->format = EXPLAIN_FORMAT_JSON;
	es->extra->groupingstack = list_make1_int(0);
	gpexplain_formatSlicesOutput(&showStatCtx, eState, es);
	assert_string_equal(es->str->data, "\n"
									   "\"Slice statistics\": [\n"
									   "  {\n"
									   "    \"Slice\": 0,\n"
									   "    \"Virtual Memory\": {\n"
									   "      \"Average\": 17179869184,\n"
									   "      \"Workers\": 2,\n"
									   "      \"Maximum Memory Used\": 34359738368\n"
									   "    }\n"
									   "  }\n"
									   "]");

	// Test Work Maximum Memory
	explain_memory_verbosity = true;
	showStatCtx.nslice = 1;
	showStatCtx.slices = palloc0(sizeof(CdbExplain_SliceSummary));
	showStatCtx.slices[0].workmemused_max = (1l << 35);

	es = palloc0(sizeof(ExplainState));
	ExplainInitState(es);
	es->format = EXPLAIN_FORMAT_JSON;
	es->extra->groupingstack = list_make1_int(0);
	gpexplain_formatSlicesOutput(&showStatCtx, eState, es);
	assert_string_equal(es->str->data, "\n"
									   "\"Slice statistics\": [\n"
									   "  {\n"
									   "    \"Slice\": 0,\n"
									   "    \"Work Maximum Memory\": 34359738368\n"
									   "  }\n"
									   "]");
}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
			unit_test(test__explain_test)
	};

	MemoryContextInit();

	return run_tests(tests);
}

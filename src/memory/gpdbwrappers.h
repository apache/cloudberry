#pragma once

extern "C" {
#include "postgres.h"
#include "nodes/pg_list.h"
#include "commands/explain.h"
#include "executor/instrument.h"
#include "access/htup.h"
}

namespace gpdb {

// Functions that call palloc().
// Make sure correct memory context is set.
void *palloc(Size size);
void *palloc0(Size size);
char *pstrdup(const char *str);
char *get_database_name(Oid dbid) noexcept;
bool split_identifier_string(char *rawstring, char separator,
                           List **namelist) noexcept;
ExplainState get_explain_state(QueryDesc *query_desc, bool costs) noexcept;
ExplainState get_analyze_state_json(QueryDesc *query_desc, bool analyze) noexcept;
Instrumentation *instr_alloc(size_t n, int instrument_options);
HeapTuple heap_form_tuple(TupleDesc tupleDescriptor, Datum *values, bool *isnull);
CdbExplain_ShowStatCtx *cdbexplain_showExecStatsBegin(QueryDesc *query_desc,
                                                     instr_time starttime);
void instr_end_loop(Instrumentation *instr);
char *gen_normquery(const char *query);
StringInfo gen_normplan(const char *executionPlan);

// Palloc-free functions.
void pfree(void *pointer);
MemoryContext mem_ctx_switch_to(MemoryContext context) noexcept;
const char *get_config_option(const char *name, bool missing_ok,
                            bool restrict_superuser) noexcept;
void list_free(List *list);

} // namespace gpdb

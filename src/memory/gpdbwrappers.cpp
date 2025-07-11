#include "gpdbwrappers.h"

extern "C" {
#include "postgres.h"
#include "utils/guc.h"
#include "commands/dbcommands.h"
#include "commands/resgroupcmds.h"
#include "utils/builtins.h"
#include "nodes/pg_list.h"
#include "commands/explain.h"
#include "executor/instrument.h"
#include "access/tupdesc.h"
#include "access/htup.h"
#include "utils/elog.h"
#include "cdb/cdbexplain.h"
#include "stat_statements_parser/pg_stat_statements_ya_parser.h"
}

void *gpdb::palloc(Size size) { return detail::wrap_throw(::palloc, size); }

void *gpdb::palloc0(Size size) { return detail::wrap_throw(::palloc0, size); }

char *gpdb::pstrdup(const char *str) {
  return detail::wrap_throw(::pstrdup, str);
}

char *gpdb::get_database_name(Oid dbid) noexcept {
  return detail::wrap_noexcept(::get_database_name, dbid);
}

bool gpdb::split_identifier_string(char *rawstring, char separator,
                                   List **namelist) noexcept {
  return detail::wrap_noexcept(SplitIdentifierString, rawstring, separator,
                               namelist);
}

ExplainState gpdb::get_explain_state(QueryDesc *query_desc,
                                     bool costs) noexcept {
  return detail::wrap_noexcept([&]() {
    ExplainState es;
    ExplainInitState(&es);
    es.costs = costs;
    es.verbose = true;
    es.format = EXPLAIN_FORMAT_TEXT;
    ExplainBeginOutput(&es);
    ExplainPrintPlan(&es, query_desc);
    ExplainEndOutput(&es);
    return es;
  });
}

ExplainState gpdb::get_analyze_state_json(QueryDesc *query_desc,
                                          bool analyze) noexcept {
  return detail::wrap_noexcept([&]() {
    ExplainState es;
    ExplainInitState(&es);
    es.analyze = analyze;
    es.verbose = true;
    es.buffers = es.analyze;
    es.timing = es.analyze;
    es.summary = es.analyze;
    es.format = EXPLAIN_FORMAT_JSON;
    ExplainBeginOutput(&es);
    if (analyze) {
      ExplainPrintPlan(&es, query_desc);
      ExplainPrintExecStatsEnd(&es, query_desc);
    }
    ExplainEndOutput(&es);
    return es;
  });
}

Instrumentation *gpdb::instr_alloc(size_t n, int instrument_options) {
  return detail::wrap_throw(InstrAlloc, n, instrument_options);
}

HeapTuple gpdb::heap_form_tuple(TupleDesc tupleDescriptor, Datum *values,
                                bool *isnull) {
  if (!tupleDescriptor || !values || !isnull)
    throw std::runtime_error(
        "Invalid input parameters for heap tuple formation");

  return detail::wrap_throw(::heap_form_tuple, tupleDescriptor, values, isnull);
}

void gpdb::pfree(void *pointer) noexcept {
  // Note that ::pfree asserts that pointer != NULL.
  if (!pointer)
    return;

  detail::wrap_noexcept(::pfree, pointer);
}

MemoryContext gpdb::mem_ctx_switch_to(MemoryContext context) noexcept {
  return MemoryContextSwitchTo(context);
}

const char *gpdb::get_config_option(const char *name, bool missing_ok,
                                    bool restrict_superuser) noexcept {
  if (!name)
    return nullptr;

  return detail::wrap_noexcept(GetConfigOption, name, missing_ok,
                               restrict_superuser);
}

void gpdb::list_free(List *list) noexcept {
  if (!list)
    return;

  detail::wrap_noexcept(::list_free, list);
}

CdbExplain_ShowStatCtx *
gpdb::cdbexplain_showExecStatsBegin(QueryDesc *query_desc,
                                    instr_time starttime) {
  if (!query_desc)
    throw std::runtime_error("Invalid query descriptor");

  return detail::wrap_throw(::cdbexplain_showExecStatsBegin, query_desc,
                            starttime);
}

void gpdb::instr_end_loop(Instrumentation *instr) {
  if (!instr)
    throw std::runtime_error("Invalid instrumentation pointer");

  detail::wrap_throw(::InstrEndLoop, instr);
}

char *gpdb::gen_normquery(const char *query) {
  return detail::wrap_throw(::gen_normquery, query);
}

StringInfo gpdb::gen_normplan(const char *exec_plan) {
  if (!exec_plan)
    throw std::runtime_error("Invalid execution plan string");

  return detail::wrap_throw(::gen_normplan, exec_plan);
}

char *gpdb::get_rg_name_for_id(Oid group_id) {
  return detail::wrap_throw(GetResGroupNameForId, group_id);
}

Oid gpdb::get_rg_id_by_session_id(int session_id) {
  return detail::wrap_throw(ResGroupGetGroupIdBySessionId, session_id);
}
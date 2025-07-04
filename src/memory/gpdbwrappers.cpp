#include "gpdbwrappers.h"

extern "C" {
#include "postgres.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "commands/dbcommands.h"
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

#include <stdexcept>

void *gpdb::palloc(Size size) {
  void *result = nullptr;
  bool success;

  PG_TRY();
  {
    result = ::palloc(size);
    success = true;
  }
  PG_CATCH();
  {
    FlushErrorState();
    success = false;
  }
  PG_END_TRY();

  if (!success)
    throw std::runtime_error("Memory allocation failed");

  return result;
}

void *gpdb::palloc0(Size size) {
  void *result = nullptr;
  bool success;

  PG_TRY();
  {
    result = ::palloc0(size);
    success = true;
  }
  PG_CATCH();
  {
    FlushErrorState();
    success = false;
  }
  PG_END_TRY();

  if (!success)
    throw std::runtime_error("Zero init memory allocation failed");

  return result;
}

char *gpdb::pstrdup(const char *str) {
  char *result = nullptr;
  bool success;

  PG_TRY();
  {
    result = ::pstrdup(str);
    success = true;
  }
  PG_CATCH();
  {
    FlushErrorState();
    success = false;
  }
  PG_END_TRY();

  if (!success)
    throw std::runtime_error("String duplication failed");

  return result;
}

char *gpdb::get_database_name(Oid dbid) noexcept {
  char *result = nullptr;

  PG_TRY();
  {
    result = ::get_database_name(dbid);
  }
  PG_CATCH();
  {
    FlushErrorState();
  }
  PG_END_TRY();

  return result;
}

bool gpdb::split_identifier_string(char *rawstring, char separator,
                                 List **namelist) noexcept {
  bool result = false;

  PG_TRY();
  { 
    result = SplitIdentifierString(rawstring, separator, namelist); 
  }
  PG_CATCH();
  {
    FlushErrorState();
  }
  PG_END_TRY();

  return result;
}

ExplainState gpdb::get_explain_state(QueryDesc *query_desc, bool costs) noexcept {
  ExplainState es = {0};

  PG_TRY();
  {
    ExplainInitState(&es);
    es.costs = costs;
    es.verbose = true;
    es.format = EXPLAIN_FORMAT_TEXT;
    ExplainBeginOutput(&es);
    ExplainPrintPlan(&es, query_desc);
    ExplainEndOutput(&es);
  }
  PG_CATCH();
  {
    // PG and GP both have known and yet unknown bugs in EXPLAIN VERBOSE
    // implementation. We don't want any queries to fail due to those bugs, so
    // we report the bug here for future investigatin and continue collecting
    // metrics w/o reporting any plans
    if (es.str && es.str->data) {
      resetStringInfo(es.str);
    }
    // appendStringInfo() can ereport(ERROR), do not call it in PG_CATCH().
    // appendStringInfo(
    //     es.str,
    //     "Unable to restore query plan due to PostgreSQL internal error. "
    //     "See logs for more information");
    ereport(INFO,
            (errmsg("YAGPCC failed to reconstruct explain text for query: %s",
                    query_desc->sourceText)));
    FlushErrorState();
  }
  PG_END_TRY();

  return es;
}

ExplainState gpdb::get_analyze_state_json(QueryDesc *query_desc,
                                       bool analyze) noexcept {
  ExplainState es = {0};

  PG_TRY();
  {
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
  }
  PG_CATCH();
  {
    // PG and GP both have known and yet unknown bugs in EXPLAIN VERBOSE
    // implementation. We don't want any queries to fail due to those bugs, so
    // we report the bug here for future investigatin and continue collecting
    // metrics w/o reporting any plans
    if (es.str && es.str->data) {
      resetStringInfo(es.str);
    }
    // appendStringInfo() can ereport(ERROR), do not call it in PG_CATCH().
    // appendStringInfo(
    //     es.str,
    //     "Unable to restore analyze plan due to PostgreSQL internal error. "
    //     "See logs for more information");
    ereport(INFO,
            (errmsg("YAGPCC failed to reconstruct analyze text for query: %s",
                    query_desc->sourceText)));
    FlushErrorState();
  }
  PG_END_TRY();

  return es;
}

Instrumentation *gpdb::instr_alloc(size_t n, int instrument_options) {
  Instrumentation *result = nullptr;
  bool success;

  PG_TRY();
  {
    result = InstrAlloc(n, instrument_options);
    success = true;
  }
  PG_CATCH();
  {
    FlushErrorState();
    success = false;
  }
  PG_END_TRY();

  if (!success)
    throw std::runtime_error("Instrumentation allocation failed");

  return result;
}

HeapTuple gpdb::heap_form_tuple(TupleDesc tupleDescriptor, Datum *values,
                              bool *isnull) {
  if (!tupleDescriptor || !values || !isnull)
    throw std::runtime_error(
        "Invalid input parameters for heap tuple formation");

  HeapTuple result = nullptr;
  bool success;

  PG_TRY();
  {
    result = ::heap_form_tuple(tupleDescriptor, values, isnull);
    success = true;
  }
  PG_CATCH();
  {
    FlushErrorState();
    success = false;
  }
  PG_END_TRY();

  if (!success)
    throw std::runtime_error("Heap tuple formation failed");

  return result;
}

void gpdb::pfree(void *pointer) {
  if (!pointer)
    return;

  bool success;

  PG_TRY();
  {
    ::pfree(pointer);
    success = true;
  }
  PG_CATCH();
  {
    FlushErrorState();
    success = false;
  }
  PG_END_TRY();

  if (!success)
    throw std::runtime_error("Memory deallocation failed");
}

MemoryContext gpdb::mem_ctx_switch_to(MemoryContext context) noexcept {
  return MemoryContextSwitchTo(context);
}

const char *gpdb::get_config_option(const char *name, bool missing_ok,
                                  bool restrict_superuser) noexcept {
  if (!name)
    return nullptr;

  const char *result = nullptr;

  PG_TRY();
  {
    result = GetConfigOption(name, missing_ok, restrict_superuser);
  }
  PG_CATCH();
  {
    FlushErrorState();
  }
  PG_END_TRY();

  return result;
}

void gpdb::list_free(List *list) {
  if (!list)
    return;

  bool success;

  PG_TRY();
  {
    ::list_free(list);
    success = true;
  }
  PG_CATCH();
  {
    FlushErrorState();
    success = false;
  }
  PG_END_TRY();

  if (!success)
    throw std::runtime_error("List deallocation failed");
}

CdbExplain_ShowStatCtx *
gpdb::cdbexplain_showExecStatsBegin(QueryDesc *query_desc,
                                   instr_time starttime) {
  if (!query_desc)
    throw std::runtime_error("Invalid query descriptor");

  CdbExplain_ShowStatCtx *result = nullptr;
  bool success;

  PG_TRY();
  {
    result = ::cdbexplain_showExecStatsBegin(query_desc, starttime);
    success = true;
  }
  PG_CATCH();
  {
    FlushErrorState();
    success = false;
  }
  PG_END_TRY();

  if (!success)
    throw std::runtime_error("CdbExplain ShowExecStatsBegin failed");

  return result;
}

void gpdb::instr_end_loop(Instrumentation *instr) {
  if (!instr)
    throw std::runtime_error("Invalid instrumentation pointer");

  bool success;

  PG_TRY();
  {
    ::InstrEndLoop(instr);
    success = true;
  }
  PG_CATCH();
  {
    FlushErrorState();
    success = false;
  }
  PG_END_TRY();

  if (!success)
    throw std::runtime_error("InstrEndLoop failed");
}

char *gpdb::gen_normquery(const char *query) {
  char *result = nullptr;
  bool success;

  PG_TRY();
  {
    result = ::gen_normquery(query);
    success = true;
  }
  PG_CATCH();
  {
    FlushErrorState();
    success = false;
  }
  PG_END_TRY();

  if (!success)
    throw std::runtime_error("gen_normquery failed");

  return result;
}
  
StringInfo gpdb::gen_normplan(const char *exec_plan) {
  if (!exec_plan)
    throw std::runtime_error("Invalid execution plan string");

  StringInfo result = nullptr;
  bool success;

  PG_TRY();
  {
    result = ::gen_normplan(exec_plan);
    success = true;
  }
  PG_CATCH();
  {
    FlushErrorState();
    success = false;
  }
  PG_END_TRY();

  if (!success)
    throw std::runtime_error("gen_normplan failed");

  return result;
}
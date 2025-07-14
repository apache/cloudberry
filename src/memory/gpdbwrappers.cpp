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

namespace {

template <bool Throws, typename Func, typename... Args>
auto wrap(Func &&func, Args &&...args) noexcept(!Throws)
    -> decltype(func(std::forward<Args>(args)...)) {

  using RetType = decltype(func(std::forward<Args>(args)...));

  // Empty struct for void return type.
  struct VoidResult {};
  using ResultHolder = std::conditional_t<std::is_void_v<RetType>, VoidResult,
                                          std::optional<RetType>>;

  bool success;
  ErrorData *edata;
  ResultHolder result_holder;

  PG_TRY();
  {
    if constexpr (!std::is_void_v<RetType>) {
      result_holder.emplace(func(std::forward<Args>(args)...));
    } else {
      func(std::forward<Args>(args)...);
    }
    edata = NULL;
    success = true;
  }
  PG_CATCH();
  {
    MemoryContext oldctx = MemoryContextSwitchTo(TopMemoryContext);
    edata = CopyErrorData();
    MemoryContextSwitchTo(oldctx);
    FlushErrorState();
    success = false;
  }
  PG_END_TRY();

  if (!success) {
    std::string err;
    if (edata && edata->message) {
      err = std::string(edata->message);
    } else {
      err = "Unknown error occurred";
    }

    if (edata) {
      FreeErrorData(edata);
    }

    if constexpr (Throws) {
      throw std::runtime_error(err);
    }

    if constexpr (!std::is_void_v<RetType>) {
      return RetType{};
    } else {
      return;
    }
  }

  if constexpr (!std::is_void_v<RetType>) {
    return *std::move(result_holder);
  } else {
    return;
  }
}

template <typename Func, typename... Args>
auto wrap_throw(Func &&func, Args &&...args)
    -> decltype(func(std::forward<Args>(args)...)) {
  return wrap<true>(std::forward<Func>(func), std::forward<Args>(args)...);
}

template <typename Func, typename... Args>
auto wrap_noexcept(Func &&func, Args &&...args) noexcept
    -> decltype(func(std::forward<Args>(args)...)) {
  return wrap<false>(std::forward<Func>(func), std::forward<Args>(args)...);
}
} // namespace

void *ya_gpdb::palloc(Size size) { return wrap_throw(::palloc, size); }

void *ya_gpdb::palloc0(Size size) { return wrap_throw(::palloc0, size); }

char *ya_gpdb::pstrdup(const char *str) { return wrap_throw(::pstrdup, str); }

char *ya_gpdb::get_database_name(Oid dbid) noexcept {
  return wrap_noexcept(::get_database_name, dbid);
}

bool ya_gpdb::split_identifier_string(char *rawstring, char separator,
                                      List **namelist) noexcept {
  return wrap_noexcept(SplitIdentifierString, rawstring, separator, namelist);
}

ExplainState ya_gpdb::get_explain_state(QueryDesc *query_desc,
                                        bool costs) noexcept {
  return wrap_noexcept([&]() {
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

ExplainState ya_gpdb::get_analyze_state_json(QueryDesc *query_desc,
                                             bool analyze) noexcept {
  return wrap_noexcept([&]() {
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

Instrumentation *ya_gpdb::instr_alloc(size_t n, int instrument_options) {
  return wrap_throw(InstrAlloc, n, instrument_options);
}

HeapTuple ya_gpdb::heap_form_tuple(TupleDesc tupleDescriptor, Datum *values,
                                   bool *isnull) {
  if (!tupleDescriptor || !values || !isnull)
    throw std::runtime_error(
        "Invalid input parameters for heap tuple formation");

  return wrap_throw(::heap_form_tuple, tupleDescriptor, values, isnull);
}

void ya_gpdb::pfree(void *pointer) noexcept {
  // Note that ::pfree asserts that pointer != NULL.
  if (!pointer)
    return;

  wrap_noexcept(::pfree, pointer);
}

MemoryContext ya_gpdb::mem_ctx_switch_to(MemoryContext context) noexcept {
  return MemoryContextSwitchTo(context);
}

const char *ya_gpdb::get_config_option(const char *name, bool missing_ok,
                                       bool restrict_superuser) noexcept {
  if (!name)
    return nullptr;

  return wrap_noexcept(GetConfigOption, name, missing_ok, restrict_superuser);
}

void ya_gpdb::list_free(List *list) noexcept {
  if (!list)
    return;

  wrap_noexcept(::list_free, list);
}

CdbExplain_ShowStatCtx *
ya_gpdb::cdbexplain_showExecStatsBegin(QueryDesc *query_desc,
                                       instr_time starttime) {
  if (!query_desc)
    throw std::runtime_error("Invalid query descriptor");

  return wrap_throw(::cdbexplain_showExecStatsBegin, query_desc, starttime);
}

void ya_gpdb::instr_end_loop(Instrumentation *instr) {
  if (!instr)
    throw std::runtime_error("Invalid instrumentation pointer");

  wrap_throw(::InstrEndLoop, instr);
}

char *ya_gpdb::gen_normquery(const char *query) {
  return wrap_throw(::gen_normquery, query);
}

StringInfo ya_gpdb::gen_normplan(const char *exec_plan) {
  if (!exec_plan)
    throw std::runtime_error("Invalid execution plan string");

  return wrap_throw(::gen_normplan, exec_plan);
}

char *ya_gpdb::get_rg_name_for_id(Oid group_id) {
  return wrap_throw(GetResGroupNameForId, group_id);
}

Oid ya_gpdb::get_rg_id_by_session_id(int session_id) {
  return wrap_throw(ResGroupGetGroupIdBySessionId, session_id);
}
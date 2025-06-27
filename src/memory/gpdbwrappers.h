#pragma once

extern "C" {
#include "postgres.h"
#include "nodes/pg_list.h"
#include "commands/explain.h"
#include "executor/instrument.h"
#include "access/htup.h"
#include "utils/elog.h"
#include "utils/memutils.h"
}

#include <type_traits>
#include <stdexcept>
#include <optional>
#include <utility>
#include <string>

namespace gpdb {
namespace detail {

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
  return detail::wrap<true>(std::forward<Func>(func),
                            std::forward<Args>(args)...);
}

template <typename Func, typename... Args>
auto wrap_noexcept(Func &&func, Args &&...args) noexcept
    -> decltype(func(std::forward<Args>(args)...)) {
  return detail::wrap<false>(std::forward<Func>(func),
                             std::forward<Args>(args)...);
}
} // namespace detail

// Functions that call palloc().
// Make sure correct memory context is set.
void *palloc(Size size);
void *palloc0(Size size);
char *pstrdup(const char *str);
char *get_database_name(Oid dbid) noexcept;
bool split_identifier_string(char *rawstring, char separator,
                             List **namelist) noexcept;
ExplainState get_explain_state(QueryDesc *query_desc, bool costs) noexcept;
ExplainState get_analyze_state_json(QueryDesc *query_desc,
                                    bool analyze) noexcept;
Instrumentation *instr_alloc(size_t n, int instrument_options);
HeapTuple heap_form_tuple(TupleDesc tupleDescriptor, Datum *values,
                          bool *isnull);
CdbExplain_ShowStatCtx *cdbexplain_showExecStatsBegin(QueryDesc *query_desc,
                                                      instr_time starttime);
void instr_end_loop(Instrumentation *instr);
char *gen_normquery(const char *query);
StringInfo gen_normplan(const char *executionPlan);
char *get_rg_name_for_id(Oid group_id);

// Palloc-free functions.
void pfree(void *pointer) noexcept;
MemoryContext mem_ctx_switch_to(MemoryContext context) noexcept;
const char *get_config_option(const char *name, bool missing_ok,
                              bool restrict_superuser) noexcept;
void list_free(List *list) noexcept;
Oid get_rg_id_by_session_id(int session_id);

} // namespace gpdb

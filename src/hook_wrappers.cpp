#include "hook_wrappers.h"
#include "EventSender.h"

extern "C" {
#include "postgres.h"
#include "utils/metrics_utils.h"
#include "utils/elog.h"
#include "executor/executor.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbexplain.h"

#include "tcop/utility.h"
}

static ExecutorStart_hook_type previous_ExecutorStart_hook = nullptr;
static ExecutorFinish_hook_type previous_ExecutorFinish_hook = nullptr;

static void ya_ExecutorStart_hook(QueryDesc *queryDesc, int eflags);
static void ya_ExecutorFinish_hook(QueryDesc *queryDesc);

#define REPLACE_HOOK(hookName) \
    previous_##hookName = hookName; \
    hookName = ya_##hookName;

void hooks_init() {
    REPLACE_HOOK(ExecutorStart_hook);
    REPLACE_HOOK(ExecutorFinish_hook);
}

void hooks_deinit() {
    ExecutorStart_hook = previous_ExecutorStart_hook;
    ExecutorFinish_hook = ExecutorFinish_hook;
}

#define CREATE_HOOK_WRAPPER(hookName, ...)                                  \
    PG_TRY();                                                               \
    {                                                                       \
        EventSender::instance()->hookName(__VA_ARGS__);                     \
    }                                                                       \
    PG_CATCH();                                                             \
    {                                                                       \
        ereport(WARNING, (errmsg("EventSender failed in %s", #hookName)));  \
        PG_RE_THROW();                                                      \
    }                                                                       \
    PG_END_TRY();                                                           \
    if (previous_##hookName##_hook)                                         \
        (*previous_##hookName##_hook)(__VA_ARGS__);                         \
    else                                                                    \
        standard_##hookName(__VA_ARGS__);

void ya_ExecutorStart_hook(QueryDesc *queryDesc, int eflags) {
    CREATE_HOOK_WRAPPER(ExecutorStart, queryDesc, eflags);
}

void ya_ExecutorFinish_hook(QueryDesc *queryDesc) {
    CREATE_HOOK_WRAPPER(ExecutorFinish, queryDesc);
}
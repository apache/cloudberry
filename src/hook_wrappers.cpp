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

#define ReplaceHook(hook_name) \
    previous_##hook_name = hook_name; \
    hook_name = ya_##hook_name;

void hooks_init() {
    ReplaceHook(ExecutorStart_hook);
    ReplaceHook(ExecutorFinish_hook);
}

void hooks_deinit() {
    ExecutorStart_hook = previous_ExecutorStart_hook;
    ExecutorFinish_hook = ExecutorFinish_hook;
}

void ya_ExecutorStart_hook(QueryDesc *queryDesc, int eflags) {
    EventSender::instance()->ExecutorStart(queryDesc, eflags);

    if (previous_ExecutorStart_hook)
        (*previous_ExecutorStart_hook)(queryDesc, eflags);
    else
        standard_ExecutorStart(queryDesc, eflags);
}

void ya_ExecutorFinish_hook(QueryDesc *queryDesc) {
    EventSender::instance()->ExecutorFinish(queryDesc);

    if (previous_ExecutorFinish_hook)
        (*previous_ExecutorFinish_hook)(queryDesc);
    else
        standard_ExecutorFinish(queryDesc);
}
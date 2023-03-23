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

void EventSender::ExecutorStart(QueryDesc *queryDesc, int eflags) {
    elog(DEBUG1, "Query %s started", queryDesc->sourceText);
}

void EventSender::ExecutorFinish(QueryDesc *queryDesc) {
    elog(DEBUG1, "Query %s finished", queryDesc->sourceText);
}
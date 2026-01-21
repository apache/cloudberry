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
 * yagp_hooks_collector.c
 *
 * IDENTIFICATION
 *	  gpcontrib/yagp_hooks_collector/src/yagp_hooks_collector.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "cdb/cdbvars.h"
#include "funcapi.h"
#include "utils/builtins.h"

#include "hook_wrappers.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);
PG_FUNCTION_INFO_V1(yagp_stat_messages_reset);
PG_FUNCTION_INFO_V1(yagp_stat_messages);
PG_FUNCTION_INFO_V1(yagp_init_log);
PG_FUNCTION_INFO_V1(yagp_truncate_log);

PG_FUNCTION_INFO_V1(yagp_test_uds_start_server);
PG_FUNCTION_INFO_V1(yagp_test_uds_receive);
PG_FUNCTION_INFO_V1(yagp_test_uds_stop_server);

void _PG_init(void) {
  if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE)
    hooks_init();
}

void _PG_fini(void) {
  if (Gp_role == GP_ROLE_DISPATCH || Gp_role == GP_ROLE_EXECUTE)
    hooks_deinit();
}

Datum yagp_stat_messages_reset(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    yagp_functions_reset();
  }

  funcctx = SRF_PERCALL_SETUP();
  SRF_RETURN_DONE(funcctx);
}

Datum yagp_stat_messages(PG_FUNCTION_ARGS) {
  return yagp_functions_get(fcinfo);
}

Datum yagp_init_log(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    init_log();
  }

  funcctx = SRF_PERCALL_SETUP();
  SRF_RETURN_DONE(funcctx);
}

Datum yagp_truncate_log(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    truncate_log();
  }

  funcctx = SRF_PERCALL_SETUP();
  SRF_RETURN_DONE(funcctx);
}

Datum yagp_test_uds_start_server(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    char *path = text_to_cstring(PG_GETARG_TEXT_PP(0));
    test_uds_start_server(path);
    pfree(path);
  }

  funcctx = SRF_PERCALL_SETUP();
  SRF_RETURN_DONE(funcctx);
}

Datum yagp_test_uds_receive(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;
  int64 *result;

  if (SRF_IS_FIRSTCALL()) {
    MemoryContext oldcontext;

    funcctx = SRF_FIRSTCALL_INIT();
    oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
    result = (int64 *)palloc(sizeof(int64));
    funcctx->user_fctx = result;
    funcctx->max_calls = 1;
    MemoryContextSwitchTo(oldcontext);

    int timeout_ms = PG_GETARG_INT32(0);
    *result = test_uds_receive(timeout_ms);
  }

  funcctx = SRF_PERCALL_SETUP();

  if (funcctx->call_cntr < funcctx->max_calls) {
    result = (int64 *)funcctx->user_fctx;
    SRF_RETURN_NEXT(funcctx, Int64GetDatum(*result));
  }

  SRF_RETURN_DONE(funcctx);
}

Datum yagp_test_uds_stop_server(PG_FUNCTION_ARGS) {
  FuncCallContext *funcctx;

  if (SRF_IS_FIRSTCALL()) {
    funcctx = SRF_FIRSTCALL_INIT();
    test_uds_stop_server();
  }

  funcctx = SRF_PERCALL_SETUP();
  SRF_RETURN_DONE(funcctx);
}

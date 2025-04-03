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
 * pax_oper_udf.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/oper/pax_oper_udf.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/oper/pax_oper.h"

#include "comm/cbdb_wrappers.h"
#include "storage/oper/pax_stats.h"

typedef struct MinMaxOperForUDF {
  StrategyNumber strategy;
  Oid oprleft;
  Oid oprright;
} MinMaxOperForUDF;

typedef struct MinMaxOperForUDFContext {
	MinMaxOperForUDF *oper_for_udf;
	size_t length_of_opers;
	size_t current_index;
} MinMaxOperForUDFContext;

void PrepareMinMaxOpers(MinMaxOperForUDFContext *context) noexcept {
  context->length_of_opers = pax::min_max_opers.size();

  context->oper_for_udf = (MinMaxOperForUDF *)palloc0(sizeof(MinMaxOperForUDF) 
  	* context->length_of_opers);
  context->current_index = 0;

  int i = 0;
  Oid oprleft, oprright;
  StrategyNumber strategy;
  for (auto &kv: pax::min_max_opers) {
    std::tie(oprleft, oprright, strategy) = kv.first;
	context->oper_for_udf[i].strategy = strategy;
	context->oper_for_udf[i].oprleft = oprleft;
	context->oper_for_udf[i].oprright = oprright;
	i++;
  }
}

extern "C" {
// CREATE OR REPLACE FUNCTION pax_get_operators()
// RETURNS TABLE (
//   opername TEXT,
//   strategy int4,
//   oprleft oid,
//   oprright oid
// )
// AS '$libdir/pax', 'pax_get_operators' LANGUAGE C
// IMMUTABLE;

// CREATE VIEW pax_operators AS
// select 
//  po.opername, 
//  po.strategy, 
//  ltype.typname as lefttype, 
//  rtype.typname as righttype 
// from pax_get_operators() as po 
// left join 
//  pg_type as ltype on po.oprleft=ltype.oid 
// left join 
//  pg_type as rtype on po.oprright=rtype.oid;
extern Datum pax_get_operators(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pax_get_operators);
}

#define PAX_OPER_RET_NUMS 4

Datum
pax_get_operators(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	MemoryContext oldcontext;
	Datum		values[PAX_OPER_RET_NUMS];
	bool		nulls[PAX_OPER_RET_NUMS];
	HeapTuple	tuple;
    MinMaxOperForUDFContext *context;

	/* no need return in not master segments */
	if (!IS_QUERY_DISPATCHER()) {
		PG_RETURN_NULL();
	}

	if (SRF_IS_FIRSTCALL())
	{
		/* create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		context = (MinMaxOperForUDFContext *)palloc0(sizeof(MinMaxOperForUDFContext));
		PrepareMinMaxOpers(context);

		/* build tuple descriptor */
		TupleDesc	tupdesc = CreateTemplateTupleDesc(PAX_OPER_RET_NUMS);

		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "opername", TEXTOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "strategy", INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "oprleft", OIDOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "oprright", OIDOID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);
		funcctx->user_fctx = (void *) context;

		/* return to original context when allocating transient memory */
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	context =  (MinMaxOperForUDFContext *) funcctx->user_fctx;

	if (context->current_index < context->length_of_opers) 
	{
		Datum		result;
		MinMaxOperForUDF *min_max_oper = &context->oper_for_udf[context->current_index];

		MemSet(values, 0, sizeof(values));
		MemSet(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(StrategyToOpername(min_max_oper->strategy));
		values[1] = Int32GetDatum(min_max_oper->strategy);
		values[2] = ObjectIdGetDatum(min_max_oper->oprleft);
		values[3] = ObjectIdGetDatum(min_max_oper->oprright);

		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		context->current_index++;
		SRF_RETURN_NEXT(funcctx, result);
	} else  {
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		pfree(context->oper_for_udf);
		pfree(context);
		MemoryContextSwitchTo(oldcontext);
		SRF_RETURN_DONE(funcctx);
	}
}

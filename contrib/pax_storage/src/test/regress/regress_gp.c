/*------------------------------------------------------------------------
 *
 * regress_gp.c
 *	 Greenplum specific code for various C-language functions defined as
 *	 part of the regression tests.
 *
 * Portions Copyright (c) 1996-2009, PostgreSQL Global Development Group
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 * src/test/regress/regress_gp.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "funcapi.h"
#include "tablefuncapi.h"
#include "miscadmin.h"

#include <float.h>
#include <math.h>
#include <unistd.h>

#include "libpq-fe.h"
#include "pgstat.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/pg_language.h"
#include "catalog/pg_type.h"
#include "cdb/memquota.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbfts.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbutil.h"
#include "cdb/cdbvars.h"
#include "cdb/ml_ipc.h"
#include "commands/sequence.h"
#include "commands/trigger.h"
#include "executor/executor.h"
#include "executor/spi.h"
#include "port/atomics.h"
#include "parser/parse_expr.h"
#include "storage/bufmgr.h"
#include "storage/buf_internals.h"
#include "libpq/auth.h"
#include "libpq/hba.h"
#include "utils/builtins.h"
#include "utils/geo_decls.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resource_manager.h"
#include "utils/timestamp.h"

/* table_functions test */
extern Datum multiset_example(PG_FUNCTION_ARGS);
extern Datum multiset_scalar_null(PG_FUNCTION_ARGS);
extern Datum multiset_scalar_value(PG_FUNCTION_ARGS);
extern Datum multiset_scalar_tuple(PG_FUNCTION_ARGS);
extern Datum multiset_setof_null(PG_FUNCTION_ARGS);
extern Datum multiset_setof_value(PG_FUNCTION_ARGS);
extern Datum multiset_materialize_good(PG_FUNCTION_ARGS);
extern Datum multiset_materialize_bad(PG_FUNCTION_ARGS);

/* table functions + dynamic type support */
extern Datum sessionize(PG_FUNCTION_ARGS);
extern Datum describe(PG_FUNCTION_ARGS);
extern Datum project(PG_FUNCTION_ARGS);
extern Datum project_describe(PG_FUNCTION_ARGS);
extern Datum noop_project(PG_FUNCTION_ARGS);
extern Datum userdata_describe(PG_FUNCTION_ARGS);
extern Datum userdata_project(PG_FUNCTION_ARGS);

/* Resource queue/group support */
extern Datum checkResourceQueueMemoryLimits(PG_FUNCTION_ARGS);
extern Datum repeatPalloc(PG_FUNCTION_ARGS);

/* Gang management test support */
extern Datum gangRaiseInfo(PG_FUNCTION_ARGS);
extern Datum cleanupAllGangs(PG_FUNCTION_ARGS);
extern Datum hasGangsExist(PG_FUNCTION_ARGS);
extern Datum numActiveMotionConns(PG_FUNCTION_ARGS);
extern Datum hasBackendsExist(PG_FUNCTION_ARGS);

/* Transient types */
extern Datum assign_new_record(PG_FUNCTION_ARGS);

/* guc_env_var */
extern Datum udf_setenv(PG_FUNCTION_ARGS);
extern Datum udf_unsetenv(PG_FUNCTION_ARGS);

/* Auth Constraints */
extern Datum check_auth_time_constraints(PG_FUNCTION_ARGS);

/* XID wraparound */
extern Datum test_consume_xids(PG_FUNCTION_ARGS);
extern Datum gp_execute_on_server(PG_FUNCTION_ARGS);

/* Check shared buffer cache for a database Oid */
extern Datum check_shared_buffer_cache_for_dboid(PG_FUNCTION_ARGS);

/* oid wraparound tests */
extern Datum gp_set_next_oid(PG_FUNCTION_ARGS);
extern Datum gp_get_next_oid(PG_FUNCTION_ARGS);

/* Broken output function, for testing */
extern Datum broken_int4out(PG_FUNCTION_ARGS);


/* Triggers */

typedef struct
{
	char	   *ident;
	int			nplans;
	void	  **splan;
}	EPlan;

extern Datum trigger_udf_return_new_oid(PG_FUNCTION_ARGS);


PG_FUNCTION_INFO_V1(multiset_scalar_null);
Datum
multiset_scalar_null(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(multiset_scalar_value);
Datum
multiset_scalar_value(PG_FUNCTION_ARGS)
{
	AnyTable             scan;
	HeapTuple            tuple;
	TupleDesc            in_tupdesc;
	Datum                values[1];
	bool				 nulls[1];
	int					 result;

	/*
	 * Sanity checking, shouldn't occur if our CREATE FUNCTION in SQL is done
	 * correctly.
	 */
	if (PG_NARGS() < 1 || PG_ARGISNULL(0))
		elog(ERROR, "invalid invocation of multiset_scalar_value");
	scan = PG_GETARG_ANYTABLE(0);  /* Should be the first parameter */

	/* Get the next value from the input scan */
	in_tupdesc  = AnyTable_GetTupleDesc(scan);
	tuple       = AnyTable_GetNextTuple(scan);

	/* check for end of scan */
	if (tuple == NULL)
		PG_RETURN_NULL();

	/*
	 * We expect an input of one integer column for this stupid
	 * table function, if that is not what we got then complain.
	 */
	if (in_tupdesc->natts != 1 || TupleDescAttr(in_tupdesc, 0)->atttypid != INT4OID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CANNOT_COERCE),
				 errmsg("invalid input tuple for function multiset_scalar_value"),
				 errhint("expected (integer, text) ")));
	}

	/* -----
	 * Extract fields from input tuple, there are several possibilities
	 * depending on if we want to fetch the rows by name, by number, or extract
	 * the full tuple contents.
	 *
	 *    - values[0] = GetAttributeByName(tuple->t_data, "a", &nulls[0]);
	 *    - values[0] = GetAttributeByNum(tuple->t_data, 1, &nulls[0]);
	 *    - heap_deform_tuple(tuple, in_tupdesc, values, nulls);
	 *
	 * In this case we have chosen to use getAttributeByNum
	 */
	values[0] = heap_getattr(tuple, 1, in_tupdesc, &nulls[0]);

	/* Handle NULL */
	if (nulls[0])
		PG_RETURN_NULL();

	/*
	 * Convert the Datum to an integer so we can operate on it.
	 *
	 * Since we are just going to return it directly we could skip this step,
	 * and simply call PG_RETURN_DATUM(values[0]), but this is more illustrative.
	 */
	result = DatumGetInt32(values[0]);

	PG_RETURN_INT32(result);
}

PG_FUNCTION_INFO_V1(multiset_scalar_tuple);
Datum
multiset_scalar_tuple(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsi;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		values[2];
	bool		nulls[2];
	Datum		result;

	/* Double check that the output tupledesc is what we expect */
	rsi		= (ReturnSetInfo *) fcinfo->resultinfo;
	tupdesc	= rsi->expectedDesc;

	if (tupdesc->natts != 2 ||
		(TupleDescAttr(tupdesc, 0)->atttypid != INT4OID && !TupleDescAttr(tupdesc, 0)->attisdropped) ||
		(TupleDescAttr(tupdesc, 1)->atttypid != TEXTOID && !TupleDescAttr(tupdesc, 1)->attisdropped))
	{
		ereport(ERROR,
				(errcode(ERRCODE_CANNOT_COERCE),
				 errmsg("invalid output tupledesc for function multiset_scalar_tuple"),
				 errhint("Expected (integer, text).")));
	}

	/* Populate an output tuple. */
	values[0] = Int32GetDatum(1);
	values[1] = CStringGetTextDatum("Example");
	nulls[0] = nulls[1] = false;
	tuple = heap_form_tuple(tupdesc, values, nulls);
	result = HeapTupleGetDatum(tuple);

	PG_RETURN_DATUM(result);
}

PG_FUNCTION_INFO_V1(multiset_setof_null);
Datum
multiset_setof_null(PG_FUNCTION_ARGS)
{
	FuncCallContext		*fctx;
	ReturnSetInfo 		*rsi;

	if (SRF_IS_FIRSTCALL())
	{
		fctx = SRF_FIRSTCALL_INIT();
	}
	fctx = SRF_PERCALL_SETUP();

	/* This is just one way we might test that we are done: */
	if (fctx->call_cntr < 3)
	{
		/*
		 * set returning functions shouldn't return NULL results, in order to
		 * due so you need to sidestep the normal SRF_RETURN_NEXT mechanism.
		 * This is an error-condition test, not correct coding practices.
		 */
		rsi	= (ReturnSetInfo *) fcinfo->resultinfo;
		rsi->isDone = ExprMultipleResult;
		fctx->call_cntr++; /* would happen automatically with SRF_RETURN_NEXT */
		PG_RETURN_NULL();  /* see above: only for testing, don't do this */
	}
	else
	{
		/* do any user specific cleanup on last call */
		SRF_RETURN_DONE(fctx);
	}
}

PG_FUNCTION_INFO_V1(multiset_setof_value);
Datum
multiset_setof_value(PG_FUNCTION_ARGS)
{
	FuncCallContext		*fctx;

	if (SRF_IS_FIRSTCALL())
	{
		fctx = SRF_FIRSTCALL_INIT();
	}
	fctx = SRF_PERCALL_SETUP();

	/* This is just one way we might test that we are done: */
	if (fctx->call_cntr < 3)
	{
		SRF_RETURN_NEXT(fctx, Int32GetDatum(fctx->call_cntr));
	}
	else
	{
		/* do any user specific cleanup on last call */
		SRF_RETURN_DONE(fctx);
	}
}

PG_FUNCTION_INFO_V1(multiset_materialize_good);
Datum
multiset_materialize_good(PG_FUNCTION_ARGS)
{
	FuncCallContext		*fctx;
	ReturnSetInfo 		*rsi = (ReturnSetInfo *) fcinfo->resultinfo;

	/*
	 * To return SFRM_Materialize the correct convention is to first
	 * check if this is an allowed mode.  Currently it is not, so we
	 * expect this to raise an error.
	 */
	if (!rsi || !IsA(rsi, ReturnSetInfo) ||
		(rsi->allowedModes & SFRM_Materialize) == 0 ||
		rsi->expectedDesc == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	}


	if (SRF_IS_FIRSTCALL())
	{
		fctx = SRF_FIRSTCALL_INIT();
	}
	fctx = SRF_PERCALL_SETUP();
	SRF_RETURN_DONE(fctx);
}

PG_FUNCTION_INFO_V1(multiset_materialize_bad);
Datum
multiset_materialize_bad(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsi = (ReturnSetInfo *) fcinfo->resultinfo;

	/*
	 * This function is "bad" because it does not check if the caller
	 * supports SFRM_Materialize before trying to return a materialized
	 * set.
	 */
	rsi->returnMode = SFRM_Materialize;
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(multiset_example);
Datum
multiset_example(PG_FUNCTION_ARGS)
{
	FuncCallContext		*fctx;
	ReturnSetInfo 		*rsi;
	AnyTable             scan;
	HeapTuple            tuple;
	TupleDesc            in_tupdesc;
	TupleDesc            out_tupdesc;
	Datum                tup_datum;
	Datum                values[2];
	bool				 nulls[2];

	/*
	 * Sanity checking, shouldn't occur if our CREATE FUNCTION in SQL is done
	 * correctly.
	 */
	if (PG_NARGS() < 1 || PG_ARGISNULL(0))
		elog(ERROR, "invalid invocation of multiset_example");
	scan = PG_GETARG_ANYTABLE(0);  /* Should be the first parameter */

	/* Basic set-returning function (SRF) protocol, setup the context */
	if (SRF_IS_FIRSTCALL())
	{
		fctx = SRF_FIRSTCALL_INIT();
	}
	fctx = SRF_PERCALL_SETUP();

	/* Get the next value from the input scan */
	rsi			= (ReturnSetInfo *) fcinfo->resultinfo;
	out_tupdesc = rsi->expectedDesc;
	in_tupdesc  = AnyTable_GetTupleDesc(scan);
	tuple       = AnyTable_GetNextTuple(scan);

	/* check for end of scan */
	if (tuple == NULL)
		SRF_RETURN_DONE(fctx);

	/*
	 * We expect an input/output of two columns (int, text) for this stupid
	 * table function, if that is not what we got then complain.
	 */
	if (in_tupdesc->natts != 2 ||
		TupleDescAttr(in_tupdesc, 0)->atttypid != INT4OID ||
		TupleDescAttr(in_tupdesc, 1)->atttypid != TEXTOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CANNOT_COERCE),
				 errmsg("invalid input tuple for function multiset_example"),
				 errhint("Expected (integer, text).")));
	}

	/* For output tuple we also check for possibility of dropped columns */
	if (out_tupdesc->natts != 2 ||
		(TupleDescAttr(out_tupdesc, 0)->atttypid != INT4OID && !TupleDescAttr(out_tupdesc, 0)->attisdropped) ||
		(TupleDescAttr(out_tupdesc, 1)->atttypid != TEXTOID && !TupleDescAttr(out_tupdesc, 1)->attisdropped))
	{
		ereport(ERROR,
				(errcode(ERRCODE_CANNOT_COERCE),
				 errmsg("invalid output tuple for function multiset_example"),
				 errhint("Expected (integer, text).")));
	}


	/* -----
	 * Extract fields from input tuple, there are several possibilities
	 * depending on if we want to fetch the rows by name, by number, or extract
	 * the full tuple contents.
	 *
	 *    - values[0] = GetAttributeByName(tuple->t_data, "a", &nulls[0]);
	 *    - values[0] = GetAttributeByNum(tuple->t_data, 0, &nulls[0]);
	 *    - heap_deform_tuple(tuple, in_tupdesc, values, nulls);
	 *
	 * In this case we have chosen to do the whole tuple at once.
	 */
	heap_deform_tuple(tuple, in_tupdesc, values, nulls);

	/*
	 * Since we have already validated types we can form this directly
	 * into our output tuple without additional conversion.
	 */
	tuple = heap_form_tuple(out_tupdesc, values, nulls);

	/*
	 * Final output must always be a Datum, so convert the tuple as required
	 * by the API.
	 */
	tup_datum = HeapTupleGetDatum(tuple);

	/* Extract values from input tuple, build output tuple */
	SRF_RETURN_NEXT(fctx, tup_datum);
}

/*
 * Checks if memory limit of resource queue is in sync across
 * shared memory and catalog.
 * This function should ONLY be used for unit testing.
 */
PG_FUNCTION_INFO_V1(checkResourceQueueMemoryLimits);
Datum
checkResourceQueueMemoryLimits(PG_FUNCTION_ARGS)
{
	char *queueName = PG_GETARG_CSTRING(0);
	Oid queueId;
	ResQueue queue;
	double v1, v2;

	if (!IsResQueueEnabled())
		return (Datum)0;

	if (queueName == NULL)
		return (Datum)0;

	/* get OID for queue */
	queueId = GetResQueueIdForName(queueName);

	if (queueId == InvalidOid)
		return (Datum)0;

	/* ResQueueHashFind needs a lock */
	LWLockAcquire(ResQueueLock, LW_EXCLUSIVE);

	/* get shared memory version of queue */
	queue = ResQueueHashFind(queueId);

	LWLockRelease(ResQueueLock);

	if (!queue)
		return (Datum) 0;

	v1 = ceil(queue->limits[RES_MEMORY_LIMIT].threshold_value);
	v2 = ceil((double) ResourceQueueGetMemoryLimitInCatalog(queueId));

	PG_RETURN_BOOL(v1 == v2);
}

/*
 * Helper function to raise an INFO with options including DETAIL, HINT
 * From PostgreSQL 8.4, we can do this in plpgsql by RAISE statement, but now we
 * use PL/C.
 */
PG_FUNCTION_INFO_V1(gangRaiseInfo);
Datum
gangRaiseInfo(PG_FUNCTION_ARGS)
{
	ereport(INFO,
			(errmsg("testing hook function MPPnoticeReceiver"),
			 errdetail("this test aims at covering code paths not hit before"),
			 errhint("no special hint"),
			 errcontext("PL/C function defined in regress.c"),
			 errposition(0)));

	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(cleanupAllGangs);
Datum
cleanupAllGangs(PG_FUNCTION_ARGS)
{
	if (Gp_role != GP_ROLE_DISPATCH)
		elog(ERROR, "cleanupAllGangs can only be executed on master");
	DisconnectAndDestroyAllGangs(false);
	PG_RETURN_BOOL(true);
}

PG_FUNCTION_INFO_V1(hasGangsExist);
Datum
hasGangsExist(PG_FUNCTION_ARGS)
{
	if (Gp_role != GP_ROLE_DISPATCH)
		elog(ERROR, "hasGangsExist can only be executed on master");
	if (cdbcomponent_qesExist())
		PG_RETURN_BOOL(true);
	PG_RETURN_BOOL(false);
}

PG_FUNCTION_INFO_V1(numActiveMotionConns);
Datum numActiveMotionConns(PG_FUNCTION_ARGS)
{
	/* Just valid for udpifc implement, always 0 for other implements. */
	uint32 num = CurrentMotionIPCLayer->GetActiveMotionConns();
	PG_RETURN_UINT32(num);
}


PG_FUNCTION_INFO_V1(assign_new_record);
Datum
assign_new_record(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx = NULL;

	if (SRF_IS_FIRSTCALL())
	{
		funcctx = SRF_FIRSTCALL_INIT();
		TupleDesc	tupdesc;

		tupdesc = CreateTemplateTupleDesc(1);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "c", INT4OID, -1, 0);

		BlessTupleDesc(tupdesc);
		funcctx->tuple_desc = tupdesc;

		/* dummy output */
		funcctx->max_calls = 10;
	}

	if (Gp_role == GP_ROLE_DISPATCH)
		SRF_RETURN_DONE(funcctx);

	/* stuff done on every call of the function */
	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < funcctx->max_calls)
	{
		TupleDesc	tupdesc;
		HeapTuple	tuple;
		Datum		dummy_values[10];
		bool		dummy_nulls[10];
		int			i;

		tupdesc = CreateTemplateTupleDesc(funcctx->call_cntr);

		for (i = 1; i <= funcctx->call_cntr; i++)
			TupleDescInitEntry(tupdesc, (AttrNumber) i, "c", INT4OID, -1, 0);

		BlessTupleDesc(tupdesc);

		for (i = 0; i < funcctx->call_cntr + 1; i++)
		{
			dummy_values[i] = Int32GetDatum(i);
			dummy_nulls[i] = false;
		}

		tuple = heap_form_tuple(tupdesc, dummy_values, dummy_nulls);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		/* nothing left */
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * GPDB_95_MERGE_FIXME: Commit d7cdf6ee36a introduce a similar function to this
 * one with regress_putenv().  When we catch to 9.5 we should switch over to
 * using that.
 */
PG_FUNCTION_INFO_V1(udf_setenv);
Datum
udf_setenv(PG_FUNCTION_ARGS)
{
	const char *name = (const char *) PG_GETARG_CSTRING(0);
	const char *value = (const char *) PG_GETARG_CSTRING(1);
	int ret = setenv(name, value, 1);

	PG_RETURN_BOOL(ret == 0);
}

PG_FUNCTION_INFO_V1(udf_unsetenv);
Datum
udf_unsetenv(PG_FUNCTION_ARGS)
{
	const char *name = (const char *) PG_GETARG_CSTRING(0);
	int ret = unsetenv(name);
	PG_RETURN_BOOL(ret == 0);
}

PG_FUNCTION_INFO_V1(repeatPalloc);
Datum
repeatPalloc(PG_FUNCTION_ARGS)
{
	int32 size = PG_GETARG_INT32(0);
	int32 count = PG_GETARG_INT32(1);
	int i;

	for (i = 0; i < count; i++)
		MemoryContextAlloc(TopMemoryContext, size * 1024 * 1024);

	PG_RETURN_INT32(0);
}

/*
 * This is do-nothing table function that passes the input relation
 * to the output relation without any modification.
 */
PG_FUNCTION_INFO_V1(noop_project);
Datum
noop_project(PG_FUNCTION_ARGS)
{
	AnyTable			scan;
	FuncCallContext	   *fctx;
	ReturnSetInfo	   *rsi;
	HeapTuple			tuple;

	scan = PG_GETARG_ANYTABLE(0);
	if (SRF_IS_FIRSTCALL())
	{
		fctx = SRF_FIRSTCALL_INIT();
	}
	fctx = SRF_PERCALL_SETUP();
	rsi = (ReturnSetInfo *) fcinfo->resultinfo;
	tuple = AnyTable_GetNextTuple(scan);
	if (!tuple)
		SRF_RETURN_DONE(fctx);

	SRF_RETURN_NEXT(fctx, HeapTupleGetDatum(tuple));
}

/*
 * sessionize
 */
typedef struct session_state {
	int			id;
	Timestamp	time;
	int			counter;
} session_state;

PG_FUNCTION_INFO_V1(sessionize);
Datum
sessionize(PG_FUNCTION_ARGS)
{
	FuncCallContext		*fctx;
	ReturnSetInfo 		*rsi;
	AnyTable             scan;
	HeapTuple            tuple;
	TupleDesc            in_tupdesc;
	TupleDesc            out_tupdesc;
	Datum                tup_datum;
	Datum                values[3];
	bool				 nulls[3];
	session_state       *state;
	int                  newId;
	Timestamp            newTime;
	Interval            *threshold;

	/*
	 * Sanity checking, shouldn't occur if our CREATE FUNCTION in SQL is done
	 * correctly.
	 */
	if (PG_NARGS() != 2 || PG_ARGISNULL(0) || PG_ARGISNULL(1))
		elog(ERROR, "invalid invocation of sessionize");
	scan = PG_GETARG_ANYTABLE(0);  /* Should be the first parameter */
	threshold = PG_GETARG_INTERVAL_P(1);

	/* Basic set-returning function (SRF) protocol, setup the context */
	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		fctx = SRF_FIRSTCALL_INIT();

		oldcontext = MemoryContextSwitchTo(fctx->multi_call_memory_ctx);

		state = (session_state*) palloc0(sizeof(session_state));
		fctx->user_fctx = (void*) state;
		state->id = -9999;  /* gross hack: stupid special value for demo */

		MemoryContextSwitchTo(oldcontext);
	}
	fctx = SRF_PERCALL_SETUP();
	state = (session_state*) fctx->user_fctx;

	/* Get the next value from the input scan */
	rsi			= (ReturnSetInfo *) fcinfo->resultinfo;
	out_tupdesc = rsi->expectedDesc;
	in_tupdesc  = AnyTable_GetTupleDesc(scan);
	tuple       = AnyTable_GetNextTuple(scan);

	/* check for end of scan */
	if (tuple == NULL)
		SRF_RETURN_DONE(fctx);

	/*
	 * We expect an input/output of two columns (int, text) for this stupid
	 * table function, if that is not what we got then complain.
	 */
	if (in_tupdesc->natts != 2 ||
		TupleDescAttr(in_tupdesc, 0)->atttypid != INT4OID ||
		TupleDescAttr(in_tupdesc, 1)->atttypid != TIMESTAMPOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_CANNOT_COERCE),
				 errmsg("invalid input tuple for function sessionize"),
				 errhint("Expected (integer, timestamp).")));
	}

	/* For output tuple we also check for possibility of dropped columns */
	if (out_tupdesc->natts != 3 ||
		(TupleDescAttr(out_tupdesc, 0)->atttypid != INT4OID && !TupleDescAttr(out_tupdesc, 0)->attisdropped) ||
		(TupleDescAttr(out_tupdesc, 1)->atttypid != TIMESTAMPOID && !TupleDescAttr(out_tupdesc, 1)->attisdropped) ||
		(TupleDescAttr(out_tupdesc, 2)->atttypid != INT4OID && !TupleDescAttr(out_tupdesc, 2)->attisdropped))
	{
		ereport(ERROR,
				(errcode(ERRCODE_CANNOT_COERCE),
				 errmsg("invalid output tuple for function sessionize"),
				 errhint("Expected (integer, timestamp, integer).")));
	}

	/* -----
	 * Extract fields from input tuple, there are several possibilities
	 * depending on if we want to fetch the rows by name, by number, or extract
	 * the full tuple contents.
	 *
	 *    - values[0] = GetAttributeByName(tuple->t_data, "a", &nulls[0]);
	 *    - values[0] = GetAttributeByNum(tuple->t_data, 0, &nulls[0]);
	 *    - heap_deform_tuple(tuple, in_tupdesc, values, nulls);
	 *
	 * In this case we have chosen to do the whole tuple at once.
	 */
	heap_deform_tuple(tuple, in_tupdesc, values, nulls);
	newId = DatumGetInt32(values[0]);
	newTime = DatumGetTimestamp(values[1]);

	/* just skip null input */
	if (nulls[0] || nulls[1])
	{
		nulls[2] = true;
	}
	else
	{
		nulls[2] = false;

		/* handle state transition */
		if (newId == state->id)
		{
			Datum d;

			/* Calculate old timestamp + interval */
			d = DirectFunctionCall2(timestamp_pl_interval,
									TimestampGetDatum(state->time),
									IntervalPGetDatum(threshold));

			/* if that is less than new interval then bump counter */
			d = DirectFunctionCall2(timestamp_lt, d, TimestampGetDatum(newTime));

			if (DatumGetBool(d))
				state->counter++;
			state->time = newTime;
		}
		else
		{
			state->id	   = newId;
			state->time	   = newTime;
			state->counter = 1;
		}
	}

	/*
	 * Since we have already validated types we can form this directly
	 * into our output tuple without additional conversion.
	 */
	values[2] = Int32GetDatum(state->counter);
	tuple = heap_form_tuple(out_tupdesc, values, nulls);

	/*
	 * Final output must always be a Datum, so convert the tuple as required
	 * by the API.
	 */
	tup_datum = HeapTupleGetDatum(tuple);

	/* Extract values from input tuple, build output tuple */
	SRF_RETURN_NEXT(fctx, tup_datum);
}

/*
 * The absolute simplest of describe functions, ignore input and always return
 * the same tuple descriptor.  This is effectively the same as statically
 * defining the type in the CREATE TABLE definition, but this time we have
 * pushed it into a dynamic call time resolution context.
 */
PG_FUNCTION_INFO_V1(describe);
Datum
describe(PG_FUNCTION_ARGS)
{
	FuncExpr   *fexpr;
	TupleDesc	tupdesc;

	if (PG_NARGS() != 1 || PG_ARGISNULL(0))
		elog(ERROR, "invalid invocation of describe");

	fexpr = (FuncExpr*) PG_GETARG_POINTER(0);
	if (!IsA(fexpr, FuncExpr))
		ereport(ERROR, (errmsg("invalid parameters for describe")));

	/* Build a result tuple descriptor */
	tupdesc = CreateTemplateTupleDesc(3);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "id", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "time", TIMESTAMPOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "sessionnum", INT4OID, -1, 0);

	PG_RETURN_POINTER(tupdesc);
}

PG_FUNCTION_INFO_V1(project);
Datum
project(PG_FUNCTION_ARGS)
{
	FuncCallContext		*fctx;
	ReturnSetInfo		*rsi;
	AnyTable			 scan;
	HeapTuple			 tuple;
	TupleDesc			 in_tupdesc;
	TupleDesc			 out_tupdesc;
	Datum				 tup_datum;
	Datum				 values[1];
	bool				 nulls[1];
	int					 position;

	/*
	 * Sanity checking, shouldn't occur if our CREATE FUNCTION in SQL is done
	 * correctly.
	 */
	if (PG_NARGS() != 2 || PG_ARGISNULL(0) || PG_ARGISNULL(1))
		elog(ERROR, "invalid invocation of project");
	scan = PG_GETARG_ANYTABLE(0);  /* Should be the first parameter */
	position = PG_GETARG_INT32(1);

	/* Basic set-returning function (SRF) protocol, setup the context */
	if (SRF_IS_FIRSTCALL())
	{
		fctx = SRF_FIRSTCALL_INIT();
	}
	fctx = SRF_PERCALL_SETUP();

	/* Get the next value from the input scan */
	rsi			= (ReturnSetInfo *) fcinfo->resultinfo;
	out_tupdesc = rsi->expectedDesc;
	in_tupdesc  = AnyTable_GetTupleDesc(scan);
	tuple       = AnyTable_GetNextTuple(scan);

	/* Based on what the describe callback should have setup */
	if (position <= 0 || position > in_tupdesc->natts)
		ereport(ERROR, (errmsg("invalid position provided")));
	if (out_tupdesc->natts != 1)
		ereport(ERROR, (errmsg("only one expected tuple is allowed")));
	if (TupleDescAttr(out_tupdesc, 0)->atttypid != TupleDescAttr(in_tupdesc, position-1)->atttypid)
		ereport(ERROR, (errmsg("input and output types do not match")));

	/* check for end of scan */
	if (tuple == NULL)
		SRF_RETURN_DONE(fctx);

	/* -----
	 * Extract fields from input tuple, there are several possibilities
	 * depending on if we want to fetch the rows by name, by number, or extract
	 * the full tuple contents.
	 *
	 *    - values[0] = GetAttributeByName(tuple->t_data, "a", &nulls[0]);
	 *    - values[0] = GetAttributeByNum(tuple->t_data, 0, &nulls[0]);
	 *    - heap_deform_tuple(tuple, in_tupdesc, values, nulls);
	 *
	 * In this case we have chosen to do extract by position
	 */
	values[0] = GetAttributeByNum(tuple->t_data, (AttrNumber) position, &nulls[0]);

	/* Construct the output tuple and convert to a datum */
	tuple = heap_form_tuple(out_tupdesc, values, nulls);
	tup_datum = HeapTupleGetDatum(tuple);

	/* Return the next result */
	SRF_RETURN_NEXT(fctx, tup_datum);
}

/*
 * A more dynamic describe function that produces different results depending
 * on what sort of input it receives.
 */
PG_FUNCTION_INFO_V1(project_describe);
Datum
project_describe(PG_FUNCTION_ARGS)
{
	FuncExpr			*fexpr;
	List				*fargs;
	ListCell			*lc;
	Oid					*argtypes;
	int					 numargs;
	TableValueExpr		*texpr;
	Query				*qexpr;
	TupleDesc			 tdesc;
	TupleDesc			 odesc;
	int					 avalue;
	bool				 isnull;
	int					 i;

	/* Fetch and validate input */
	if (PG_NARGS() != 1 || PG_ARGISNULL(0))
		elog(ERROR, "invalid invocation of project_describe");

	fexpr = (FuncExpr*) PG_GETARG_POINTER(0);
	if (!IsA(fexpr, FuncExpr))
		ereport(ERROR, (errmsg("invalid parameters for project_describe")));

	/*
	 * We should know the type information of the arguments of our calling
	 * function, but this demonstrates how we could determine that if we
	 * didn't already know.
	 */
	fargs = fexpr->args;
	numargs = list_length(fargs);
	argtypes = palloc(sizeof(Oid)*numargs);
	foreach_with_count(lc, fargs, i)
	{
		Node *arg = lfirst(lc);
		argtypes[i] = exprType(arg);
	}

	/* --------
	 * Given that we believe we know that this function is tied to exactly
	 * one implementation, lets verify that the above types are what we
	 * were expecting:
	 *   - two arguments
	 *   - first argument "anytable"
	 *   - second argument "text"
	 * --------
	 */
	if (numargs != 2)
		ereport(ERROR, (errmsg("invalid argument number"),
				errdetail("Two arguments need to be provided to the function")));
	if (argtypes[0] != ANYTABLEOID)
		ereport(ERROR, (errmsg("first argument is not a table OID")));
	if (argtypes[1] != INT4OID)
		ereport(ERROR, (errmsg("second argument is not a integer OID")));

	/* Now get the tuple descriptor for the ANYTABLE we received */
	texpr = (TableValueExpr*) linitial(fargs);
	if (!IsA(texpr, TableValueExpr))
		ereport(ERROR, (errmsg("function argument is not a table")));

	qexpr = (Query*) texpr->subquery;
	if (!IsA(qexpr, Query))
		ereport(ERROR, (errmsg("subquery is not a Query object")));

	tdesc = ExecCleanTypeFromTL(qexpr->targetList);

	/*
	 * The intent of this table function is that it returns the Nth column
	 * from the input, which requires us to know what N is.  We get N from
	 * the second parameter to the table function.
	 *
	 * Try to evaluate that argument to a constant value.
	 */
	avalue = DatumGetInt32(ExecEvalFunctionArgToConst(fexpr, 1, &isnull));
	if (isnull)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unable to resolve type for function")));

	if (avalue < 1 || avalue > tdesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid column position %d", avalue)));

	/* Build an output tuple a single column based on the column number above */
	odesc = CreateTemplateTupleDesc(1);
	TupleDescInitEntry(odesc, 1,
					   NameStr(TupleDescAttr(tdesc, avalue-1)->attname),
					   TupleDescAttr(tdesc, avalue-1)->atttypid,
					   TupleDescAttr(tdesc, avalue-1)->atttypmod,
					   0);

	/* Finally return that tupdesc */
	PG_RETURN_POINTER(odesc);
}

PG_FUNCTION_INFO_V1(userdata_project);
Datum
userdata_project(PG_FUNCTION_ARGS)
{
	FuncCallContext		*fctx;
	ReturnSetInfo		*rsi;
	AnyTable			 scan;
	HeapTuple			 tuple;
	TupleDesc			 out_tupdesc;
	Datum				 tup_datum;
	Datum				 values[1];
	bool				 nulls[1];
	bytea				*userdata;
	char				*message;

	/*
	 * Sanity checking, shouldn't occur if our CREATE FUNCTION in SQL is done
	 * correctly.
	 */
	if (PG_NARGS() != 1 || PG_ARGISNULL(0))
		elog(ERROR, "invalid invocation of userdata_project");
	scan = PG_GETARG_ANYTABLE(0);  /* Should be the first parameter */
	if (SRF_IS_FIRSTCALL())
	{
		fctx = SRF_FIRSTCALL_INIT();
	}
	fctx = SRF_PERCALL_SETUP();

	/* Get the next value from the input scan */
	rsi			= (ReturnSetInfo *) fcinfo->resultinfo;
	out_tupdesc = rsi->expectedDesc;
	tuple       = AnyTable_GetNextTuple(scan);
	if (tuple == NULL)
		SRF_RETURN_DONE(fctx);

	/* Receive message from describe function */
	userdata = TF_GET_USERDATA();
	if (userdata != NULL)
	{
		message = (char *) VARDATA(userdata);
		values[0] = CStringGetTextDatum(message);
		nulls[0] = false;
	}
	else
	{
		values[0] = (Datum) 0;
		nulls[0] = true;
	}
	/* Construct the output tuple and convert to a datum */
	tuple = heap_form_tuple(out_tupdesc, values, nulls);
	tup_datum = HeapTupleGetDatum(tuple);

	/* Return the next result */
	SRF_RETURN_NEXT(fctx, tup_datum);
}

PG_FUNCTION_INFO_V1(userdata_describe);
Datum
userdata_describe(PG_FUNCTION_ARGS)
{
	FuncExpr	   *fexpr;
	TupleDesc		tupdesc;
	bytea		   *userdata;
	size_t			bytes;
	const char	   *message = "copied data from describe function";

	/* Fetch and validate input */
	if (PG_NARGS() != 1 || PG_ARGISNULL(0))
		elog(ERROR, "invalid invocation of userdata_describe");

	fexpr = (FuncExpr*) PG_GETARG_POINTER(0);
	if (!IsA(fexpr, FuncExpr))
		ereport(ERROR, (errmsg("invalid parameters for userdata_describe")));

	/* Build a result tuple descriptor */
	tupdesc = CreateTemplateTupleDesc(1);
	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "message", TEXTOID, -1, 0);

	/* Prepare user data */
	bytes = VARHDRSZ + 256;
	userdata = (bytea *) palloc0(bytes);
	SET_VARSIZE(userdata, bytes);
	strcpy(VARDATA(userdata), message);

	/* Set to send */
	TF_SET_USERDATA(userdata);

	PG_RETURN_POINTER(tupdesc);
}

/*
 * Simply invoke CheckAuthTimeConstraints from libpq/auth.h with given rolname.
 */
PG_FUNCTION_INFO_V1(check_auth_time_constraints);
Datum
check_auth_time_constraints(PG_FUNCTION_ARGS)
{
	char		   *rolname = PG_GETARG_CSTRING(0);
	TimestampTz 	timestamp = PG_GETARG_TIMESTAMPTZ(1);

	PG_RETURN_BOOL(check_auth_time_constraints_internal(rolname, timestamp) == STATUS_OK);
}

/*
 * check if backends exist
 * Args:
 * timeout: = 0, return result immediately
 * timeout: > 0, block until no backends exist or timeout expired.
 */
PG_FUNCTION_INFO_V1(hasBackendsExist);
Datum
hasBackendsExist(PG_FUNCTION_ARGS)
{
	int beid;
	int32 result = 0;
	int timeout = PG_GETARG_INT32(0);

	if (timeout < 0)
		elog(ERROR, "timeout is expected not to be negative");

	int pid = getpid();

	while (timeout >= 0)
	{
		result = 0;
		pgstat_clear_snapshot();
		int tot_backends = pgstat_fetch_stat_numbackends();
		for (beid = 1; beid <= tot_backends; beid++)
		{
			PgBackendStatus *beentry = pgstat_get_beentry_by_backend_id(beid);
			if (beentry && beentry->st_procpid >0 && beentry->st_procpid != pid &&
				beentry->st_session_id == gp_session_id)
				result++;
		}
		if (result == 0 || timeout == 0)
			break;
		sleep(1); /* 1 second */
		timeout--;
	}

	if (result > 0)
		PG_RETURN_BOOL(true);
	PG_RETURN_BOOL(false);
}

/*
 * test_consume_xids(int4), for rapidly consuming XIDs, to test wraparound.
 *
 * Used by the 'autovacuum' test.
 */
PG_FUNCTION_INFO_V1(test_consume_xids);
Datum
test_consume_xids(PG_FUNCTION_ARGS)
{
	int32		nxids = PG_GETARG_INT32(0);
	TransactionId topxid;
	FullTransactionId fullxid;
	TransactionId xid;
	TransactionId targetxid;

	/* make sure we have a top-XID first */
	topxid = GetCurrentTransactionId();

	xid = ReadNextTransactionId();

	/* xid is the "next xid" now, so minus one here */
	targetxid = xid + nxids - 1;
	while (targetxid < FirstNormalTransactionId)
		targetxid++;

	while (TransactionIdPrecedes(xid, targetxid))
	{
		elog(DEBUG1, "xid: %u", xid);
		fullxid = GetNewTransactionId(true);
		xid = XidFromFullTransactionId(fullxid);
	}

	PG_RETURN_VOID();
}

/*
 * Function to execute a DML/DDL command on segment with specified content id.
 * To use:
 *
 * CREATE FUNCTION gp_execute_on_server(content int, query text) returns text
 * language C as '$libdir/regress.so', 'gp_execute_on_server';
 */
PG_FUNCTION_INFO_V1(gp_execute_on_server);
Datum
gp_execute_on_server(PG_FUNCTION_ARGS)
{
	int32		content = PG_GETARG_INT32(0);
	char	   *query = TextDatumGetCString(PG_GETARG_TEXT_PP(1));
	CdbPgResults cdb_pgresults;
	StringInfoData result_str;

	if (!IS_QUERY_DISPATCHER())
		elog(ERROR, "cannot use gp_execute_on_server() when not in QD mode");

	CdbDispatchCommandToSegments(query,
								 DF_CANCEL_ON_ERROR | DF_WITH_SNAPSHOT,
								 list_make1_int(content),
								 &cdb_pgresults);

	/*
	 * Collect the results.
	 *
	 * All the result fields are appended to a string, with minimal
	 * formatting. That's not very pretty, but is good enough for
	 * regression tests.
	 */
	initStringInfo(&result_str);
	for (int resultno = 0; resultno < cdb_pgresults.numResults; resultno++)
	{
		struct pg_result *pgresult = cdb_pgresults.pg_results[resultno];

		if (PQresultStatus(pgresult) != PGRES_TUPLES_OK &&
			PQresultStatus(pgresult) != PGRES_COMMAND_OK)
		{
			cdbdisp_clearCdbPgResults(&cdb_pgresults);
			elog(ERROR, "execution failed with status %d", PQresultStatus(pgresult));
		}

		for (int rowno = 0; rowno < PQntuples(pgresult); rowno++)
		{
			if (rowno > 0)
				appendStringInfoString(&result_str, "\n");
			for (int colno = 0; colno < PQnfields(pgresult); colno++)
			{
				if (colno > 0)
					appendStringInfoString(&result_str, " ");
				appendStringInfoString(&result_str, PQgetvalue(pgresult, rowno, colno));
			}
		}
	}

	cdbdisp_clearCdbPgResults(&cdb_pgresults);
	PG_RETURN_TEXT_P(result_str.data);
}

/*
 * Check if the shared buffer cache contains any pages that have the specified
 * database OID in their buffer tag. Return true if an entry is found, else
 * return false.
 */
PG_FUNCTION_INFO_V1(check_shared_buffer_cache_for_dboid);
Datum
check_shared_buffer_cache_for_dboid(PG_FUNCTION_ARGS)
{
	Oid databaseOid = PG_GETARG_OID(0);
	int i;

	for (i = 0; i < NBuffers; i++)
	{
		volatile BufferDesc *bufHdr = GetBufferDescriptor(i);

		if (bufHdr->tag.dbOid == databaseOid)
			PG_RETURN_BOOL(true);
	}

	PG_RETURN_BOOL(false);
}

PG_FUNCTION_INFO_V1(gp_set_next_oid);
Datum
gp_set_next_oid(PG_FUNCTION_ARGS)
{
	Oid new_oid = PG_GETARG_OID(0);

	LWLockAcquire(OidGenLock, LW_EXCLUSIVE);

	ShmemVariableCache->nextOid = new_oid;

	LWLockRelease(OidGenLock);

	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(gp_get_next_oid);
Datum
gp_get_next_oid(PG_FUNCTION_ARGS)
{
	PG_RETURN_OID(ShmemVariableCache->nextOid);
}

/*
 * This is like int4out, but throws an error on '1234'.
 *
 * Used in the error handling test in 'gpcopy'.
 */
PG_FUNCTION_INFO_V1(broken_int4out);
Datum
broken_int4out(PG_FUNCTION_ARGS)
{
	int32		arg = PG_GETARG_INT32(0);

	if (arg == 1234)
		ereport(ERROR,
				(errcode(ERRCODE_FAULT_INJECT),
				 errmsg("testing failure in output function"),
				 errdetail("The trigger value was 1234")));

	return DirectFunctionCall1(int4out, Int32GetDatum(arg));
}

#if defined(TCP_KEEPIDLE)
/* TCP_KEEPIDLE is the name of this option on Linux and *BSD */
#define PG_TCP_KEEPALIVE_IDLE TCP_KEEPIDLE
#define PG_TCP_KEEPALIVE_IDLE_STR "TCP_KEEPIDLE"
#elif defined(TCP_KEEPALIVE) && defined(__darwin__)
/* TCP_KEEPALIVE is the name of this option on macOS */
#define PG_TCP_KEEPALIVE_IDLE TCP_KEEPALIVE
#define PG_TCP_KEEPALIVE_IDLE_STR "TCP_KEEPALIVE"
#endif

/*
 * This test function obtains the TCP socket keepalive settings for the
 * cdbgang QD/QE libpq connections.
 */
PG_FUNCTION_INFO_V1(gp_keepalives_check);
Datum
gp_keepalives_check(PG_FUNCTION_ARGS)
{
	typedef struct Context
	{
		int index;
		CdbComponentDatabases *cdbs;
		ListCell *currentQE;
	} Context;

	FuncCallContext *funcctx;
	Context *context;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc tupdesc;
		MemoryContext oldcontext;

		/* Create a function context for cross-call persistence */
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch to memory context for appropriate multiple function call */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		/* Create tupdesc for result */
		tupdesc = CreateTemplateTupleDesc(6);
		TupleDescInitEntry(tupdesc, (AttrNumber) 1, "qe_id",
						   INT2OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 2, "is_writer",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 3, "keepalives_enabled",
						   BOOLOID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 4, "keepalives_interval",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 5, "keepalives_count",
						   INT4OID, -1, 0);
		TupleDescInitEntry(tupdesc, (AttrNumber) 6, "keepalives_idle",
						   INT4OID, -1, 0);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		context = (Context *) palloc(sizeof(Context));
		context->index = 0;
		context->cdbs = (cdbcomponent_getComponentInfo(MASTER_CONTENT_ID))->cdbs;
		context->currentQE = NULL;

		funcctx->user_fctx = (void *) context;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	context = (Context *) funcctx->user_fctx;

	/* Loop through all QE segments in the cdbgang */
	while (context->index < context->cdbs->total_segment_dbs)
	{
		Datum values[6];
		bool nulls[6];
		HeapTuple tuple;
		Datum result;
		int keepalives_enabled;
		int keepalives_interval;
		int keepalives_count;
		int keepalives_idle;
		int socket_fd;
		uint size = sizeof(int);
		SegmentDatabaseDescriptor *segDesc;

		/* The QE segment freelist contains the idle QE info that stores the PGconn objects */
		if (context->cdbs->segment_db_info[context->index].freelist == NULL
			|| (context->currentQE != NULL &&
				lnext(context->cdbs->segment_db_info[context->index].freelist, context->currentQE) == NULL))
		{
			/* This QE segment freelist is empty or we've reached the end of the freelist */
			context->currentQE = NULL;
			context->index++;
			continue;
		}
		else if (context->currentQE == NULL)
		{
			/* Start at the head of this QE segment freelist */
			context->currentQE = list_head(context->cdbs->segment_db_info[context->index].freelist);
		}
		else
		{
			/* Continue to next item in the QE segment freelist (e.g. readers and writers) */
			context->currentQE = lnext(context->cdbs->segment_db_info[context->index].freelist, context->currentQE);
		}

		/* Obtain the socket file descriptor from each libpq connection */
		segDesc = (SegmentDatabaseDescriptor *)(lfirst(context->currentQE));
		socket_fd = PQsocket(segDesc->conn);

		/* Obtain the TCP socket keepalive settings */
		if (getsockopt(socket_fd, SOL_SOCKET, SO_KEEPALIVE,
					   &keepalives_enabled, &size) < 0)
			elog(ERROR, "getsockopt(%s) failed: %m", "SO_KEEPALIVE");

		if (getsockopt(socket_fd, IPPROTO_TCP, TCP_KEEPINTVL,
					   (char *) &keepalives_interval, &size) < 0)
			elog(ERROR, "getsockopt(%s) failed: %m", "TCP_KEEPINTVL");

		if (getsockopt(socket_fd, IPPROTO_TCP, TCP_KEEPCNT,
					   (char *) &keepalives_count, &size) < 0)
			elog(ERROR, "getsockopt(%s) failed: %m", "TCP_KEEPCNT");

		if (getsockopt(socket_fd, IPPROTO_TCP, PG_TCP_KEEPALIVE_IDLE,
					   (char *) &keepalives_idle, &size) < 0)
			elog(ERROR, "getsockopt(%s) failed: %m", PG_TCP_KEEPALIVE_IDLE_STR);

		/* Form tuple with appropriate data */
		MemSet(values, 0, sizeof(values));
		MemSet(nulls, false, sizeof(nulls));

		values[0] = Int16GetDatum(segDesc->segindex);
		values[1] = BoolGetDatum(segDesc->isWriter);
		values[2] = BoolGetDatum(keepalives_enabled > 0);
		values[3] = Int32GetDatum(keepalives_interval);
		values[4] = Int32GetDatum(keepalives_count);
		values[5] = Int32GetDatum(keepalives_idle);
		tuple = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

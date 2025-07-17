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
 * try_convert.c
 *		  Error-safe type cast function
 *
 * IDENTIFICATION
 *		  contrib/try_convert/try_convert.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_cast.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "nodes/nodeFuncs.h"
// #include "nodes/miscnodes.h"

#include "executor/spi.h"

#include "funcapi.h"

#define USE_PG_TRY_CATCH

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(try_convert);


typedef enum ConversionType
{
	CONVERSION_TYPE_FUNC,
	CONVERSION_TYPE_RELABEL,
	CONVERSION_TYPE_VIA_IO,
	CONVERSION_TYPE_ARRAY,	
	CONVERSION_TYPE_NONE	
} ConversionType;


static ConversionType find_conversion_way(Oid targetTypeId, Oid sourceTypeId, Oid *funcId);
static ConversionType find_typmod_conversion_function(Oid typeId, Oid *funcId);
static Datum convert_from_function(Datum value, int32 typmod, Oid funcId, bool *is_failed);
static Datum convert_via_io(Datum value, Oid sourceTypeId, 
							Oid targetTypeId, int32 targetTypMod, 
							bool *is_failed);
static int32 get_call_expr_argtypmod(Node *expr, int argnum);
static Oid get_fn_expr_argtypmod(FmgrInfo *flinfo, int argnum);
static Datum convert(Datum value, ConversionType conversion_type, 
					 Oid funcId, Oid sourceTypeId, 
					 Oid targetTypeId, int32 targetTypMod, 
					 bool *is_failed);
static Datum convert_type_typmod(Datum value, int32 sourceTypMod, 
								 Oid targetTypeId, int32 targetTypMod, 
								 bool *is_failed);


static ConversionType
find_conversion_way(Oid targetTypeId, Oid sourceTypeId, Oid *funcId)
{
	ConversionType result = CONVERSION_TYPE_NONE;
	HeapTuple	tuple;

	*funcId = InvalidOid;

	/* Perhaps the types are domains; if so, look at their base types */
	if (OidIsValid(sourceTypeId))
		sourceTypeId = getBaseType(sourceTypeId);
	if (OidIsValid(targetTypeId))
		targetTypeId = getBaseType(targetTypeId);

	/* Domains are always coercible to and from their base type */
	if (sourceTypeId == targetTypeId)
		return CONVERSION_TYPE_RELABEL;

	/* SELECT castcontext from pg_cast */

	/* Look in pg_cast */
	tuple = SearchSysCache2(CASTSOURCETARGET,
							ObjectIdGetDatum(sourceTypeId),
							ObjectIdGetDatum(targetTypeId));

	if (HeapTupleIsValid(tuple))
	{
		Form_pg_cast castForm = (Form_pg_cast) GETSTRUCT(tuple);

		switch (castForm->castmethod)
		{
			case COERCION_METHOD_FUNCTION:
				*funcId = castForm->castfunc;
				result = CONVERSION_TYPE_FUNC;
				break;
			case COERCION_METHOD_INOUT:
				result = CONVERSION_TYPE_VIA_IO;
				break;
			case COERCION_METHOD_BINARY:
				result = CONVERSION_TYPE_RELABEL;
				break;
			default:
				elog(ERROR, "unrecognized castmethod: %d",
						(int) castForm->castmethod);
				break;
		}
	

		ReleaseSysCache(tuple);
	}
	else
	{
		/*
		 * If there's no pg_cast entry, perhaps we are dealing with a pair of
		 * array types.  If so, and if the element types have a suitable cast,
		 * report that we can coerce with an ArrayCoerceExpr.
		 *
		 * Note that the source type can be a domain over array, but not the
		 * target, because ArrayCoerceExpr won't check domain constraints.
		 *
		 * Hack: disallow coercions to oidvector and int2vector, which
		 * otherwise tend to capture coercions that should go to "real" array
		 * types.  We want those types to be considered "real" arrays for many
		 * purposes, but not this one.  (Also, ArrayCoerceExpr isn't
		 * guaranteed to produce an output that meets the restrictions of
		 * these datatypes, such as being 1-dimensional.)
		 */
		if (targetTypeId != OIDVECTOROID && targetTypeId != INT2VECTOROID)
		{
			Oid			targetElem;
			Oid			sourceElem;

			if ((targetElem = get_element_type(targetTypeId)) != InvalidOid &&
			(sourceElem = get_base_element_type(sourceTypeId)) != InvalidOid)
			{
				ConversionType elempathtype;
				Oid			   elemfuncid;

				elempathtype = find_conversion_way(targetElem,
												   sourceElem,
												  &elemfuncid);
				if (elempathtype != CONVERSION_TYPE_NONE &&
					elempathtype != CONVERSION_TYPE_ARRAY)
				{
					*funcId = elemfuncid;
					if (elempathtype == CONVERSION_TYPE_VIA_IO)
						result = CONVERSION_TYPE_VIA_IO;
					else
						result = CONVERSION_TYPE_ARRAY;
				}
			}
		}

		/*
		 * If we still haven't found a possibility, consider automatic casting
		 * using I/O functions.  We allow assignment casts to string types and
		 * explicit casts from string types to be handled this way. (The
		 * CoerceViaIO mechanism is a lot more general than that, but this is
		 * all we want to allow in the absence of a pg_cast entry.) It would
		 * probably be better to insist on explicit casts in both directions,
		 * but this is a compromise to preserve something of the pre-8.3
		 * behavior that many types had implicit (yipes!) casts to text.
		 */
		if (result == CONVERSION_TYPE_NONE)
		{
			if (TypeCategory(targetTypeId) == TYPCATEGORY_STRING)
				result = CONVERSION_TYPE_VIA_IO;
			else if (TypeCategory(sourceTypeId) == TYPCATEGORY_STRING)
				result = CONVERSION_TYPE_VIA_IO;
		}
	}

	return result;
}

static ConversionType
find_typmod_conversion_function(Oid typeId, Oid *funcId)
{
	ConversionType result;
	HeapTuple	tuple;

	*funcId = InvalidOid;
	result = CONVERSION_TYPE_FUNC;

	/* Look in pg_cast */
	tuple = SearchSysCache2(CASTSOURCETARGET,
							ObjectIdGetDatum(typeId),
							ObjectIdGetDatum(typeId));

	if (HeapTupleIsValid(tuple))
	{
		Form_pg_cast castForm = (Form_pg_cast) GETSTRUCT(tuple);

		*funcId = castForm->castfunc;
		ReleaseSysCache(tuple);

		if (!OidIsValid(*funcId))
			result = CONVERSION_TYPE_NONE;
	}
	else
	{
		result = CONVERSION_TYPE_NONE;
	}

	return result;
}

static Datum
convert_from_function(Datum value, int32 typmod, Oid funcId, bool *is_failed)
{
	Datum res = 0;

	// ErrorSaveContext escontext = {T_ErrorSaveContext, false};

#ifdef USE_PG_TRY_CATCH
	PG_TRY();
	{
#endif

	res = OidFunctionCall3(funcId, value, typmod, true);

	// if (escontext.error_occurred) {
	// 	*is_failed = true;
	// }

#ifdef USE_PG_TRY_CATCH
	}
	PG_CATCH();
	{
		*is_failed = true;
		FlushErrorState();
	}
	PG_END_TRY();
#endif


	return res;
}


static Datum
convert_via_io(Datum value, Oid sourceTypeId, 
			   Oid targetTypeId, int32 targetTypMod, 
			   bool *is_failed)
{
	FmgrInfo outfunc;

	Oid infuncId = InvalidOid;
	Oid outfuncId = InvalidOid;

	bool outtypisvarlena = false;
	Oid intypioparam = InvalidOid;

	/* Perhaps the types are domains; if so, look at their base types */
	if (OidIsValid(sourceTypeId))
		sourceTypeId = getBaseType(sourceTypeId);
	if (OidIsValid(targetTypeId))
		targetTypeId = getBaseType(targetTypeId);

	/* lookup the input type's output function */
	getTypeOutputInfo(sourceTypeId, &outfuncId, &outtypisvarlena);
	fmgr_info(outfuncId, &outfunc);

	getTypeInputInfo(targetTypeId, &infuncId, &intypioparam);

	Datum res = 0;
	char* string;
	bool pushed;

	// ErrorSaveContext escontext = {T_ErrorSaveContext, false};

#ifdef USE_PG_TRY_CATCH
	PG_TRY();
	{
#endif

	// value cannot be null
	string = OutputFunctionCall(&outfunc, value);

	pushed = SPI_push_conditional();
	res = OidFunctionCall3(infuncId,
						   CStringGetDatum(string),
						   ObjectIdGetDatum(intypioparam),
						   Int32GetDatum(-1));
	SPI_pop_conditional(pushed);

	// if (escontext.error_occurred) {
	// 	*is_failed = true;
	// }

#ifdef USE_PG_TRY_CATCH
	}
	PG_CATCH();
	{
		*is_failed = true;
		FlushErrorState();
	}
	PG_END_TRY();
#endif

	return res;
}


/*
 * Get the actual typmod int32 of a specific function argument (counting from 0),
 * but working from the calling expression tree instead of FmgrInfo
 *
 * Returns -1 if information is not available
 */
static int32
get_call_expr_argtypmod(Node *expr, int argnum)
{
	List	   *args;
	int32		argtypmod;

	if (expr == NULL)
		return -1;

	if (IsA(expr, FuncExpr))
		args = ((FuncExpr *) expr)->args;
	else if (IsA(expr, OpExpr))
		args = ((OpExpr *) expr)->args;
	else if (IsA(expr, DistinctExpr))
		args = ((DistinctExpr *) expr)->args;
	else if (IsA(expr, ScalarArrayOpExpr))
		args = ((ScalarArrayOpExpr *) expr)->args;
	else if (IsA(expr, ArrayCoerceExpr))
		args = list_make1(((ArrayCoerceExpr *) expr)->arg);
	else if (IsA(expr, NullIfExpr))
		args = ((NullIfExpr *) expr)->args;
	else if (IsA(expr, WindowFunc))
		args = ((WindowFunc *) expr)->args;
	else
		return -1;

	if (argnum < 0 || argnum >= list_length(args))
		return -1;

	argtypmod = exprTypmod((Node *) list_nth(args, argnum));

	return argtypmod;
}

/*
 * Get the actual typemod of a specific function argument (counting from 0)
 *
 * Returns -1 if information is not available
 */
static Oid
get_fn_expr_argtypmod(FmgrInfo *flinfo, int argnum)
{
	/*
	 * can't return anything useful if we have no FmgrInfo or if its fn_expr
	 * node has not been initialized
	 */
	if (!flinfo || !flinfo->fn_expr)
		return -1;

	return get_call_expr_argtypmod(flinfo->fn_expr, argnum);
}


static Datum
convert(Datum value, ConversionType conversion_type, 
		Oid funcId, Oid sourceTypeId, 
		Oid targetTypeId, int32 targetTypMod, 
		bool *is_failed)
{
	
	switch (conversion_type)
	{
	case CONVERSION_TYPE_RELABEL:
		return value;

	case CONVERSION_TYPE_FUNC:
		return convert_from_function(value, targetTypMod, funcId, is_failed);

	case CONVERSION_TYPE_VIA_IO:
		return convert_via_io(value, sourceTypeId, targetTypeId, targetTypMod, is_failed);

	case CONVERSION_TYPE_ARRAY:
		elog(ERROR, "no support for ARRAY CONVERSION");
		*is_failed = true;
		break;

	case CONVERSION_TYPE_NONE:
		elog(ERROR, "no cast wat found");
		*is_failed = true;
		return 0;
	
	default:
		/// TODO RAISE ERROR
		elog(ERROR, "unrecognized conversion method: %d",
						 (int) conversion_type);
		break;
	}

	return 0;
}

static Datum
convert_type_typmod(Datum value, int32 sourceTypMod, 
					Oid targetTypeId, int32 targetTypMod, 
					bool *is_failed)
{
	if (targetTypMod < 0 || targetTypMod == sourceTypMod)
		return value;

	Oid funcId = InvalidOid;
	ConversionType conversion_type = find_typmod_conversion_function(targetTypeId, &funcId);
	
	return convert(value, conversion_type, funcId, InvalidOid, targetTypeId, targetTypMod, is_failed);
}


Datum
try_convert(PG_FUNCTION_ARGS)
{
	if (fcinfo->args[0].isnull) {
		PG_RETURN_NULL();
	}

	Oid sourceTypeId = get_fn_expr_argtype(fcinfo->flinfo, 0);
	int32 sourceTypMod = get_fn_expr_argtypmod(fcinfo->flinfo, 0);

	Oid targetTypeId = get_fn_expr_argtype(fcinfo->flinfo, 1);
	int32 targetTypMod = get_fn_expr_argtypmod(fcinfo->flinfo, 1);

	int32 baseTypMod = targetTypMod;
	Oid baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypMod);

	if (targetTypeId != baseTypeId) {
		elog(ERROR, "no support for DOMAIN CONVERSION");
	}

	Oid funcId;

	ConversionType conversion_type = find_conversion_way(targetTypeId, sourceTypeId, &funcId);

	Datum value = fcinfo->args[0].value;

	Datum res = value;
	Oid resTypeId = sourceTypeId;
	int32 resTypMod = sourceTypMod;

	bool is_failed = false;

	if (conversion_type != CONVERSION_TYPE_RELABEL) {

		res = convert(value, conversion_type, funcId, sourceTypeId, baseTypeId, baseTypMod, &is_failed);

		if (is_failed) {
			fcinfo->isnull = fcinfo->args[1].isnull;
			res = fcinfo->args[1].value;
		}

		resTypeId = baseTypeId;
		resTypMod = -1;

		// if (targetTypeId != baseTypeId) {
		// COERCE_DOMAIN();
		// }
	} else {

		res = value;

		// if (targetTypeId != baseTypeId) {
		// COERCE_DOMAIN();
		// }
	}

	res = convert_type_typmod(res, -1, targetTypeId, targetTypMod, &is_failed);

	if (is_failed) {
		fcinfo->isnull = fcinfo->args[1].isnull;
		res = fcinfo->args[1].value;
	}
	
	return res;
}
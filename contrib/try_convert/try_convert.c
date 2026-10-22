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
 * try_convert(source_value, default_value) casts source_value to the type of
 * default_value.  Whenever the cast fails because of the data being converted,
 * default_value is returned instead of an error being raised.
 *
 * The conversion to use is looked up the same way the parser does it in
 * coerce_type(), see src/backend/parser/parse_coerce.c.
 *
 * IDENTIFICATION
 *		  contrib/try_convert/try_convert.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_cast.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_coerce.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(try_convert);

/*
 * How a value has to be converted; a subset of the CoercionPathType values
 * used by the parser.
 */
typedef enum ConversionType
{
	CONVERSION_TYPE_FUNC,
	CONVERSION_TYPE_RELABEL,
	CONVERSION_TYPE_VIA_IO,
	CONVERSION_TYPE_ARRAY,
	CONVERSION_TYPE_NONE
} ConversionType;

static ConversionType find_conversion_way(Oid targetTypeId, Oid sourceTypeId,
										  Oid *funcId);
static ConversionType find_typmod_conversion_function(Oid typeId, Oid *funcId);
static void report_conversion_error(MemoryContext oldcontext, bool *is_failed);
static Datum convert_from_function(Datum value, int32 typmod, Oid funcId,
								   bool *is_failed);
static Datum convert_via_io(Datum value, Oid sourceTypeId, Oid targetTypeId,
							bool *is_failed);
static int32 get_call_expr_argtypmod(Node *expr, int argnum);
static int32 get_fn_expr_argtypmod(FmgrInfo *flinfo, int argnum);
static Datum convert(Datum value, ConversionType conversion_type, Oid funcId,
					 Oid sourceTypeId, Oid targetTypeId, int32 targetTypMod,
					 bool *is_failed);
static Datum convert_type_typmod(Datum value, int32 sourceTypMod,
								 Oid targetTypeId, int32 targetTypMod,
								 bool *is_failed);


/*
 * Determine how to convert sourceTypeId to targetTypeId, mirroring
 * find_coercion_pathway() without the coercion context: try_convert() always
 * performs an explicit cast.
 *
 * Returns CONVERSION_TYPE_NONE if no conversion is possible.  *funcId is set
 * to the conversion function when one is needed, to InvalidOid otherwise.
 */
static ConversionType
find_conversion_way(Oid targetTypeId, Oid sourceTypeId, Oid *funcId)
{
	ConversionType result = CONVERSION_TYPE_NONE;
	HeapTuple	tuple;

	*funcId = InvalidOid;

	/*
	 * Both types have to be known.  An invalid type OID means the caller could
	 * not resolve the argument type, and the lookups below -- TypeCategory()
	 * in particular -- do not accept one.
	 */
	if (!OidIsValid(sourceTypeId) || !OidIsValid(targetTypeId))
		return CONVERSION_TYPE_NONE;

	/* Perhaps the types are domains; if so, look at their base types */
	sourceTypeId = getBaseType(sourceTypeId);
	targetTypeId = getBaseType(targetTypeId);

	/* Domains are always coercible to and from their base type */
	if (sourceTypeId == targetTypeId)
		return CONVERSION_TYPE_RELABEL;

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
				Oid			elemfuncid;

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

/*
 * Look up the length coercion function of a type, that is the cast from the
 * type to itself, mirroring find_typmod_coercion_function().
 *
 * Returns CONVERSION_TYPE_NONE when the type has no length coercion function,
 * in which case the value has to be left alone.
 */
static ConversionType
find_typmod_conversion_function(Oid typeId, Oid *funcId)
{
	ConversionType result = CONVERSION_TYPE_NONE;
	HeapTuple	tuple;

	*funcId = InvalidOid;

	/* Look in pg_cast */
	tuple = SearchSysCache2(CASTSOURCETARGET,
							ObjectIdGetDatum(typeId),
							ObjectIdGetDatum(typeId));

	if (HeapTupleIsValid(tuple))
	{
		Form_pg_cast castForm = (Form_pg_cast) GETSTRUCT(tuple);

		*funcId = castForm->castfunc;
		ReleaseSysCache(tuple);

		/*
		 * A binary-coercible self-cast carries no function, so there is
		 * nothing we could call to apply the typmod.
		 */
		if (OidIsValid(*funcId))
			result = CONVERSION_TYPE_FUNC;
	}

	return result;
}

/*
 * Common tail of the PG_CATCH() blocks below.
 *
 * The conversion functions called by this module report a failure the only way
 * they can, by throwing an error, so the only way to keep going is to catch it.
 * Ideally we would use the "soft" error handling infrastructure added by
 * PostgreSQL 17 (commit ccff2d20ed) instead, which lets an input function
 * report a conversion failure without throwing; that requires converting the
 * datatype input functions first, so until then we trap the error here.
 *
 * Trapping an error outside of a subtransaction is only safe as long as the
 * called function leaves no global state behind, which holds for the cast and
 * type input/output functions we call.  Errors that do not come from the data
 * being converted must not be swallowed, so query cancellation and assertion
 * failures are re-thrown, following what plpgsql does for "EXCEPTION WHEN
 * others".
 */
static void
report_conversion_error(MemoryContext oldcontext, bool *is_failed)
{
	int			sqlerrcode = geterrcode();

	if (sqlerrcode == ERRCODE_QUERY_CANCELED ||
		sqlerrcode == ERRCODE_ASSERT_FAILURE)
		PG_RE_THROW();

	MemoryContextSwitchTo(oldcontext);
	FlushErrorState();

	*is_failed = true;
}

/*
 * Convert a value by calling the cast function funcId.
 */
static Datum
convert_from_function(Datum value, int32 typmod, Oid funcId, bool *is_failed)
{
	MemoryContext oldcontext = CurrentMemoryContext;
	volatile Datum res = (Datum) 0;

	PG_TRY();
	{
		/*
		 * Cast functions take either one argument or three, the extra ones
		 * being the target typmod and the explicit-cast flag.  Passing three
		 * arguments to a one-argument function is harmless, the callee simply
		 * ignores them.
		 */
		res = OidFunctionCall3(funcId,
							   value,
							   Int32GetDatum(typmod),
							   BoolGetDatum(true));
	}
	PG_CATCH();
	{
		report_conversion_error(oldcontext, is_failed);
	}
	PG_END_TRY();

	return res;
}

/*
 * Convert a value by running it through the output function of the source type
 * and the input function of the target type.
 *
 * The typmod is not applied here, convert_type_typmod() takes care of it.
 */
static Datum
convert_via_io(Datum value, Oid sourceTypeId, Oid targetTypeId,
			   bool *is_failed)
{
	FmgrInfo	outfunc;
	Oid			infuncId = InvalidOid;
	Oid			outfuncId = InvalidOid;
	Oid			intypioparam = InvalidOid;
	bool		outtypisvarlena = false;
	MemoryContext oldcontext = CurrentMemoryContext;
	volatile Datum res = (Datum) 0;

	/* Perhaps the types are domains; if so, look at their base types */
	sourceTypeId = getBaseType(sourceTypeId);
	targetTypeId = getBaseType(targetTypeId);

	getTypeOutputInfo(sourceTypeId, &outfuncId, &outtypisvarlena);
	fmgr_info(outfuncId, &outfunc);

	getTypeInputInfo(targetTypeId, &infuncId, &intypioparam);

	PG_TRY();
	{
		char	   *string;

		/* the caller has already rejected a NULL input value */
		string = OutputFunctionCall(&outfunc, value);

		res = OidFunctionCall3(infuncId,
							   CStringGetDatum(string),
							   ObjectIdGetDatum(intypioparam),
							   Int32GetDatum(-1));

		pfree(string);
	}
	PG_CATCH();
	{
		report_conversion_error(oldcontext, is_failed);
	}
	PG_END_TRY();

	return res;
}

/*
 * Get the actual typmod of a specific function argument (counting from 0),
 * but working from the calling expression tree instead of FmgrInfo.
 *
 * Returns -1 if information is not available.
 */
static int32
get_call_expr_argtypmod(Node *expr, int argnum)
{
	List	   *args;

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

	return exprTypmod((Node *) list_nth(args, argnum));
}

/*
 * Get the actual typmod of a specific function argument (counting from 0).
 *
 * Returns -1 if information is not available.
 */
static int32
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

/*
 * Apply the conversion found by find_conversion_way() or
 * find_typmod_conversion_function().
 *
 * A conversion that cannot be performed at all is a query error and is
 * reported as such; only failures caused by the data being converted are
 * reported through *is_failed.
 */
static Datum
convert(Datum value, ConversionType conversion_type, Oid funcId,
		Oid sourceTypeId, Oid targetTypeId, int32 targetTypMod,
		bool *is_failed)
{
	switch (conversion_type)
	{
		case CONVERSION_TYPE_RELABEL:
			return value;

		case CONVERSION_TYPE_FUNC:
			return convert_from_function(value, targetTypMod, funcId,
										 is_failed);

		case CONVERSION_TYPE_VIA_IO:
			return convert_via_io(value, sourceTypeId, targetTypeId,
								  is_failed);

		case CONVERSION_TYPE_ARRAY:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("try_convert() does not support casts between array types"),
					 errdetail("Cannot cast type %s to %s.",
							   format_type_be(sourceTypeId),
							   format_type_be(targetTypeId))));
			break;

		case CONVERSION_TYPE_NONE:
			ereport(ERROR,
					(errcode(ERRCODE_CANNOT_COERCE),
					 errmsg("cannot cast type %s to %s",
							format_type_be(sourceTypeId),
							format_type_be(targetTypeId))));
			break;
	}

	elog(ERROR, "unrecognized conversion method: %d", (int) conversion_type);
	return (Datum) 0;			/* keep compiler quiet */
}

/*
 * Coerce a value of targetTypeId to targetTypMod.
 */
static Datum
convert_type_typmod(Datum value, int32 sourceTypMod, Oid targetTypeId,
					int32 targetTypMod, bool *is_failed)
{
	ConversionType conversion_type;
	Oid			funcId;

	if (targetTypMod < 0 || targetTypMod == sourceTypMod)
		return value;

	conversion_type = find_typmod_conversion_function(targetTypeId, &funcId);

	/*
	 * If the target type has no length coercion function, just leave the value
	 * alone, the same way coerce_type_typmod() does.
	 */
	if (conversion_type == CONVERSION_TYPE_NONE)
		return value;

	return convert(value, conversion_type, funcId, targetTypeId, targetTypeId,
				   targetTypMod, is_failed);
}

/*
 * try_convert(source_value, default_value) -> converted value or default_value
 *
 * The target type is taken from the second argument, which is also the value
 * returned when the conversion fails.
 */
Datum
try_convert(PG_FUNCTION_ARGS)
{
	Oid			sourceTypeId;
	int32		sourceTypMod;
	Oid			targetTypeId;
	int32		targetTypMod;
	Oid			baseTypeId;
	int32		baseTypMod;
	Oid			funcId;
	ConversionType conversion_type;
	Datum		value;
	Datum		res;
	int32		resTypMod;
	bool		is_failed = false;

	/* A NULL input converts to NULL, whatever the default value is */
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	sourceTypeId = get_fn_expr_argtype(fcinfo->flinfo, 0);
	sourceTypMod = get_fn_expr_argtypmod(fcinfo->flinfo, 0);

	targetTypeId = get_fn_expr_argtype(fcinfo->flinfo, 1);
	targetTypMod = get_fn_expr_argtypmod(fcinfo->flinfo, 1);

	if (!OidIsValid(sourceTypeId) || !OidIsValid(targetTypeId))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not determine the argument types of try_convert()")));

	baseTypMod = targetTypMod;
	baseTypeId = getBaseTypeAndTypmod(targetTypeId, &baseTypMod);

	if (targetTypeId != baseTypeId)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("try_convert() does not support casts to domain types"),
				 errdetail("Cannot cast type %s to %s.",
						   format_type_be(sourceTypeId),
						   format_type_be(targetTypeId))));

	value = PG_GETARG_DATUM(0);

	conversion_type = find_conversion_way(targetTypeId, sourceTypeId, &funcId);

	if (conversion_type == CONVERSION_TYPE_RELABEL)
	{
		res = value;
		resTypMod = sourceTypMod;
	}
	else
	{
		res = convert(value, conversion_type, funcId, sourceTypeId, baseTypeId,
					  baseTypMod, &is_failed);
		resTypMod = -1;
	}

	if (!is_failed)
		res = convert_type_typmod(res, resTypMod, targetTypeId, targetTypMod,
								  &is_failed);

	if (is_failed)
	{
		/* the value could not be converted, fall back to the default one */
		fcinfo->isnull = PG_ARGISNULL(1);
		return PG_GETARG_DATUM(1);
	}

	return res;
}

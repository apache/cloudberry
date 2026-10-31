#include "postgres.h"
#include "funcapi.h"
#include "dbms_assert.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/syscache.h"
#include "catalog/namespace.h"
#include "catalog/pg_namespace_d.h"

#include "orafce.h"
#include "builtins.h"
#include "utils/regproc.h"

PG_FUNCTION_INFO_V1(dbms_assert_enquote_literal);
PG_FUNCTION_INFO_V1(dbms_assert_enquote_name);
PG_FUNCTION_INFO_V1(dbms_assert_noop);
PG_FUNCTION_INFO_V1(dbms_assert_qualified_sql_name);
PG_FUNCTION_INFO_V1(dbms_assert_schema_name);
PG_FUNCTION_INFO_V1(dbms_assert_simple_sql_name);
PG_FUNCTION_INFO_V1(dbms_assert_object_name);
PG_FUNCTION_INFO_V1(dbms_alert_defered_signal);


#define CUSTOM_EXCEPTION(code, msg) \
	ereport(ERROR, \
		(errcode(ERRCODE_ORA_PACKAGES_##code), \
		 errmsg(msg)))

#define INVALID_SCHEMA_NAME_EXCEPTION()	\
	CUSTOM_EXCEPTION(INVALID_SCHEMA_NAME, "invalid schema name")

#define INVALID_OBJECT_NAME_EXCEPTION() \
	CUSTOM_EXCEPTION(INVALID_OBJECT_NAME, "invalid object name")

#define ISNOT_SIMPLE_SQL_NAME_EXCEPTION() \
	CUSTOM_EXCEPTION(ISNOT_SIMPLE_SQL_NAME, "string is not simple SQL name")

#define ISNOT_QUALIFIED_SQL_NAME_EXCEPTION() \
	CUSTOM_EXCEPTION(ISNOT_QUALIFIED_SQL_NAME, "string is not qualified SQL name")

#define EMPTY_STR(str)		(VARSIZE_ANY_EXHDR(str) == 0)

static bool check_sql_name(char *cp, int len);
static bool ParseIdentifierString(char *str, int len);


/*
 * Is character a valid identifier start?
 * Must match scan.l's {ident_start} character class.
 */
static bool
orafce_is_ident_start(unsigned char c)
{
	/* Underscores and ASCII letters are OK */
	if (c == '_')
		return true;
	if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'))
		return true;
	/* Any high-bit-set character is OK (might be part of a multibyte char) */
	if (IS_HIGHBIT_SET(c))
		return true;
	return false;
}

/*
 * Is character a valid identifier continuation?
 * Must match scan.l's {ident_cont} character class.
 */
static bool
orafce_is_ident_cont(unsigned char c)
{
	/* Can be digit or dollar sign ... */
	if ((c >= '0' && c <= '9') || c == '$')
		return true;
	/* ... or an identifier start character */
	return orafce_is_ident_start(c);
}

/*
 * Procedure ParseIdentifierString is based on SplitIdentifierString
 * from varlena.c. We need different behave of quote symbol evaluation,
 * but it is modified for work with size specified string.
 */
static bool
ParseIdentifierString(char *str, int len)
{
	char	   *ptr = str;
	char	   *endp = str + len;
	bool		result = false;

	/* skip leading whitespace */
	while (ptr < endp && isspace((unsigned char) *ptr))
		ptr++;

	/* emoty string are not allowed */
	if (ptr == endp)
		return false;

	/* At the top of the loop, we are at start of a new identifier. */
	while (ptr < endp)
	{
		/* there is one nonzero nonspace char */
		result = true;

		if (*ptr == '\"')
		{
			ptr++;

			/* Quoted name --- collapse quote-quote pairs, no downcasing */
			for (;;)
			{
				if (ptr < endp)
				{
					ptr = memchr(ptr, '\"', endp - ptr);
					/* mismatched quotes */
					if (!ptr)
						return false;

					if (ptr + 1 < endp && ptr[1] == '\"')
					{
						ptr += 2;
						continue;
					}

					ptr++;
					break;
				}
				else
					return false;
			}
		}
		else
		{
			/* Unquoted name --- extends to separator or whitespace */
			if (orafce_is_ident_start(*ptr))
			{
				ptr++;

				while (ptr < endp && *ptr && orafce_is_ident_cont(*ptr))
					ptr++;
			}
			else
				return false;
		}

		/* skip trailing whitespace */
		while (ptr < endp && isspace((unsigned char) *ptr))
			ptr++;

		if (ptr < endp)
		{
			if (*ptr == '.')
			{
				/* we need to find another identifier */
				result = false;

				ptr++;

				/* skip leading whitespace for next */
				while (ptr < endp && isspace((unsigned char) *ptr))
					ptr++;
			}
			else
				/* invalid syntax */
				return false;
		}
	}

	return result;
}

/****************************************************************
 * DBMS_ASSERT.ENQUOTE_LITERAL
 *
 * Syntax:
 *   FUNCTION ENQUOTE_LITERAL(str varchar) RETURNS varchar;
 *
 * Purpouse:
 *   Add leading and trailing quotes, verify that all single quotes
 *   are paired with adjacent single quotes.
 *
 ****************************************************************/

Datum
dbms_assert_enquote_literal(PG_FUNCTION_ARGS)
{
	PG_RETURN_DATUM(DirectFunctionCall1(quote_literal, PG_GETARG_DATUM(0)));
}


/****************************************************************
 * DBMS_ASSERT.ENQUOTE_NAME
 *
 * Syntax:
 *   FUNCTION ENQUOTE_NAME(str varchar) RETURNS varchar;
 *   FUNCTION ENQUOTE_NAME(str varchar, loweralize boolean := true)
 *      RETURNS varchar;
 * Purpouse:
 *   Enclose name in double quotes.
 * Atention!:
 *   On Oracle is second parameter capitalize!
 *
 ****************************************************************/

Datum
dbms_assert_enquote_name(PG_FUNCTION_ARGS)
{
	Datum		name = PG_GETARG_DATUM(0);
	bool		loweralize = PG_GETARG_BOOL(1);
	Oid			collation = PG_GET_COLLATION();

	name = DirectFunctionCall1(quote_ident, name);

	if (loweralize)
		name = DirectFunctionCall1Coll(lower, collation, name);

	PG_RETURN_DATUM(name);
}


/****************************************************************
 * DBMS_ASSERT.NOOP
 *
 * Syntax:
 *   FUNCTION NOOP(str varchar) RETURNS varchar;
 *
 * Purpouse:
 *   Returns value without any checking.
 *
 ****************************************************************/

Datum
dbms_assert_noop(PG_FUNCTION_ARGS)
{
	text	   *str = PG_GETARG_TEXT_PP(0);

	PG_RETURN_TEXT_P(TextPCopy(str));
}


/****************************************************************
 * DBMS_ASSERT.QUALIFIED_SQL_NAME
 *
 * Syntax:
 *   FUNCTION QUALIFIED_SQL_NAME(str varchar) RETURNS varchar;
 *
 * Purpouse:
 *   This function verifies that the input string is qualified SQL
 *   name.
 * Exception: 44004 string is not a qualified SQL name
 *
 ****************************************************************/

Datum
dbms_assert_qualified_sql_name(PG_FUNCTION_ARGS)
{
	text	   *qname;
	int			len;
	char	   *str;

	if (PG_ARGISNULL(0))
		ISNOT_QUALIFIED_SQL_NAME_EXCEPTION();

	qname = PG_GETARG_TEXT_PP(0);

	str = VARDATA_ANY(qname);
	len = VARSIZE_ANY_EXHDR(qname);
	if (len == 0)
		ISNOT_QUALIFIED_SQL_NAME_EXCEPTION();

	if (!ParseIdentifierString(str, len))
		ISNOT_QUALIFIED_SQL_NAME_EXCEPTION();

	PG_RETURN_TEXT_P(qname);
}


/****************************************************************
 * DBMS_ASSERT.SCHEMA_NAME
 *
 * Syntax:
 *   FUNCTION SCHEMA_NAME(str varchar) RETURNS varchar;
 *
 * Purpouse:
 *   Function verifies that input string is an existing schema
 *   name.
 * Exception: 44001 Invalid schema name
 *
 ****************************************************************/

Datum
dbms_assert_schema_name(PG_FUNCTION_ARGS)
{
	Oid			namespaceId;
	AclResult	aclresult;
	text	   *sname;
	char	   *nspname;
	List	   *names;

	if (PG_ARGISNULL(0))
		INVALID_SCHEMA_NAME_EXCEPTION();

	sname = PG_GETARG_TEXT_PP(0);
	if (EMPTY_STR(sname))
		INVALID_SCHEMA_NAME_EXCEPTION();

	nspname = text_to_cstring(sname);

#if PG_VERSION_NUM >= 160000

	names = stringToQualifiedNameList(nspname, NULL);

#else

	names = stringToQualifiedNameList(nspname);

#endif

	if (list_length(names) != 1)
		INVALID_SCHEMA_NAME_EXCEPTION();

	namespaceId = GetSysCacheOid(NAMESPACENAME, Anum_pg_namespace_oid,
								 CStringGetDatum(strVal(linitial(names))),
								 0, 0, 0);

	if (!OidIsValid(namespaceId))
		INVALID_SCHEMA_NAME_EXCEPTION();

#if PG_VERSION_NUM >= 160000

	aclresult = object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(),
								ACL_USAGE);

#else

	aclresult = object_aclcheck(NamespaceRelationId, namespaceId, GetUserId(), ACL_USAGE);

#endif

	if (aclresult != ACLCHECK_OK)
		INVALID_SCHEMA_NAME_EXCEPTION();

	PG_RETURN_TEXT_P(sname);
}


/****************************************************************
 * DBMS_ASSERT.SIMPLE_SQL_NAME
 *
 * Syntax:
 *   FUNCTION SIMPLE_SQL_NAME(str varchar) RETURNS varchar;
 *
 * Purpouse:
 *   This function verifies that the input string is simple SQL
 *   name.
 * Exception: 44003 String is not a simple SQL name
 *
 ****************************************************************/

static bool
check_sql_name(char *cp, int len)
{
	if (*cp == '"')
	{
		char	   *last = cp + len - 1;

		/* don't allow empty identifier */
		if (len < 3)
			return false;

		/* last char should be double quote */
		if (*last != '"')
			return false;

		cp += 1;

		while (*cp && cp < last)
		{
			if (*cp++ == '"')
			{
				if (cp < last)
				{
					if (*cp++ != '"')
						return false;
				}
				else
					return false;
			}
		}

		return true;
	}
	else
	{
		if (orafce_is_ident_start(*cp))
		{
			char	   *last = cp + len;

			cp += 1;

			while (cp < last)
			{
				if (!orafce_is_ident_cont(*cp++))
					return false;
			}
		}
		else
			return false;
	}

	return true;
}

Datum
dbms_assert_simple_sql_name(PG_FUNCTION_ARGS)
{
	text	   *sname;
	int			len;
	char	   *cp;

	if (PG_ARGISNULL(0))
		ISNOT_SIMPLE_SQL_NAME_EXCEPTION();

	sname = PG_GETARG_TEXT_PP(0);
	if (EMPTY_STR(sname))
		ISNOT_SIMPLE_SQL_NAME_EXCEPTION();

	len = VARSIZE_ANY_EXHDR(sname);
	cp = VARDATA_ANY(sname);

	if (!check_sql_name(cp, len))
		ISNOT_SIMPLE_SQL_NAME_EXCEPTION();

	PG_RETURN_TEXT_P(sname);
}


/****************************************************************
 * DBMS_ASSERT.OBJECT_NAME
 *
 * Syntax:
 *   FUNCTION OBJECT_NAME(str varchar) RETURNS varchar;
 *
 * Purpouse:
 *   Verifies that input string is qualified SQL identifier of
 *   an existing SQL object.
 * Exception: 44002 Invalid object name
 *
 ****************************************************************/

Datum
dbms_assert_object_name(PG_FUNCTION_ARGS)
{
	List	   *names;
	text	   *str;
	char	   *object_name;
	Oid			classId;

	if (PG_ARGISNULL(0))
		INVALID_OBJECT_NAME_EXCEPTION();

	str = PG_GETARG_TEXT_PP(0);
	if (EMPTY_STR(str))
		INVALID_OBJECT_NAME_EXCEPTION();

	object_name = text_to_cstring(str);

#if PG_VERSION_NUM >= 160000

	names = stringToQualifiedNameList(object_name, NULL);

#else

	names = stringToQualifiedNameList(object_name);

#endif

	classId = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, true);
	if (!OidIsValid(classId))
		INVALID_OBJECT_NAME_EXCEPTION();

	PG_RETURN_TEXT_P(str);
}

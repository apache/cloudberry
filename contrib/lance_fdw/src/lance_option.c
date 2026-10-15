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
 * lance_option.c
 *	  Option parsing and validation for lance_fdw.
 *
 * The table below is the single source of truth for which option is legal on
 * which object; the validator, the error hints and README's option table all
 * come from it.
 *
 * Credentials only ever move from the user mapping into the key/value array
 * handed to lance-c.  They are never written into a plan, an option string on
 * a table, or an error message.
 *
 * Who may read what follows file_fdw and postgres_fdw.  A dataset uri that
 * leaves the server's base_uri, a base_uri on the local file system and the
 * options that choose where to connect are checked when they are set, against
 * the role setting them; a non-superuser reading from an object store must
 * bring credentials of its own, and that is checked whenever a dataset is
 * opened, against the role whose user mapping is used.
 *
 * IDENTIFICATION
 *	  src/lance_option.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdlib.h>

#include "lance_fdw.h"
#include "lance_option.h"

#include "access/reloptions.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_authid.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

typedef struct LanceOptionDef
{
	const char *name;
	Oid			catalog;
	bool		is_secret;		/* never repeat the value in a message */
} LanceOptionDef;

static const LanceOptionDef lance_options[] = {
	/*
	 * mpp_execute is Cloudberry's own knob (FDW/server/table); the extension
	 * script sets it to 'all segments' on the wrapper, and the validator has
	 * to accept it wherever the user may repeat it.
	 */
	{"mpp_execute", ForeignDataWrapperRelationId, false},
	{"mpp_execute", ForeignServerRelationId, false},
	{"mpp_execute", ForeignTableRelationId, false},

	{"base_uri", ForeignServerRelationId, false},
	{"aws_endpoint", ForeignServerRelationId, false},
	{"aws_region", ForeignServerRelationId, false},
	{"allow_http", ForeignServerRelationId, false},
	{"virtual_hosted_style_request", ForeignServerRelationId, false},
	{"timeout", ForeignServerRelationId, false},
	{"connect_timeout", ForeignServerRelationId, false},
	{"client_max_retries", ForeignServerRelationId, false},
	{"client_retry_timeout", ForeignServerRelationId, false},

	{"aws_access_key_id", UserMappingRelationId, true},
	{"aws_secret_access_key", UserMappingRelationId, true},
	{"aws_session_token", UserMappingRelationId, true},
	{"credentials_required", UserMappingRelationId, false},

	{"uri", ForeignTableRelationId, false},
	{"version", ForeignTableRelationId, false},
	{"batch_size", ForeignTableRelationId, false},
	{"batch_size_bytes", ForeignTableRelationId, false},
	{"rows_hint", ForeignTableRelationId, false},

	{"column_name", AttributeRelationId, false},

	{NULL, InvalidOid, false}
};

/* Object-store options forwarded to lance-c, in the order lance documents. */
static const char *const lance_server_storage_options[] = {
	"aws_endpoint",
	"aws_region",
	"allow_http",
	"virtual_hosted_style_request",
	"timeout",
	"connect_timeout",
	"client_max_retries",
	"client_retry_timeout",
	NULL
};

static const char *const lance_user_storage_options[] = {
	"aws_access_key_id",
	"aws_secret_access_key",
	"aws_session_token",
	NULL
};

static const LanceOptionDef *
lance_find_option(const char *name, Oid catalog)
{
	const LanceOptionDef *opt;

	for (opt = lance_options; opt->name != NULL; opt++)
	{
		if (opt->catalog == catalog && strcmp(opt->name, name) == 0)
			return opt;
	}
	return NULL;
}

static char *
lance_option_names_for(Oid catalog)
{
	const LanceOptionDef *opt;
	StringInfoData buf;

	initStringInfo(&buf);
	for (opt = lance_options; opt->name != NULL; opt++)
	{
		if (opt->catalog != catalog)
			continue;
		if (buf.len > 0)
			appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, opt->name);
	}
	return buf.data;
}

static void lance_reject_value(const char *name, const char *value,
							   const char *expected) pg_attribute_noreturn();

static void
lance_reject_value(const char *name, const char *value, const char *expected)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid value for option \"%s\": \"%s\"", name, value),
			 errdetail("%s", expected)));
}

#define LANCE_VERSION_EXPECTED \
	"The version must be a non-negative integer; 0 means the latest version."

static uint64
lance_parse_version(const char *name, const char *value)
{
	char	   *endptr;
	unsigned long long parsed;

	if (value[0] == '\0')
		lance_reject_value(name, value, LANCE_VERSION_EXPECTED);

	/* strtoull happily eats a leading '-'; Lance versions are unsigned. */
	for (endptr = (char *) value; *endptr != '\0'; endptr++)
	{
		if (*endptr < '0' || *endptr > '9')
			lance_reject_value(name, value, LANCE_VERSION_EXPECTED);
	}

	errno = 0;
	parsed = strtoull(value, &endptr, 10);
	if (errno != 0 || *endptr != '\0')
		lance_reject_value(name, value, LANCE_VERSION_EXPECTED);

	return (uint64) parsed;
}

static int64
lance_parse_batch_size(const char *name, const char *value)
{
	char	   *endptr;
	long long	parsed;

	errno = 0;
	parsed = strtoll(value, &endptr, 10);
	if (errno != 0 || endptr == value || *endptr != '\0' || parsed <= 0)
		lance_reject_value(name, value,
						   "The batch size must be a positive integer.");

	return (int64) parsed;
}

/*
 * A byte limit on one output batch.  lance lets this override the row limit,
 * which is why the two may not be set together (lance_validate_options).
 */
static int64
lance_parse_batch_size_bytes(const char *name, const char *value)
{
	char	   *endptr;
	long long	parsed;

	errno = 0;
	parsed = strtoll(value, &endptr, 10);
	if (errno != 0 || endptr == value || *endptr != '\0' || parsed <= 0)
		lance_reject_value(name, value,
						   "The batch size in bytes must be a positive integer.");

	return (int64) parsed;
}

static double
lance_parse_rows_hint(const char *name, const char *value)
{
	char	   *endptr;
	double		parsed;

	errno = 0;
	parsed = strtod(value, &endptr);
	if (errno != 0 || endptr == value || *endptr != '\0' ||
		isnan(parsed) || isinf(parsed) || parsed <= 0.0)
		lance_reject_value(name, value,
						   "The row estimate must be a positive number.");

	return parsed;
}

/*
 * lance reads client_max_retries and client_retry_timeout itself, and quietly
 * keeps its defaults (3 retries, 180 seconds) for a value it cannot parse, so
 * a typo has to be caught here.  timeout and connect_timeout need no such
 * check: object_store refuses a duration it cannot parse when the dataset is
 * opened, naming the value.
 */
static void
lance_parse_count(const char *name, const char *value)
{
	const char *p;

	for (p = value; *p != '\0'; p++)
	{
		if (*p < '0' || *p > '9')
			break;
	}
	if (value[0] == '\0' || *p != '\0' || strlen(value) > 9)
		lance_reject_value(name, value,
						   "The value must be a non-negative integer.");
}

static bool
lance_parse_boolean(const char *name, const char *value)
{
	bool		result;

	if (!parse_bool(value, &result))
		lance_reject_value(name, value,
						   "The value must be a boolean, for example true or false.");
	return result;
}

static void
lance_check_mpp_execute(const char *name, const char *value)
{
	if (pg_strcasecmp(value, "any") == 0 ||
		pg_strcasecmp(value, "coordinator") == 0 ||
		pg_strcasecmp(value, "master") == 0 ||
		pg_strcasecmp(value, "all segments") == 0)
		return;

	lance_reject_value(name, value,
					   "Valid values are any, coordinator and all segments.");
}

/*
 * Validate one option value.  Anything that costs I/O belongs elsewhere: the
 * validator runs inside DDL and must not touch the object store.
 */
static void
lance_validate_value(const LanceOptionDef *opt, DefElem *def)
{
	const char *value = defGetString(def);

	if (opt->is_secret)
	{
		if (value[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("option \"%s\" must not be empty", opt->name)));
		return;					/* never look at, or echo, the value itself */
	}

	if (strcmp(opt->name, "version") == 0)
		(void) lance_parse_version(opt->name, value);
	else if (strcmp(opt->name, "batch_size") == 0)
		(void) lance_parse_batch_size(opt->name, value);
	else if (strcmp(opt->name, "batch_size_bytes") == 0)
		(void) lance_parse_batch_size_bytes(opt->name, value);
	else if (strcmp(opt->name, "rows_hint") == 0)
		(void) lance_parse_rows_hint(opt->name, value);
	else if (strcmp(opt->name, "allow_http") == 0 ||
			 strcmp(opt->name, "virtual_hosted_style_request") == 0 ||
			 strcmp(opt->name, "credentials_required") == 0)
		(void) lance_parse_boolean(opt->name, value);
	else if (strcmp(opt->name, "mpp_execute") == 0)
		lance_check_mpp_execute(opt->name, value);
	else if (strcmp(opt->name, "client_max_retries") == 0 ||
			 strcmp(opt->name, "client_retry_timeout") == 0)
		lance_parse_count(opt->name, value);
	else if (value[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("option \"%s\" must not be empty", opt->name)));
}

/*
 * Privileges
 */
static bool
lance_uri_is_absolute(const char *name)
{
	if (name[0] == '/')
		return true;
	return strstr(name, "://") != NULL;
}

/* Does an absolute uri name the local file system rather than a store? */
static bool
lance_uri_is_local(const char *uri)
{
	if (pg_strncasecmp(uri, "file://", strlen("file://")) == 0)
		return true;
	return strstr(uri, "://") == NULL;
}

/* file_fdw's rule for reading the server's file system. */
static bool
lance_may_read_server_files(void)
{
	return superuser() ||
		has_privs_of_role(GetUserId(), ROLE_PG_READ_SERVER_FILES);
}

/*
 * Could `name`, appended to base_uri, reach outside it?  A ".." component
 * can, and so can one spelled with %2e: the uri parser on the Rust side
 * decodes that to a dot before it resolves the path.  A backslash is a
 * separator in a file:// uri, and %2f and %5c decode to separators.
 */
static bool
lance_uri_may_escape(const char *name)
{
	const char *p = name;

	while (*p != '\0')
	{
		int			dots = 0;
		bool		other = false;

		for (; *p != '\0' && *p != '/'; p++)
		{
			if (*p == '\\')
				return true;
			if (*p == '%')
			{
				if (pg_strncasecmp(p, "%2e", 3) == 0)
				{
					dots++;
					p += 2;
					continue;
				}
				if (pg_strncasecmp(p, "%2f", 3) == 0 ||
					pg_strncasecmp(p, "%5c", 3) == 0)
					return true;
			}
			if (*p == '.')
				dots++;
			else
				other = true;
		}

		if (!other && dots == 2)
			return true;
		if (*p == '/')
			p++;
	}

	return false;
}

void
lance_check_uri_privilege(const char *name)
{
	if (superuser())
		return;

	if (lance_uri_is_absolute(name))
	{
		if (lance_uri_is_local(name) && lance_may_read_server_files())
			return;
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to use an absolute dataset uri"),
				 errdetail("Only superusers, and members of pg_read_server_files for a local path, may name a dataset outside the server's base_uri."),
				 errhint("Give the dataset relative to the server's base_uri.")));
	}

	if (lance_uri_may_escape(name))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to use dataset uri \"%s\"", name),
				 errdetail("Only superusers may use a \"..\" component or a backslash in a relative uri, since it could reach outside the server's base_uri.")));
}

/*
 * The options that decide what a server reaches: a base_uri on this host's
 * file system, and where and how lance-c connects.
 */
static void
lance_check_server_privilege(DefElem *def)
{
	const char *value = defGetString(def);

	if (superuser())
		return;

	if (strcmp(def->defname, "base_uri") == 0 && lance_uri_is_local(value) &&
		!lance_may_read_server_files())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to use a local base_uri"),
				 errdetail("Only superusers and members of pg_read_server_files may read the server's file system.")));

	if (strcmp(def->defname, "aws_endpoint") == 0 ||
		strcmp(def->defname, "allow_http") == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("permission denied to set option \"%s\"", def->defname),
				 errdetail("Only superusers may choose where lance_fdw connects to, and how.")));
}

static bool
lance_credentials_required(List *user_options)
{
	ListCell   *lc;

	foreach(lc, user_options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, "credentials_required") == 0)
			return lance_parse_boolean(def->defname, defGetString(def));
	}
	return true;
}

void
lance_validate_options(List *options_list, Oid catalog)
{
	ListCell   *lc;
	bool		saw_uri = false;
	bool		saw_batch_rows = false;
	bool		saw_batch_bytes = false;

	foreach(lc, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		const LanceOptionDef *opt = lance_find_option(def->defname, catalog);

		if (opt == NULL)
		{
			char	   *valid = lance_option_names_for(catalog);

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 valid[0] != '\0'
					 ? errhint("Valid options in this context are: %s.", valid)
					 : errhint("There are no valid options in this context.")));
		}

		lance_validate_value(opt, def);

		if (catalog == ForeignServerRelationId)
			lance_check_server_privilege(def);
		else if (catalog == UserMappingRelationId &&
				 strcmp(def->defname, "credentials_required") == 0 &&
				 !lance_parse_boolean(def->defname, defGetString(def)) &&
				 !superuser())
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
					 errmsg("permission denied to set option \"credentials_required\" to false"),
					 errdetail("Only superusers may let lance_fdw fall back to the credentials of the host it runs on.")));

		if (strcmp(def->defname, "uri") == 0)
		{
			saw_uri = true;
			lance_check_uri_privilege(defGetString(def));
		}
		else if (strcmp(def->defname, "batch_size") == 0)
			saw_batch_rows = true;
		else if (strcmp(def->defname, "batch_size_bytes") == 0)
			saw_batch_bytes = true;
	}

	/*
	 * lance lets a byte limit take precedence over a row limit, so accepting
	 * both would silently ignore one of them.  Say so instead.
	 */
	if (saw_batch_rows && saw_batch_bytes)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("options \"batch_size\" and \"batch_size_bytes\" cannot be used together"),
				 errdetail("lance lets the byte limit take precedence, so "
						   "\"batch_size\" would have no effect."),
				 errhint("Keep whichever limit you meant.")));

	if (catalog == ForeignTableRelationId && !saw_uri)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("option \"uri\" is required for lance_fdw foreign tables"),
				 errhint("Give the dataset uri, absolute or relative to the "
						 "server's base_uri.")));
}

/*
 * Option lookup helpers
 */
static char *
lance_find_string_option(List *options, const char *name)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, name) == 0)
			return defGetString(def);
	}
	return NULL;
}

char *
lance_server_base_uri(Oid serverid)
{
	ForeignServer *server = GetForeignServer(serverid);

	return lance_find_string_option(server->options, "base_uri");
}

char *
lance_resolve_uri(const char *base_uri, const char *name)
{
	size_t		baselen;

	if (name == NULL || name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("lance_fdw: dataset uri must not be empty")));

	if (lance_uri_is_absolute(name))
		return pstrdup(name);

	if (base_uri == NULL || base_uri[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("lance_fdw: relative uri \"%s\" needs the server option \"base_uri\"",
						name),
				 errhint("Either set base_uri on the server or use an absolute "
						 "uri such as file:///path/to/dataset.lance.")));

	baselen = strlen(base_uri);
	while (baselen > 0 && base_uri[baselen - 1] == '/')
		baselen--;

	return psprintf("%.*s/%s", (int) baselen, base_uri, name);
}

void
lance_get_table_options(Oid foreigntableid, LanceTableOptions *opts)
{
	ForeignTable *table = GetForeignTable(foreigntableid);
	ListCell   *lc;

	memset(opts, 0, sizeof(*opts));
	opts->serverid = table->serverid;

	foreach(lc, table->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		const char *value = defGetString(def);

		if (strcmp(def->defname, "uri") == 0)
			opts->raw_uri = pstrdup(value);
		else if (strcmp(def->defname, "version") == 0)
			opts->version = lance_parse_version(def->defname, value);
		else if (strcmp(def->defname, "batch_size") == 0)
			opts->batch_size = lance_parse_batch_size(def->defname, value);
		else if (strcmp(def->defname, "batch_size_bytes") == 0)
			opts->batch_size_bytes =
				lance_parse_batch_size_bytes(def->defname, value);
		else if (strcmp(def->defname, "rows_hint") == 0)
			opts->rows_hint = lance_parse_rows_hint(def->defname, value);
		/* mpp_execute is Cloudberry's, and is handled by the planner */
	}

	if (opts->raw_uri == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_DYNAMIC_PARAMETER_VALUE_NEEDED),
				 errmsg("option \"uri\" is required for lance_fdw foreign tables")));

	opts->uri = lance_resolve_uri(lance_server_base_uri(table->serverid),
								  opts->raw_uri);
}

char *
lance_get_column_name(Oid foreigntableid, AttrNumber attnum,
					  const char *attname)
{
	List	   *options = GetForeignColumnOptions(foreigntableid, attnum);
	char	   *name = lance_find_string_option(options, "column_name");

	return pstrdup(name != NULL ? name : attname);
}

/*
 * User mapping options for (userid, serverid), falling back to the PUBLIC
 * mapping.  This is the lookup GetUserMapping() does, minus its hard error for
 * servers that need no credentials at all - reading a file:// dataset should
 * not require a user mapping.
 */
static List *
lance_user_mapping_options(Oid userid, Oid serverid)
{
	HeapTuple	tp;
	Datum		datum;
	bool		isnull;
	List	   *options;

	tp = SearchSysCache2(USERMAPPINGUSERSERVER,
						 ObjectIdGetDatum(userid),
						 ObjectIdGetDatum(serverid));
	if (!HeapTupleIsValid(tp))
		tp = SearchSysCache2(USERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid),
							 ObjectIdGetDatum(serverid));
	if (!HeapTupleIsValid(tp))
		return NIL;

	datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp,
							Anum_pg_user_mapping_umoptions, &isnull);
	options = isnull ? NIL : untransformRelOptions(datum);

	ReleaseSysCache(tp);

	return options;
}

static void
lance_append_storage_option(const char **array, int *nkv, const char *key,
							const char *value)
{
	array[*nkv * 2] = key;
	array[*nkv * 2 + 1] = value;
	(*nkv)++;
}

/*
 * postgres_fdw's password_required, for an object store.  Without keys in the
 * user mapping, lance falls back to the host's: the environment, ~/.aws, or
 * the instance's IAM role, whatever the cluster itself may read.  Only s3://
 * takes the mapping's keys at all, so any other scheme is the host's too.
 */
static void
lance_check_credentials(Oid userid, const char *uri, List *user_options)
{
	if (lance_uri_is_local(uri) || superuser_arg(userid) ||
		!lance_credentials_required(user_options))
		return;

	if (pg_strncasecmp(uri, "s3://", strlen("s3://")) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
				 errmsg("lance_fdw: non-superusers can only read local and s3:// datasets"),
				 errdetail("Credentials in a user mapping only reach s3://; any other store would be read with the host's."),
				 errhint("Have a superuser set credentials_required 'false' on the user mapping to allow that.")));

	if (lance_find_string_option(user_options, "aws_access_key_id") == NULL ||
		lance_find_string_option(user_options, "aws_secret_access_key") == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
				 errmsg("lance_fdw: non-superusers must give aws_access_key_id and aws_secret_access_key in the user mapping"),
				 errdetail("Without them lance would use the credentials of the host the cluster runs on."),
				 errhint("Add both to the user mapping, or have a superuser set credentials_required 'false' on it.")));
}

const char **
lance_build_storage_options(Oid serverid, Oid userid, const char *uri)
{
	ForeignServer *server = GetForeignServer(serverid);
	List	   *user_options = lance_user_mapping_options(userid, serverid);
	const char **array;
	int			maxkv;
	int			nkv = 0;
	int			i;

	lance_check_credentials(userid, uri, user_options);

	maxkv = list_length(server->options) + list_length(user_options);
	array = (const char **) palloc0(sizeof(char *) * (maxkv * 2 + 1));

	for (i = 0; lance_server_storage_options[i] != NULL; i++)
	{
		const char *key = lance_server_storage_options[i];
		const char *value = lance_find_string_option(server->options, key);

		if (value == NULL)
			continue;

		/* lance wants the spelling object_store understands */
		if (strcmp(key, "allow_http") == 0 ||
			strcmp(key, "virtual_hosted_style_request") == 0)
			value = lance_parse_boolean(key, value) ? "true" : "false";

		lance_append_storage_option(array, &nkv, key, value);
	}

	for (i = 0; lance_user_storage_options[i] != NULL; i++)
	{
		const char *key = lance_user_storage_options[i];
		const char *value = lance_find_string_option(user_options, key);

		if (value != NULL)
			lance_append_storage_option(array, &nkv, key, value);
	}

	array[nkv * 2] = NULL;

	return array;
}

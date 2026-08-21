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
 * iceberg_catalog_fdw.c
 *	  Option validator for Iceberg catalog foreign servers.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/iceberg_catalog_fdw/iceberg_catalog_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/reloptions.h"
#include "am_iceberg/pg_iceberg_reject.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "common/dl_option_util.h"
#include "common/parser_option.h"
#include "fmgr.h"
#include "iceberg_catalog_fdw/iceberg_catalog_option.h"

PG_FUNCTION_INFO_V1(iceberg_catalog_fdw_validator);

static bool is_catalog_server_option(const char *name);
static bool is_catalog_user_mapping_option(const char *name);
static void check_catalog_server_type(const char *server_type);

/*
 * The server options this wrapper accepts.  A storage protocol is never among
 * them: where the data files live is decided by the volume server.
 */
static bool
is_catalog_server_option(const char *name)
{
	return strcmp(name, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_CATALOG_URL) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_CATALOG_NAME) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_CATALOG_WAREHOUSE_LOCATION_PREFIX) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_REALM) == 0;
}

/*
 * The user mapping options this wrapper accepts: the union over catalog types,
 * because a mapping is validated without reference to the server it belongs to.
 * parse_iceberg_catalog_user_mapping_options() is what narrows them by type.
 */
static bool
is_catalog_user_mapping_option(const char *name)
{
	return strcmp(name, DATALAKE_ICEBERG_CATALOG_USERNAME) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_CATALOG_AUTH_METHOD) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_CATALOG_KRB_SERVICE_PRINCIPAL) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_CATALOG_KRB_CLIENT_PRINCIPAL) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_CATALOG_KRB_CLIENT_KEYTAB) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_CATALOG_CLIENT_ID) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_CATALOG_CLIENT_SECRET) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_CATALOG_SCOPE) == 0;
}

static void
check_catalog_server_type(const char *server_type)
{
	if (pg_strcasecmp(server_type,
					  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HIVE) == 0 ||
		pg_strcasecmp(server_type,
					  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST) == 0 ||
		pg_strcasecmp(server_type,
					  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS) == 0 ||
		pg_strcasecmp(server_type,
					  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_BUILTIN) == 0)
		return;

	/*
	 * Names the vocabulary defines but this module cannot serve yet.  Refusing
	 * them is what keeps a server from being created against a catalog no
	 * statement could subsequently use.
	 */
	if (pg_strcasecmp(server_type,
					  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HADOOP) == 0 ||
		pg_strcasecmp(server_type,
					  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_S3) == 0)
		pg_iceberg_not_supported(psprintf("catalog type \"%s\"", server_type));

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid iceberg catalog type \"%s\"", server_type),
			 errhint("Allowed types are \"%s\", \"%s\" and \"%s\"; \"%s\" is accepted as an alias of \"%s\".",
					 DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HIVE,
					 DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST,
					 DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_BUILTIN,
					 DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS,
					 DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST)));
}

Datum
iceberg_catalog_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *lc;
	const char *server_type;
	const char *url;
	const char *realm;

	/*
	 * CREATE FOREIGN DATA WRAPPER invokes its validator with an empty array.
	 * Permit that bootstrap call, but this FDW has no wrapper-level options.
	 */
	if (catalog == ForeignDataWrapperRelationId && options == NIL)
		PG_RETURN_VOID();

	if (catalog != ForeignServerRelationId &&
		catalog != UserMappingRelationId)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("iceberg_catalog_fdw has no options in this context")));

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		const char *name = def->defname;

		if (catalog == ForeignServerRelationId)
		{
			if (dl_is_credential_option(name))
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("credential option \"%s\" is not allowed on an iceberg catalog server",
								name),
						 errhint("credentials belong in CREATE USER MAPPING ... OPTIONS (...)")));

			if (!is_catalog_server_option(name))
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid iceberg catalog server option \"%s\"",
								name),
						 errhint("Allowed options are \"%s\", \"%s\", \"%s\", \"%s\" and \"%s\".",
								 DATALAKE_ICEBERG_CATALOG_SERVER_TYPE,
								 DATALAKE_ICEBERG_CATALOG_URL,
								 DATALAKE_ICEBERG_CATALOG_NAME,
								 DATALAKE_ICEBERG_CATALOG_WAREHOUSE_LOCATION_PREFIX,
								 DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_REALM)));

			/* Reject an empty value here rather than at first use. */
			if (defGetString(def)[0] == '\0')
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("iceberg catalog server option \"%s\" cannot be empty",
								name)));
		}
		else if (!is_catalog_user_mapping_option(name))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid iceberg catalog user mapping option \"%s\"",
							name)));
	}

	if (catalog == UserMappingRelationId)
		PG_RETURN_VOID();

	/*
	 * Cross-option rules run once the whole list has been seen, so that they do
	 * not depend on the order the options were written in.
	 */
	server_type = get_string_option(options,
									DATALAKE_ICEBERG_CATALOG_SERVER_TYPE);
	url = get_string_option(options, DATALAKE_ICEBERG_CATALOG_URL);
	realm = get_string_option(options,
							  DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_REALM);

	if (server_type == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iceberg catalog server option \"%s\" is required",
						DATALAKE_ICEBERG_CATALOG_SERVER_TYPE)));

	check_catalog_server_type(server_type);

	if (pg_strcasecmp(server_type,
					  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_BUILTIN) == 0)
	{
		if (url != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("iceberg catalog type \"%s\" forbids server option \"%s\"",
							server_type, DATALAKE_ICEBERG_CATALOG_URL)));
	}
	else if (url == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iceberg catalog type \"%s\" requires server option \"%s\"",
						server_type, DATALAKE_ICEBERG_CATALOG_URL)));

	if (realm != NULL &&
		pg_strcasecmp(server_type,
					  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST) != 0 &&
		pg_strcasecmp(server_type,
					  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iceberg catalog server option \"%s\" applies only to catalog type \"%s\"",
						DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_REALM,
						DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST)));

	PG_RETURN_VOID();
}

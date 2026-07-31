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
 * iceberg_volume_fdw.c
 *	  Option validator for Iceberg volume foreign servers.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/iceberg_volume_fdw/iceberg_volume_fdw.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/reloptions.h"
#include "am_iceberg/pg_iceberg_options.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "common/dl_option_util.h"
#include "fmgr.h"
#include "iceberg_volume_fdw/iceberg_volume_option.h"

PG_FUNCTION_INFO_V1(iceberg_volume_fdw_validator);

static bool is_volume_server_option(const char *name);
static bool is_volume_user_mapping_option(const char *name);

static bool
is_volume_server_option(const char *name)
{
	return strcmp(name, DATALAKE_ICEBERG_VOLUME_BASE_PATH) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_VOLUME_ENDPOINT) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_VOLUME_REGION) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_VOLUME_PATH_STYLE_ACCESS) == 0;
}

/*
 * Every credential here is optional, so that ambient storage credentials -- an
 * instance profile, a ticket cache -- remain a valid deployment choice.
 */
static bool
is_volume_user_mapping_option(const char *name)
{
	return strcmp(name, DATALAKE_ICEBERG_VOLUME_USERNAME) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_VOLUME_AWS_ACCESS_KEY_ID) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_VOLUME_AWS_SECRET_ACCESS_KEY) == 0 ||
		strcmp(name, DATALAKE_ICEBERG_VOLUME_AWS_SESSION_TOKEN) == 0;
}

Datum
iceberg_volume_fdw_validator(PG_FUNCTION_ARGS)
{
	List	   *options = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *lc;
	IcebergVolumeServerOptions server_options;
	IcebergForeignVolumeOptions volume_options;
	DatalakeLocation location;
	char	   *parse_detail = NULL;
	DlErrCode	parse_result;

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
				 errmsg("iceberg_volume_fdw has no options in this context")));

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		const char *name = def->defname;

		if (catalog == ForeignServerRelationId)
		{
			if (dl_is_credential_option(name))
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("credential option \"%s\" is not allowed on an iceberg volume server",
								name),
						 errhint("credentials belong in CREATE USER MAPPING ... OPTIONS (...)")));

			if (!is_volume_server_option(name))
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid iceberg volume server option \"%s\"",
								name),
						 errhint("Allowed options are \"%s\", \"%s\", \"%s\" and \"%s\".",
								 DATALAKE_ICEBERG_VOLUME_BASE_PATH,
								 DATALAKE_ICEBERG_VOLUME_ENDPOINT,
								 DATALAKE_ICEBERG_VOLUME_REGION,
								 DATALAKE_ICEBERG_VOLUME_PATH_STYLE_ACCESS)));
		}
		else if (!is_volume_user_mapping_option(name))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid iceberg volume user mapping option \"%s\"",
							name)));
	}

	if (catalog == UserMappingRelationId)
		PG_RETURN_VOID();

	/*
	 * Parse with the same functions the use points parse with, so a server this
	 * validator accepted cannot fail to parse afterwards.  path_style_access is
	 * checked as a side effect: the accessor refuses a non-boolean value.
	 */
	memset(&server_options, 0, sizeof(server_options));
	memset(&volume_options, 0, sizeof(volume_options));
	parse_iceberg_volume_server_options(&server_options, options);
	parse_iceberg_foreign_volume_options(&volume_options, options);

	if (volume_options.base_path == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iceberg volume server option \"%s\" is required",
						DATALAKE_ICEBERG_VOLUME_BASE_PATH)));

	parse_result = pg_iceberg_parse_location(volume_options.base_path,
											 server_options.endpoint,
											 server_options.region,
											 &location, &parse_detail);
	if (parse_result != DL_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid iceberg volume %s \"%s\"",
						DATALAKE_ICEBERG_VOLUME_BASE_PATH,
						volume_options.base_path),
				 errdetail("%s", parse_detail)));

	PG_RETURN_VOID();
}

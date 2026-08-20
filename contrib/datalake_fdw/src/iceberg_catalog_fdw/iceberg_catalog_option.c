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
 * iceberg_catalog_option.c
 *	  Option vocabulary and parsed forms for Iceberg catalog servers.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/iceberg_catalog_fdw/iceberg_catalog_option.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/parser_option.h"
#include "iceberg_catalog_fdw/iceberg_catalog_option.h"
#include "utils/builtins.h"

static void parse_hive_catalog_server_options(IcebergCatalogServerOptions *options,
											  List *server_options);
static void parse_rest_catalog_server_options(IcebergCatalogServerOptions *options,
											  List *server_options);
static void parse_hive_user_mapping_options(HiveUserMappingOptions *options,
											List *user_options);
static void parse_polaris_user_mapping_options(PolarisUserMappingOptions *options,
											   List *user_options);

/*
 * Reference implementation: parseHiveCatalogServerOptions().
 *
 * That version also accepts "hive_metastore_uri" as a second spelling of the
 * same option, for servers created before the key was renamed.  This extension
 * has never been released, so there is nothing to be compatible with and only
 * one spelling is accepted.
 */
static void
parse_hive_catalog_server_options(IcebergCatalogServerOptions *options,
								  List *server_options)
{
	options->hive_metastore_uri =
		get_string_option(server_options, DATALAKE_ICEBERG_CATALOG_URL);
}

/* Reference implementation: parsePolarisCatalogServerOptions() */
static void
parse_rest_catalog_server_options(IcebergCatalogServerOptions *options,
								  List *server_options)
{
	options->polaris_server_url =
		get_string_option(server_options, DATALAKE_ICEBERG_CATALOG_URL);

	/*
	 * Sent as the realm header on every request.  Optional: the metadata engine
	 * applies its own default when unset.
	 */
	options->polaris_server_realm =
		get_string_option(server_options,
						  DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_REALM);
}

void
parse_iceberg_catalog_server_options(IcebergCatalogServerOptions *options,
									 List *server_options)
{
	options->server_type =
		get_string_option(server_options, DATALAKE_ICEBERG_CATALOG_SERVER_TYPE);

	if (options->server_type == NULL)
		return;

	if (pg_strcasecmp(options->server_type,
					  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HIVE) == 0)
		parse_hive_catalog_server_options(options, server_options);
	else if (pg_strcasecmp(options->server_type,
						   DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST) == 0 ||
			 pg_strcasecmp(options->server_type,
						   DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS) == 0)
		parse_rest_catalog_server_options(options, server_options);
}

/* Reference implementation: parseHiveUserMappingOptions() */
static void
parse_hive_user_mapping_options(HiveUserMappingOptions *options,
								List *user_options)
{
	options->username =
		get_string_option(user_options, DATALAKE_ICEBERG_CATALOG_USERNAME);
	options->auth_method =
		get_string_option(user_options, DATALAKE_ICEBERG_CATALOG_AUTH_METHOD);
	options->krb_service_principal =
		get_string_option(user_options,
						  DATALAKE_ICEBERG_CATALOG_KRB_SERVICE_PRINCIPAL);
	options->krb_client_principal =
		get_string_option(user_options,
						  DATALAKE_ICEBERG_CATALOG_KRB_CLIENT_PRINCIPAL);
	options->krb_client_keytab =
		get_string_option(user_options,
						  DATALAKE_ICEBERG_CATALOG_KRB_CLIENT_KEYTAB);
}

/* Reference implementation: parsePolarisUserMappingOptions() */
static void
parse_polaris_user_mapping_options(PolarisUserMappingOptions *options,
								   List *user_options)
{
	options->client_id =
		get_string_option(user_options, DATALAKE_ICEBERG_CATALOG_CLIENT_ID);
	options->client_secret =
		get_string_option(user_options, DATALAKE_ICEBERG_CATALOG_CLIENT_SECRET);
	options->scope =
		get_string_option(user_options, DATALAKE_ICEBERG_CATALOG_SCOPE);
}

void
parse_iceberg_catalog_user_mapping_options(IcebergCatalogUserMappingOptions *options,
										   List *user_options,
										   const char *server_type)
{
	if (server_type == NULL)
		return;

	if (pg_strcasecmp(server_type,
					  DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HIVE) == 0)
		parse_hive_user_mapping_options(&options->hive, user_options);
	else if (pg_strcasecmp(server_type,
						   DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST) == 0 ||
			 pg_strcasecmp(server_type,
						   DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS) == 0)
		parse_polaris_user_mapping_options(&options->polaris, user_options);
}

/*
 * Reference implementation: parseIcebergForeignCatalogOptions().
 *
 * server_name supplies the default catalog name, because a catalog server here
 * names exactly one Iceberg catalog.
 */
void
parse_iceberg_foreign_catalog_options(IcebergForeignCatalogOptions *options,
									  List *catalog_options,
									  const char *server_name)
{
	options->catalog_name =
		get_string_option(catalog_options, DATALAKE_ICEBERG_CATALOG_NAME);
	if (options->catalog_name == NULL)
		options->catalog_name = pstrdup(server_name);

	options->warehouse_location_prefix =
		get_string_option(catalog_options,
						  DATALAKE_ICEBERG_CATALOG_WAREHOUSE_LOCATION_PREFIX);
}

IcebergCatalogOptions *
get_iceberg_catalog_options(ForeignServer *server)
{
	IcebergCatalogOptions *options;

	Assert(server != NULL);

	options = (IcebergCatalogOptions *) palloc0(sizeof(IcebergCatalogOptions));

	parse_iceberg_catalog_server_options(&options->catalog_server,
										 server->options);
	parse_iceberg_foreign_catalog_options(&options->foreign_catalog,
										  server->options,
										  server->servername);

	return options;
}

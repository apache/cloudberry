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
 * iceberg_catalog_option.h
 *	  Option vocabulary and parsed forms for Iceberg catalog servers.
 *
 * "The reference implementation", here and in the other option modules, means
 * the existing implementation of this feature that this work derives from and
 * is meant to replace.  Its option key macros, struct names and field names are
 * reproduced exactly, so that a parser for a further catalog type can move
 * between the two as an addition rather than a rewrite.  Where the two models
 * genuinely differ, the difference is called out on the field.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/iceberg_catalog_fdw/iceberg_catalog_option.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ICEBERG_CATALOG_OPTION_H
#define ICEBERG_CATALOG_OPTION_H

#include "postgres.h"

#include "common/dl_option_util.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"

/* Catalog server options */
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE "type"
#define DATALAKE_ICEBERG_CATALOG_URL "uri"
#define DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_REALM "polaris_server_realm"

/*
 * Recognized catalog server types.
 *
 * The names are Apache Iceberg's, not this module's: a catalog reached over the
 * REST protocol is "rest", because the specification defines one protocol that
 * several implementations answer.  "polaris" is accepted as an alias for it --
 * Polaris is one such implementation, and it is the spelling the reference
 * implementation uses, so servers written for that one keep working.
 *
 * A storage protocol is never a catalog type -- where the data files live is
 * volume business -- but the reference implementation defines these two names,
 * so they are kept here to stay one vocabulary; the validator refuses them
 * until an implementation exists.
 */
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HIVE "hive"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_REST "rest"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_POLARIS "polaris"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_BUILTIN "builtin"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_HADOOP "hadoop"
#define DATALAKE_ICEBERG_CATALOG_SERVER_TYPE_S3 "s3"

/* Catalog user mapping options */
#define DATALAKE_ICEBERG_CATALOG_USERNAME DL_OPTION_KEY_USERNAME
#define DATALAKE_ICEBERG_CATALOG_AUTH_METHOD "auth_method"
#define DATALAKE_ICEBERG_CATALOG_KRB_SERVICE_PRINCIPAL "krb_service_principal"
#define DATALAKE_ICEBERG_CATALOG_KRB_CLIENT_PRINCIPAL "krb_client_principal"
#define DATALAKE_ICEBERG_CATALOG_KRB_CLIENT_KEYTAB DL_OPTION_KEY_KRB_CLIENT_KEYTAB
#define DATALAKE_ICEBERG_CATALOG_CLIENT_ID DL_OPTION_KEY_CLIENT_ID
#define DATALAKE_ICEBERG_CATALOG_CLIENT_SECRET DL_OPTION_KEY_CLIENT_SECRET
#define DATALAKE_ICEBERG_CATALOG_SCOPE "scope"

/* Catalog identity options */
#define DATALAKE_ICEBERG_CATALOG_NAME "catalog_name"
#define DATALAKE_ICEBERG_CATALOG_WAREHOUSE_LOCATION_PREFIX "warehouse"

typedef struct IcebergCatalogServerOptions
{
	char	   *server_type;		/* DATALAKE_ICEBERG_CATALOG_SERVER_TYPE */
	char	   *hive_metastore_uri; /* DATALAKE_ICEBERG_CATALOG_URL, hive */
	char	   *polaris_server_url; /* DATALAKE_ICEBERG_CATALOG_URL, rest */
	char	   *polaris_server_realm;	/* DATALAKE_ICEBERG_CATALOG_POLARIS_SERVER_REALM */

	/*
	 * The reference implementation also carries server_name, naming a section
	 * of a site configuration file.  There is no such file here: an option that
	 * would be accepted and then ignored is worse than one that is refused, so
	 * it is left out until site configuration exists.
	 */
} IcebergCatalogServerOptions;

typedef struct HiveUserMappingOptions
{
	char	   *username;
	char	   *auth_method;
	char	   *krb_service_principal;
	char	   *krb_client_principal;
	char	   *krb_client_keytab;
} HiveUserMappingOptions;

typedef struct PolarisUserMappingOptions
{
	char	   *client_id;
	char	   *client_secret;
	char	   *scope;
} PolarisUserMappingOptions;

typedef struct IcebergCatalogUserMappingOptions
{
	HiveUserMappingOptions hive;
	PolarisUserMappingOptions polaris;
} IcebergCatalogUserMappingOptions;

typedef struct IcebergForeignCatalogOptions
{
	/*
	 * The reference implementation reads these from a foreign catalog object
	 * that a server can hold several of.  This extension cannot add a catalog
	 * of its own to the system catalogs, so a catalog server names exactly one
	 * Iceberg catalog and both values come from that server's options;
	 * catalog_name defaults to the server name when unset.
	 */
	char	   *catalog_name;		/* DATALAKE_ICEBERG_CATALOG_NAME */
	char	   *warehouse_location_prefix;	/* DATALAKE_ICEBERG_CATALOG_WAREHOUSE_LOCATION_PREFIX */

	/*
	 * Deferred, with the reference implementation's names kept for the port:
	 * enable_metadata_cache / metadata_cache_ttl / auto_refresh_metadata /
	 * total_segment / split_size / filter_string.
	 */
} IcebergForeignCatalogOptions;

typedef struct IcebergCatalogOptions
{
	IcebergCatalogServerOptions catalog_server;
	IcebergCatalogUserMappingOptions catalog_user;
	IcebergForeignCatalogOptions foreign_catalog;
} IcebergCatalogOptions;

/* Reference implementation: parseIcebergCatalogServerOptions() */
extern void parse_iceberg_catalog_server_options(IcebergCatalogServerOptions *options,
												 List *server_options);

/* Reference implementation: parseIcebergCatalogUserMappingOptions() */
extern void parse_iceberg_catalog_user_mapping_options(IcebergCatalogUserMappingOptions *options,
													   List *user_options,
													   const char *server_type);

/* Reference implementation: parseIcebergForeignCatalogOptions() */
extern void parse_iceberg_foreign_catalog_options(IcebergForeignCatalogOptions *options,
												  List *catalog_options,
												  const char *server_name);

/*
 * Reference implementation: getIcebergCatalogOptions(), which additionally
 * fills catalog_user from the invoking role's user mapping.  Credentials are
 * resolved separately and lazily here, because a DDL path must be able to
 * describe a table without reading anyone's secrets; catalog_user is left
 * zeroed by this function.
 */
extern IcebergCatalogOptions *get_iceberg_catalog_options(ForeignServer *server);

#endif							/* ICEBERG_CATALOG_OPTION_H */

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
 * iceberg_volume_option.h
 *	  Option vocabulary and parsed forms for Iceberg volume servers.
 *
 * As on the catalog side, the key macros, struct names and field names are
 * reproduced from the reference implementation -- the existing implementation
 * of this feature that this work derives from and is meant to replace -- so
 * that support for a further storage protocol moves across as an addition.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/iceberg_volume_fdw/iceberg_volume_option.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef ICEBERG_VOLUME_OPTION_H
#define ICEBERG_VOLUME_OPTION_H

#include "postgres.h"

#include "common/dl_option_util.h"
#include "foreign/foreign.h"
#include "nodes/pg_list.h"

/* Volume server options */
#define DATALAKE_ICEBERG_VOLUME_ENDPOINT "endpoint"
#define DATALAKE_ICEBERG_VOLUME_REGION "region"
#define DATALAKE_ICEBERG_VOLUME_PATH_STYLE_ACCESS "path_style_access"

/* Volume user mapping options */
#define DATALAKE_ICEBERG_VOLUME_USERNAME DL_OPTION_KEY_USERNAME
#define DATALAKE_ICEBERG_VOLUME_AWS_ACCESS_KEY_ID DL_OPTION_KEY_ACCESS_KEY_ID
#define DATALAKE_ICEBERG_VOLUME_AWS_SECRET_ACCESS_KEY DL_OPTION_KEY_SECRET_ACCESS_KEY
#define DATALAKE_ICEBERG_VOLUME_AWS_SESSION_TOKEN DL_OPTION_KEY_SESSION_TOKEN

/* Volume location option */
#define DATALAKE_ICEBERG_VOLUME_BASE_PATH "base_path"

/*
 * Storage protocols.  Unlike the reference implementation, no server option
 * names the protocol: base_path carries a URI, so its scheme already says which
 * protocol this volume speaks, and a separate option could only disagree with
 * it.  The names are kept because the parsed location is compared against them.
 */
#define DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_S3 "s3"
#define DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_HDFS "hdfs"

typedef struct IcebergVolumeServerOptions
{
	char	   *endpoint;			/* DATALAKE_ICEBERG_VOLUME_ENDPOINT */
	char	   *region;				/* DATALAKE_ICEBERG_VOLUME_REGION */
	bool		path_style_access;	/* DATALAKE_ICEBERG_VOLUME_PATH_STYLE_ACCESS */
	bool		path_style_access_set;	/* user actually wrote path_style_access */

	/*
	 * Deferred, with the reference implementation's names kept for the port.
	 * server_type and bucket_name are absent by design instead: both are
	 * derived from base_path, and DatalakeLocation is the parsed form.
	 *
	 * AWS: role_arn / external_id / user_arn / current_kms_key /
	 *      allowed_kms_keys / sts_endpoint / sts_unavailable / endpoint_internal
	 * Azure: tenant_id / multi_tenant_app_name / consent_url / hierarchical
	 * HDFS: hdfs_namenodes / hdfs_port / hdfs_auth_method / krb_principal /
	 *      krb_principal_keytab / krb_service_principal /
	 *      hadoop_rpc_protection / data_transfer_protocol / is_ha_supported /
	 *      dfs_nameservices / dfs_ha_namenodes / dfs_namenode_rpc_address /
	 *      dfs_client_failover_proxy_provider /
	 *      dfs_client_use_datanode_hostname
	 */
} IcebergVolumeServerOptions;

typedef struct IcebergVolumeUserMappingOptions
{
	char	   *username;			/* DATALAKE_ICEBERG_VOLUME_USERNAME */
	char	   *aws_access_key_id;	/* DATALAKE_ICEBERG_VOLUME_AWS_ACCESS_KEY_ID */
	char	   *aws_secret_access_key;	/* DATALAKE_ICEBERG_VOLUME_AWS_SECRET_ACCESS_KEY */

	/*
	 * Temporary credentials are three values, not two, and are what AWS
	 * recommends over long-lived keys; without this field the mapping could
	 * only express the long-lived form.
	 */
	char	   *aws_session_token;	/* DATALAKE_ICEBERG_VOLUME_AWS_SESSION_TOKEN */
} IcebergVolumeUserMappingOptions;

typedef struct IcebergForeignVolumeOptions
{
	/*
	 * The reference implementation reads these from a foreign volume object; as
	 * with the catalog side, a volume server here names exactly one volume and
	 * the value comes from that server's options.
	 *
	 * Deferred, names kept: enable_caching / allow_writes / fileIOConfig /
	 * table_identifier.
	 */
	char	   *base_path;			/* DATALAKE_ICEBERG_VOLUME_BASE_PATH */
} IcebergForeignVolumeOptions;

typedef struct IcebergVolumeOptions
{
	IcebergVolumeServerOptions volume_server;
	IcebergVolumeUserMappingOptions volume_user;
	IcebergForeignVolumeOptions foreign_volume;
} IcebergVolumeOptions;

/* Reference implementation: parseIcebergVolumeServerOptions() */
extern void parse_iceberg_volume_server_options(IcebergVolumeServerOptions *options,
												List *server_options);

/* Reference implementation: parseIcebergVolumeUserMappingOptions() */
extern void parse_iceberg_volume_user_mapping_options(IcebergVolumeUserMappingOptions *options,
													  List *user_options);

/* Reference implementation: parseIcebergForeignVolumeOptions() */
extern void parse_iceberg_foreign_volume_options(IcebergForeignVolumeOptions *options,
												 List *volume_options);

/*
 * Reference implementation: getIcebergVolumeOptions().  volume_user is left
 * zeroed for the same reason as on the catalog side.
 *
 * The reference implementation's buildVolumeBasePath() has no counterpart:
 * base_path is parsed once into a DatalakeLocation, and every layer below
 * receives that instead of re-parsing a URI.
 */
extern IcebergVolumeOptions *get_iceberg_volume_options(ForeignServer *server);

#endif							/* ICEBERG_VOLUME_OPTION_H */

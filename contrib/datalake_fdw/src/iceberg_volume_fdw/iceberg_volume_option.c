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
 * iceberg_volume_option.c
 *	  Option vocabulary and parsed forms for Iceberg volume servers.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/iceberg_volume_fdw/iceberg_volume_option.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/parser_option.h"
#include "iceberg_volume_fdw/iceberg_volume_option.h"

void
parse_iceberg_volume_server_options(IcebergVolumeServerOptions *options,
									List *server_options)
{
	options->endpoint =
		get_string_option(server_options, DATALAKE_ICEBERG_VOLUME_ENDPOINT);
	options->region =
		get_string_option(server_options, DATALAKE_ICEBERG_VOLUME_REGION);

	/*
	 * Absence has to stay distinguishable from an explicit false: the metadata
	 * engine merges what a server states over its own defaults per key, so an
	 * unset boolean must not arrive as one the user chose.
	 */
	options->path_style_access =
		get_bool_option_ex(server_options,
						   DATALAKE_ICEBERG_VOLUME_PATH_STYLE_ACCESS,
						   false, &options->path_style_access_set);
}

void
parse_iceberg_volume_user_mapping_options(IcebergVolumeUserMappingOptions *options,
										  List *user_options)
{
	options->username =
		get_string_option(user_options, DATALAKE_ICEBERG_VOLUME_USERNAME);
	options->aws_access_key_id =
		get_string_option(user_options,
						  DATALAKE_ICEBERG_VOLUME_AWS_ACCESS_KEY_ID);
	options->aws_secret_access_key =
		get_string_option(user_options,
						  DATALAKE_ICEBERG_VOLUME_AWS_SECRET_ACCESS_KEY);
	options->aws_session_token =
		get_string_option(user_options,
						  DATALAKE_ICEBERG_VOLUME_AWS_SESSION_TOKEN);
}

void
parse_iceberg_foreign_volume_options(IcebergForeignVolumeOptions *options,
									 List *volume_options)
{
	options->base_path =
		get_string_option(volume_options, DATALAKE_ICEBERG_VOLUME_BASE_PATH);
}

IcebergVolumeOptions *
get_iceberg_volume_options(ForeignServer *server)
{
	IcebergVolumeOptions *options;

	Assert(server != NULL);

	options = (IcebergVolumeOptions *) palloc0(sizeof(IcebergVolumeOptions));

	parse_iceberg_volume_server_options(&options->volume_server,
										server->options);
	parse_iceberg_foreign_volume_options(&options->foreign_volume,
										 server->options);

	return options;
}

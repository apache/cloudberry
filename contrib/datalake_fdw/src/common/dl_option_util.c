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
 * dl_option_util.c
 *	  Option policy shared by the catalog and volume option validators.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/dl_option_util.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/dl_option_util.h"

bool
dl_is_credential_option(const char *name)
{
	static const char *const credential_options[] = {
		DL_OPTION_KEY_USERNAME,
		DL_OPTION_KEY_KRB_CLIENT_KEYTAB,
		DL_OPTION_KEY_CLIENT_ID,
		DL_OPTION_KEY_CLIENT_SECRET,
		DL_OPTION_KEY_ACCESS_KEY_ID,
		DL_OPTION_KEY_SECRET_ACCESS_KEY,
		DL_OPTION_KEY_SESSION_TOKEN,

		/*
		 * Not options this module accepts anywhere, but names users reach for
		 * out of habit.  Listing them turns "unrecognized option" into the hint
		 * that says where credentials actually go.
		 */
		"user",
		"password",
		"token",
		"access_key",
		"secret_key"
	};
	int			i;

	for (i = 0; i < lengthof(credential_options); i++)
	{
		if (strcmp(name, credential_options[i]) == 0)
			return true;
	}

	return false;
}

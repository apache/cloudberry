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
 * backend_registry.cpp
 *	  Registry of the storage backends, one per protocol.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/backend_registry.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "common/dl_pg_api.h"

#include <string.h>

#include "common/backend_registry.h"
#include "common/dl_wrappers.h"

typedef struct DatalakeStorageBackend
{
	const char *scheme;
	const struct DatalakeStorageOps *ops;
} DatalakeStorageBackend;

/* Room for s3 and hdfs, plus space to grow without revisiting this. */
static DatalakeStorageBackend storage_backends[4];
static int	nstorage_backends;

extern DlErrCode datalake_register_s3_backend(void);

static bool
storage_ops_are_complete(const struct DatalakeStorageOps *ops)
{
	/*
	 * A partially filled table would turn into a null call at the first
	 * operation the backend forgot, so refuse it at registration instead.
	 */
	return ops != NULL &&
		ops->fs_open != NULL &&
		ops->fs_close != NULL &&
		ops->fs_list != NULL &&
		ops->file_open != NULL &&
		ops->file_read != NULL &&
		ops->file_write != NULL &&
		ops->file_close != NULL &&
		ops->file_abort != NULL;
}

DlErrCode
datalake_register_storage_backend(const char *scheme,
								  const struct DatalakeStorageOps *ops)
{
	int			i;

	if (scheme == NULL || scheme[0] == '\0' || !storage_ops_are_complete(ops))
		return DL_ERR_INVALID_OPTION;

	for (i = 0; i < nstorage_backends; i++)
	{
		if (strcmp(storage_backends[i].scheme, scheme) == 0)
			return DL_ERR_ALREADY_EXISTS;
	}

	if (nstorage_backends >= (int) lengthof(storage_backends))
		return DL_ERR_INTERNAL;

	storage_backends[nstorage_backends].scheme = scheme;
	storage_backends[nstorage_backends].ops = ops;
	nstorage_backends++;

	return DL_OK;
}

const struct DatalakeStorageOps *
datalake_lookup_storage_backend(const char *scheme)
{
	int			i;

	if (scheme == NULL)
		return NULL;

	for (i = 0; i < nstorage_backends; i++)
	{
		if (strcmp(storage_backends[i].scheme, scheme) == 0)
			return storage_backends[i].ops;
	}

	return NULL;
}

extern "C" void
datalake_register_storage_backends(void)
{
	DL_TRY
	{
		DlErrCode	rc = datalake_register_s3_backend();

		/* Registering twice is harmless; anything else is a coding error. */
		if (rc != DL_OK && rc != DL_ERR_ALREADY_EXISTS)
			ereport(ERROR,
					(errmsg("datalake_fdw: could not register the s3 storage backend: %s",
							dl_err_message(rc))));
	}
	DL_CATCH_END();
}

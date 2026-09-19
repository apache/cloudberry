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
 * file_system_wrapper.cpp
 *	  Storage facade dispatching to the registered backend.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/file_system_wrapper.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "common/dl_pg_api.h"

#include "common/backend_registry.h"
#include "common/dl_wrappers.h"
#include "common/file_system_wrapper.h"

/*
 * Dispatch only: each call finds the backend registered for the location's
 * scheme and hands the work over.  Nothing here is reachable from SQL in this
 * skeleton, so what the regression suite asserts is the behaviour of the
 * layers above.
 */

extern "C" DlErrCode
datalake_fs_open(const DatalakeLocation *location,
				 const DlKeyValue *credentials, int ncredentials,
				 DatalakeFileSystem *fs_out)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		const struct DatalakeStorageOps *ops;

		if (fs_out == NULL || location == NULL || location->scheme == NULL ||
			ncredentials < 0)
			rc = DL_ERR_INVALID_OPTION;
		else
		{
			*fs_out = NULL;
			ops = datalake_lookup_storage_backend(location->scheme);

			if (ops == NULL)
				rc = DL_ERR_NOT_SUPPORTED;
			else
			{
				rc = ops->fs_open(location, credentials, ncredentials, fs_out);

				if (rc == DL_OK && *fs_out == NULL)
					rc = DL_ERR_INTERNAL;
				else if (rc == DL_OK)
					(*fs_out)->ops = ops;
			}
		}
	}
	DL_ABI_GUARD_END(rc, "fs_open");

	return rc;
}

extern "C" void
datalake_fs_close(DatalakeFileSystem *fs)
{
	DL_CLEANUP_GUARD_BEGIN
	{
		/*
		 * Clear the caller's handle before releasing it, so that a repeated
		 * close -- the normal shape of resource-owner cleanup after an error
		 * that already closed things -- finds nothing to do instead of
		 * reaching a backend that has freed itself.
		 */
		if (fs != NULL && *fs != NULL)
		{
			DatalakeFileSystem doomed = *fs;

			*fs = NULL;
			doomed->ops->fs_close(doomed);
		}
	}
	DL_CLEANUP_GUARD_END;
}

extern "C" DlErrCode
datalake_fs_list(DatalakeFileSystem fs, const char *prefix,
				 char ***names_out, int *nnames_out)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (fs == NULL || prefix == NULL || names_out == NULL ||
			nnames_out == NULL)
			rc = DL_ERR_INVALID_OPTION;
		else
		{
			*names_out = NULL;
			*nnames_out = 0;
			rc = fs->ops->fs_list(fs, prefix, names_out, nnames_out);
		}
	}
	DL_ABI_GUARD_END(rc, "fs_list");

	return rc;
}

extern "C" DlErrCode
datalake_file_open(DatalakeFileSystem fs, const char *path,
				   DatalakeFileMode mode, DatalakeFile *file_out)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (file_out == NULL || fs == NULL || path == NULL)
			rc = DL_ERR_INVALID_OPTION;
		else if (mode != DATALAKE_FILE_READ && mode != DATALAKE_FILE_WRITE)
			rc = DL_ERR_INVALID_OPTION;
		else
		{
			*file_out = NULL;
			rc = fs->ops->file_open(fs, path, mode, file_out);

			if (rc == DL_OK && *file_out == NULL)
				rc = DL_ERR_INTERNAL;
			else if (rc == DL_OK)
				(*file_out)->ops = fs->ops;
		}
	}
	DL_ABI_GUARD_END(rc, "file_open");

	return rc;
}

extern "C" DlErrCode
datalake_file_read(DatalakeFile file, void *buffer, int64_t length,
				   int64_t *nread)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (file == NULL || nread == NULL || length < 0)
			rc = DL_ERR_INVALID_OPTION;
		else
		{
			*nread = 0;
			rc = file->ops->file_read(file, buffer, length, nread);
		}
	}
	DL_ABI_GUARD_END(rc, "file_read");

	return rc;
}

extern "C" DlErrCode
datalake_file_write(DatalakeFile file, const void *buffer, int64_t length)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (file == NULL || length < 0)
			rc = DL_ERR_INVALID_OPTION;
		else
			rc = file->ops->file_write(file, buffer, length);
	}
	DL_ABI_GUARD_END(rc, "file_write");

	return rc;
}

extern "C" DlErrCode
datalake_file_close(DatalakeFile *file)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (file == NULL || *file == NULL)
			rc = DL_ERR_INVALID_OPTION;
		else
		{
			DatalakeFile doomed = *file;

			/*
			 * The handle is consumed even when the close reports an error:
			 * the backend has released it either way, and there is nothing
			 * left to retry the close against.
			 */
			*file = NULL;
			rc = doomed->ops->file_close(doomed);
		}
	}
	DL_ABI_GUARD_END(rc, "file_close");

	return rc;
}

extern "C" void
datalake_file_abort(DatalakeFile *file)
{
	DL_CLEANUP_GUARD_BEGIN
	{
		/* Cleared first, so a repeated abort finds nothing to do. */
		if (file != NULL && *file != NULL)
		{
			DatalakeFile doomed = *file;

			*file = NULL;
			doomed->ops->file_abort(doomed);
		}
	}
	DL_CLEANUP_GUARD_END;
}

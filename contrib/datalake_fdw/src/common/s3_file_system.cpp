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
 * s3_file_system.cpp
 *	  The S3 storage backend.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/s3_file_system.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "common/dl_pg_api.h"

#include "common/backend_registry.h"
#include "common/dl_wrappers.h"

#include <memory>

/*
 * The S3 backend, without an S3 client yet: the shape a backend takes is what
 * this file establishes, so that the change adding a real client replaces
 * method bodies rather than the structure around them.  Every entry point
 * reports that the operation is not supported.
 */
class S3FileSystem
{
public:
	DlErrCode
	Initialize(const DatalakeLocation *location, const DlKeyValue *credentials,
			   int ncredentials)
	{
		(void) location;
		(void) credentials;
		(void) ncredentials;

		return DL_ERR_NOT_SUPPORTED;
	}

	DlErrCode
	OpenFile(const char *path, DatalakeFileMode mode, DatalakeFile *file_out)
	{
		(void) path;
		(void) mode;

		if (file_out != NULL)
			*file_out = NULL;

		return DL_ERR_NOT_SUPPORTED;
	}

	DlErrCode
	List(const char *prefix, char ***names_out, int *nnames_out)
	{
		(void) prefix;

		if (names_out != NULL)
			*names_out = NULL;
		if (nnames_out != NULL)
			*nnames_out = 0;

		return DL_ERR_NOT_SUPPORTED;
	}
};

/*
 * A handle the facade can hold.
 *
 * Deriving from the C struct rather than embedding it as a first member is what
 * makes recovering the handle defined behaviour: a derived-to-base pointer
 * conversion and a static_cast back are guaranteed for any class, while the
 * first-member trick is only guaranteed for standard-layout types -- which this
 * is not, because of the unique_ptr.  The C side still sees a plain
 * DatalakeFileSystemData, since that is what the base subobject is.
 */
struct S3FileSystemHandle : public DatalakeFileSystemData
{
	std::unique_ptr<S3FileSystem> impl;
};

static DlErrCode
s3_fs_open(const DatalakeLocation *location, const DlKeyValue *credentials,
		   int ncredentials, DatalakeFileSystem *fs_out)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (fs_out == NULL)
			rc = DL_ERR_INVALID_OPTION;
		else
		{
			/*
			 * Owned by unique_ptr until the handle is published, so that an
			 * exception from the second allocation or from Initialize() --
			 * which the guard below turns into an error code -- cannot leave
			 * the first allocation behind.
			 */
			std::unique_ptr<S3FileSystemHandle> handle(new S3FileSystemHandle());

			*fs_out = NULL;
			handle->impl.reset(new S3FileSystem());
			rc = handle->impl->Initialize(location, credentials, ncredentials);

			if (rc == DL_OK)
				*fs_out = handle.release();
		}
	}
	DL_ABI_GUARD_END(rc);

	return rc;
}

static void
s3_fs_close(DatalakeFileSystem fs)
{
	DL_CLEANUP_GUARD_BEGIN
	{
		/* The facade has already cleared its caller's handle. */
		delete static_cast<S3FileSystemHandle *>(fs);
	}
	DL_CLEANUP_GUARD_END;
}

static DlErrCode
s3_fs_list(DatalakeFileSystem fs, const char *prefix, char ***names_out,
		   int *nnames_out)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		S3FileSystemHandle *handle = static_cast<S3FileSystemHandle *>(fs);

		if (handle == NULL)
			rc = DL_ERR_INVALID_OPTION;
		else
			rc = handle->impl->List(prefix, names_out, nnames_out);
	}
	DL_ABI_GUARD_END(rc);

	return rc;
}

static DlErrCode
s3_file_open(DatalakeFileSystem fs, const char *path, DatalakeFileMode mode,
			 DatalakeFile *file_out)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		S3FileSystemHandle *handle = static_cast<S3FileSystemHandle *>(fs);

		if (handle == NULL)
			rc = DL_ERR_INVALID_OPTION;
		else
			rc = handle->impl->OpenFile(path, mode, file_out);
	}
	DL_ABI_GUARD_END(rc);

	return rc;
}

static DlErrCode
s3_file_read(DatalakeFile file, void *buffer, int64_t length, int64_t *nread)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		(void) file;
		(void) buffer;
		(void) length;

		if (nread != NULL)
			*nread = 0;

		rc = DL_ERR_NOT_SUPPORTED;
	}
	DL_ABI_GUARD_END(rc);

	return rc;
}

static DlErrCode
s3_file_write(DatalakeFile file, const void *buffer, int64_t length)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		(void) file;
		(void) buffer;
		(void) length;

		rc = DL_ERR_NOT_SUPPORTED;
	}
	DL_ABI_GUARD_END(rc);

	return rc;
}

static DlErrCode
s3_file_close(DatalakeFile file)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		(void) file;

		rc = DL_ERR_NOT_SUPPORTED;
	}
	DL_ABI_GUARD_END(rc);

	return rc;
}

static void
s3_file_abort(DatalakeFile file)
{
	DL_CLEANUP_GUARD_BEGIN
	{
		(void) file;
	}
	DL_CLEANUP_GUARD_END;
}

static const struct DatalakeStorageOps s3_storage_ops = {
	s3_fs_open,
	s3_fs_close,
	s3_fs_list,
	s3_file_open,
	s3_file_read,
	s3_file_write,
	s3_file_close,
	s3_file_abort
};

DlErrCode
datalake_register_s3_backend(void)
{
	return datalake_register_storage_backend("s3", &s3_storage_ops);
}

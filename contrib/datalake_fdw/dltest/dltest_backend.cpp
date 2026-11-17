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
 * dltest_backend.cpp
 *	  A storage backend in a library of its own, so that the registration
 *	  contract is exercised the way a third party would meet it.
 *
 * This is not part of datalake_fdw.  It is built as a separate module, loaded
 * only by a server started to run the regression suite, and it reaches
 * datalake_fdw through nothing but the five installed headers and
 * datalake_storage_register().  Compiling it into the production library
 * instead would put a test-only scheme on every cluster -- one a volume could
 * be created against and stored in the catalog -- and would prove nothing
 * about the boundary it exists to prove.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/dltest/dltest_backend.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cstring>

#include <arrow/memory_pool.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/util/config.h>

#include "common/local_file_system.h"
#include "common/storage_backend.h"
#include "common/storage_backend_register.h"

/*
 * A second backend registered through the public contract, so the storage
 * cases prove that a backend which is not the built-in one works the same
 * way.  It mounts the same create-only local file system under a subtree --
 * compiled into this library from the same source rather than borrowed from
 * the other one, which is what a backend written elsewhere would have to do
 * and keeps the write and abort semantics from drifting into a second
 * implementation.
 */
static arrow::Result<DatalakeMountedFs>
mount_dltest(const DatalakeLocation *location, const DlKeyValue *, int,
			 const DatalakeStorageHost *host)
{
	if (location == NULL || location->path_prefix == NULL)
		return arrow::Status::Invalid("dltest location has no path");

	auto local = std::make_shared<LocalCreateOnlyFileSystem>(
		arrow::io::IOContext(dl_storage_host_pool(host)));
	DatalakeMountedFs mounted;

	mounted.fs = std::make_shared<arrow::fs::SubTreeFileSystem>(
		location->path_prefix, local);
	mounted.root = "";
	return mounted;
}

static const DatalakeStorageBackend dltest_backend = {
	DL_STORAGE_ABI_VERSION, sizeof(DatalakeStorageBackend), "dltest",
	ARROW_VERSION_STRING, DL_STORAGE_ABI_FINGERPRINT, mount_dltest, NULL, NULL
};

extern "C"
{
	PG_MODULE_MAGIC;
	void		_PG_init(void);
}

void
_PG_init(void)
{
	DlErrCode	rc = datalake_storage_register(&dltest_backend);

	/*
	 * Registration only counts during preload, so a failure here is a
	 * misconfigured server rather than something to carry on from.
	 */
	if (rc != DL_OK && rc != DL_ERR_ALREADY_EXISTS)
		elog(ERROR, "could not register the \"dltest\" storage backend");
}

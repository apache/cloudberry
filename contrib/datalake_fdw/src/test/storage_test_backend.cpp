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
 * storage_test_backend.cpp
 *	  A storage backend that exists so the registration contract can be
 *	  tested from inside this module.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/test/storage_test_backend.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cstring>

#include <arrow/memory_pool.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/util/config.h>

#include "common/local_file_system.h"
#include "common/storage_backend.h"

/*
 * A second backend registered through the public contract, so the storage
 * cases prove that a backend which is not the built-in one works the same
 * way.  It mounts the same create-only local file system under a subtree,
 * which keeps one implementation of the write and abort semantics rather
 * than a second one that could drift.
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

extern "C" DlErrCode
datalake_register_test_storage_backend(void)
{
	return datalake_register_storage_backend(&dltest_backend);
}

/*
 * Each kind breaks exactly one of the registration checks, so a case can
 * assert the message that check produces.  The scheme stays "dltest", which
 * is already registered: a check that stopped working would fall through to
 * the duplicate-scheme rejection, and the cases tell those apart by naming
 * the field and the value they expect to see reported.
 */
extern "C" DlErrCode
datalake_test_register_bad_storage_backend(const char *kind)
{
	DatalakeStorageBackend bad = dltest_backend;

	if (strcmp(kind, "abi_version") == 0)
		bad.abi_version++;
	else if (strcmp(kind, "struct_size") == 0)
		bad.struct_size = 0;
	else if (strcmp(kind, "arrow_version") == 0)
		bad.arrow_version = "0.0.0-test";
	else if (strcmp(kind, "abi_fingerprint") == 0)
		bad.abi_fingerprint = "gcc0;cxx11abi=9;arrow=0.0.0-test";
	else if (strcmp(kind, "duplicate") != 0)
		return DL_ARG_ERROR("register bad storage backend");

	return datalake_register_storage_backend(&bad);
}

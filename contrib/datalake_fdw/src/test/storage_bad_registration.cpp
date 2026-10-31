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
 * storage_bad_registration.cpp
 *	  Offering the registry a backend it has to refuse.
 *
 * The registration checks are the only part of the contract a test can drive
 * from SQL, so they are driven from here rather than from a library that
 * would have to be loaded for a case about loading to run.  Nothing in this
 * file registers anything at startup; it only builds structures that are
 * meant to be rejected.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/test/storage_bad_registration.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cstring>

#include <arrow/filesystem/filesystem.h>
#include <arrow/util/config.h>

#include "common/storage_backend.h"

/*
 * Never called: every structure built here fails a check before the registry
 * would have anything to mount.  It exists because a null mount is itself one
 * of the things the registry is entitled to refuse, and refusing it for that
 * reason would hide the check each case is about.
 */
static arrow::Result<DatalakeMountedFs>
mount_never(const DatalakeLocation *, const DlKeyValue *, int,
			const DatalakeStorageHost *)
{
	return arrow::Status::NotImplemented("this backend exists to be refused");
}

/*
 * Each kind breaks exactly one of the registration checks, so a case can
 * assert the message that check produces.  The scheme stays "dltest", which
 * the dltest module registered during preload: a check that stopped working
 * would fall through to the duplicate-scheme rejection, and the cases tell
 * those apart by naming the field and the value they expect to see reported.
 */
extern "C" DlErrCode
datalake_test_register_bad_storage_backend(const char *kind)
{
	/*
	 * Static, because the registry keeps the pointer it is given.  Every case
	 * is meant to be refused, but "duplicate" is refused only while dltest is
	 * preloaded, and a check that stopped working would let any of them in;
	 * either way what got registered must not be a dead stack frame.
	 */
	static DatalakeStorageBackend bad;

	bad = {
		DL_STORAGE_ABI_VERSION, sizeof(DatalakeStorageBackend), "dltest",
		ARROW_VERSION_STRING, DL_STORAGE_ABI_FINGERPRINT, mount_never,
		NULL, NULL
	};

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

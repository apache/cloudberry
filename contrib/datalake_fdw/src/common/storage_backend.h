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
 * storage_backend.h
 *	  Public contract for pluggable storage backends.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/storage_backend.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STORAGE_BACKEND_H
#define STORAGE_BACKEND_H

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>

#include <arrow/memory_pool.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/util/config.h>

#include "datalake_location.h"
#include "dl_err.h"
#include "dl_kv.h"

#define DL_STORAGE_ABI_VERSION 1

#define DL_STORAGE_STRINGIFY_DETAIL(value) #value
#define DL_STORAGE_STRINGIFY(value) DL_STORAGE_STRINGIFY_DETAIL(value)

#if defined(__clang__)
#define DL_STORAGE_COMPILER_FINGERPRINT \
	"clang" DL_STORAGE_STRINGIFY(__clang_major__)
#elif defined(__GNUC__)
#define DL_STORAGE_COMPILER_FINGERPRINT \
	"gcc" DL_STORAGE_STRINGIFY(__GNUC__)
#else
#define DL_STORAGE_COMPILER_FINGERPRINT "unknown"
#endif

#ifdef _GLIBCXX_USE_CXX11_ABI
#define DL_STORAGE_CXX11_ABI_FINGERPRINT \
	DL_STORAGE_STRINGIFY(_GLIBCXX_USE_CXX11_ABI)
#else
#define DL_STORAGE_CXX11_ABI_FINGERPRINT "na"
#endif

#define DL_STORAGE_ABI_FINGERPRINT \
	DL_STORAGE_COMPILER_FINGERPRINT ";cxx11abi=" \
	DL_STORAGE_CXX11_ABI_FINGERPRINT ";arrow=" ARROW_VERSION_STRING

struct DatalakeMountedFs
{
	std::shared_ptr<arrow::fs::FileSystem> fs;
	std::string root;
};

/*
 * Services supplied by the host.  Future versions may append fields, so a
 * backend reads a field only after struct_size says it is there -- which is
 * what dl_storage_host_pool() below does for the one field there is today.
 */
struct DatalakeStorageHost
{
	uint32_t	struct_size;
	arrow::MemoryPool *pool;	/* memory a backend allocates through is
								 * charged to the query, so IOContext and
								 * every Buffer must come from here */
};

/*
 * The pool to allocate through, or Arrow's default when the host predates the
 * field.  Allocating outside the host's pool means the memory escapes
 * Cloudberry's accounting, so a backend should always route Arrow through it.
 */
static inline arrow::MemoryPool *
dl_storage_host_pool(const DatalakeStorageHost *host)
{
	size_t		needed = offsetof(DatalakeStorageHost, pool) +
		sizeof(((DatalakeStorageHost *) nullptr)->pool);

	if (host == nullptr || host->struct_size < needed || host->pool == nullptr)
		return arrow::default_memory_pool();
	return host->pool;
}

/*
 * A backend only constructs an Arrow filesystem.  The facade uses precisely
 * GetFileInfo(path), GetFileInfo(FileSelector), OpenInputFile and its
 * GetSize/ReadAt/Read methods, OpenOutputStream and its Write/Close/Abort
 * methods, and DeleteFile.  Other methods may return NotImplemented.
 *
 * Instances are borrowed by the registry and therefore need static lifetime.
 * struct_size is prefix-compatible: future versions may append fields.
 */
struct DatalakeStorageBackend
{
	uint32_t	abi_version;
	uint32_t	struct_size;
	const char *uri_scheme;
	const char *arrow_version;
	const char *abi_fingerprint;
	arrow::Result<DatalakeMountedFs> (*mount) (const DatalakeLocation *,
											 const DlKeyValue *kv, int nkv,
											 const DatalakeStorageHost *host);
	arrow::Status (*initialize) (void);
	void		(*finalize) (void);
};

#ifdef __cplusplus
extern "C"
{
#endif

extern DlErrCode datalake_register_storage_backend(
	const DatalakeStorageBackend *backend);

#ifdef __cplusplus
}
#endif

#endif							/* STORAGE_BACKEND_H */

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
 * backend_registry.h
 *	  Registry of the storage backends, one per protocol.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/backend_registry.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BACKEND_REGISTRY_H
#define BACKEND_REGISTRY_H

#include <stdint.h>

#include "common/file_system_wrapper.h"

#ifdef __cplusplus

/*
 * One storage protocol's implementation of the facade in
 * common/file_system_wrapper.h.  The operations mirror it one for one, so a
 * backend is written against the same contract its callers see.
 */
struct DatalakeStorageOps
{
	DlErrCode	(*fs_open) (const DatalakeLocation *location,
							const DlKeyValue *credentials, int ncredentials,
							DatalakeFileSystem *fs_out);
	void		(*fs_close) (DatalakeFileSystem fs);	/* releases fs */
	DlErrCode	(*fs_list) (DatalakeFileSystem fs, const char *prefix,
							char ***names_out, int *nnames_out);
	DlErrCode	(*file_open) (DatalakeFileSystem fs, const char *path,
							  DatalakeFileMode mode, DatalakeFile *file_out);
	DlErrCode	(*file_read) (DatalakeFile file, void *buffer, int64_t length,
							  int64_t *nread);
	DlErrCode	(*file_write) (DatalakeFile file, const void *buffer,
							   int64_t length);
	DlErrCode	(*file_close) (DatalakeFile file);	/* releases file */
	void		(*file_abort) (DatalakeFile file);	/* releases file */
};

/*
 * Every handle a backend hands out starts with this field, which is how the
 * facade finds its way back to the right operations.  A handle lives until a
 * cleanup entry point consumes it; there is no closed-but-alive state, because
 * keeping one would mean either leaking every handle or letting a backend free
 * memory the facade still reads.
 */
struct DatalakeFileSystemData
{
	const struct DatalakeStorageOps *ops;
};

struct DatalakeFileData
{
	const struct DatalakeStorageOps *ops;
};

extern DlErrCode datalake_register_storage_backend(const char *scheme,
												   const struct DatalakeStorageOps *ops);
extern const struct DatalakeStorageOps *datalake_lookup_storage_backend(const char *scheme);

#endif							/* __cplusplus */

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * Registration is an explicit call rather than a static initializer: the order
 * static initializers run in a shared module is not something to depend on,
 * and _PG_init is where this is meant to happen.
 */
extern void datalake_register_storage_backends(void);

#ifdef __cplusplus
}
#endif

#endif							/* BACKEND_REGISTRY_H */

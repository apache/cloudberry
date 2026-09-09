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
 * file_system_wrapper.h
 *	  Storage facade over one protocol: open, read, write, list.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/file_system_wrapper.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef FILE_SYSTEM_WRAPPER_H
#define FILE_SYSTEM_WRAPPER_H

#include <stdint.h>

#include "common/datalake_location.h"
#include "common/dl_err.h"
#include "common/dl_kv.h"

/*
 * A file system reached over one storage protocol, and an open file in it.
 * Both are opaque: callers hold a handle and pass it back, exactly as they do
 * for a File or a BufFile, so a backend can keep whatever state it needs
 * without any of it becoming part of this interface.
 *
 * This is deliberately a facade over open/read/write/close/list and not a
 * storage framework.  Its only consumer is the format layer, and keeping the
 * surface this narrow is what lets the implementation be replaced -- by an
 * Arrow filesystem, say -- without the layers above noticing.
 */
typedef struct DatalakeFileSystemData *DatalakeFileSystem;
typedef struct DatalakeFileData *DatalakeFile;

typedef enum DatalakeFileMode
{
	DATALAKE_FILE_READ,
	DATALAKE_FILE_WRITE
} DatalakeFileMode;

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * The location names the protocol and the bucket or namenode; credentials are
 * resolved separately and may be empty, in which case the backend falls back
 * to whatever ambient credentials it finds.
 */
extern DlErrCode datalake_fs_open(const DatalakeLocation *location,
								  const DlKeyValue *credentials,
								  int ncredentials,
								  DatalakeFileSystem *fs_out);

/*
 * Cleanup entry point: releases the file system and clears the caller's
 * handle, so a repeated call has nothing left to act on.  It never raises,
 * because it runs on the resource-owner path during transaction abort.
 * Passing a handle by value could not clear it, and the second call would
 * then reach a backend that had already freed itself.
 */
extern void datalake_fs_close(DatalakeFileSystem *fs);

extern DlErrCode datalake_fs_list(DatalakeFileSystem fs, const char *prefix,
								  char ***names_out, int *nnames_out);

extern DlErrCode datalake_file_open(DatalakeFileSystem fs, const char *path,
									DatalakeFileMode mode,
									DatalakeFile *file_out);

extern DlErrCode datalake_file_read(DatalakeFile file, void *buffer,
									int64_t length, int64_t *nread);

extern DlErrCode datalake_file_write(DatalakeFile file, const void *buffer,
									 int64_t length);

/*
 * Finishes the file and clears the caller's handle.  Errors worth reporting
 * surface here, and the handle is consumed whether or not one does: there is
 * nothing left to retry against.
 */
extern DlErrCode datalake_file_close(DatalakeFile *file);

/*
 * Cleanup entry point for the failure path: discards the file and clears the
 * caller's handle.  Never raises; anything worth reporting comes out of
 * datalake_file_close() instead.
 */
extern void datalake_file_abort(DatalakeFile *file);

#ifdef __cplusplus
}
#endif

#endif							/* FILE_SYSTEM_WRAPPER_H */

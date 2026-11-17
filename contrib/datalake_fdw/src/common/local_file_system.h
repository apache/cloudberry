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
 * local_file_system.h
 *	  The create-only local file system, shared with the test backend.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/local_file_system.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef LOCAL_FILE_SYSTEM_H
#define LOCAL_FILE_SYSTEM_H

#include <memory>
#include <string>

#include <arrow/filesystem/localfs.h>
#include <arrow/io/interfaces.h>

/*
 * A local file system whose OpenOutputStream creates the file and fails if it
 * is already there, and whose streams clean up after themselves.
 *
 * Arrow's own LocalFileSystem opens output with O_TRUNC, which would let a
 * failed write destroy an existing file; this keeps the O_EXCL rule the
 * Parquet writer has had since #1951.  The returned stream owns what it
 * created: Abort(), and a Close() that fails, unlink exactly the path this
 * stream created, and nothing else ever deletes on its behalf.
 *
 * The test backend mounts the same implementation through SubTreeFileSystem,
 * so the storage conformance cases exercise one implementation, not two.
 */
class LocalCreateOnlyFileSystem : public arrow::fs::LocalFileSystem
{
public:
	explicit LocalCreateOnlyFileSystem(const arrow::io::IOContext &io_context)
		: arrow::fs::LocalFileSystem(io_context)
	{
	}

	arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenOutputStream(
		const std::string &path,
		const std::shared_ptr<const arrow::KeyValueMetadata> &metadata) override;
};

#endif							/* LOCAL_FILE_SYSTEM_H */

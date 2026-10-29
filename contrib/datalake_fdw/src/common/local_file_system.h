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
#include <vector>

#include <arrow/filesystem/localfs.h>
#include <arrow/io/interfaces.h>

/*
 * A create-only local file system. Output is staged in a private directory
 * and published by an atomic, non-overwriting link on successful Close().
 * Abort and failed Close clean up only staging data, never the destination.
 * Concurrent opens may succeed; only one can publish to a given pathname.
 * The .datalake-write- directory prefix is reserved and hidden from listings.
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

	using arrow::fs::LocalFileSystem::GetFileInfo;
	arrow::Result<std::vector<arrow::fs::FileInfo>> GetFileInfo(
		const arrow::fs::FileSelector &selector) override;

	arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenOutputStream(
		const std::string &path,
		const std::shared_ptr<const arrow::KeyValueMetadata> &metadata) override;
};

#endif							/* LOCAL_FILE_SYSTEM_H */

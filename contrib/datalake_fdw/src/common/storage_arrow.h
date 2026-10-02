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
 * storage_arrow.h
 *	  C++ access to the Arrow objects behind the storage facade.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/storage_arrow.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STORAGE_ARROW_H
#define STORAGE_ARROW_H

#include <string>
#include <vector>

#include <arrow/memory_pool.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>
#include <arrow/status.h>

#include "common/file_system_wrapper.h"

arrow::fs::FileSystem *dl_storage_arrow_fs(DatalakeFileSystem fs);

/*
 * Open a file below a mount, for the format layer, under the same rules the C
 * facade applies: a read must find the file, and a write must not.  Both take
 * a path relative to the mount root, and both refuse one that could climb out
 * of it.  They exist so that "creating a file never replaces one" is decided
 * once, rather than once per format.
 */
arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
			dl_storage_open_input(DatalakeFileSystem fs, const char *relative);
arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
			dl_storage_open_output(DatalakeFileSystem fs, const char *relative);
std::string dl_storage_native_path(DatalakeFileSystem fs,
								   const char *relative);

class DlStatusDetail : public arrow::StatusDetail
{
public:
	DlStatusDetail(DlErrCode code, std::string type);
	const char *type_id() const override;
	std::string ToString() const override;
	DlErrCode code() const;
	const std::string &type() const;

private:
	DlErrCode	code_;
	std::string type_;
};

/*
 * Hand the mount's credential values to the error layer, which removes them
 * from everything recorded afterwards.  Called once per mount; nothing below
 * has to carry them around.
 */
void		dl_storage_remember_secrets(const DlKeyValue *kv, int nkv);

/* Turn an Arrow status into a DlErrCode and record it. */
DlErrCode dl_storage_status_to_err(const arrow::Status &status,
								   const char *operation);

/*
 * Whether a path may be joined onto a mount root: relative, and free of "."
 * and ".." components.  The root names where a volume lives, it does not
 * confine what a path can reach, so the check belongs before the join.
 */
bool dl_storage_path_is_safe(const char *relative);

/* The same status with its DlErrCode attached, for a caller that hands it on. */
arrow::Status dl_storage_classify(arrow::Status status);

#endif							/* STORAGE_ARROW_H */

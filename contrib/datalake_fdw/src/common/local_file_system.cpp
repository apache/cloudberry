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
 * local_file_system.cpp
 *	  The create-only local storage backend.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/local_file_system.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <errno.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include <utility>

#include <arrow/memory_pool.h>
#include <arrow/io/file.h>
#include <arrow/util/config.h>

#include "common/local_file_system.h"
#include "common/storage_backend.h"

extern "C"
{
#include "postgres.h"
#include "common/file_perm.h"
}

namespace
{

/*
 * The descriptor between open() and the point where Arrow takes it over.  Any
 * error in between has to undo the creation, or an empty file is left behind
 * and every later attempt at the same path fails with ALREADY_EXISTS.
 */
class CreatedFile
{
public:
	CreatedFile(int fd, std::string path) : fd_(fd), path_(std::move(path)) {}

	~CreatedFile()
	{
		if (fd_ >= 0)
		{
			(void) close(fd_);
			(void) unlink(path_.c_str());
		}
	}

	CreatedFile(const CreatedFile &) = delete;
	CreatedFile &operator=(const CreatedFile &) = delete;

	int fd() const { return fd_; }
	void release() { fd_ = -1; }

private:
	int			fd_;
	std::string path_;
};

/*
 * Wraps the Arrow stream so that giving up removes what this writer created
 * and nothing else.  Only the path this stream created with O_EXCL is ever
 * unlinked, which is what keeps a failed writer from destroying a file that
 * belongs to somebody else.
 */
class CreateOnlyOutputStream : public arrow::io::OutputStream
{
public:
	CreateOnlyOutputStream(std::shared_ptr<arrow::io::OutputStream> file,
						   std::string path)
		: file_(std::move(file)), path_(std::move(path))
	{
	}

	/*
	 * Nothing should reach this without a Close or an Abort, but a C++
	 * exception on the way out of a caller can, and what it would leave is an
	 * empty file that makes every later attempt at the name fail.
	 */
	~CreateOnlyOutputStream() override
	{
		if (!done_)
			(void) Abort();
	}

	arrow::Status Close() override
	{
		if (done_)
			return arrow::Status::OK();
		done_ = true;

		arrow::Status status = file_->Close();

		/* A close that fails leaves a file nobody asked for. */
		if (!status.ok())
			(void) unlink(path_.c_str());
		return status;
	}

	/*
	 * Abort is the caller saying "this object must not exist".  Close the
	 * descriptor, then remove the file this stream created.
	 */
	arrow::Status Abort() override
	{
		if (done_)
			return arrow::Status::OK();
		done_ = true;

		arrow::Status status = file_->Close();

		if (unlink(path_.c_str()) != 0 && errno != ENOENT)
			return arrow::Status::IOError("could not remove \"", path_,
										  "\": ", strerror(errno));
		return status;
	}

	bool closed() const override { return done_ || file_->closed(); }

	arrow::Result<int64_t> Tell() const override { return file_->Tell(); }

	arrow::Status Write(const void *data, int64_t nbytes) override
	{
		return file_->Write(data, nbytes);
	}

	arrow::Status Write(const std::shared_ptr<arrow::Buffer> &data) override
	{
		return file_->Write(data);
	}

	arrow::Status Flush() override { return file_->Flush(); }

private:
	std::shared_ptr<arrow::io::OutputStream> file_;
	std::string path_;
	bool		done_ = false;
};

}							/* namespace */

arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
LocalCreateOnlyFileSystem::OpenOutputStream(
	const std::string &path,
	const std::shared_ptr<const arrow::KeyValueMetadata> &metadata)
{
	int			fd;

	(void) metadata;
	fd = open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, pg_file_create_mode);
	if (fd < 0)
	{
		if (errno == EEXIST)
			return arrow::Status::AlreadyExists("\"", path, "\" already exists");
		return arrow::Status::IOError("could not create \"", path, "\": ",
									  strerror(errno));
	}

	CreatedFile created(fd, path);
	auto		stream = arrow::io::FileOutputStream::Open(fd);

	if (!stream.ok())
		return stream.status();		/* CreatedFile closes and unlinks */

	/*
	 * Built before the guard is disarmed: allocating the wrapper can throw,
	 * and the file must not survive that either.
	 */
	auto		owned = std::make_shared<CreateOnlyOutputStream>(*stream, path);

	created.release();				/* the wrapper owns the file now */
	return owned;
}

static arrow::Result<DatalakeMountedFs>
mount_local(const DatalakeLocation *location, const DlKeyValue *kv, int nkv,
			const DatalakeStorageHost *host)
{
	(void) kv;
	(void) nkv;

	if (location == NULL || location->path_prefix == NULL)
		return arrow::Status::Invalid("file location has no path");

	DatalakeMountedFs mounted;

	mounted.fs = std::make_shared<LocalCreateOnlyFileSystem>(
		arrow::io::IOContext(dl_storage_host_pool(host)));
	mounted.root = location->path_prefix;
	return mounted;
}

static const DatalakeStorageBackend local_storage_backend = {
	DL_STORAGE_ABI_VERSION,
	sizeof(DatalakeStorageBackend),
	"file",
	ARROW_VERSION_STRING,
	DL_STORAGE_ABI_FINGERPRINT,
	mount_local,
	NULL,
	NULL
};

DlErrCode
datalake_register_local_backend(void)
{
	return datalake_register_storage_backend(&local_storage_backend);
}

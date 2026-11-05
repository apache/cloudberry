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
#include <sys/stat.h>
#include <unistd.h>

#include <random>
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
#include "utils/faultinjector.h"
}

namespace
{

/* Reserved staging directories never contain published storage objects. */
constexpr const char *STAGING_PREFIX = ".datalake-write-";

bool
is_staging_path(const std::string &path)
{
	size_t start = 0;

	while (start < path.size())
	{
		if (path.compare(start, strlen(STAGING_PREFIX), STAGING_PREFIX) == 0)
			return true;
		size_t slash = path.find('/', start);
		if (slash == std::string::npos)
			return false;
		start = slash + 1;
	}
	return false;
}

/*
 * Write into a private directory on the destination filesystem.  Directory
 * descriptors anchor both publication and cleanup even if a parent is renamed.
 * Cleanup NEVER unlinks the destination: linkat publishes without replacing
 * an existing name, and a failed publication leaves only private staging data.
 */
class PendingFile
{
public:
	~PendingFile()
	{
		if (fd_ >= 0)
			(void) close(fd_);
		if (staging_fd_ >= 0)
		{
			(void) unlinkat(staging_fd_, "data", 0);
			(void) close(staging_fd_);
		}
		if (created_dir_)
			(void) unlinkat(parent_fd_, staging_name_.c_str(), AT_REMOVEDIR);
		if (parent_fd_ >= 0)
			(void) close(parent_fd_);
	}

	static arrow::Result<std::unique_ptr<PendingFile>> Make(const std::string &path)
	{
		if (is_staging_path(path))
			return arrow::Status::Invalid("reserved storage staging path: ", path);
		auto pending = std::unique_ptr<PendingFile>(new PendingFile());
		size_t slash = path.find_last_of('/');
		std::string parent = slash == std::string::npos ? "." :
			(slash == 0 ? "/" : path.substr(0, slash));
		pending->name_ = slash == std::string::npos ? path : path.substr(slash + 1);
		if (pending->name_.empty() || pending->name_ == "." || pending->name_ == "..")
			return arrow::Status::Invalid("invalid output path: ", path);
		pending->parent_fd_ = open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
		if (pending->parent_fd_ < 0)
			return arrow::Status::IOError("could not open output directory: ", strerror(errno));

		/* Preserve the early AlreadyExists result, including dangling symlinks.
		 * This is only an optimization: linkat enforces exclusivity at commit. */
		struct stat existing;
		if (fstatat(pending->parent_fd_, pending->name_.c_str(), &existing,
					AT_SYMLINK_NOFOLLOW) == 0)
			return arrow::Status::AlreadyExists("\"", path, "\" already exists");
		if (errno != ENOENT)
			return arrow::Status::IOError("could not stat output path: ", strerror(errno));

		std::random_device random;
		for (int attempt = 0; attempt < 128; ++attempt)
		{
			pending->staging_name_ = std::string(STAGING_PREFIX) +
				std::to_string(random()) + "-" + std::to_string(random());
			if (mkdirat(pending->parent_fd_, pending->staging_name_.c_str(), 0700) == 0)
			{
				pending->created_dir_ = true;
				break;
			}
			if (errno != EEXIST)
				return arrow::Status::IOError("could not create staging directory: ", strerror(errno));
		}
		if (!pending->created_dir_)
			return arrow::Status::IOError("could not allocate a unique staging directory");
		pending->staging_fd_ = openat(pending->parent_fd_, pending->staging_name_.c_str(),
										O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
		if (pending->staging_fd_ < 0)
			return arrow::Status::IOError("could not open staging directory: ", strerror(errno));
		pending->fd_ = openat(pending->staging_fd_, "data",
							 O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, pg_file_create_mode);
		if (pending->fd_ < 0)
			return arrow::Status::IOError("could not create staged output: ", strerror(errno));
		return pending;
	}

	arrow::Status Publish()
	{
		if (linkat(staging_fd_, "data", parent_fd_, name_.c_str(), 0) != 0)
		{
			if (errno == EEXIST)
				return arrow::Status::AlreadyExists("\"", name_, "\" already exists");
			return arrow::Status::IOError("could not publish output file: ", strerror(errno));
		}
		return arrow::Status::OK();
	}

	int fd() const { return fd_; }
	void release_fd() { fd_ = -1; }

private:
	PendingFile() = default;
	int parent_fd_ = -1;
	int staging_fd_ = -1;
	int fd_ = -1;
	bool created_dir_ = false;
	std::string name_;
	std::string staging_name_;
};

class CreateOnlyOutputStream : public arrow::io::OutputStream
{
public:
	CreateOnlyOutputStream(std::shared_ptr<arrow::io::FileOutputStream> file,
						   std::unique_ptr<PendingFile> pending)
		: file_(std::move(file)), pending_(std::move(pending))
	{
	}

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
		if (status.ok())
			status = pending_->Publish();
		pending_.reset();
		return status;
	}

	arrow::Status Abort() override
	{
		if (done_)
			return arrow::Status::OK();
		done_ = true;
		arrow::Status status = file_->Close();
		/* Test the interval immediately before private staging cleanup. */
		SIMPLE_FAULT_INJECTOR("datalake_local_abort_before_cleanup");
		pending_.reset();
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
	std::shared_ptr<arrow::io::FileOutputStream> file_;
	std::unique_ptr<PendingFile> pending_;
	bool done_ = false;
};

}                               /* namespace */

arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
LocalCreateOnlyFileSystem::OpenOutputStream(
	const std::string &path,
	const std::shared_ptr<const arrow::KeyValueMetadata> &metadata)
{
	(void) metadata;
	ARROW_ASSIGN_OR_RAISE(auto pending, PendingFile::Make(path));
	ARROW_ASSIGN_OR_RAISE(auto stream, arrow::io::FileOutputStream::Open(pending->fd()));
	pending->release_fd();          /* Arrow now owns the descriptor. */
	return std::make_shared<CreateOnlyOutputStream>(std::move(stream), std::move(pending));
}

arrow::Result<std::vector<arrow::fs::FileInfo>>
LocalCreateOnlyFileSystem::GetFileInfo(const arrow::fs::FileSelector &selector)
{
	std::vector<arrow::fs::FileInfo> result;
	std::vector<std::pair<std::string, int>> directories = {{selector.base_dir, 0}};

	/* Do not descend into staging directories: they can disappear during
	 * cleanup, and neither their names nor their contents are storage data. */
	while (!directories.empty())
	{
		auto current = std::move(directories.back());
		directories.pop_back();
		if (is_staging_path(current.first))
			continue;
		arrow::fs::FileSelector shallow = selector;
		shallow.base_dir = current.first;
		shallow.recursive = false;
		if (current.second > 0)
			shallow.allow_not_found = true;
		ARROW_ASSIGN_OR_RAISE(auto infos, arrow::fs::LocalFileSystem::GetFileInfo(shallow));
		for (auto &info : infos)
		{
			if (is_staging_path(info.path()))
				continue;
			if (selector.recursive && info.IsDirectory() && current.second < selector.max_recursion)
				directories.emplace_back(info.path(), current.second + 1);
			result.push_back(std::move(info));
		}
	}
	return result;
}

/*
 * The "file" backend itself is the module's, not this file's: the dltest
 * module compiles this same source to get the create-only filesystem without
 * also acquiring a registrar for a scheme that is not its own -- and without
 * a reference to datalake_register_storage_backend, which lives in the other
 * library and would make this one depend on load order.
 */
#ifndef DL_LOCAL_FS_NO_REGISTER

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

#endif							/* DL_LOCAL_FS_NO_REGISTER */

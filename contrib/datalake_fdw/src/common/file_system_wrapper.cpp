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
 * file_system_wrapper.cpp
 *	  Storage facade implemented once over Arrow filesystems.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/file_system_wrapper.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <algorithm>
#include <cctype>
#include <climits>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <arrow/memory_pool.h>
#include <arrow/filesystem/filesystem.h>
#include <arrow/io/interfaces.h>

#include "common/storage_arrow.h"
#include "format/arrow_memory_pool.h"
#include "common/dl_pg_api.h"
#include "common/backend_registry.h"
#include "common/dl_wrappers.h"
#include "common/file_system_wrapper.h"

struct DatalakeFileSystemData
{
	std::shared_ptr<arrow::fs::FileSystem> fs;
	std::string root;
	const DatalakeStorageBackend *backend;
};

struct DatalakeFileData
{
	std::shared_ptr<arrow::fs::FileSystem> fs;
	std::string path;
	std::shared_ptr<arrow::io::RandomAccessFile> input;
	std::shared_ptr<arrow::io::OutputStream> output;
};

DlStatusDetail::DlStatusDetail(DlErrCode code, std::string type)
	: code_(code), type_(std::move(type))
{
}

const char *
DlStatusDetail::type_id() const
{
	return "datalake::DlStatusDetail";
}

std::string
DlStatusDetail::ToString() const
{
	return type_;
}

DlErrCode
DlStatusDetail::code() const
{
	return code_;
}

const std::string &
DlStatusDetail::type() const
{
	return type_;
}

/*
 * The option values worth hiding, handed to the error layer once at mount so
 * that everything recorded afterwards has them removed -- including text this
 * module never sees, from an SDK or an exception or a third-party backend.
 *
 * A secret access key and a session token are credentials.  An access key id
 * is an identifier that appears in request headers and audit records anyway,
 * and hiding it costs the reader the one fact that says which credential was
 * used -- worse, its value tends to occur inside bucket and prefix names,
 * which would then be masked out of every message about them.
 */
void
dl_storage_remember_secrets(const DlKeyValue *kv, int nkv)
{
	for (int i = 0; kv != NULL && i < nkv; i++)
	{
		if (kv[i].key == NULL || kv[i].value == NULL)
			continue;

		const std::string key(kv[i].key);

		if (key.find("secret") == std::string::npos &&
			key.find("token") == std::string::npos &&
			key.find("password") == std::string::npos)
			continue;

		dl_error_add_secret(kv[i].value);
	}
}

/*
 * Which DlErrCode a status means, and what to call its class.
 *
 * A backend that classified the failure itself is believed: only it can know
 * that one service's 404 and another's ENOENT are the same answer.  For any
 * other status this is the fallback, and it reads the whole status rather than
 * the message alone -- Arrow puts a failed open's errno in a detail, so the
 * words "No such file or directory" are not in the message at all.
 */
static DlErrCode
classify_status(const arrow::Status &status, std::string *type_out)
{
	if (status.detail() != NULL &&
		strcmp(status.detail()->type_id(), "datalake::DlStatusDetail") == 0)
	{
		const DlStatusDetail *detail =
			static_cast<const DlStatusDetail *>(status.detail().get());

		*type_out = detail->type();
		return detail->code();
	}

	/*
	 * Arrow prints "Unknown" for the codes it has no name for, AlreadyExists
	 * among them, and a class of "Unknown" beside a message that explains
	 * itself is worse than no class at all.
	 */
	*type_out = status.CodeAsString();
	if (*type_out == "Unknown")
		type_out->clear();

	if (status.IsNotImplemented())
		return DL_ERR_NOT_SUPPORTED;
	if (status.IsAlreadyExists())
		return DL_ERR_ALREADY_EXISTS;
	if (status.IsOutOfMemory())
		return DL_ERR_OUT_OF_MEMORY;
	if (status.IsInvalid() || status.IsTypeError() || status.IsKeyError())
		return DL_ERR_INVALID_OPTION;
	if (status.IsIOError())
	{
		/*
		 * Only phrases a filesystem writes about itself, never a bare status
		 * number: the text contains the caller's path, and a directory named
		 * "404" would otherwise turn every error about it into "not found".
		 * A backend that can tell properly attaches its own classification
		 * instead of leaving this to guess.
		 */
		const std::string whole = status.ToString();

		if (whole.find("No such file or directory") != std::string::npos ||
			whole.find("does not exist") != std::string::npos)
			return DL_ERR_NOT_FOUND;
	}
	return DL_ERR_IO;
}

/*
 * The same status, carrying its classification, for a caller that hands it on
 * rather than reporting it here -- the format layer, which opens files through
 * the storage layer but reports errors in its own terms.
 */
arrow::Status
dl_storage_classify(arrow::Status status)
{
	std::string type;

	if (status.ok() || (status.detail() != NULL &&
						strcmp(status.detail()->type_id(),
							   "datalake::DlStatusDetail") == 0))
		return status;

	DlErrCode	code = classify_status(status, &type);

	return status.WithDetail(std::make_shared<DlStatusDetail>(code, type));
}

DlErrCode
dl_storage_status_to_err(const arrow::Status &status, const char *operation)
{
	DlErrCode	code;
	std::string type;
	std::string message;

	if (status.ok())
		return DL_OK;

	code = classify_status(status, &type);
	if (message.empty())
		message = status.message();
	dl_error_set(code, operation, type.c_str(), message.c_str());
	return code;
}

bool
dl_storage_path_is_safe(const char *relative)
{
	const char *segment = relative;

	if (relative == NULL || relative[0] == '\0')
		return true;			/* the mount root itself */
	if (relative[0] == '/')
		return false;			/* absolute: not relative to the mount */

	while (segment != NULL)
	{
		const char *slash = strchr(segment, '/');
		size_t		length = slash == NULL ? strlen(segment) :
			static_cast<size_t>(slash - segment);

		if ((length == 1 && segment[0] == '.') ||
			(length == 2 && segment[0] == '.' && segment[1] == '.'))
			return false;
		segment = slash == NULL ? NULL : slash + 1;
	}
	return true;
}

/* Reports the rejection the caller should return for an unsafe path. */
static DlErrCode
reject_unsafe_path(const char *operation, const char *relative)
{
	std::string message = "storage path \"" + std::string(relative) +
		"\" must be relative to the volume and must not contain \".\" or \"..\"";

	dl_error_set(DL_ERR_INVALID_OPTION, operation, NULL, message.c_str());
	return DL_ERR_INVALID_OPTION;
}

arrow::fs::FileSystem *
dl_storage_arrow_fs(DatalakeFileSystem fs)
{
	return fs == NULL ? NULL : fs->fs.get();
}

std::string
dl_storage_native_path(DatalakeFileSystem fs, const char *relative)
{
	if (fs == NULL || relative == NULL || relative[0] == '\0')
		return fs == NULL ? std::string() : fs->root;
	if (fs->root.empty())
		return relative;
	if (fs->root.back() == '/')
		return fs->root + relative;
	return fs->root + "/" + relative;
}

extern "C" DlErrCode
datalake_fs_open(const DatalakeLocation *location, const DlKeyValue *kv,
				 int nkv, DatalakeFileSystem *fs_out)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (fs_out == NULL || location == NULL || location->scheme == NULL ||
			nkv < 0 || (nkv > 0 && kv == NULL))
			rc = DL_ARG_ERROR("fs_open");
		else
		{
			const DatalakeStorageBackend *backend;

			*fs_out = NULL;
			dl_error_reset();
			backend = datalake_lookup_storage_backend(location->scheme);
			if (backend == NULL)
			{
				std::string message = "no storage backend registered for scheme \"" +
					std::string(location->scheme) + "\"";

				dl_error_set(DL_ERR_NOT_SUPPORTED, "mount storage", NULL,
							 message.c_str());
				rc = DL_ERR_NOT_SUPPORTED;
			}
			else
			{
				arrow::Status status;

				dl_storage_remember_secrets(kv, nkv);
				status = datalake_initialize_storage_backend(backend);

				if (!status.ok())
					rc = dl_storage_status_to_err(status, "initialize storage");
				else
				{
					DatalakeStorageHost host;

					host.struct_size = sizeof(host);
					host.pool = DlArrowMemoryPool();

					auto mounted = backend->mount(location, kv, nkv, &host);

					if (!mounted.ok())
						rc = dl_storage_status_to_err(mounted.status(),
											  "mount storage");
					else if (mounted->fs == NULL)
					{
						dl_error_set(DL_ERR_INTERNAL, "mount storage", NULL,
									 "storage backend returned a null filesystem");
						rc = DL_ERR_INTERNAL;
					}
					else
					{
						std::unique_ptr<DatalakeFileSystemData> handle(
							new DatalakeFileSystemData());

						handle->fs = std::move(mounted->fs);
						handle->root = std::move(mounted->root);
						handle->backend = backend;
						*fs_out = handle.release();
						rc = DL_OK;
					}
				}
			}
		}
	}
	DL_ABI_GUARD_END(rc, "fs_open");

	return rc;
}

extern "C" void
datalake_fs_close(DatalakeFileSystem *fs)
{
	DL_CLEANUP_GUARD_BEGIN
	{
		if (fs != NULL && *fs != NULL)
		{
			DatalakeFileSystem doomed = *fs;

			*fs = NULL;
			delete doomed;
		}
	}
	DL_CLEANUP_GUARD_END;
}

extern "C" DlErrCode
datalake_fs_list(DatalakeFileSystem fs, const char *prefix,
				 char ***names_out, int *nnames_out)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (fs == NULL || prefix == NULL || names_out == NULL || nnames_out == NULL)
			rc = DL_ARG_ERROR("fs_list");
		else if (!dl_storage_path_is_safe(prefix))
			rc = reject_unsafe_path("list storage", prefix);
		else
		{
			arrow::fs::FileSelector selector;
			std::vector<std::string> paths;

			*names_out = NULL;
			*nnames_out = 0;
			selector.base_dir = dl_storage_native_path(fs, prefix);
			selector.recursive = true;
			selector.allow_not_found = true;	/* the emptiness rule is above */
			auto infos = fs->fs->GetFileInfo(selector);

			if (!infos.ok())
				rc = dl_storage_status_to_err(infos.status(), "list storage");
			else if (infos->empty())
			{
				/*
				 * Nothing at all under the prefix.  Object storage cannot
				 * tell an empty prefix from one that was never written, and a
				 * filesystem would answer differently, so the rule is made
				 * here rather than by each backend: nothing there is nothing
				 * to list, and a caller that named the wrong prefix is told
				 * so instead of reading an empty table.
				 */
				std::string message = prefix[0] == '\0' ?
					std::string("nothing is stored at the root of this "
								"location") :
					"nothing is stored under \"" + std::string(prefix) + "\"";

				dl_error_set(DL_ERR_NOT_FOUND, "list storage", NULL,
							 message.c_str());
				rc = DL_ERR_NOT_FOUND;
			}
			else
			{
				for (const auto &info : *infos)
				{
					if (info.IsFile())
						paths.push_back(info.path());
				}
				std::sort(paths.begin(), paths.end());

				/* The count leaves through an int, so it has to fit in one. */
				if (paths.size() > static_cast<size_t>(INT_MAX))
				{
					dl_error_set(DL_ERR_IO, "list storage", NULL,
								 "storage listing has too many entries to return");
					rc = DL_ERR_IO;
				}
				else
				{
					char	  **names = static_cast<char **>(
						std::calloc(paths.size(), sizeof(char *)));

					if (!paths.empty() && names == NULL)
						rc = dl_storage_status_to_err(
							arrow::Status::OutOfMemory("allocating storage listing"),
							"list storage");
					else
					{
						size_t		i = 0;

						for (; i < paths.size(); i++)
						{
							names[i] = static_cast<char *>(
								std::malloc(paths[i].size() + 1));
							if (names[i] == NULL)
								break;
							std::memcpy(names[i], paths[i].c_str(),
										paths[i].size() + 1);
						}

						if (i != paths.size())
						{
							while (i > 0)
								std::free(names[--i]);
							std::free(names);
							rc = dl_storage_status_to_err(
								arrow::Status::OutOfMemory("allocating storage listing"),
								"list storage");
						}
						else
						{
							*names_out = names;
							*nnames_out = static_cast<int>(paths.size());
							rc = DL_OK;
						}
					}
				}
			}
		}
	}
	DL_ABI_GUARD_END(rc, "fs_list");

	return rc;
}

/*
 * The existence rule both openers share.  Checking before creating is not what
 * makes a write safe -- two writers can still pass the check together, and on
 * object storage there is nothing to hold -- it is what turns a name collision
 * into an error a user can read instead of a file somebody silently lost.  The
 * backends enforce the rule where they can: the local one creates with O_EXCL.
 */
static arrow::Status
require_absent(DatalakeFileSystem fs, const std::string &native)
{
	ARROW_ASSIGN_OR_RAISE(auto info, fs->fs->GetFileInfo(native));

	if (info.type() != arrow::fs::FileType::NotFound)
		return arrow::Status::AlreadyExists("\"", native, "\" already exists");
	return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>>
dl_storage_open_input(DatalakeFileSystem fs, const char *relative)
{
	if (fs == NULL || relative == NULL)
		return arrow::Status::Invalid("no file system or path to open");
	if (!dl_storage_path_is_safe(relative))
		return arrow::Status::Invalid("storage path \"", relative,
									  "\" must be relative to the volume and "
									  "must not contain \".\" or \"..\"");
	auto		file = fs->fs->OpenInputFile(dl_storage_native_path(fs, relative));

	if (!file.ok())
		return dl_storage_classify(file.status());
	return file;
}

arrow::Result<std::shared_ptr<arrow::io::OutputStream>>
dl_storage_open_output(DatalakeFileSystem fs, const char *relative)
{
	if (fs == NULL || relative == NULL)
		return arrow::Status::Invalid("no file system or path to create");
	if (!dl_storage_path_is_safe(relative))
		return arrow::Status::Invalid("storage path \"", relative,
									  "\" must be relative to the volume and "
									  "must not contain \".\" or \"..\"");

	const std::string native = dl_storage_native_path(fs, relative);

	ARROW_RETURN_NOT_OK(dl_storage_classify(require_absent(fs, native)));

	auto		stream = fs->fs->OpenOutputStream(native);

	if (!stream.ok())
		return dl_storage_classify(stream.status());
	return stream;
}

extern "C" DlErrCode
datalake_file_delete(DatalakeFileSystem fs, const char *path)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (fs == NULL || path == NULL)
			rc = DL_ARG_ERROR("file_delete");
		else if (!dl_storage_path_is_safe(path))
			rc = reject_unsafe_path("delete storage file", path);
		else
			rc = dl_storage_status_to_err(
				fs->fs->DeleteFile(dl_storage_native_path(fs, path)),
				"delete storage file");
	}
	DL_ABI_GUARD_END(rc, "file_delete");

	return rc;
}

extern "C" DlErrCode
datalake_file_open(DatalakeFileSystem fs, const char *path,
				   DatalakeFileMode mode, DatalakeFile *file_out)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (file_out == NULL || fs == NULL || path == NULL ||
			(mode != DATALAKE_FILE_READ && mode != DATALAKE_FILE_WRITE))
			rc = DL_ARG_ERROR("file_open");
		else if (!dl_storage_path_is_safe(path))
			rc = reject_unsafe_path("open storage file", path);
		else
		{
			std::string native_path = dl_storage_native_path(fs, path);
			auto info = fs->fs->GetFileInfo(native_path);

			*file_out = NULL;
			if (!info.ok())
				rc = dl_storage_status_to_err(info.status(), "inspect storage file");
			else if (mode == DATALAKE_FILE_READ &&
					 info->type() == arrow::fs::FileType::NotFound)
			{
				std::string message = "storage file \"" + native_path +
					"\" does not exist";

				dl_error_set(DL_ERR_NOT_FOUND, "open storage file", NULL,
							 message.c_str());
				rc = DL_ERR_NOT_FOUND;
			}
			else if (mode == DATALAKE_FILE_WRITE &&
					 info->type() != arrow::fs::FileType::NotFound)
			{
				std::string message = "storage file \"" + native_path +
					"\" already exists";

				dl_error_set(DL_ERR_ALREADY_EXISTS, "create storage file", NULL,
							 message.c_str());
				rc = DL_ERR_ALREADY_EXISTS;
			}
			else
			{
				std::unique_ptr<DatalakeFileData> handle(new DatalakeFileData());

				handle->fs = fs->fs;
				handle->path = native_path;
				if (mode == DATALAKE_FILE_READ)
				{
					auto input = fs->fs->OpenInputFile(native_path);

					if (!input.ok())
						rc = dl_storage_status_to_err(input.status(),
											  "open storage file");
					else
					{
						handle->input = *input;
						rc = DL_OK;
					}
				}
				else
				{
					auto output = fs->fs->OpenOutputStream(native_path);

					if (!output.ok())
						rc = dl_storage_status_to_err(output.status(),
											  "create storage file");
					else
					{
						handle->output = *output;
						rc = DL_OK;
					}
				}

				if (rc == DL_OK)
					*file_out = handle.release();
			}
		}
	}
	DL_ABI_GUARD_END(rc, "file_open");

	return rc;
}

extern "C" DlErrCode
datalake_file_read(DatalakeFile file, void *buffer, int64_t length,
				   int64_t *nread)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (file == NULL || file->input == NULL || nread == NULL || length < 0 ||
			(length > 0 && buffer == NULL))
			rc = DL_ARG_ERROR("file_read");
		else
		{
			auto read = file->input->Read(length, buffer);

			*nread = 0;
			if (!read.ok())
				rc = dl_storage_status_to_err(read.status(), "read storage file");
			else
			{
				*nread = *read;
				rc = DL_OK;
			}
		}
	}
	DL_ABI_GUARD_END(rc, "file_read");

	return rc;
}

extern "C" DlErrCode
datalake_file_write(DatalakeFile file, const void *buffer, int64_t length)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (file == NULL || file->output == NULL || length < 0 ||
			(length > 0 && buffer == NULL))
			rc = DL_ARG_ERROR("file_write");
		else
			rc = dl_storage_status_to_err(file->output->Write(buffer, length),
										   "write storage file");
	}
	DL_ABI_GUARD_END(rc, "file_write");

	return rc;
}

extern "C" DlErrCode
datalake_file_close(DatalakeFile *file)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		if (file == NULL || *file == NULL)
			rc = DL_ARG_ERROR("file_close");
		else
		{
			std::unique_ptr<DatalakeFileData> doomed(*file);
			arrow::Status status;

			*file = NULL;

			/*
			 * A stream cleans up after itself: the output stream removes what
			 * it created, and only that.  Deleting by path from here would
			 * reach an object that some other writer created in the meantime.
			 */
			status = doomed->input != NULL ? doomed->input->Close() :
				doomed->output->Close();
			if (!status.ok() && doomed->output != NULL)
				(void) doomed->output->Abort();
			rc = dl_storage_status_to_err(status, "close storage file");
		}
	}
	DL_ABI_GUARD_END(rc, "file_close");

	return rc;
}

extern "C" void
datalake_file_abort(DatalakeFile *file)
{
	DL_CLEANUP_GUARD_BEGIN
	{
		if (file != NULL && *file != NULL)
		{
			std::unique_ptr<DatalakeFileData> doomed(*file);

			*file = NULL;
			if (doomed->output != NULL)
				(void) doomed->output->Abort();
			else if (doomed->input != NULL)
				(void) doomed->input->Close();
		}
	}
	DL_CLEANUP_GUARD_END;
}

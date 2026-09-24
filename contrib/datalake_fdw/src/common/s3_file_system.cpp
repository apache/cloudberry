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
 * s3_file_system.cpp
 *	  The S3 storage backend, over the AWS SDK for C++.
 *
 * Arrow can be built with an S3 file system of its own, but no RPM of it is:
 * Apache's own spec turns the option off, and so does EPEL, which is every
 * Arrow a Rocky or RHEL user can install.  So the file system is ours, built
 * on the SDK directly, and the rest of the module neither knows nor cares --
 * it sees an arrow::fs::FileSystem like any other backend produces.
 *
 * Only the synchronous S3Client is used.  A PostgreSQL backend is a single
 * thread that must stay interruptible and must account for its own memory, so
 * the CRT client and the transfer manager, which bring their own thread pools
 * and buffers, would both be working against us.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/s3_file_system.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <algorithm>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/memory_pool.h>
#include <arrow/util/config.h>

#include "common/storage_backend.h"

#ifdef DL_HAVE_AWS_SDK

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/http/Scheme.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "common/storage_arrow.h"

extern "C"
{
#include "postgres.h"
#include "miscadmin.h"
}

namespace
{

const char *const DL_S3_ALLOC_TAG = "datalake_fdw";

/* Big enough to clear S3's 5 MiB minimum for every part but the last. */
constexpr int64_t DL_S3_PART_SIZE = 8 * 1024 * 1024;

Aws::SDKOptions sdk_options;

/* ----------------------------------------------------------------------
 * Errors
 * ---------------------------------------------------------------------- */

template <typename AwsError>
bool
is_not_found(const AwsError &error)
{
	return error.GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND ||
		error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY ||
		error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_BUCKET ||
		error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND;
}

/*
 * An SDK failure as an Arrow status the facade can classify.  The exception
 * name travels in the detail, so the user sees "AccessDenied" rather than a
 * bare I/O error, and DlErrCode is decided here rather than by matching on
 * the text of the message further up.
 *
 * What goes in the message is the operation, the bucket and the key -- never
 * a header or a credential, which is why the SDK's own message is the last
 * thing appended and nothing else about the request is.
 */
template <typename AwsError>
arrow::Status
status_from_aws(const char *operation, const std::string &bucket,
				const std::string &key, const AwsError &error)
{
	DlErrCode	code = is_not_found(error) ? DL_ERR_NOT_FOUND : DL_ERR_IO;
	std::string name(error.GetExceptionName().c_str());
	std::string message = std::string(operation) + " s3://" + bucket + "/" +
		key + " failed";

	if (name.empty())
		name = "S3Error";
	if (error.GetMessage().size() > 0)
		message += ": " + std::string(error.GetMessage().c_str());

	return arrow::Status::IOError(message)
		.WithDetail(std::make_shared<DlStatusDetail>(code, name));
}

/* ----------------------------------------------------------------------
 * Paths
 * ---------------------------------------------------------------------- */

/* The facade hands down "bucket/key"; the SDK wants the two apart. */
void
split_path(const std::string &path, std::string *bucket, std::string *key)
{
	std::string::size_type slash = path.find('/');

	if (slash == std::string::npos)
	{
		*bucket = path;
		key->clear();
		return;
	}
	*bucket = path.substr(0, slash);
	*key = path.substr(slash + 1);
	while (!key->empty() && key->back() == '/')
		key->pop_back();
}

/* ----------------------------------------------------------------------
 * Reading
 * ---------------------------------------------------------------------- */

class S3InputFile : public arrow::io::RandomAccessFile
{
public:
	S3InputFile(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket,
				std::string key, int64_t size, arrow::MemoryPool *pool)
		: client_(std::move(client)), bucket_(std::move(bucket)),
		  key_(std::move(key)), size_(size), pool_(pool)
	{
	}

	arrow::Status Close() override
	{
		closed_ = true;
		return arrow::Status::OK();
	}

	bool closed() const override { return closed_; }

	arrow::Result<int64_t> Tell() const override
	{
		if (closed_)
			return arrow::Status::Invalid("the file is closed");
		return position_;
	}

	arrow::Status Seek(int64_t position) override
	{
		if (closed_)
			return arrow::Status::Invalid("the file is closed");
		if (position < 0)
			return arrow::Status::Invalid("cannot seek to a negative position");
		position_ = position;
		return arrow::Status::OK();
	}

	arrow::Result<int64_t> GetSize() override
	{
		if (closed_)
			return arrow::Status::Invalid("the file is closed");
		return size_;
	}

	/* The one call that actually fetches: a ranged GET into the caller's
	 * memory, so nothing is copied through a buffer of the SDK's own. */
	arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes,
								  void *out) override
	{
		if (closed_)
			return arrow::Status::Invalid("the file is closed");
		if (position < 0 || nbytes < 0)
			return arrow::Status::Invalid("read position and length must not "
										  "be negative");

		nbytes = std::min(nbytes, std::max<int64_t>(0, size_ - position));
		if (nbytes == 0)
			return 0;

		Aws::S3::Model::GetObjectRequest request;
		char		range[64];

		snprintf(range, sizeof(range), "bytes=%lld-%lld",
				 (long long) position, (long long) (position + nbytes - 1));
		request.SetBucket(bucket_.c_str());
		request.SetKey(key_.c_str());
		request.SetRange(range);

		/*
		 * Hand the SDK the destination instead of taking its stringstream:
		 * the response body is written straight into memory the caller (and
		 * so the query's memory accounting) already owns.  The stream buffer
		 * outlives the outcome because it is declared before it.
		 */
		Aws::Utils::Stream::PreallocatedStreamBuf stream_buf(
			reinterpret_cast<unsigned char *>(out), (uint64_t) nbytes);

		request.SetResponseStreamFactory([&stream_buf]() {
			return Aws::New<Aws::IOStream>(DL_S3_ALLOC_TAG, &stream_buf);
		});

		auto		outcome = client_->GetObject(request);

		if (!outcome.IsSuccess())
			return status_from_aws("read", bucket_, key_, outcome.GetError());

		/*
		 * The buffer handed to the SDK is exactly nbytes long, so a service
		 * that ignored the range and sent more has already been stopped by
		 * the stream buffer; what must not happen is reporting those bytes as
		 * read, which would hand the caller a length its memory does not
		 * cover.
		 */
		int64_t		got = outcome.GetResult().GetContentLength();

		if (got < 0 || got > nbytes)
			return arrow::Status::IOError(
				"read s3://", bucket_, "/", key_, " returned ", got,
				" bytes for a ", nbytes, " byte range")
				.WithDetail(std::make_shared<DlStatusDetail>(DL_ERR_IO,
															 "ShortRead"));
		return got;
	}

	arrow::Result<std::shared_ptr<arrow::Buffer>> ReadAt(int64_t position,
														 int64_t nbytes) override
	{
		ARROW_ASSIGN_OR_RAISE(auto buffer,
							  arrow::AllocateResizableBuffer(nbytes, pool_));

		ARROW_ASSIGN_OR_RAISE(int64_t read,
							  ReadAt(position, nbytes, buffer->mutable_data()));
		ARROW_RETURN_NOT_OK(buffer->Resize(read, /* shrink_to_fit = */ false));
		return std::shared_ptr<arrow::Buffer>(std::move(buffer));
	}

	arrow::Result<int64_t> Read(int64_t nbytes, void *out) override
	{
		ARROW_ASSIGN_OR_RAISE(int64_t read, ReadAt(position_, nbytes, out));
		position_ += read;
		return read;
	}

	arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override
	{
		ARROW_ASSIGN_OR_RAISE(auto buffer, ReadAt(position_, nbytes));
		position_ += buffer->size();
		return buffer;
	}

private:
	std::shared_ptr<Aws::S3::S3Client> client_;
	std::string bucket_;
	std::string key_;
	int64_t		size_;
	arrow::MemoryPool *pool_;
	int64_t		position_ = 0;
	bool		closed_ = false;
};

/* ----------------------------------------------------------------------
 * Writing
 * ---------------------------------------------------------------------- */

/*
 * Buffers up to a part at a time.  A write that stays under the part size
 * never starts a multipart upload and goes out as a single PutObject on
 * close, so the common case of a small file costs one request; a larger one
 * starts the upload only when it has a full part to send.
 *
 * Nothing exists under the key until Close(), and Abort() removes the one
 * thing this stream may have created, the multipart upload.  That is what
 * lets the facade stop deleting by path: an abandoned write leaves nothing
 * to delete, and an object at that key belongs to somebody else.
 */
class S3OutputStream : public arrow::io::OutputStream
{
public:
	S3OutputStream(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket,
				   std::string key, arrow::MemoryPool *pool)
		: client_(std::move(client)), bucket_(std::move(bucket)),
		  key_(std::move(key)), pool_(pool)
	{
	}

	~S3OutputStream() override
	{
		/* A live upload is billable, so it does not outlive the stream. */
		if (!upload_id_.empty())
			(void) AbortUpload();
	}

	arrow::Status Init()
	{
		ARROW_ASSIGN_OR_RAISE(buffer_,
							  arrow::AllocateResizableBuffer(0, pool_));
		ARROW_RETURN_NOT_OK(buffer_->Reserve(DL_S3_PART_SIZE));
		return arrow::Status::OK();
	}

	arrow::Status Write(const void *data, int64_t nbytes) override
	{
		if (closed_)
			return arrow::Status::Invalid("the stream is closed");
		if (nbytes < 0)
			return arrow::Status::Invalid("cannot write a negative length");

		const uint8_t *from = reinterpret_cast<const uint8_t *>(data);

		while (nbytes > 0)
		{
			int64_t		room = DL_S3_PART_SIZE - buffer_->size();
			int64_t		take = std::min(room, nbytes);
			int64_t		filled = buffer_->size();

			ARROW_RETURN_NOT_OK(buffer_->Resize(filled + take, false));
			memcpy(buffer_->mutable_data() + filled, from, (size_t) take);
			from += take;
			nbytes -= take;
			position_ += take;

			if (buffer_->size() == DL_S3_PART_SIZE)
				ARROW_RETURN_NOT_OK(UploadPart());
		}
		return arrow::Status::OK();
	}

	arrow::Status Flush() override { return arrow::Status::OK(); }

	arrow::Result<int64_t> Tell() const override { return position_; }

	bool closed() const override { return closed_; }

	/*
	 * Closing is not the same as being done with the upload.  A tail part or
	 * a completion can fail, and the upload is still out there afterwards --
	 * so what decides whether there is anything to clean up is whether an
	 * upload id is still live, never whether Close was called.  A failure
	 * here abandons the upload and reports the original error.
	 */
	arrow::Status Close() override
	{
		if (closed_)
			return arrow::Status::OK();
		closed_ = true;

		if (upload_id_.empty())
			return PutWholeObject();

		arrow::Status status;

		if (buffer_->size() > 0)
			status = UploadPart();
		if (status.ok())
			status = CompleteUpload();
		if (!status.ok() && !upload_id_.empty())
			(void) AbortUpload();
		return status;
	}

	arrow::Status Abort() override
	{
		closed_ = true;
		if (upload_id_.empty())
			return arrow::Status::OK();	/* nothing was ever created */
		return AbortUpload();
	}

private:
	arrow::Status StartUpload()
	{
		Aws::S3::Model::CreateMultipartUploadRequest request;

		request.SetBucket(bucket_.c_str());
		request.SetKey(key_.c_str());

		auto		outcome = client_->CreateMultipartUpload(request);

		if (!outcome.IsSuccess())
			return status_from_aws("start upload to", bucket_, key_,
								   outcome.GetError());
		upload_id_ = outcome.GetResult().GetUploadId().c_str();
		return arrow::Status::OK();
	}

	arrow::Status UploadPart()
	{
		if (upload_id_.empty())
			ARROW_RETURN_NOT_OK(StartUpload());

		/* Declared before the request, so it outlives the body stream. */
		Aws::Utils::Stream::PreallocatedStreamBuf stream_buf(
			buffer_->mutable_data(), (uint64_t) buffer_->size());
		Aws::S3::Model::UploadPartRequest request;
		int			part = (int) etags_.size() + 1;

		request.SetBucket(bucket_.c_str());
		request.SetKey(key_.c_str());
		request.SetUploadId(upload_id_.c_str());
		request.SetPartNumber(part);
		request.SetContentLength(buffer_->size());
		request.SetBody(Aws::MakeShared<Aws::IOStream>(DL_S3_ALLOC_TAG,
													   &stream_buf));

		auto		outcome = client_->UploadPart(request);

		if (!outcome.IsSuccess())
			return status_from_aws("upload part to", bucket_, key_,
								   outcome.GetError());
		etags_.push_back(outcome.GetResult().GetETag().c_str());
		return buffer_->Resize(0, false);
	}

	arrow::Status PutWholeObject()
	{
		/* Declared before the request, so it outlives the body stream. */
		Aws::Utils::Stream::PreallocatedStreamBuf stream_buf(
			buffer_->mutable_data(), (uint64_t) buffer_->size());
		Aws::S3::Model::PutObjectRequest request;

		request.SetBucket(bucket_.c_str());
		request.SetKey(key_.c_str());
		request.SetContentLength(buffer_->size());
		request.SetBody(Aws::MakeShared<Aws::IOStream>(DL_S3_ALLOC_TAG,
													   &stream_buf));

		auto		outcome = client_->PutObject(request);

		if (!outcome.IsSuccess())
			return status_from_aws("write", bucket_, key_, outcome.GetError());
		return arrow::Status::OK();
	}

	arrow::Status CompleteUpload()
	{
		Aws::S3::Model::CompletedMultipartUpload completed;
		Aws::S3::Model::CompleteMultipartUploadRequest request;

		for (size_t i = 0; i < etags_.size(); i++)
		{
			Aws::S3::Model::CompletedPart part;

			part.SetPartNumber((int) i + 1);
			part.SetETag(etags_[i].c_str());
			completed.AddParts(part);
		}

		request.SetBucket(bucket_.c_str());
		request.SetKey(key_.c_str());
		request.SetUploadId(upload_id_.c_str());
		request.SetMultipartUpload(completed);

		auto		outcome = client_->CompleteMultipartUpload(request);

		if (!outcome.IsSuccess())
		{
			arrow::Status status = status_from_aws("finish writing", bucket_,
												   key_, outcome.GetError());

			/* The parts are billable until they are abandoned explicitly. */
			(void) AbortUpload();
			return status;
		}
		upload_id_.clear();
		return arrow::Status::OK();
	}

	arrow::Status AbortUpload()
	{
		Aws::S3::Model::AbortMultipartUploadRequest request;

		request.SetBucket(bucket_.c_str());
		request.SetKey(key_.c_str());
		request.SetUploadId(upload_id_.c_str());

		auto		outcome = client_->AbortMultipartUpload(request);

		upload_id_.clear();
		if (!outcome.IsSuccess())
			return status_from_aws("abandon the upload to", bucket_, key_,
								   outcome.GetError());
		return arrow::Status::OK();
	}

	std::shared_ptr<Aws::S3::S3Client> client_;
	std::string bucket_;
	std::string key_;
	arrow::MemoryPool *pool_;
	std::shared_ptr<arrow::ResizableBuffer> buffer_;
	std::string upload_id_;
	std::vector<std::string> etags_;
	int64_t		position_ = 0;
	bool		closed_ = false;
};

/* ----------------------------------------------------------------------
 * The file system
 * ---------------------------------------------------------------------- */

class S3FileSystem : public arrow::fs::FileSystem
{
public:
	S3FileSystem(std::shared_ptr<Aws::S3::S3Client> client,
				 const arrow::io::IOContext &io_context)
		: arrow::fs::FileSystem(io_context), client_(std::move(client))
	{
	}

	std::string type_name() const override { return "dl_s3"; }

	bool Equals(const arrow::fs::FileSystem &other) const override
	{
		return this == &other;
	}

	arrow::Result<arrow::fs::FileInfo> GetFileInfo(const std::string &path) override
	{
		std::string bucket,
					key;

		split_path(path, &bucket, &key);
		if (key.empty())
		{
			/*
			 * The bucket itself.  Ask before answering: reporting a bucket
			 * that is not there as a directory would turn a typo into an
			 * empty scan instead of an error.
			 */
			Aws::S3::Model::ListObjectsV2Request probe;

			probe.SetBucket(bucket.c_str());
			probe.SetMaxKeys(1);

			auto		outcome = client_->ListObjectsV2(probe);
			arrow::fs::FileInfo info(path);

			if (!outcome.IsSuccess())
			{
				if (!is_not_found(outcome.GetError()))
					return status_from_aws("inspect", bucket, key,
										   outcome.GetError());
				info.set_type(arrow::fs::FileType::NotFound);
				return info;
			}
			info.set_type(arrow::fs::FileType::Directory);
			return info;
		}

		Aws::S3::Model::HeadObjectRequest request;

		request.SetBucket(bucket.c_str());
		request.SetKey(key.c_str());

		auto		outcome = client_->HeadObject(request);

		if (outcome.IsSuccess())
		{
			arrow::fs::FileInfo info(path);

			info.set_type(arrow::fs::FileType::File);
			info.set_size(outcome.GetResult().GetContentLength());
			info.set_mtime(std::chrono::system_clock::time_point(
				std::chrono::milliseconds(
					outcome.GetResult().GetLastModified().Millis())));
			return info;
		}
		if (!is_not_found(outcome.GetError()))
			return status_from_aws("inspect", bucket, key, outcome.GetError());

		/* No object by that name; it may still be a prefix with objects. */
		ARROW_ASSIGN_OR_RAISE(bool is_prefix, PrefixExists(bucket, key));

		arrow::fs::FileInfo info(path);

		info.set_type(is_prefix ? arrow::fs::FileType::Directory :
					  arrow::fs::FileType::NotFound);
		return info;
	}

	arrow::Result<arrow::fs::FileInfoVector> GetFileInfo(
		const arrow::fs::FileSelector &select) override
	{
		std::string bucket,
					key;
		arrow::fs::FileInfoVector infos;
		Aws::String token;
		bool		more = true;

		split_path(select.base_dir, &bucket, &key);

		const std::string prefix = key.empty() ? std::string() : key + "/";

		while (more)
		{
			Aws::S3::Model::ListObjectsV2Request request;

			request.SetBucket(bucket.c_str());
			if (!prefix.empty())
				request.SetPrefix(prefix.c_str());
			if (!select.recursive)
				request.SetDelimiter("/");
			if (!token.empty())
				request.SetContinuationToken(token);

			auto		outcome = client_->ListObjectsV2(request);

			if (!outcome.IsSuccess())
				return status_from_aws("list", bucket, key, outcome.GetError());

			const auto &result = outcome.GetResult();

			for (const auto &object : result.GetContents())
			{
				std::string object_key(object.GetKey().c_str());

				/* The prefix marker some tools write for an empty folder. */
				if (!object_key.empty() && object_key.back() == '/')
					continue;

				arrow::fs::FileInfo info(bucket + "/" + object_key);

				info.set_type(arrow::fs::FileType::File);
				info.set_size(object.GetSize());
				info.set_mtime(std::chrono::system_clock::time_point(
					std::chrono::milliseconds(object.GetLastModified().Millis())));
				infos.push_back(std::move(info));
			}

			for (const auto &common : result.GetCommonPrefixes())
			{
				std::string dir(common.GetPrefix().c_str());

				while (!dir.empty() && dir.back() == '/')
					dir.pop_back();

				arrow::fs::FileInfo info(bucket + "/" + dir);

				info.set_type(arrow::fs::FileType::Directory);
				infos.push_back(std::move(info));
			}

			/*
			 * A service that says "more" without moving the token would spin
			 * here forever, and this loop is inside a C++ frame where an
			 * interrupt cannot be checked.
			 */
			Aws::String next = result.GetNextContinuationToken();

			more = result.GetIsTruncated();
			if (more && (next.empty() || next == token))
				return arrow::Status::IOError(
					"listing s3://", bucket, "/", key,
					" did not advance past a truncated page")
					.WithDetail(std::make_shared<DlStatusDetail>(DL_ERR_IO,
																 "ListStalled"));
			token = next;
		}

		/*
		 * What an empty answer means is the same question for every backend,
		 * so the facade decides it rather than each of us.
		 */
		return infos;
	}

	arrow::Result<std::shared_ptr<arrow::io::InputStream>> OpenInputStream(
		const std::string &path) override
	{
		ARROW_ASSIGN_OR_RAISE(auto file, OpenInputFile(path));
		return file;
	}

	arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(
		const std::string &path) override
	{
		std::string bucket,
					key;

		split_path(path, &bucket, &key);
		if (key.empty())
			return arrow::Status::IOError("s3://", bucket,
										  " names a bucket, not an object");

		Aws::S3::Model::HeadObjectRequest request;

		request.SetBucket(bucket.c_str());
		request.SetKey(key.c_str());

		auto		outcome = client_->HeadObject(request);

		if (!outcome.IsSuccess())
			return status_from_aws("open", bucket, key, outcome.GetError());

		return std::make_shared<S3InputFile>(client_, bucket, key,
											 outcome.GetResult().GetContentLength(),
											 io_context().pool());
	}

	arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenOutputStream(
		const std::string &path,
		const std::shared_ptr<const arrow::KeyValueMetadata> &metadata) override
	{
		std::string bucket,
					key;

		(void) metadata;
		split_path(path, &bucket, &key);
		if (key.empty())
			return arrow::Status::IOError("s3://", bucket,
										  " names a bucket, not an object");

		auto		stream = std::make_shared<S3OutputStream>(
			client_, bucket, key, io_context().pool());

		ARROW_RETURN_NOT_OK(stream->Init());
		return stream;
	}

	arrow::Status DeleteFile(const std::string &path) override
	{
		std::string bucket,
					key;

		split_path(path, &bucket, &key);
		if (key.empty())
			return arrow::Status::IOError("s3://", bucket,
										  " names a bucket, not an object");

		Aws::S3::Model::HeadObjectRequest head;

		head.SetBucket(bucket.c_str());
		head.SetKey(key.c_str());

		auto		found = client_->HeadObject(head);

		if (!found.IsSuccess())
			return status_from_aws("delete", bucket, key, found.GetError());

		Aws::S3::Model::DeleteObjectRequest request;

		request.SetBucket(bucket.c_str());
		request.SetKey(key.c_str());

		auto		outcome = client_->DeleteObject(request);

		if (!outcome.IsSuccess())
			return status_from_aws("delete", bucket, key, outcome.GetError());
		return arrow::Status::OK();
	}

	/*
	 * The rest of the interface is not part of what this module asks of a
	 * backend (see storage_backend.h), and object storage has no directories
	 * to create or rename anyway.
	 */
	arrow::Status CreateDir(const std::string &path, bool recursive) override
	{
		(void) path;
		(void) recursive;
		return arrow::Status::NotImplemented("s3: creating a directory");
	}

	arrow::Status DeleteDir(const std::string &path) override
	{
		(void) path;
		return arrow::Status::NotImplemented("s3: deleting a directory");
	}

	arrow::Status DeleteDirContents(const std::string &path,
									bool missing_dir_ok) override
	{
		(void) path;
		(void) missing_dir_ok;
		return arrow::Status::NotImplemented("s3: deleting a directory");
	}

	arrow::Status DeleteRootDirContents() override
	{
		return arrow::Status::NotImplemented("s3: deleting a directory");
	}

	arrow::Status Move(const std::string &src, const std::string &dest) override
	{
		(void) src;
		(void) dest;
		return arrow::Status::NotImplemented("s3: moving an object");
	}

	arrow::Status CopyFile(const std::string &src, const std::string &dest) override
	{
		(void) src;
		(void) dest;
		return arrow::Status::NotImplemented("s3: copying an object");
	}

	arrow::Result<std::shared_ptr<arrow::io::OutputStream>> OpenAppendStream(
		const std::string &path,
		const std::shared_ptr<const arrow::KeyValueMetadata> &metadata) override
	{
		(void) path;
		(void) metadata;
		return arrow::Status::NotImplemented("s3: appending to an object");
	}

private:
	arrow::Result<bool> PrefixExists(const std::string &bucket,
									 const std::string &key)
	{
		Aws::S3::Model::ListObjectsV2Request request;

		/*
		 * Without the trailing slash this would also match a sibling whose
		 * name merely starts with the same characters; with it, an object
		 * written as the folder marker itself still counts, which is what
		 * tools that create empty folders leave behind.
		 */
		request.SetBucket(bucket.c_str());
		request.SetPrefix((key + "/").c_str());
		request.SetMaxKeys(1);

		auto		outcome = client_->ListObjectsV2(request);

		if (!outcome.IsSuccess())
			return status_from_aws("inspect", bucket, key, outcome.GetError());
		return outcome.GetResult().GetKeyCount() > 0;
	}

	std::shared_ptr<Aws::S3::S3Client> client_;
};

/* ----------------------------------------------------------------------
 * Mounting
 * ---------------------------------------------------------------------- */

const char *
option_value(const DlKeyValue *kv, int nkv, const char *key)
{
	for (int i = 0; i < nkv; i++)
	{
		if (kv[i].key != NULL && strcmp(kv[i].key, key) == 0 &&
			kv[i].value != NULL && kv[i].value[0] != '\0')
			return kv[i].value;
	}
	return NULL;
}

bool
option_is_true(const char *value)
{
	return value != NULL &&
		(strcasecmp(value, "true") == 0 || strcasecmp(value, "on") == 0 ||
		 strcasecmp(value, "yes") == 0 || strcasecmp(value, "t") == 0 ||
		 strcasecmp(value, "y") == 0 || strcmp(value, "1") == 0);
}

arrow::Status
initialize_s3(void)
{
	/*
	 * The SDK starts threads and opens handles, neither of which survives a
	 * fork, so this may only ever run in a backend.  The facade calls it
	 * before the first mount in each process and registers the matching
	 * shutdown there.
	 */
	Assert(MyProcPid != PostmasterPid);
	Aws::InitAPI(sdk_options);
	return arrow::Status::OK();
}

void
finalize_s3(void)
{
	Aws::ShutdownAPI(sdk_options);
}

arrow::Result<DatalakeMountedFs>
mount_s3(const DatalakeLocation *location, const DlKeyValue *kv, int nkv,
		 const DatalakeStorageHost *host)
{
	if (location == NULL || location->authority == NULL ||
		location->authority[0] == '\0')
		return arrow::Status::Invalid("s3 location has no bucket");

	const char *endpoint = option_value(kv, nkv, "endpoint");
	const char *region = option_value(kv, nkv, "region");
	const char *path_style = option_value(kv, nkv, "path_style_access");
	/*
	 * Named as the DDL names them: these arrive as the options of a user
	 * mapping, and a backend reading them under some other spelling would be
	 * a second vocabulary for one thing.
	 */
	const char *access_key = option_value(kv, nkv, "access_key_id");
	const char *secret_key = option_value(kv, nkv, "secret_access_key");
	const char *session_token = option_value(kv, nkv, "session_token");

	if (endpoint == NULL)
		endpoint = location->endpoint;
	if (region == NULL)
		region = location->region;

	Aws::S3::S3ClientConfiguration config;

	config.region = region != NULL ? region : "us-east-1";

	/*
	 * Bounded rather than left to the SDK's defaults: a backend blocked on a
	 * socket is a session that cannot be cancelled, and an unreachable
	 * endpoint has to become an error while someone is still waiting for it.
	 */
	config.connectTimeoutMs = 5000;
	config.requestTimeoutMs = 300000;
	config.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(3);

	/*
	 * Virtual-host addressing asks DNS for bucket.host, which is right for
	 * AWS and wrong for most things you can run yourself.  So the server's
	 * setting decides, and where there is no setting, an explicit endpoint
	 * means path style and its absence means AWS.
	 */
	if (path_style != NULL)
		config.useVirtualAddressing = !option_is_true(path_style);
	else
		config.useVirtualAddressing = endpoint == NULL || endpoint[0] == '\0';

	if (endpoint != NULL && endpoint[0] != '\0')
	{
		std::string url(endpoint);

		if (url.compare(0, 7, "http://") == 0)
		{
			config.scheme = Aws::Http::Scheme::HTTP;
			url = url.substr(7);
		}
		else if (url.compare(0, 8, "https://") == 0)
		{
			config.scheme = Aws::Http::Scheme::HTTPS;
			url = url.substr(8);
		}
		while (!url.empty() && url.back() == '/')
			url.pop_back();
		config.endpointOverride = url.c_str();
	}

	/*
	 * Falling back to the host's own credentials because half a pair was
	 * given would run the query as whoever the host is, which is not what
	 * the user who wrote that mapping asked for.
	 */
	if ((access_key == NULL) != (secret_key == NULL))
		return arrow::Status::Invalid(
			"the user mapping has ", access_key != NULL ?
			"access_key_id but no secret_access_key" :
			"secret_access_key but no access_key_id");
	if (access_key == NULL && session_token != NULL)
		return arrow::Status::Invalid(
			"the user mapping has session_token but no access_key_id");

	std::shared_ptr<Aws::S3::S3Client> client;

	if (access_key != NULL && secret_key != NULL)
	{
		/* Credentials the user gave us, through a user mapping. */
		auto		provider =
			Aws::MakeShared<Aws::Auth::SimpleAWSCredentialsProvider>(
				DL_S3_ALLOC_TAG, access_key, secret_key,
				session_token != NULL ? session_token : "");

		client = std::make_shared<Aws::S3::S3Client>(provider, nullptr, config);
	}
	else
	{
		/* None given: whatever the environment already grants this host. */
		auto		provider =
			Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>(
				DL_S3_ALLOC_TAG);

		client = std::make_shared<Aws::S3::S3Client>(provider, nullptr, config);
	}

	DatalakeMountedFs mounted;

	mounted.fs = std::make_shared<S3FileSystem>(
		client, arrow::io::IOContext(dl_storage_host_pool(host)));
	mounted.root = std::string(location->authority) +
		(location->path_prefix != NULL ? location->path_prefix : "");
	return mounted;
}

}							/* namespace */

static const DatalakeStorageBackend s3_storage_backend = {
	DL_STORAGE_ABI_VERSION,
	sizeof(DatalakeStorageBackend),
	"s3",
	ARROW_VERSION_STRING,
	DL_STORAGE_ABI_FINGERPRINT,
	mount_s3,
	initialize_s3,
	finalize_s3
};

#else							/* !DL_HAVE_AWS_SDK */

static arrow::Result<DatalakeMountedFs>
mount_s3(const DatalakeLocation *, const DlKeyValue *, int,
		 const DatalakeStorageHost *)
{
	return arrow::Status::NotImplemented(
		"datalake_fdw was built without the AWS SDK for C++, so s3:// "
		"locations cannot be opened; rebuild the extension with "
		"AWS_SDK_PREFIX=<prefix> pointing at an installed SDK");
}

static const DatalakeStorageBackend s3_storage_backend = {
	DL_STORAGE_ABI_VERSION,
	sizeof(DatalakeStorageBackend),
	"s3",
	ARROW_VERSION_STRING,
	DL_STORAGE_ABI_FINGERPRINT,
	mount_s3,
	NULL,
	NULL
};

#endif							/* DL_HAVE_AWS_SDK */

DlErrCode
datalake_register_s3_backend(void)
{
	return datalake_register_storage_backend(&s3_storage_backend);
}

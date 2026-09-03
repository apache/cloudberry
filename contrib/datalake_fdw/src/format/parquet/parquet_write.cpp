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
 * parquet_write.cpp
 *	  Writing a Parquet file.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/parquet/parquet_write.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <cstdio>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>
#include <parquet/properties.h>

#include "format/arrow_support.h"

#include "common/dl_resource.h"
#include "common/dl_wrappers.h"
#include "format/parquet/parquet_internal.h"

struct ParquetWriter
{
	FormatWriter base;
	std::string path;
	std::shared_ptr<arrow::Schema> schema;
	std::shared_ptr<arrow::io::FileOutputStream> sink;
	std::unique_ptr<parquet::arrow::FileWriter> writer;

	/*
	 * A row group is written whole, so the batches that go into one are held
	 * until there are enough of them.  This is not a buffer we chose to add:
	 * Parquet cannot begin a row group it does not have, and the alternative
	 * -- one row group per batch -- would produce files whose row groups are
	 * a thousand rows, where a reader expects something nearer a million and
	 * pays a seek for each one.
	 */
	std::vector<std::shared_ptr<arrow::RecordBatch>> pending;
	int64_t		pending_rows;
	int64_t		row_group_size;
};

/*
 * Releases a batch unless something already has.  The C data interface clears
 * the callback when ownership moves, so this is a no-op on the path where Arrow
 * took the batch, and the release on every other path.
 */
class ParquetReleaseBatch
{
public:
	explicit ParquetReleaseBatch(struct ArrowArray *batch) : batch_(batch) {}
	~ParquetReleaseBatch()
	{
		if (batch_ != nullptr && batch_->release != nullptr)
			batch_->release(batch_);
	}

private:
	struct ArrowArray *batch_;
};

/*
 * Gives up on the file being written.  The sink is closed first and the writer
 * left to its destructor: a Parquet writer writes the footer when it closes,
 * and against a sink that is already closed it cannot -- which is what stops a
 * complete, valid, truncated file appearing at the path if the unlink does not
 * take.  What it leaves then has no footer, so nothing can read it, and that is
 * why the unlink's result is not worth reporting.
 */
static void
parquet_discard(ParquetWriter *impl)
{
	if (impl->sink != nullptr)
		(void) impl->sink->Close();

	(void) unlink(impl->path.c_str());
}

/*
 * What the resource owner calls if nothing else did.  A file that got this far
 * was never finished, so it is discarded rather than left: the same thing
 * abort() does, and for the same reason.  C linkage because a C function
 * pointer is what it is handed to.
 */
extern "C" void
parquet_writer_release(void *arg)
{
	DL_CLEANUP_GUARD_BEGIN
	{
		std::unique_ptr<ParquetWriter> impl(static_cast<ParquetWriter *>(arg));

		parquet_discard(impl.get());
	}
	DL_CLEANUP_GUARD_END;
}

/*
 * Discards the file unless the scope it guards clears it.  finish() has to get
 * rid of a file it could not complete on every way out, and one of those ways
 * is an exception that the guard around it turns into an error code -- past any
 * cleanup written as a statement.
 */
class ParquetDiscardOnFailure
{
public:
	explicit ParquetDiscardOnFailure(ParquetWriter *writer) : writer_(writer) {}
	~ParquetDiscardOnFailure()
	{
		if (writer_ != nullptr)
			parquet_discard(writer_);
	}
	void Keep() { writer_ = nullptr; }

private:
	ParquetWriter *writer_;
};

static arrow::Status
parquet_flush_row_group(ParquetWriter *impl)
{
	if (impl->pending.empty())
		return arrow::Status::OK();

	ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Table> table,
						  arrow::Table::FromRecordBatches(impl->schema,
														  impl->pending));

	impl->pending.clear();
	impl->pending_rows = 0;

	return impl->writer->WriteTable(*table, impl->row_group_size);
}

static DlErrCode
parquet_writer_write_batch(FormatWriter *writer, struct ArrowArray *batch)
{
	DlErrCode	result = DL_OK;

	if (batch == NULL)
		return DL_ARG_ERROR("write_batch");

	/*
	 * The interface promises the batch is consumed whether or not the write
	 * succeeds, and that has to hold for the ways out that are not a return:
	 * importing allocates before it takes ownership, so it can throw with the
	 * batch still live.  On the ordinary path the import has already cleared
	 * the callback and this does nothing.
	 */
	ParquetReleaseBatch release_batch(batch);

	if (writer == NULL)
		return DL_ARG_ERROR("write_batch");

	DL_ABI_GUARD_BEGIN
	{
		ParquetWriter *impl = static_cast<ParquetWriter *>(writer->impl);

		arrow::Result<std::shared_ptr<arrow::RecordBatch>> imported =
			arrow::ImportRecordBatch(batch, impl->schema);

		if (!imported.ok())
			return DlArrowStatus(imported.status(), "import an Arrow batch");

		int64_t		rows = (*imported)->num_rows();

		/*
		 * Flush before the batch that would take the group past its size, not
		 * after.  Flushing afterwards leaves a remainder that WriteTable emits
		 * as a second, tiny row group -- so a batch size that does not divide
		 * the row group size would produce exactly the file of many small row
		 * groups this buffering exists to avoid.
		 */
		if (impl->pending_rows > 0 &&
			impl->pending_rows + rows > impl->row_group_size)
		{
			arrow::Status status = parquet_flush_row_group(impl);

			if (!status.ok())
				return DlArrowStatus(status, "write a Parquet row group");
		}

		impl->pending_rows += rows;
		impl->pending.push_back(*imported);
	}
	DL_ABI_GUARD_END(result, "write_batch");

	return result;
}

static DlErrCode
parquet_writer_bytes_written(FormatWriter *writer, int64_t *out)
{
	DlErrCode	result = DL_OK;

	if (writer == NULL || out == NULL)
		return DL_ARG_ERROR("bytes_written");

	DL_ABI_GUARD_BEGIN
	{
		ParquetWriter *impl = static_cast<ParquetWriter *>(writer->impl);

		/*
		 * What has reached the file, which trails what has been handed over:
		 * a row group is written whole, so the batches waiting for one are
		 * not in this number.  The layer that rolls files reads it to decide
		 * when a file is big enough, and the undercount costs it one row group
		 * of overshoot -- the tolerance the interface is written with.
		 */
		arrow::Result<int64_t> position = impl->sink->Tell();

		if (!position.ok())
			return DlArrowStatus(position.status(), "measure a Parquet file");

		*out = *position;
	}
	DL_ABI_GUARD_END(result, "bytes_written");

	return result;
}

static DlErrCode
parquet_writer_finish(FormatWriter **writer, FileMeta **meta)
{
	DlErrCode	result = DL_OK;

	if (writer == NULL || *writer == NULL)
		return DL_ARG_ERROR("finish_writer");

	if (meta != NULL)
		*meta = NULL;			/* what a commit needs is not collected yet */

	DL_ABI_GUARD_BEGIN
	{
		/*
		 * Taking ownership here is what makes "consumed either way" true even
		 * of the paths that leave through an exception: the caller's handle is
		 * cleared before anything that could fail.
		 */
		std::unique_ptr<ParquetWriter> impl(
			static_cast<ParquetWriter *>((*writer)->impl));
		ParquetDiscardOnFailure discard(impl.get());

		*writer = NULL;
		dl_resource_forget(parquet_writer_release, impl.get());

		/*
		 * Closing the writer is what writes the footer, so a failure here
		 * leaves an unreadable file behind and has to be reported -- this is
		 * the one place in the writer's interface where close-time errors can
		 * still reach a caller.
		 */
		arrow::Status status = parquet_flush_row_group(impl.get());

		if (status.ok())
			status = impl->writer->Close();
		if (status.ok())
			status = impl->sink->Close();

		if (!status.ok())
			return DlArrowStatus(status, "finish a Parquet file");

		discard.Keep();
	}
	DL_ABI_GUARD_END(result, "finish_writer");

	return result;
}

static void
parquet_writer_abort(FormatWriter **writer)
{
	if (writer == NULL || *writer == NULL)
		return;

	/* Cleared first; see the note in parquet_reader_close(). */
	ParquetWriter *impl = static_cast<ParquetWriter *>((*writer)->impl);

	*writer = NULL;
	dl_resource_forget(parquet_writer_release, impl);

	/*
	 * A file that was never finished has no footer, so nothing can read it and
	 * leaving it behind only costs space and confusion.  Failures are ignored:
	 * this runs while an error is already being handled.
	 */
	parquet_writer_release(impl);
}

static const FormatWriterOps parquet_writer_ops = {
	parquet_writer_write_batch,
	parquet_writer_bytes_written,
	parquet_writer_finish,
	parquet_writer_abort
};

static DlErrCode
parquet_compression(const char *name, arrow::Compression::type *out)
{
	std::string	requested(name);
	char		message[128];

	if (requested == "none" || requested == "uncompressed")
		*out = arrow::Compression::UNCOMPRESSED;
	else if (requested == "snappy")
		*out = arrow::Compression::SNAPPY;
	else if (requested == "gzip")
		*out = arrow::Compression::GZIP;
	else if (requested == "zstd")
		*out = arrow::Compression::ZSTD;
	else
	{
		snprintf(message, sizeof(message),
				 "\"%s\" is not a compression this build can write", name);
		dl_error_set(DL_ERR_INVALID_OPTION, "open a Parquet file", NULL, message);
		return DL_ERR_INVALID_OPTION;
	}

	return DL_OK;
}

DlErrCode
parquet_open_writer(const char *path, void *tupdesc_arg,
					const WriterOptions *options, FormatWriter **out)
{
	DlErrCode	result = DL_OK;

	if (out == NULL)
		return DL_ARG_ERROR("open_writer");
	*out = NULL;

	if (path == NULL || tupdesc_arg == NULL)
		return DL_ARG_ERROR("open_writer");

	DL_ABI_GUARD_BEGIN
	{
		std::unique_ptr<ParquetWriter> impl(new ParquetWriter());
		arrow::MemoryPool *pool = arrow::default_memory_pool();
		parquet::WriterProperties::Builder properties;
		arrow::Compression::type compression = arrow::Compression::SNAPPY;
		DlErrCode	rc;

		impl->path = path;
		impl->schema = DlArrowSchemaFromTupleDesc((TupleDesc) tupdesc_arg);
		if (impl->schema == nullptr)
			return DL_ERR_NOT_SUPPORTED;	/* detail already recorded */

		if (options != NULL && options->compression != NULL)
		{
			rc = parquet_compression(options->compression, &compression);
			if (rc != DL_OK)
				return rc;
		}
		properties.compression(compression);

		if (options != NULL && options->row_group_size > 0)
			properties.max_row_group_length(options->row_group_size);

		std::shared_ptr<parquet::WriterProperties> built = properties.build();

		/* Whether it was asked for or left to Parquet, this is the size. */
		impl->row_group_size = built->max_row_group_length();
		impl->pending_rows = 0;

		arrow::Result<std::shared_ptr<arrow::io::FileOutputStream>> sink =
			arrow::io::FileOutputStream::Open(path);

		if (!sink.ok())
			return DlArrowStatus(sink.status(), "create a Parquet file");
		impl->sink = *sink;

		/*
		 * The Arrow schema is deliberately not stored in the file's metadata.
		 * With it, reading back would restore the types from our own note
		 * rather than from Parquet's, and a round trip would agree with itself
		 * no matter what it had written; without it, what comes back is what
		 * any other reader of the file sees.
		 */
		/*
		 * Arrow 11 deprecated the form that returns its writer through an out
		 * parameter.  Both spellings have to be here because the versions this
		 * builds against range from 9 to 17 depending on the distribution, and
		 * the older one warns on the newer Arrow rather than failing -- which is
		 * the kind of warning that stops being read.
		 */
#if ARROW_VERSION_MAJOR >= 11
		arrow::Result<std::unique_ptr<parquet::arrow::FileWriter>> writer =
			parquet::arrow::FileWriter::Open(*impl->schema, pool, impl->sink,
											 built,
											 parquet::default_arrow_writer_properties());
		arrow::Status status = writer.status();

		if (status.ok())
			impl->writer = std::move(*writer);
#else
		arrow::Status status =
			parquet::arrow::FileWriter::Open(*impl->schema, pool, impl->sink,
											 built,
											 parquet::default_arrow_writer_properties(),
											 &impl->writer);
#endif

		if (!status.ok())
		{
			(void) impl->sink->Close();
			unlink(path);
			return DlArrowStatus(status, "create a Parquet file");
		}

		impl->base.ops = &parquet_writer_ops;
		impl->base.impl = impl.get();

		/* Last thing that may fail; see parquet_open_reader(). */
		if (!dl_resource_remember(parquet_writer_release, impl.get()))
		{
			parquet_discard(impl.get());
			dl_error_set(DL_ERR_INTERNAL, "open_writer", NULL,
						 "could not record the open file for cleanup");
			return DL_ERR_INTERNAL;
		}

		*out = &impl.release()->base;
	}
	DL_ABI_GUARD_END(result, "open_writer");

	return result;
}

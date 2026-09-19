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
 * parquet_read.cpp
 *	  Reading a Parquet file.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/parquet/parquet_read.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <algorithm>
#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/properties.h>

#include "format/arrow_support.h"

#include "am_iceberg/pg_iceberg_guc.h"
#include "common/dl_resource.h"
#include "common/dl_wrappers.h"
#include "format/arrow_memory_pool.h"
#include "format/parquet/parquet_internal.h"

struct ParquetReader
{
	FormatReader base;
	std::shared_ptr<arrow::io::RandomAccessFile> file;
	std::unique_ptr<parquet::arrow::FileReader> reader;
	std::shared_ptr<arrow::RecordBatchReader> batches;

	/*
	 * How a batch the file yields becomes the batch the caller asked for: one
	 * entry per requested field, holding that field's position among the
	 * columns read, or -1 for a field the file does not have.  Empty when the
	 * caller asked for the file as it is, and batches pass straight through.
	 */
	std::vector<int> output_columns;
	std::shared_ptr<arrow::Schema> output_schema;
};

static DlErrCode
parquet_reader_next_batch(FormatReader *reader, struct ArrowArray *out,
						  struct ArrowSchema *schema, bool *eof)
{
	DlErrCode	result = DL_OK;

	if (reader == NULL || out == NULL || eof == NULL)
		return DL_ARG_ERROR("next_batch");

	*eof = false;

	DL_ABI_GUARD_BEGIN
	{
		ParquetReader *impl = static_cast<ParquetReader *>(reader->impl);
		std::shared_ptr<arrow::RecordBatch> batch;
		arrow::Status status = impl->batches->ReadNext(&batch);

		if (!status.ok())
			return DlArrowStatus(status, "read a Parquet batch");

		if (batch == nullptr)
		{
			*eof = true;
			return DL_OK;
		}

		if (impl->output_columns.empty())
			return DlArrowStatus(arrow::ExportRecordBatch(*batch, out, schema),
								 "export a Parquet batch");

		std::vector<std::shared_ptr<arrow::Array>> columns;

		columns.reserve(impl->output_columns.size());

		for (int source : impl->output_columns)
		{
			if (source >= 0)
			{
				columns.push_back(batch->column(source));
				continue;
			}

			/*
			 * A field the file does not have: NULL in every row, which is what
			 * the Iceberg spec says a column added after the file was written
			 * holds.  The null type carries no buffers, so this costs nothing
			 * per row.
			 */
			arrow::Result<std::shared_ptr<arrow::Array>> nulls =
				arrow::MakeArrayOfNull(arrow::null(), batch->num_rows(),
									   DlArrowMemoryPool());

			if (!nulls.ok())
				return DlArrowStatus(nulls.status(), "read a Parquet batch");

			columns.push_back(*nulls);
		}

		std::shared_ptr<arrow::RecordBatch> projected =
			arrow::RecordBatch::Make(impl->output_schema, batch->num_rows(),
									 std::move(columns));

		return DlArrowStatus(arrow::ExportRecordBatch(*projected, out, schema),
							 "export a Parquet batch");
	}
	DL_ABI_GUARD_END(result, "next_batch");

	return result;
}

/*
 * What the resource owner calls if nothing else did.  C linkage because a C
 * function pointer is what it is handed to.
 */
extern "C" void
parquet_reader_release(void *arg)
{
	DL_CLEANUP_GUARD_BEGIN
	{
		delete static_cast<ParquetReader *>(arg);
	}
	DL_CLEANUP_GUARD_END;
}

static void
parquet_reader_close(FormatReader **reader)
{
	if (reader == NULL || *reader == NULL)
		return;

	/*
	 * Clear the caller's handle first.  DL_CLEANUP_GUARD_END can elog(WARNING),
	 * and an escalation there would longjmp past the assignment -- leaving the
	 * caller holding a reader that has already been released, which is the
	 * thing taking the handle by address exists to prevent.
	 */
	ParquetReader *impl = static_cast<ParquetReader *>((*reader)->impl);

	*reader = NULL;
	dl_resource_forget(parquet_reader_release, impl);

	parquet_reader_release(impl);
}

static const FormatReaderOps parquet_reader_ops = {
	parquet_reader_next_batch,
	parquet_reader_close
};

/*
 * Which row groups this fragment covers.  A fragment is a range rather than a
 * whole file so that one large file can be read by several segments at once;
 * an empty range is legal and reads nothing.
 */
static DlErrCode
parquet_row_groups(const Fragment *fragment, int total,
				   std::vector<int> *row_groups)
{
	int			first = fragment->first_row_group;
	int			count = fragment->n_row_groups;

	/*
	 * The last test is written as a subtraction because the addition it
	 * replaces overflows: first + INT_MAX wraps negative, passes the check, and
	 * the loop below then builds a two-billion-element vector out of a range
	 * that should have been rejected.
	 */
	if (first < 0 || first > total || count < 0 || count > total - first)
	{
		char		message[160];

		snprintf(message, sizeof(message),
				 "row groups %d..%d were asked for from a file that has %d",
				 first, count > 0 ? first + count - 1 : first, total);
		dl_error_set(DL_ERR_INVALID_OPTION, "open a Parquet file", NULL, message);
		return DL_ERR_INVALID_OPTION;
	}

	if (count == 0)
		count = total - first;

	for (int i = 0; i < count; i++)
		row_groups->push_back(first + i);

	return DL_OK;
}

/*
 * Which of the file's columns to read, and how to lay them out for the caller.
 *
 * Matching is by field id -- see ProjectionSet -- and never by position or by
 * name.  The columns are read in file order whatever order they were asked for
 * in, because file order is the order Parquet's reader hands them back in;
 * output_columns is what then puts them in the caller's order, and supplies
 * the fields the file does not have.
 */
static DlErrCode
parquet_project(ParquetReader *impl, const arrow::Schema &file_schema,
				const ProjectionSet *projection, std::vector<int> *columns)
{
	std::map<int32_t, int> file_column_by_id;
	std::vector<std::shared_ptr<arrow::Field>> fields;
	char		message[160];

	for (int i = 0; i < file_schema.num_fields(); i++)
	{
		int32_t		field_id = DlArrowFieldId(*file_schema.field(i));

		/* A column that carries no id can never be matched to anything. */
		if (field_id < 0)
			continue;

		/*
		 * Two columns with one id is a file nothing can read by id.  Taking
		 * either would be guessing about data.
		 */
		if (!file_column_by_id.emplace(field_id, i).second)
		{
			snprintf(message, sizeof(message),
					 "the file has two columns with field id %d", (int) field_id);
			dl_error_set(DL_ERR_INVALID_OPTION, "open a Parquet file", NULL, message);
			return DL_ERR_INVALID_OPTION;
		}
	}

	/* The file columns to read: ascending, each once. */
	for (int k = 0; k < projection->nfields; k++)
	{
		auto		found = file_column_by_id.find(projection->field_ids[k]);

		if (found != file_column_by_id.end())
			columns->push_back(found->second);
	}
	std::sort(columns->begin(), columns->end());
	columns->erase(std::unique(columns->begin(), columns->end()), columns->end());

	fields.reserve(projection->nfields);
	impl->output_columns.reserve(projection->nfields);

	for (int k = 0; k < projection->nfields; k++)
	{
		int32_t		field_id = projection->field_ids[k];
		auto		found = file_column_by_id.find(field_id);

		if (found == file_column_by_id.end())
		{
			impl->output_columns.push_back(-1);
			fields.push_back(arrow::field("field_id_" + std::to_string(field_id),
										  arrow::null()));
			continue;
		}

		impl->output_columns.push_back(
			(int) (std::lower_bound(columns->begin(), columns->end(),
									found->second) - columns->begin()));
		fields.push_back(file_schema.field(found->second));
	}

	impl->output_schema = arrow::schema(fields);

	return DL_OK;
}

DlErrCode
parquet_open_reader(const Fragment *fragment, const ProjectionSet *projection,
					const RowGroupFilterSet *filters, FormatReader **out)
{
	DlErrCode	result = DL_OK;

	if (out == NULL)
		return DL_ARG_ERROR("open_reader");
	*out = NULL;

	if (fragment == NULL || fragment->path == NULL)
		return DL_ARG_ERROR("open_reader");

	/*
	 * Statistics-based row group pruning is not implemented.  Accepting the
	 * filters and ignoring them would still give the right rows, so nothing
	 * would fail -- which is exactly why it is refused instead: a caller that
	 * believed the pruning had happened would have no way to find out.
	 */
	if (filters != NULL)
	{
		dl_error_set(DL_ERR_NOT_SUPPORTED, "open a Parquet file", NULL,
					 "row group filtering is not implemented yet");
		return DL_ERR_NOT_SUPPORTED;
	}

	DL_ABI_GUARD_BEGIN
	{
		std::unique_ptr<ParquetReader> impl(new ParquetReader());
		arrow::MemoryPool *pool = DlArrowMemoryPool();
		std::shared_ptr<arrow::Schema> file_schema;
		std::vector<int> row_groups;
		std::vector<int> columns;
		DlErrCode	rc;

		parquet::arrow::FileReaderBuilder builder;
		parquet::ArrowReaderProperties properties;

		arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> file =
			arrow::io::ReadableFile::Open(fragment->path, pool);

		if (!file.ok())
			return DlArrowStatus(file.status(), "open a Parquet file");
		impl->file = *file;

		/*
		 * The properties are passed so that the pool is: left to its default
		 * argument, Open() would decode Parquet pages out of Arrow's own pool,
		 * unseen by the memory accounting the module's pool exists for.
		 */
		arrow::Status status = builder.Open(impl->file,
											parquet::ReaderProperties(pool));

		if (!status.ok())
			return DlArrowStatus(status, "open a Parquet file");

		/* The same batch size the write side accumulates to. */
		properties.set_batch_size(iceberg_batch_rows);

		/*
		 * A backend is not a thread pool.  Arrow will read column chunks in
		 * parallel if asked, and a worker thread that hits an error has no way
		 * to report it through PostgreSQL's error handling, so this reads on
		 * the thread it was called on.  Pre-buffering is the other way Arrow
		 * moves work onto its I/O threads -- and from Arrow 13 it is on by
		 * default -- so it is switched off by name rather than left to a
		 * default that changes between the versions this builds against.
		 */
		properties.set_use_threads(false);
		properties.set_pre_buffer(false);

		/*
		 * Spark, Hive and Impala wrote timestamps as INT96 for years, and
		 * Arrow surfaces those as nanoseconds unless told otherwise.  A
		 * PostgreSQL timestamp is microseconds, so that is what they are read
		 * as: INT96 has no unit of its own to lose, and nanoseconds would
		 * confine the values to 1677..2262 -- the "end of time" dates a
		 * warehouse keeps as sentinels lie outside that, and Arrow wraps them
		 * silently rather than refusing.
		 *
		 * One caveat, Arrow's rather than ours: INT96 is a day number plus the
		 * nanoseconds into that day, and Arrow's microsecond conversion takes
		 * the second part as non-negative, which is what the writers above
		 * store.  pyarrow's deprecated INT96 writer stores a negative one for
		 * instants before 1970, and those come back wrong -- from pyarrow
		 * itself with the same setting, as from here.
		 */
		properties.set_coerce_int96_timestamp_unit(arrow::TimeUnit::MICRO);

		status = builder.memory_pool(pool)->properties(properties)
			->Build(&impl->reader);

		if (!status.ok())
			return DlArrowStatus(status, "open a Parquet file");

		rc = parquet_row_groups(fragment, impl->reader->num_row_groups(),
								&row_groups);
		if (rc != DL_OK)
			return rc;

		status = impl->reader->GetSchema(&file_schema);
		if (!status.ok())
			return DlArrowStatus(status, "read a Parquet schema");

		if (projection != NULL && projection->nfields > 0)
		{
			rc = parquet_project(impl.get(), *file_schema, projection, &columns);
			if (rc != DL_OK)
				return rc;
		}
		else
		{
			for (int i = 0; i < file_schema->num_fields(); i++)
				columns.push_back(i);
		}

		/*
		 * Arrow 21 deprecates this in favour of a Result-returning one that
		 * Arrow 9 does not have, so whoever raises the floor past 21 gets a
		 * warning here and a version guard to write -- the one around
		 * FileWriter::Open in parquet_write.cpp is the shape of it.
		 */
		status = impl->reader->GetRecordBatchReader(row_groups, columns,
													&impl->batches);
		if (!status.ok())
			return DlArrowStatus(status, "open a Parquet batch reader");

		impl->base.ops = &parquet_reader_ops;
		impl->base.impl = impl.get();

		/*
		 * The file is open from here, so this is the last thing that may fail:
		 * past it, nothing can lose track of the descriptor.
		 */
		if (!dl_resource_remember(parquet_reader_release, impl.get()))
		{
			dl_error_set(DL_ERR_INTERNAL, "open_reader", NULL,
						 "could not record the open file for cleanup");
			return DL_ERR_INTERNAL;
		}

		*out = &impl.release()->base;
	}
	DL_ABI_GUARD_END(result, "open_reader");

	return result;
}

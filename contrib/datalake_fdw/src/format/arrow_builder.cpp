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
 * arrow_builder.cpp
 *	  Accumulation of PostgreSQL tuples into an Arrow batch.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/arrow_builder.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <memory>
#include <vector>

#include <arrow/api.h>
#include <arrow/c/bridge.h>

#include "format/arrow_support.h"

#include "common/dl_resource.h"
#include "common/dl_wrappers.h"
#include "format/arrow_builder.h"

extern "C"
{
#include "catalog/pg_type.h"
#include "utils/date.h"
#include "utils/timestamp.h"
#include "varatt.h"
}

/*
 * PostgreSQL counts from 2000-01-01 and Arrow from 1970-01-01.  Everything
 * below that touches a date or a timestamp shifts by this, and getting the sign
 * wrong is a 30-year error that no round trip through our own code would
 * notice -- both halves would agree.  It is written once, here.
 */
#define DL_EPOCH_DELTA_DAYS		((int32) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE))
#define DL_EPOCH_DELTA_USECS	(((int64) DL_EPOCH_DELTA_DAYS) * USECS_PER_DAY)

struct DlArrowBuilderData
{
	std::shared_ptr<arrow::Schema> schema;
	std::vector<Oid> types;
	std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders;
	int64_t		nrows;
};

/*
 * One value into the builder for its column.  The Datum has already been
 * detoasted by the caller, so nothing here allocates and nothing here can
 * raise.
 */
static arrow::Status
dl_append_datum(arrow::ArrayBuilder *builder, Oid atttypid, Datum value)
{
	switch (atttypid)
	{
		case BOOLOID:
			return static_cast<arrow::BooleanBuilder *>(builder)
				->Append(DatumGetBool(value));
		case INT2OID:
			return static_cast<arrow::Int16Builder *>(builder)
				->Append(DatumGetInt16(value));
		case INT4OID:
			return static_cast<arrow::Int32Builder *>(builder)
				->Append(DatumGetInt32(value));
		case INT8OID:
			return static_cast<arrow::Int64Builder *>(builder)
				->Append(DatumGetInt64(value));
		case FLOAT4OID:
			return static_cast<arrow::FloatBuilder *>(builder)
				->Append(DatumGetFloat4(value));
		case FLOAT8OID:
			return static_cast<arrow::DoubleBuilder *>(builder)
				->Append(DatumGetFloat8(value));

		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
			{
				struct varlena *v = (struct varlena *) DatumGetPointer(value);

				return static_cast<arrow::StringBuilder *>(builder)
					->Append(VARDATA_ANY(v), VARSIZE_ANY_EXHDR(v));
			}

		case BYTEAOID:
			{
				struct varlena *v = (struct varlena *) DatumGetPointer(value);

				return static_cast<arrow::BinaryBuilder *>(builder)
					->Append(VARDATA_ANY(v), VARSIZE_ANY_EXHDR(v));
			}

		case DATEOID:
			{
				DateADT		date = DatumGetDateADT(value);

				/*
				 * Parquet has no way to say "infinity", and writing the
				 * sentinel would hand the next reader a date 5.8 million years
				 * out as if it were a real one.
				 */
				if (DATE_NOT_FINITE(date))
					return arrow::Status::NotImplemented(
						"an infinite date cannot be written to a data file");

				return static_cast<arrow::Date32Builder *>(builder)
					->Append(date + DL_EPOCH_DELTA_DAYS);
			}

		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				Timestamp	ts = DatumGetTimestamp(value);

				if (TIMESTAMP_NOT_FINITE(ts))
					return arrow::Status::NotImplemented(
						"an infinite timestamp cannot be written to a data file");

				/*
				 * PostgreSQL's range runs about 34 years past the last instant
				 * Arrow can hold in microseconds from 1970, so the shift below
				 * is not always representable.  Without this the addition wraps
				 * -- quietly, because the build sets -fwrapv -- and a year
				 * 294250 timestamp is written as one 292000 years before the
				 * epoch, with the write reporting success.  The read side has
				 * the mirror of this guard, and neither can stand in for the
				 * other: a round trip through both would agree.
				 */
				if (ts > PG_INT64_MAX - DL_EPOCH_DELTA_USECS)
					return arrow::Status::Invalid(
						"timestamp is too far in the future to be written to a "
						"data file");

				return static_cast<arrow::TimestampBuilder *>(builder)
					->Append(ts + DL_EPOCH_DELTA_USECS);
			}

		default:

			/*
			 * Unreachable until a new type is added to one of the two
			 * switches and not the other, which is why it names the OID.  A
			 * number and not a name: resolving one means a catalog lookup, and
			 * nothing on this side of the ABI may allocate or raise.
			 */
			return arrow::Status::NotImplemented(
				"no Arrow type is mapped for PostgreSQL type OID ", atttypid);
	}
}

/* What the resource owner calls if nothing else did. */
extern "C" void
dl_arrow_builder_release(void *arg)
{
	DL_CLEANUP_GUARD_BEGIN
	{
		delete static_cast<DlArrowBuilderData *>(arg);
	}
	DL_CLEANUP_GUARD_END;
}

extern "C" DlErrCode
dl_arrow_builder_open(void *tupdesc_arg, DlArrowBuilder *out)
{
	DlErrCode	result = DL_OK;

	if (out == NULL)
		return DL_ARG_ERROR("open_builder");
	*out = NULL;

	if (tupdesc_arg == NULL)
		return DL_ARG_ERROR("open_builder");

	DL_ABI_GUARD_BEGIN
	{
		TupleDesc	tupdesc = (TupleDesc) tupdesc_arg;
		std::unique_ptr<DlArrowBuilderData> builder(new DlArrowBuilderData());

		builder->schema = DlArrowSchemaFromTupleDesc(tupdesc);
		if (builder->schema == nullptr)
			return DL_ERR_NOT_SUPPORTED;	/* detail already recorded */

		builder->nrows = 0;
		builder->types.reserve(tupdesc->natts);
		builder->builders.reserve(tupdesc->natts);

		for (int i = 0; i < tupdesc->natts; i++)
		{
			std::unique_ptr<arrow::ArrayBuilder> column;
			arrow::Status status = arrow::MakeBuilder(arrow::default_memory_pool(),
													  builder->schema->field(i)->type(),
													  &column);

			if (!status.ok())
				return DlArrowStatus(status, "create an Arrow array builder");

			builder->types.push_back(TupleDescAttr(tupdesc, i)->atttypid);
			builder->builders.push_back(std::move(column));
		}

		/*
		 * The buffers behind the builder come from Arrow's allocator, which
		 * transaction abort knows nothing about.  Last thing that may fail.
		 */
		if (!dl_resource_remember(dl_arrow_builder_release, builder.get()))
		{
			dl_error_set(DL_ERR_INTERNAL, "open_builder", NULL,
						 "could not record the batch builder for cleanup");
			return DL_ERR_INTERNAL;
		}

		*out = builder.release();
	}
	DL_ABI_GUARD_END(result, "open_builder");

	return result;
}

extern "C" DlErrCode
dl_arrow_builder_append(DlArrowBuilder builder, const Datum *values,
						const bool *nulls, int nvalues)
{
	DlErrCode	result = DL_OK;

	if (builder == NULL || values == NULL || nulls == NULL)
		return DL_ARG_ERROR("append_row");

	if (nvalues != (int) builder->builders.size())
	{
		dl_error_set(DL_ERR_INTERNAL, "append an Arrow row", NULL,
					 "the row has a different number of columns than the batch");
		return DL_ERR_INTERNAL;
	}

	DL_ABI_GUARD_BEGIN
	{
		for (int i = 0; i < nvalues; i++)
		{
			arrow::Status status = nulls[i]
				? builder->builders[i]->AppendNull()
				: dl_append_datum(builder->builders[i].get(), builder->types[i],
								  values[i]);

			if (!status.ok())
				return DlArrowStatus(status, "append a value to an Arrow array");
		}

		builder->nrows++;
	}
	DL_ABI_GUARD_END(result, "append_row");

	return result;
}

extern "C" int64_t
dl_arrow_builder_nrows(DlArrowBuilder builder)
{
	return builder == NULL ? 0 : builder->nrows;
}

extern "C" DlErrCode
dl_arrow_builder_flush(DlArrowBuilder builder, struct ArrowArray *out)
{
	DlErrCode	result = DL_OK;

	if (builder == NULL || out == NULL)
		return DL_ARG_ERROR("build_batch");

	DL_ABI_GUARD_BEGIN
	{
		std::vector<std::shared_ptr<arrow::Array>> columns;

		columns.reserve(builder->builders.size());

		for (auto &column : builder->builders)
		{
			std::shared_ptr<arrow::Array> array;

			/* Finish() also resets the builder, so the next batch starts here. */
			arrow::Status status = column->Finish(&array);

			if (!status.ok())
				return DlArrowStatus(status, "finish an Arrow array");

			columns.push_back(std::move(array));
		}

		std::shared_ptr<arrow::RecordBatch> batch =
			arrow::RecordBatch::Make(builder->schema, builder->nrows, columns);

		/*
		 * The schema travels with the writer, which was opened from the same
		 * descriptor, so exporting it with every batch would be a copy nobody
		 * reads.
		 */
		arrow::Status status = arrow::ExportRecordBatch(*batch, out, nullptr);

		if (!status.ok())
			return DlArrowStatus(status, "export an Arrow batch");

		builder->nrows = 0;
	}
	DL_ABI_GUARD_END(result, "build_batch");

	return result;
}

extern "C" void
dl_arrow_builder_close(DlArrowBuilder *builder)
{
	if (builder == NULL || *builder == NULL)
		return;

	/* Cleared first; see the note in parquet_reader_close(). */
	DlArrowBuilderData *impl = *builder;

	*builder = NULL;
	dl_resource_forget(dl_arrow_builder_release, impl);

	dl_arrow_builder_release(impl);
}

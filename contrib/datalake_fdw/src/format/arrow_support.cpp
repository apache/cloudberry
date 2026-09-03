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
 * arrow_support.cpp
 *	  The type mapping and the error translation shared by the Arrow-facing
 *	  parts of this module.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/arrow_support.cpp
 *
 *-------------------------------------------------------------------------
 */

/*
 * Arrow's headers come first throughout this module: PostgreSQL's c.h defines
 * Abs, Min and Max as macros, and a template header has no way to defend
 * itself against them.
 */
#include <string>
#include <vector>

#include <arrow/api.h>

#include "format/arrow_support.h"

extern "C"
{
#include "catalog/pg_type.h"
}

DlErrCode
DlArrowStatus(const arrow::Status &status, const char *operation)
{
	DlErrCode	code;

	if (status.ok())
		return DL_OK;

	switch (status.code())
	{
		case arrow::StatusCode::IOError:
			code = DL_ERR_IO;
			break;
		case arrow::StatusCode::NotImplemented:
			code = DL_ERR_NOT_SUPPORTED;
			break;
		case arrow::StatusCode::Invalid:
		case arrow::StatusCode::TypeError:
		case arrow::StatusCode::KeyError:
			code = DL_ERR_INVALID_OPTION;
			break;
		default:
			code = DL_ERR_INTERNAL;
			break;
	}

	dl_error_set(code, operation, arrow::Status::CodeAsString(status.code()).c_str(),
				 status.message().c_str());
	return code;
}

std::shared_ptr<arrow::DataType>
DlArrowTypeForPgType(Oid atttypid)
{
	switch (atttypid)
	{
		case BOOLOID:
			return arrow::boolean();
		case INT2OID:
			return arrow::int16();
		case INT4OID:
			return arrow::int32();
		case INT8OID:
			return arrow::int64();
		case FLOAT4OID:
			return arrow::float32();
		case FLOAT8OID:
			return arrow::float64();

			/*
			 * All three of PostgreSQL's string types are one Arrow type: the
			 * length limit is a constraint PostgreSQL enforces before a value
			 * reaches us, and Parquet has nowhere to record it.  A char(n)
			 * value arrives already padded, so what is written is what
			 * PostgreSQL stores.
			 */
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
			return arrow::utf8();

		case BYTEAOID:
			return arrow::binary();
		case DATEOID:
			return arrow::date32();

			/*
			 * PostgreSQL keeps both timestamp types in microseconds, so
			 * microseconds is the unit that loses nothing.  timestamptz is a
			 * point in time held in UTC, which is exactly what an Arrow
			 * timestamp with a "UTC" zone means; timestamp without time zone
			 * has no zone, and Arrow says that by leaving it empty.
			 */
		case TIMESTAMPOID:
			return arrow::timestamp(arrow::TimeUnit::MICRO);
		case TIMESTAMPTZOID:
			return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");

		default:
			return nullptr;
	}
}

std::shared_ptr<arrow::Schema>
DlArrowSchemaFromTupleDesc(TupleDesc tupdesc)
{
	std::vector<std::shared_ptr<arrow::Field>> fields;

	fields.reserve(tupdesc->natts);

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		/*
		 * A dropped column has no type to write and no name worth recording.
		 * Leaving a placeholder in the file would keep column positions
		 * aligned, but nothing reads such a file yet, so refusing is the
		 * answer that cannot be silently wrong.
		 */
		if (attr->attisdropped)
		{
			dl_error_set(DL_ERR_NOT_SUPPORTED, "arrow schema", nullptr,
						 "a dropped column cannot be written to a data file");
			return nullptr;
		}

		std::shared_ptr<arrow::DataType> type = DlArrowTypeForPgType(attr->atttypid);

		if (type == nullptr)
		{
			std::string message = std::string("column \"") +
				NameStr(attr->attname) + "\" has a type that lake tables "
				"cannot store yet";

			dl_error_set(DL_ERR_NOT_SUPPORTED, "arrow schema", nullptr,
						 message.c_str());
			return nullptr;
		}

		/*
		 * Every field is nullable, including one PostgreSQL marked NOT NULL.
		 * Recording it as required would buy nothing -- PostgreSQL has already
		 * rejected the nulls before a tuple reaches this layer -- and would
		 * turn any later relaxation of the constraint into a write failure
		 * against files already on disk.
		 */
		fields.push_back(arrow::field(NameStr(attr->attname), type,
									  /* nullable */ true));
	}

	return arrow::schema(fields);
}

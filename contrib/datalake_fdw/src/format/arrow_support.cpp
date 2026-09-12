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
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include <arrow/api.h>

#include "format/arrow_support.h"

extern "C"
{
#include "catalog/pg_type.h"
#include "mb/pg_wchar.h"
#include "utils/uuid.h"
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
		case arrow::StatusCode::OutOfMemory:
			code = DL_ERR_OUT_OF_MEMORY;
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
			 * Both of PostgreSQL's unbounded string types are the one string
			 * type a lake table has.  The bounded forms -- varchar(n), char(n)
			 * -- are refused by dl_format_type_refusal() rather than mapped:
			 * Parquet has nowhere to record the bound, so a file written by
			 * anything else could hold values that break it.
			 */
		case TEXTOID:
		case VARCHAROID:
			return arrow::utf8();

		case BYTEAOID:
			return arrow::binary();
		case DATEOID:
			return arrow::date32();

			/*
			 * PostgreSQL keeps a time as microseconds since midnight, which is
			 * exactly what an Iceberg time is; no epoch to shift.
			 */
		case TIMEOID:
			return arrow::time64(arrow::TimeUnit::MICRO);

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

			/*
			 * An Iceberg uuid is 16 bytes, and so is PostgreSQL's -- the same
			 * 16 bytes, in the same order.
			 */
		case UUIDOID:
			return arrow::fixed_size_binary(UUID_LEN);

		default:
			return nullptr;
	}
}

extern "C" const char *
dl_format_type_refusal(Oid typid, int32 typmod)
{
	if (typid == BPCHAROID)
		return "is padded to a declared length, and a lake table has no way to "
			"store that; use text";

	if (typid == VARCHAROID && typmod >= 0)
		return "has a length limit, and a lake table has no way to store one; "
			"use text or varchar without a length";

	if (DlArrowTypeForPgType(typid) == nullptr)
		return "cannot be stored in a lake table";

	return nullptr;
}

std::shared_ptr<arrow::Schema>
DlArrowSchemaFromTupleDesc(TupleDesc tupdesc, const int32_t *field_ids)
{
	std::vector<std::shared_ptr<arrow::Field>> fields;
	char		message[256];
	int32_t		next_field_id = 1;

	/*
	 * Parquet defines its string type as UTF-8 and its column names likewise,
	 * and every other reader of the file will take them as such.  Bytes in any
	 * other encoding would be written under a label that lies about them, so
	 * the question is settled once, here, for the whole descriptor.
	 */
	if (GetDatabaseEncoding() != PG_UTF8)
	{
		snprintf(message, sizeof(message),
				 "a lake table stores strings and column names as UTF-8, and "
				 "this database's encoding is %s", GetDatabaseEncodingName());
		dl_error_set(DL_ERR_NOT_SUPPORTED, "arrow schema", nullptr, message);
		return nullptr;
	}

	fields.reserve(tupdesc->natts);

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		const char *refusal;
		int32_t		field_id;

		/*
		 * A dropped column is a tombstone in the descriptor, not a column of
		 * the table: it has no name and no type, and the file has no place
		 * for it.  Skipping it is what lets a table stay writable after ALTER
		 * TABLE DROP COLUMN; the batch builder skips the same attributes.
		 */
		if (attr->attisdropped)
			continue;

		refusal = dl_format_type_refusal(attr->atttypid, attr->atttypmod);
		if (refusal != nullptr)
		{
			snprintf(message, sizeof(message), "column \"%s\" has a type that %s",
					 NameStr(attr->attname), refusal);
			dl_error_set(DL_ERR_NOT_SUPPORTED, "arrow schema", nullptr, message);
			return nullptr;
		}

		/*
		 * Parquet's bridge writes a negative id as no id at all, silently.  A
		 * caller that hands one over has a bug, and a file whose columns can
		 * never be matched is not the way to find out about it.
		 */
		field_id = field_ids != nullptr ? field_ids[i] : next_field_id++;
		if (field_id < 0)
		{
			snprintf(message, sizeof(message), "column \"%s\" has no field id",
					 NameStr(attr->attname));
			dl_error_set(DL_ERR_INTERNAL, "arrow schema", nullptr, message);
			return nullptr;
		}

		/*
		 * Every field is nullable, including one PostgreSQL marked NOT NULL.
		 * Recording it as required would buy nothing -- PostgreSQL has already
		 * rejected the nulls before a tuple reaches this layer -- and would
		 * turn any later relaxation of the constraint into a write failure
		 * against files already on disk.
		 */
		fields.push_back(arrow::field(NameStr(attr->attname),
									  DlArrowTypeForPgType(attr->atttypid),
									  /* nullable */ true,
									  arrow::key_value_metadata(
										  {DL_ARROW_FIELD_ID_KEY},
										  {std::to_string(field_id)})));
	}

	return arrow::schema(fields);
}

int32_t
DlArrowFieldId(const arrow::Field &field)
{
	const std::shared_ptr<const arrow::KeyValueMetadata> &metadata = field.metadata();
	int			key;
	const char *text;
	char	   *end;
	long		id;

	if (metadata == nullptr)
		return -1;

	key = metadata->FindKey(DL_ARROW_FIELD_ID_KEY);
	if (key < 0)
		return -1;

	/*
	 * Parquet's bridge wrote this from an int, so anything that is not one is
	 * not a field id, whatever else it might be.
	 */
	text = metadata->value(key).c_str();
	id = strtol(text, &end, 10);
	if (end == text || *end != '\0' || id < 0 || id > INT32_MAX)
		return -1;

	return (int32_t) id;
}

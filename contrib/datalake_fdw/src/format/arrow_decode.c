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
 * arrow_decode.c
 *	  PostgreSQL values out of an Arrow batch.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/arrow_decode.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <string.h>

#include "catalog/pg_type.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/fmgrprotos.h"
#include "utils/timestamp.h"
#include "varatt.h"

#include "format/arrow_decode.h"

/*
 * The same shift as in arrow_builder.cpp, in the other direction: PostgreSQL
 * counts from 2000-01-01 and Arrow from 1970-01-01.
 */
#define DL_EPOCH_DELTA_DAYS		((int32) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE))
#define DL_EPOCH_DELTA_USECS	(((int64) DL_EPOCH_DELTA_DAYS) * USECS_PER_DAY)

/*
 * Arrow spells its types as a short string.  Only the ones a column of ours can
 * be stored as are listed; anything else is a file we did not write, or one
 * written by a version that knows more types than this one.
 */
#define DL_ARROW_FORMAT_BOOL		"b"
#define DL_ARROW_FORMAT_INT16		"s"
#define DL_ARROW_FORMAT_INT32		"i"
#define DL_ARROW_FORMAT_INT64		"l"
#define DL_ARROW_FORMAT_FLOAT32		"f"
#define DL_ARROW_FORMAT_FLOAT64		"g"
#define DL_ARROW_FORMAT_UTF8		"u"
#define DL_ARROW_FORMAT_BINARY		"z"
#define DL_ARROW_FORMAT_DATE32		"tdD"

/* A timestamp is "tsu:" followed by the time zone, which may be empty. */
#define DL_ARROW_FORMAT_TIMESTAMP_US	"tsu:"

static DlErrCode
dl_arrow_decode_refuse(const char *arrow_format, Oid atttypid)
{
	char		message[256];

	snprintf(message, sizeof(message),
			 "a column stored as Arrow type \"%s\" cannot be read as %s",
			 arrow_format == NULL ? "" : arrow_format,
			 format_type_be(atttypid));

	dl_error_set(DL_ERR_NOT_SUPPORTED, "decode an Arrow column", NULL, message);
	return DL_ERR_NOT_SUPPORTED;
}

DlErrCode
dl_arrow_decode_check(const struct ArrowSchema *field, Oid atttypid)
{
	const char *format;
	const char *expected;

	if (field == NULL || field->format == NULL)
	{
		dl_error_set(DL_ERR_INTERNAL, "decode an Arrow column", NULL,
					 "the batch has a column with no type");
		return DL_ERR_INTERNAL;
	}

	format = field->format;

	switch (atttypid)
	{
		case BOOLOID:
			expected = DL_ARROW_FORMAT_BOOL;
			break;
		case INT2OID:
			expected = DL_ARROW_FORMAT_INT16;
			break;
		case INT4OID:
			expected = DL_ARROW_FORMAT_INT32;
			break;
		case INT8OID:
			expected = DL_ARROW_FORMAT_INT64;
			break;
		case FLOAT4OID:
			expected = DL_ARROW_FORMAT_FLOAT32;
			break;
		case FLOAT8OID:
			expected = DL_ARROW_FORMAT_FLOAT64;
			break;
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
			expected = DL_ARROW_FORMAT_UTF8;
			break;
		case BYTEAOID:
			expected = DL_ARROW_FORMAT_BINARY;
			break;
		case DATEOID:
			expected = DL_ARROW_FORMAT_DATE32;
			break;

		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				const char *zone;
				size_t		prefix_len = strlen(DL_ARROW_FORMAT_TIMESTAMP_US);

				if (strncmp(format, DL_ARROW_FORMAT_TIMESTAMP_US, prefix_len) != 0)
					return dl_arrow_decode_refuse(format, atttypid);

				/*
				 * Arrow stores a zoned timestamp as the instant in UTC and
				 * keeps the zone only to display it, so which zone the file
				 * names does not change the value -- but whether it names one
				 * at all is the difference between the two PostgreSQL types,
				 * and reading one as the other would shift every value by the
				 * session's offset from UTC.
				 */
				zone = format + prefix_len;
				if ((zone[0] != '\0') != (atttypid == TIMESTAMPTZOID))
					return dl_arrow_decode_refuse(format, atttypid);

				return DL_OK;
			}

		default:
			return dl_arrow_decode_refuse(format, atttypid);
	}

	if (strcmp(format, expected) != 0)
		return dl_arrow_decode_refuse(format, atttypid);

	return DL_OK;
}

/*
 * Arrow keeps the validity bitmap in the first buffer, and a column with no
 * nulls may leave it out entirely.  Bit set means present.
 */
static bool
dl_arrow_value_is_null(const struct ArrowArray *column, int64_t row)
{
	const uint8 *validity;
	int64		index;

	if (column->n_buffers < 1)
		return false;

	validity = (const uint8 *) column->buffers[0];
	if (validity == NULL)
		return false;

	index = column->offset + row;
	return (validity[index >> 3] & (1 << (index & 7))) == 0;
}

/* The values buffer of a fixed-width column, already advanced past the offset. */
#define DL_ARROW_VALUES(column, type) \
	(((const type *) (column)->buffers[1]) + (column)->offset)

static DlErrCode
dl_arrow_out_of_range(const char *what)
{
	char		message[128];

	snprintf(message, sizeof(message),
			 "the file holds a %s outside the range PostgreSQL can represent",
			 what);

	dl_error_set(DL_ERR_INVALID_OPTION, "decode an Arrow column", NULL, message);
	return DL_ERR_INVALID_OPTION;
}

/*
 * A variable-length value: an offsets buffer of int32 and one run of bytes.
 * Both text and bytea are laid out this way, and differ only in the header the
 * copy gets.
 */
static void
dl_arrow_varlen(const struct ArrowArray *column, int64_t row,
				const char **data, int32 *length)
{
	const int32 *offsets = DL_ARROW_VALUES(column, int32);
	const char *bytes = (const char *) column->buffers[2];

	*data = bytes + offsets[row];
	*length = offsets[row + 1] - offsets[row];
}

DlErrCode
dl_arrow_decode_value(const struct ArrowArray *column, int64_t row,
					  Oid atttypid, int32 atttypmod,
					  Datum *value, bool *isnull)
{
	*value = (Datum) 0;
	*isnull = true;

	if (row < 0 || row >= column->length)
	{
		dl_error_set(DL_ERR_INTERNAL, "decode an Arrow column", NULL,
					 "a row was asked for past the end of the batch");
		return DL_ERR_INTERNAL;
	}

	if (dl_arrow_value_is_null(column, row))
		return DL_OK;

	*isnull = false;

	switch (atttypid)
	{
		case BOOLOID:
			{
				/* Booleans are a bitmap of their own, not a byte per value. */
				const uint8 *bits = (const uint8 *) column->buffers[1];
				int64		index = column->offset + row;

				*value = BoolGetDatum((bits[index >> 3] & (1 << (index & 7))) != 0);
				return DL_OK;
			}

		case INT2OID:
			*value = Int16GetDatum(DL_ARROW_VALUES(column, int16)[row]);
			return DL_OK;
		case INT4OID:
			*value = Int32GetDatum(DL_ARROW_VALUES(column, int32)[row]);
			return DL_OK;
		case INT8OID:
			*value = Int64GetDatum(DL_ARROW_VALUES(column, int64)[row]);
			return DL_OK;
		case FLOAT4OID:
			*value = Float4GetDatum(DL_ARROW_VALUES(column, float)[row]);
			return DL_OK;
		case FLOAT8OID:
			*value = Float8GetDatum(DL_ARROW_VALUES(column, double)[row]);
			return DL_OK;

		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
			{
				const char *data;
				int32		length;

				dl_arrow_varlen(column, row, &data, &length);
				*value = PointerGetDatum(cstring_to_text_with_len(data, length));

				/*
				 * The file records the bytes and nothing about the length the
				 * column was declared with, so the value is put through the
				 * same coercion an inserted one would be: char(n) comes back
				 * padded to n, and a value too long for a varchar(n) is an
				 * error rather than something the executor has to meet later.
				 */
				if (atttypmod >= 0 && atttypid == BPCHAROID)
					*value = DirectFunctionCall3(bpchar, *value,
												 Int32GetDatum(atttypmod),
												 BoolGetDatum(false));
				else if (atttypmod >= 0 && atttypid == VARCHAROID)
					*value = DirectFunctionCall3(varchar, *value,
												 Int32GetDatum(atttypmod),
												 BoolGetDatum(false));

				return DL_OK;
			}

		case BYTEAOID:
			{
				const char *data;
				int32		length;
				bytea	   *result;

				dl_arrow_varlen(column, row, &data, &length);
				result = (bytea *) palloc(VARHDRSZ + length);
				SET_VARSIZE(result, VARHDRSZ + length);
				memcpy(VARDATA(result), data, length);
				*value = PointerGetDatum(result);
				return DL_OK;
			}

		case DATEOID:
			{
				int32		days = DL_ARROW_VALUES(column, int32)[row];
				DateADT		date;

				/*
				 * Shifting the epoch is a subtraction that can leave the range
				 * of the type it lands in, so the guard has to come first: by
				 * the time an overflowed value could be checked it is already
				 * a different, plausible-looking date.
				 */
				if (days < DATETIME_MIN_JULIAN - UNIX_EPOCH_JDATE)
					return dl_arrow_out_of_range("date");

				date = days - DL_EPOCH_DELTA_DAYS;
				if (!IS_VALID_DATE(date))
					return dl_arrow_out_of_range("date");

				*value = DateADTGetDatum(date);
				return DL_OK;
			}

		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				int64		micros = DL_ARROW_VALUES(column, int64)[row];
				Timestamp	ts;

				if (micros < MIN_TIMESTAMP + DL_EPOCH_DELTA_USECS)
					return dl_arrow_out_of_range("timestamp");

				ts = micros - DL_EPOCH_DELTA_USECS;
				if (!IS_VALID_TIMESTAMP(ts))
					return dl_arrow_out_of_range("timestamp");

				*value = TimestampGetDatum(ts);
				return DL_OK;
			}

		default:

			/*
			 * Unreachable: dl_arrow_decode_check() refused every type this
			 * switch does not list.
			 */
			*isnull = true;
			return dl_arrow_decode_refuse(NULL, atttypid);
	}
}

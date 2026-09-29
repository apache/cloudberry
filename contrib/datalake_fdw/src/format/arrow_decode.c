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
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/fmgrprotos.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"
#include "varatt.h"

#include "format/arrow_decode.h"
#include "format/format_types.h"

/*
 * The same shift as in arrow_builder.cpp, in the other direction: PostgreSQL
 * counts from 2000-01-01 and Arrow from 1970-01-01.
 */
#define DL_EPOCH_DELTA_DAYS		((int32) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE))
#define DL_EPOCH_DELTA_USECS	(((int64) DL_EPOCH_DELTA_DAYS) * USECS_PER_DAY)

/*
 * Arrow spells its types as a short string.  Listed are the ones a column of
 * ours can be stored as, plus the ones it can be read from without loss;
 * anything else is a file we did not write, or one written by a version that
 * knows more types than this one.
 */
#define DL_ARROW_FORMAT_NULL		"n"
#define DL_ARROW_FORMAT_BOOL		"b"
#define DL_ARROW_FORMAT_INT16		"s"
#define DL_ARROW_FORMAT_INT32		"i"
#define DL_ARROW_FORMAT_INT64		"l"
#define DL_ARROW_FORMAT_FLOAT32		"f"
#define DL_ARROW_FORMAT_FLOAT64		"g"
#define DL_ARROW_FORMAT_UTF8		"u"
#define DL_ARROW_FORMAT_LARGE_UTF8	"U"
#define DL_ARROW_FORMAT_BINARY		"z"
#define DL_ARROW_FORMAT_LARGE_BINARY "Z"
#define DL_ARROW_FORMAT_DATE32		"tdD"
#define DL_ARROW_FORMAT_TIME64_US	"ttu"
#define DL_ARROW_FORMAT_UUID		"w:16"	/* fixed-size binary of 16 */

/* A timestamp is "tsu:" followed by the time zone, which may be empty. */
#define DL_ARROW_FORMAT_TIMESTAMP_US	"tsu:"

static DlErrCode
dl_arrow_decode_refuse(const char *arrow_format, Oid atttypid, int32 atttypmod)
{
	char		message[256];

	snprintf(message, sizeof(message),
			 "a column stored as Arrow type \"%s\" cannot be read as %s",
			 arrow_format == NULL ? "" : arrow_format,
			 format_type_with_typemod(atttypid, atttypmod));

	dl_error_set(DL_ERR_NOT_SUPPORTED, "decode an Arrow column", NULL, message);
	return DL_ERR_NOT_SUPPORTED;
}

static bool
dl_arrow_format_in(const char *format, const char *const *accepted)
{
	for (; *accepted != NULL; accepted++)
	{
		if (strcmp(format, *accepted) == 0)
			return true;
	}

	return false;
}

DlErrCode
dl_arrow_decode_check(const struct ArrowSchema *field, Oid atttypid,
					  int32 atttypmod)
{
	/*
	 * The type's own storage first, then what it can be read from without
	 * loss.  Iceberg lets a column be promoted from int to long and from float
	 * to double, and a file written before the promotion holds the old type;
	 * the values fit, so they are read.  Nothing narrows.
	 */
	static const char *const bool_formats[] = {DL_ARROW_FORMAT_BOOL, NULL};
	static const char *const int2_formats[] = {DL_ARROW_FORMAT_INT16, NULL};
	static const char *const int4_formats[] = {DL_ARROW_FORMAT_INT32,
		DL_ARROW_FORMAT_INT16, NULL};
	static const char *const int8_formats[] = {DL_ARROW_FORMAT_INT64,
		DL_ARROW_FORMAT_INT32, DL_ARROW_FORMAT_INT16, NULL};
	static const char *const float4_formats[] = {DL_ARROW_FORMAT_FLOAT32, NULL};
	static const char *const float8_formats[] = {DL_ARROW_FORMAT_FLOAT64,
		DL_ARROW_FORMAT_FLOAT32, NULL};
	static const char *const text_formats[] = {DL_ARROW_FORMAT_UTF8,
		DL_ARROW_FORMAT_LARGE_UTF8, NULL};
	static const char *const bytea_formats[] = {DL_ARROW_FORMAT_BINARY,
		DL_ARROW_FORMAT_LARGE_BINARY, NULL};
	static const char *const date_formats[] = {DL_ARROW_FORMAT_DATE32, NULL};
	static const char *const time_formats[] = {DL_ARROW_FORMAT_TIME64_US, NULL};
	static const char *const uuid_formats[] = {DL_ARROW_FORMAT_UUID, NULL};

	const char *format;
	const char *refusal;
	const char *const *accepted;
	char		message[256];

	if (field == NULL || field->format == NULL)
	{
		dl_error_set(DL_ERR_INTERNAL, "decode an Arrow column", NULL,
					 "the batch has a column with no type");
		return DL_ERR_INTERNAL;
	}

	format = field->format;

	/*
	 * A type this module could not have stored is not one it reads either, and
	 * the rule about modifiers is the one CREATE TABLE applies: a varchar(n)
	 * column cannot exist in a lake table, so a file cannot be read as one.
	 */
	refusal = dl_format_type_refusal(atttypid, atttypmod);
	if (refusal != NULL)
	{
		snprintf(message, sizeof(message), "a column cannot be read as %s, which %s",
				 format_type_with_typemod(atttypid, atttypmod), refusal);
		dl_error_set(DL_ERR_NOT_SUPPORTED, "decode an Arrow column", NULL, message);
		return DL_ERR_NOT_SUPPORTED;
	}

	/*
	 * A dictionary-encoded column describes its indexes in the format string
	 * and keeps its values in a separate array.  The indexes are integers, so
	 * without this a dictionary of strings would pass as an integer column and
	 * every value read would be a position in a table nobody looked at.  Arrow
	 * restores this encoding from a note pyarrow leaves in the file whenever
	 * pandas wrote a categorical, so it is not a rare file.
	 */
	if (field->dictionary != NULL)
	{
		snprintf(message, sizeof(message),
				 "a dictionary-encoded column cannot be read as %s",
				 format_type_with_typemod(atttypid, atttypmod));
		dl_error_set(DL_ERR_NOT_SUPPORTED, "decode an Arrow column", NULL, message);
		return DL_ERR_NOT_SUPPORTED;
	}

	/*
	 * A column of the null type has no values, only nulls, and every type can
	 * hold those.  It is what the reader produces for a field the file does
	 * not have -- a column added to the table after the file was written.
	 */
	if (strcmp(format, DL_ARROW_FORMAT_NULL) == 0)
		return DL_OK;

	switch (atttypid)
	{
		case BOOLOID:
			accepted = bool_formats;
			break;
		case INT2OID:
			accepted = int2_formats;
			break;
		case INT4OID:
			accepted = int4_formats;
			break;
		case INT8OID:
			accepted = int8_formats;
			break;
		case FLOAT4OID:
			accepted = float4_formats;
			break;
		case FLOAT8OID:
			accepted = float8_formats;
			break;

		case TEXTOID:
		case VARCHAROID:

			/*
			 * The file's strings are UTF-8.  Into a database of any other
			 * encoding they would have to be converted, and until they are,
			 * copying the bytes would make text values no function of the
			 * server can interpret.
			 */
			if (GetDatabaseEncoding() != PG_UTF8)
			{
				snprintf(message, sizeof(message),
						 "a lake table's strings are UTF-8, and this database's "
						 "encoding is %s", GetDatabaseEncodingName());
				dl_error_set(DL_ERR_NOT_SUPPORTED, "decode an Arrow column", NULL,
							 message);
				return DL_ERR_NOT_SUPPORTED;
			}
			accepted = text_formats;
			break;

		case BYTEAOID:
			accepted = bytea_formats;
			break;
		case DATEOID:
			accepted = date_formats;
			break;
		case TIMEOID:
			accepted = time_formats;
			break;
		case UUIDOID:
			accepted = uuid_formats;
			break;

		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				const char *zone;
				size_t		prefix_len = strlen(DL_ARROW_FORMAT_TIMESTAMP_US);

				if (strncmp(format, DL_ARROW_FORMAT_TIMESTAMP_US, prefix_len) != 0)
					return dl_arrow_decode_refuse(format, atttypid, atttypmod);

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
					return dl_arrow_decode_refuse(format, atttypid, atttypmod);

				return DL_OK;
			}

		default:
			return dl_arrow_decode_refuse(format, atttypid, atttypmod);
	}

	if (!dl_arrow_format_in(format, accepted))
		return dl_arrow_decode_refuse(format, atttypid, atttypmod);

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
 * An integer of whichever width the column has, widened.  Only reached for a
 * width dl_arrow_decode_check() accepted for the target type, so nothing here
 * can truncate.
 */
static int64
dl_arrow_integer(const struct ArrowArray *column, const char *format,
				 int64_t row)
{
	switch (format[0])
	{
		case 's':
			return DL_ARROW_VALUES(column, int16)[row];
		case 'i':
			return DL_ARROW_VALUES(column, int32)[row];
		default:
			return DL_ARROW_VALUES(column, int64)[row];
	}
}

/*
 * A variable-length value: an offsets buffer and one run of bytes.  Text and
 * bytea are laid out this way and differ only in the header the copy gets; the
 * "large" forms differ only in the width of the offsets, which is what lets a
 * single column exceed 2 GB -- and what lets a single value exceed what a
 * PostgreSQL varlena can hold, which is why the length is checked.
 */
static DlErrCode
dl_arrow_varlen(const struct ArrowArray *column, const char *format,
				int64_t row, const char **data, int64 *length)
{
	const char *bytes = (const char *) column->buffers[2];
	int64		start;
	int64		end;

	if (format[0] == 'U' || format[0] == 'Z')
	{
		const int64 *offsets = DL_ARROW_VALUES(column, int64);

		start = offsets[row];
		end = offsets[row + 1];
	}
	else
	{
		const int32 *offsets = DL_ARROW_VALUES(column, int32);

		start = offsets[row];
		end = offsets[row + 1];
	}

	if (end - start > (int64) (MaxAllocSize - VARHDRSZ))
	{
		dl_error_set(DL_ERR_INVALID_OPTION, "decode an Arrow column", NULL,
					 "the file holds a value longer than a PostgreSQL value can be");
		return DL_ERR_INVALID_OPTION;
	}

	*data = bytes + start;
	*length = end - start;

	return DL_OK;
}

DlErrCode
dl_arrow_decode_value(const struct ArrowSchema *field,
					  const struct ArrowArray *column, int64_t row,
					  Oid atttypid, Datum *value, bool *isnull)
{
	const char *format = field->format;

	*value = (Datum) 0;
	*isnull = true;

	if (row < 0 || row >= column->length)
	{
		dl_error_set(DL_ERR_INTERNAL, "decode an Arrow column", NULL,
					 "a row was asked for past the end of the batch");
		return DL_ERR_INTERNAL;
	}

	/*
	 * The null type has no buffers at all, not even a validity bitmap, so it
	 * is answered before anything looks for one.
	 */
	if (strcmp(format, DL_ARROW_FORMAT_NULL) == 0)
		return DL_OK;

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
			*value = Int16GetDatum((int16) dl_arrow_integer(column, format, row));
			return DL_OK;
		case INT4OID:
			*value = Int32GetDatum((int32) dl_arrow_integer(column, format, row));
			return DL_OK;
		case INT8OID:
			*value = Int64GetDatum(dl_arrow_integer(column, format, row));
			return DL_OK;
		case FLOAT4OID:
			*value = Float4GetDatum(DL_ARROW_VALUES(column, float)[row]);
			return DL_OK;
		case FLOAT8OID:
			if (format[0] == 'f')
				*value = Float8GetDatum((double) DL_ARROW_VALUES(column, float)[row]);
			else
				*value = Float8GetDatum(DL_ARROW_VALUES(column, double)[row]);
			return DL_OK;

		case TEXTOID:
		case VARCHAROID:
			{
				const char *data;
				int64		length;
				DlErrCode	rc;

				rc = dl_arrow_varlen(column, format, row, &data, &length);
				if (rc != DL_OK)
					return rc;

				/*
				 * PostgreSQL's own input paths verify every string before it
				 * becomes a text, and this is an input path: the file says its
				 * strings are UTF-8, and a file this module did not write may
				 * be lying.  An invalid sequence is refused here, where it can
				 * be named, rather than met later by whichever function first
				 * walks the characters.
				 */
				pg_verifymbstr(data, (int) length, false);

				*value = PointerGetDatum(cstring_to_text_with_len(data, (int) length));
				return DL_OK;
			}

		case BYTEAOID:
			{
				const char *data;
				int64		length;
				bytea	   *result;
				DlErrCode	rc;

				rc = dl_arrow_varlen(column, format, row, &data, &length);
				if (rc != DL_OK)
					return rc;

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

		case TIMEOID:
			{
				int64		micros = DL_ARROW_VALUES(column, int64)[row];

				/* PostgreSQL admits 24:00:00, so the upper bound is inclusive. */
				if (micros < 0 || micros > USECS_PER_DAY)
					return dl_arrow_out_of_range("time");

				*value = TimeADTGetDatum(micros);
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

		case UUIDOID:
			{
				/* Fixed-size binary: the values buffer is rows of 16 bytes. */
				const uint8 *bytes = ((const uint8 *) column->buffers[1]) +
					(column->offset + row) * UUID_LEN;
				pg_uuid_t  *uuid = (pg_uuid_t *) palloc(sizeof(pg_uuid_t));

				memcpy(uuid->data, bytes, UUID_LEN);
				*value = UUIDPGetDatum(uuid);
				return DL_OK;
			}

		default:

			/*
			 * Unreachable: dl_arrow_decode_check() refused every type this
			 * switch does not list.
			 */
			*isnull = true;
			return dl_arrow_decode_refuse(format, atttypid, -1);
	}
}

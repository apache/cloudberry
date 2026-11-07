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
 * lance_arrow.c
 *	  Arrow schema -> PostgreSQL type mapping.
 *
 * A-tier is everything this block can hand to the executor without losing
 * information; B-tier is everything else, and B-tier is loud rather than
 * silent: skipped with a NOTICE on import, refused with an error on scan.
 *
 * IDENTIFICATION
 *	  src/lance_arrow.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <math.h>

#include "lance_fdw.h"
#include "lance_arrow.h"

#include "access/htup_details.h"
#include "catalog/pg_type_d.h"
#include "common/int.h"
#include "varatt.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/fmgrprotos.h"
#include "mb/pg_wchar.h"
#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/typcache.h"

#define LANCE_BLOB_METADATA_KEY "lance-encoding"
#define LANCE_BLOB_METADATA_VALUE "blob"

/*
 * Lance's v2 blob encoding.  The key is Arrow's own extension mechanism, so it
 * is the field metadata that carries it - and only on the dataset schema, which
 * is why there is a second rule below.
 */
#define LANCE_EXTENSION_NAME_KEY "ARROW:extension:name"
#define LANCE_BLOB_V2_EXTENSION_NAME "lance.blob.v2"

/*
 * PostgreSQL keeps microseconds, so every Arrow timestamp becomes a
 * timestamp(6); a coarser Arrow unit is exact in it and a finer one (ns) is
 * rejected value by value at scan time rather than silently rounded.
 */
#define LANCE_TIMESTAMP_TYPMOD 6

/*
 * Arrow counts from 1970-01-01 and PostgreSQL from 2000-01-01; both epochs are
 * UTC, so the difference is a constant.
 */
#define LANCE_EPOCH_DIFF_USEC \
	(((int64) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)) * SECS_PER_DAY * USECS_PER_SEC)

const char *
lance_arrow_format(const struct ArrowSchema *field)
{
	if (field == NULL || field->format == NULL)
		return "?";
	return field->format;
}

static const char *lance_arrow_name_or(const struct ArrowSchema *field,
									   const char *fallback);

const char *
lance_arrow_type_name(const struct ArrowSchema *field)
{
	struct ArrowSchemaView view;
	struct ArrowError error;

	if (field == NULL || field->format == NULL)
		return NULL;

	memset(&error, 0, sizeof(error));
	if (ArrowSchemaViewInit(&view, field, &error) != NANOARROW_OK)
		return NULL;

	return ArrowTypeString(view.type);
}

/*
 * Look one key up in an Arrow field's metadata.  The metadata is the binary
 * key/value encoding of the C data interface, so it goes through nanoarrow's
 * reader rather than any string handling of ours.
 */
static bool
lance_arrow_metadata_lookup(const struct ArrowSchema *field, const char *key,
							struct ArrowStringView *value)
{
	struct ArrowMetadataReader reader;
	struct ArrowStringView wanted = ArrowCharView(key);
	struct ArrowStringView k;
	struct ArrowStringView v;

	if (field == NULL || field->metadata == NULL)
		return false;

	if (ArrowMetadataReaderInit(&reader, field->metadata) != NANOARROW_OK)
		return false;

	while (reader.remaining_keys > 0)
	{
		if (ArrowMetadataReaderRead(&reader, &k, &v) != NANOARROW_OK)
			return false;

		if (k.size_bytes == wanted.size_bytes &&
			(k.size_bytes == 0 ||
			 memcmp(k.data, wanted.data, (size_t) k.size_bytes) == 0))
		{
			*value = v;
			return true;
		}
	}

	return false;
}

static bool
lance_arrow_metadata_equals(const struct ArrowSchema *field, const char *key,
							const char *wanted, size_t wantedlen)
{
	struct ArrowStringView value;

	if (!lance_arrow_metadata_lookup(field, key, &value))
		return false;

	return value.size_bytes == (int64) wantedlen &&
		memcmp(value.data, wanted, wantedlen) == 0;
}

bool
lance_arrow_is_blob_encoded(const struct ArrowSchema *field)
{
	return lance_arrow_metadata_equals(field, LANCE_BLOB_METADATA_KEY,
									   LANCE_BLOB_METADATA_VALUE,
									   sizeof(LANCE_BLOB_METADATA_VALUE) - 1);
}

/*
 * The five children a Lance Blob v2 descriptor has, in order.  This is the scan
 * half of the rule below and it is the fragile half: see lance_arrow_is_blob_v2.
 */
static const char *const lance_blob_v2_children[] = {
	"kind", "position", "size", "blob_id", "blob_uri"
};

static bool
lance_arrow_has_blob_v2_children(const struct ArrowSchema *field)
{
	int			i;
	const int	n = lengthof(lance_blob_v2_children);

	if (field->format == NULL || strcmp(field->format, "+s") != 0)
		return false;
	if (field->n_children != n || field->children == NULL)
		return false;

	for (i = 0; i < n; i++)
	{
		const struct ArrowSchema *child = field->children[i];

		if (child == NULL || child->name == NULL ||
			strcmp(child->name, lance_blob_v2_children[i]) != 0)
			return false;
	}

	return true;
}

/*
 * Is this field Lance's v2 blob encoding?
 *
 * Refusing it is a policy choice: the descriptor is Lance's
 * internal representation, and the natural shape of such a column once lance-c
 * can fetch the payload is bytea, not a five-field composite.  Handing the
 * descriptor out now would have to be taken back later.
 *
 * The rule needs two halves because the two schemas differ (measured):
 * lance_dataset_schema() keeps the Arrow extension name, which IMPORT reads
 * here and which is exact.  lance_scanner_to_arrow_stream() strips it and hands
 * out the descriptor itself, so a scan can only go by the shape - the five
 * child names.  That half is fragile in one direction: if Lance renames a child
 * or adds one, the guard stops matching and the column reads as an ordinary
 * struct of offsets.  It is written up in the README's Known Limits and the
 * question of keeping the extension name on the stream schema is open with
 * upstream (issue #76).
 *
 * Measured against lance-c v0.1.9, by taking each half out in turn and running
 * the suites: the metadata half is what stops IMPORT declaring the column, and
 * without it the descriptor becomes a five-field composite type in the user's
 * schema.  The stream half is a second line of defence rather than the only
 * one, because the descriptor's "position" child arrives as uint64, which is
 * B-tier in its own right; that is a property of today's descriptor and not
 * something to rely on.
 */
static bool
lance_arrow_is_blob_v2(const struct ArrowSchema *field)
{
	return lance_arrow_metadata_equals(field, LANCE_EXTENSION_NAME_KEY,
									   LANCE_BLOB_V2_EXTENSION_NAME,
									   sizeof(LANCE_BLOB_V2_EXTENSION_NAME) - 1) ||
		lance_arrow_has_blob_v2_children(field);
}

/*
 * A-tier scalars.  Integers widen into the smallest PostgreSQL type that holds
 * every value of the Arrow type - uint32 goes to bigint, not integer, because
 * integer cannot hold 4294967295.
 */
static bool
lance_arrow_map_scalar(const struct ArrowSchemaView *view, Oid *typid,
					   int32 *typmod)
{
	*typmod = -1;

	switch (view->type)
	{
		case NANOARROW_TYPE_BOOL:
			*typid = BOOLOID;
			return true;

		case NANOARROW_TYPE_INT8:
		case NANOARROW_TYPE_INT16:
		case NANOARROW_TYPE_UINT8:
			*typid = INT2OID;
			return true;

		case NANOARROW_TYPE_INT32:
		case NANOARROW_TYPE_UINT16:
			*typid = INT4OID;
			return true;

		case NANOARROW_TYPE_INT64:
		case NANOARROW_TYPE_UINT32:
			*typid = INT8OID;
			return true;

		case NANOARROW_TYPE_HALF_FLOAT:
		case NANOARROW_TYPE_FLOAT:
			*typid = FLOAT4OID;
			return true;

		case NANOARROW_TYPE_DOUBLE:
			*typid = FLOAT8OID;
			return true;

		case NANOARROW_TYPE_STRING:
		case NANOARROW_TYPE_LARGE_STRING:
			*typid = TEXTOID;
			return true;

		case NANOARROW_TYPE_BINARY:
		case NANOARROW_TYPE_LARGE_BINARY:
			*typid = BYTEAOID;
			return true;

		case NANOARROW_TYPE_DATE32:
			*typid = DATEOID;
			return true;

		case NANOARROW_TYPE_TIMESTAMP:

			/*
			 * The zone name is not kept: an Arrow timestamp with a zone is a
			 * UTC epoch, which is exactly what timestamptz stores.
			 */
			*typid = (view->timezone != NULL && view->timezone[0] != '\0')
				? TIMESTAMPTZOID : TIMESTAMPOID;
			*typmod = LANCE_TIMESTAMP_TYPMOD;
			return true;

		case NANOARROW_TYPE_DECIMAL128:
			if (view->decimal_precision < 1 ||
				view->decimal_precision > 1000 ||
				view->decimal_scale < 0 ||
				view->decimal_scale > view->decimal_precision)
				return false;
			*typid = NUMERICOID;
			*typmod = (int32) (((view->decimal_precision << 16) |
								view->decimal_scale) + VARHDRSZ);
			return true;

		default:
			return false;
	}
}

/*
 * fixed_size_list is only A-tier over float32/float64, which is the embedding
 * shape this block is about; other element types stay B-tier until someone
 * needs them.
 */
static bool
lance_arrow_fsl_element_ok(const struct ArrowSchemaView *element)
{
	return element->type == NANOARROW_TYPE_FLOAT ||
		element->type == NANOARROW_TYPE_DOUBLE;
}

static bool lance_arrow_map_field(const struct ArrowSchema *field, Oid *typid,
								  int32 *typmod);

static bool
lance_arrow_is_list_shaped(enum ArrowType arrow_type)
{
	return arrow_type == NANOARROW_TYPE_LIST ||
		arrow_type == NANOARROW_TYPE_LARGE_LIST ||
		arrow_type == NANOARROW_TYPE_FIXED_SIZE_LIST;
}

/* The one child of a list, a fixed_size_list or a map; NULL if it is missing. */
static struct ArrowSchema *
lance_arrow_only_child(const struct ArrowSchema *field)
{
	if (field->n_children != 1 || field->children == NULL)
		return NULL;
	return field->children[0];
}

/*
 * A list is A-tier when its element is, with one exception: a list of lists
 * stays B-tier, because a PostgreSQL array of arrays is not one - it is a
 * two-dimensional array whose rows all have the same length, which a Lance
 * list of lists does not promise.  The element may be a struct or a map, which
 * gives an array of the composite type or of jsonb.
 */
static bool
lance_arrow_map_list(const struct ArrowSchema *field,
					 const struct ArrowSchemaView *view, Oid *typid,
					 int32 *typmod)
{
	struct ArrowSchema *child = lance_arrow_only_child(field);
	struct ArrowSchemaView childview;
	struct ArrowError error;
	Oid			elemtypid;
	int32		elemtypmod;
	Oid			arraytypid;

	if (child == NULL)
		return false;

	memset(&error, 0, sizeof(error));
	if (ArrowSchemaViewInit(&childview, child, &error) != NANOARROW_OK)
		return false;

	if (view->type == NANOARROW_TYPE_FIXED_SIZE_LIST)
	{
		/* The embedding shape, and only that: float32 or float64 elements. */
		if (!lance_arrow_fsl_element_ok(&childview))
			return false;
	}
	else if (lance_arrow_is_list_shaped(childview.type))
		return false;

	if (!lance_arrow_map_field(child, &elemtypid, &elemtypmod))
		return false;

	arraytypid = get_array_type(elemtypid);
	if (!OidIsValid(arraytypid))
		return false;

	*typid = arraytypid;
	*typmod = elemtypmod;
	return true;
}

/*
 * A map is A-tier as jsonb when its keys are strings and its values are A-tier
 * scalars.  A JSON object has no other kind of key, so any other
 * key type would have to be printed on the way in, and a container value would
 * have to be flattened; both are silent distortions, and pglance does them
 * where this refuses to.
 */
static bool
lance_arrow_map_map(const struct ArrowSchema *field, Oid *typid, int32 *typmod)
{
	struct ArrowSchema *entries = lance_arrow_only_child(field);
	struct ArrowSchemaView keyview;
	struct ArrowSchemaView valueview;
	struct ArrowError error;
	Oid			ignoretypid;
	int32		ignoretypmod;

	/* nanoarrow has validated the shape; this is belt and braces. */
	if (entries == NULL || entries->n_children != 2 || entries->children == NULL ||
		entries->children[0] == NULL || entries->children[1] == NULL)
		return false;

	memset(&error, 0, sizeof(error));
	if (ArrowSchemaViewInit(&keyview, entries->children[0], &error) != NANOARROW_OK)
		return false;
	if (keyview.type != NANOARROW_TYPE_STRING &&
		keyview.type != NANOARROW_TYPE_LARGE_STRING)
		return false;

	memset(&error, 0, sizeof(error));
	if (ArrowSchemaViewInit(&valueview, entries->children[1], &error) != NANOARROW_OK)
		return false;
	if (lance_arrow_is_blob_encoded(entries->children[1]) ||
		!lance_arrow_map_scalar(&valueview, &ignoretypid, &ignoretypmod))
		return false;

	*typid = JSONBOID;
	*typmod = -1;
	return true;
}

/*
 * A struct is A-tier when every subfield is, recursively, and it is read as a
 * whole or not at all: one B-tier subfield sinks the column,
 * because a composite value with fields missing for reasons the user cannot see
 * is worse than a refusal.
 *
 * *typid is RECORDOID rather than the composite type's own OID: that type is
 * named after the foreign table IMPORT FOREIGN SCHEMA is about to create, so at
 * this point it need not exist.  lance_import.c turns RECORDOID into the name,
 * and the scan resolves the converter against whatever the table declares.
 */
static bool
lance_arrow_map_struct(const struct ArrowSchema *field, Oid *typid, int32 *typmod)
{
	int64		i;

	/*
	 * A struct with no subfields carries no value to read.  PostgreSQL would
	 * take a zero-column composite type, so this is a choice rather than a
	 * limit: a column of one is a row of nothing, and refusing it loudly is
	 * what B-tier is for.  classify() calls such a struct A-tier, which is the
	 * one place the two disagree about structs; nothing in test/fixtures builds
	 * one, so neither answer is exercised.
	 */
	if (field->n_children < 1 || field->children == NULL)
		return false;

	for (i = 0; i < field->n_children; i++)
	{
		Oid			subtypid;
		int32		subtypmod;

		if (field->children[i] == NULL ||
			!lance_arrow_map_field(field->children[i], &subtypid, &subtypmod))
			return false;
	}

	*typid = RECORDOID;
	*typmod = -1;
	return true;
}

/*
 * The tier decision itself, for a column and for every field inside one.  It
 * follows classify() in test/fixtures/gen_fixtures.py: that function is what
 * the fixtures' manifest was written from, so the two have to agree over
 * everything the manifest describes or the manifest describes a database this
 * build does not produce.
 *
 * Two shapes no fixture contains are refused here and accepted there, in the
 * safe direction: large_list, which has had no converter since the scan was
 * written, and a struct with no subfields.  Adding either means adding a
 * fixture for it first.
 *
 * There is deliberately no "unrecognised means A-tier" and no "unrecognised
 * means jsonb": anything not named here is B-tier.
 */
static bool
lance_arrow_map_field(const struct ArrowSchema *field, Oid *typid, int32 *typmod)
{
	struct ArrowSchemaView view;
	struct ArrowError error;

	/* Arrow nests without a limit, so the recursion needs the usual guard. */
	check_stack_depth();

	*typid = InvalidOid;
	*typmod = -1;

	if (field == NULL || field->format == NULL)
		return false;

	/* Blob encoding overrides whatever the Arrow type claims; see lance_arrow.h. */
	if (lance_arrow_is_blob_encoded(field))
		return false;

	/*
	 * Ahead of the struct rule on purpose: a v2 descriptor is a
	 * struct of A-tier scalars, so the struct rule below would make it readable
	 * and hand out file offsets as if they were the payload.
	 */
	if (lance_arrow_is_blob_v2(field))
		return false;

	memset(&error, 0, sizeof(error));
	if (ArrowSchemaViewInit(&view, field, &error) != NANOARROW_OK)
		return false;

	switch (view.type)
	{
		case NANOARROW_TYPE_LIST:
		case NANOARROW_TYPE_FIXED_SIZE_LIST:
			return lance_arrow_map_list(field, &view, typid, typmod);

		case NANOARROW_TYPE_MAP:
			return lance_arrow_map_map(field, typid, typmod);

		case NANOARROW_TYPE_STRUCT:
			return lance_arrow_map_struct(field, typid, typmod);

		default:
			return lance_arrow_map_scalar(&view, typid, typmod);
	}
}

bool
lance_arrow_map_type(const struct ArrowSchema *field, Oid *typid,
					 int32 *typmod, bool *is_b_tier)
{
	struct ArrowSchemaView view;
	struct ArrowError error;

	*is_b_tier = true;
	*typid = InvalidOid;
	*typmod = -1;

	if (field == NULL || field->format == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("lance_fdw: Lance returned a field without a type")));

	/*
	 * A column whose own type does not parse is a broken schema rather than an
	 * unsupported type, and says so.  Inside a field the same failure is only
	 * one more reason to call it B-tier.
	 */
	memset(&error, 0, sizeof(error));
	if (ArrowSchemaViewInit(&view, field, &error) != NANOARROW_OK)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("lance_fdw: cannot parse Arrow type \"%s\" of column \"%s\"",
						lance_arrow_format(field),
						field->name != NULL ? field->name : "?"),
				 errdetail("%s", error.message)));

	if (!lance_arrow_map_field(field, typid, typmod))
		return false;

	*is_b_tier = false;
	return true;
}

/*
 * The B-tier reason, for the messages that can carry one.  It walks the same
 * rules as lance_arrow_map_field() but only far enough to find the first part
 * of the field that is not A-tier, and it asks that function for the verdict on
 * every child, so the two cannot drift apart.
 */
static void
lance_arrow_b_tier_walk(const struct ArrowSchema *field, StringInfo path,
						StringInfo out)
{
	struct ArrowSchemaView view;
	struct ArrowError error;
	Oid			typid;
	int32		typmod;
	int64		i;

	check_stack_depth();

	if (field == NULL || field->format == NULL)
		return;

	if (lance_arrow_is_blob_v2(field))
	{
		appendStringInfoString(out,
							   "This column is a Lance Blob v2 descriptor, which "
							   "lance_fdw refuses until the payload itself can be "
							   "read.");
		return;
	}

	memset(&error, 0, sizeof(error));
	if (ArrowSchemaViewInit(&view, field, &error) != NANOARROW_OK)
		return;

	switch (view.type)
	{
		case NANOARROW_TYPE_LIST:
		case NANOARROW_TYPE_FIXED_SIZE_LIST:
			{
				struct ArrowSchema *child = lance_arrow_only_child(field);

				/* Only a struct or a map element has a part worth naming. */
				if (child != NULL && child->format != NULL &&
					(strcmp(child->format, "+s") == 0 ||
					 strcmp(child->format, "+m") == 0))
					lance_arrow_b_tier_walk(child, path, out);
				return;
			}

		case NANOARROW_TYPE_MAP:
			{
				struct ArrowSchema *entries = lance_arrow_only_child(field);
				struct ArrowSchemaView keyview;

				if (entries == NULL || entries->n_children != 2 ||
					entries->children == NULL || entries->children[0] == NULL ||
					entries->children[1] == NULL)
					return;

				memset(&error, 0, sizeof(error));
				if (ArrowSchemaViewInit(&keyview, entries->children[0],
										&error) == NANOARROW_OK &&
					keyview.type != NANOARROW_TYPE_STRING &&
					keyview.type != NANOARROW_TYPE_LARGE_STRING)
					appendStringInfo(out,
									 "A map key becomes a JSON object key, so it "
									 "has to be utf8 or large_utf8, not \"%s\" (%s).",
									 lance_arrow_format(entries->children[0]),
									 lance_arrow_name_or(entries->children[0],
														 "unparsable"));
				else
					appendStringInfo(out,
									 "A map value has to be a supported scalar "
									 "type, and \"%s\" (%s) is not.",
									 lance_arrow_format(entries->children[1]),
									 lance_arrow_name_or(entries->children[1],
														 "unparsable"));
				return;
			}

		case NANOARROW_TYPE_STRUCT:
			{
				for (i = 0; i < field->n_children; i++)
				{
					struct ArrowSchema *child = field->children[i];

					if (child == NULL)
						continue;
					if (lance_arrow_map_field(child, &typid, &typmod))
						continue;

					/* The first offending subfield is the one that is named. */
					if (path->len > 0)
						appendStringInfoChar(path, '.');
					appendStringInfoString(path,
										   child->name != NULL ? child->name : "?");

					lance_arrow_b_tier_walk(child, path, out);
					if (out->len == 0)
						appendStringInfo(out,
										 "Subfield \"%s\" has Arrow type \"%s\" (%s), "
										 "which is not supported, and a struct is "
										 "read as a whole or not at all.",
										 path->data, lance_arrow_format(child),
										 lance_arrow_name_or(child, "unparsable"));
					return;
				}
				return;
			}

		default:
			return;
	}
}

char *
lance_arrow_b_tier_detail(const struct ArrowSchema *field)
{
	StringInfoData path;
	StringInfoData out;

	initStringInfo(&path);
	initStringInfo(&out);

	lance_arrow_b_tier_walk(field, &path, &out);

	pfree(path.data);

	if (out.len == 0)
	{
		pfree(out.data);
		return NULL;
	}

	return out.data;
}

/*-------------------------------------------------------------------------
 * Value converters
 *
 * Every accessor below goes through nanoarrow's array view rather than through
 * offset arithmetic of our own: array->offset is routinely non-zero once a
 * fragment has a deletion file, large_utf8 counts its offsets in 64 bits, and
 * a validity bitmap is allowed to be absent.  Those are the four places a
 * hand-written decoder gets wrong.
 *-------------------------------------------------------------------------
 */

/*
 * A value bigger than a varlena can describe is an error, not a truncation.
 * Lance itself has no such limit, so this is reachable with a large
 * enough blob.
 */
static void
lance_check_varlena_size(LanceConverter *conv, int64 size)
{
	if (size < 0 || (uint64) size > (uint64) (MaxAllocSize - VARHDRSZ))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("lance_fdw: column \"%s\": value of " INT64_FORMAT " bytes is too large for %s",
						conv->colname, size,
						format_type_with_typemod(conv->pgtypid, conv->pgtypmod))));
}

static Datum
lance_conv_bool(LanceConverter *conv, int64 row)
{
	return BoolGetDatum(ArrowArrayViewGetIntUnsafe(&conv->view, row) != 0);
}

/*
 * The three integer converters share one accessor: nanoarrow widens any Arrow
 * integer storage to int64, and the dispatch table only pairs an Arrow type
 * with a PostgreSQL type that holds every one of its values, so the narrowing
 * cast here cannot lose anything.
 */
static Datum
lance_conv_int2(LanceConverter *conv, int64 row)
{
	return Int16GetDatum((int16) ArrowArrayViewGetIntUnsafe(&conv->view, row));
}

static Datum
lance_conv_int4(LanceConverter *conv, int64 row)
{
	return Int32GetDatum((int32) ArrowArrayViewGetIntUnsafe(&conv->view, row));
}

static Datum
lance_conv_int8(LanceConverter *conv, int64 row)
{
	return Int64GetDatum(ArrowArrayViewGetIntUnsafe(&conv->view, row));
}

/*
 * float32 reaches this as a double that is exactly the float32 value, so the
 * cast back is lossless and NaN, the infinities and negative zero all survive.
 */
static Datum
lance_conv_float4(LanceConverter *conv, int64 row)
{
	return Float4GetDatum((float4) ArrowArrayViewGetDoubleUnsafe(&conv->view, row));
}

static Datum
lance_conv_float8(LanceConverter *conv, int64 row)
{
	return Float8GetDatum(ArrowArrayViewGetDoubleUnsafe(&conv->view, row));
}

static Datum
lance_conv_text(LanceConverter *conv, int64 row)
{
	struct ArrowStringView sv = ArrowArrayViewGetStringUnsafe(&conv->view, row);
	char	   *converted;
	Datum		result;

	lance_check_varlena_size(conv, sv.size_bytes);

	/* A column of nothing but empty strings has no data buffer at all. */
	if (sv.size_bytes == 0)
		return PointerGetDatum(cstring_to_text_with_len("", 0));

	/*
	 * Arrow says the bytes are UTF-8; a PostgreSQL text datum has to be in the
	 * database encoding, and may not contain a zero byte at all.  Copying the
	 * bytes in unchecked would mean mojibake in a LATIN1 database and a text
	 * value that pg_verify_mbstr would have rejected in a UTF-8 one, both of
	 * them silent.  pg_any_to_server converts where the encodings differ
	 * and validates where they do not, raising PostgreSQL's own "invalid byte
	 * sequence" error; the cost is one pass over the value, which is what
	 * every other reader of external text pays too.
	 */
	converted = pg_any_to_server(sv.data, (int) sv.size_bytes, PG_UTF8);

	if ((const char *) converted == sv.data)
		return PointerGetDatum(cstring_to_text_with_len(sv.data,
														(int) sv.size_bytes));

	result = PointerGetDatum(cstring_to_text(converted));
	pfree(converted);

	return result;
}

static Datum
lance_conv_bytea(LanceConverter *conv, int64 row)
{
	struct ArrowBufferView bv = ArrowArrayViewGetBytesUnsafe(&conv->view, row);
	bytea	   *result;

	lance_check_varlena_size(conv, bv.size_bytes);

	result = (bytea *) palloc(VARHDRSZ + bv.size_bytes);
	SET_VARSIZE(result, VARHDRSZ + bv.size_bytes);
	if (bv.size_bytes > 0)
		memcpy(VARDATA(result), bv.data.as_uint8, (size_t) bv.size_bytes);

	return PointerGetDatum(result);
}

/*
 * date32 is a day count from 1970-01-01 and PostgreSQL counts days from
 * 2000-01-01.  Arrow's range is the wider one at both ends, so a date that
 * PostgreSQL cannot represent is an error rather than a wrapped value.
 */
static Datum
lance_conv_date(LanceConverter *conv, int64 row)
{
	int64		days = ArrowArrayViewGetIntUnsafe(&conv->view, row);
	int64		pgdays = days - (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE);

	if (!IS_VALID_DATE(pgdays))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("lance_fdw: column \"%s\": date " INT64_FORMAT " days from 1970-01-01 is out of range for %s",
						conv->colname, days,
						format_type_with_typemod(conv->pgtypid, conv->pgtypmod))));

	return DateADTGetDatum((DateADT) pgdays);
}

static void lance_timestamp_out_of_range(LanceConverter *conv, int64 value,
										 const char *unit) pg_attribute_noreturn();

static void
lance_timestamp_out_of_range(LanceConverter *conv, int64 value, const char *unit)
{
	ereport(ERROR,
			(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
			 errmsg("lance_fdw: column \"%s\": timestamp " INT64_FORMAT " %s from 1970-01-01 is out of range for %s",
					conv->colname, value, unit,
					format_type_with_typemod(conv->pgtypid, conv->pgtypmod))));
}

/*
 * The tail the four unit converters share: move the epoch and check the range.
 *
 * timestamp and timestamptz have the same representation - microseconds from
 * 2000-01-01 UTC - and an Arrow timestamp is a UTC epoch whether or not it
 * carries a zone name, so the name is dropped rather than applied.
 * Which of the two the column is has already been settled in resolve().
 */
static Datum
lance_timestamp_datum(LanceConverter *conv, int64 usec, int64 value,
					  const char *unit)
{
	Timestamp	ts;

	if (pg_sub_s64_overflow(usec, LANCE_EPOCH_DIFF_USEC, &ts) ||
		!IS_VALID_TIMESTAMP(ts))
		lance_timestamp_out_of_range(conv, value, unit);

	return TimestampGetDatum(ts);
}

static Datum
lance_conv_ts_second(LanceConverter *conv, int64 row)
{
	int64		value = ArrowArrayViewGetIntUnsafe(&conv->view, row);
	int64		usec;

	if (pg_mul_s64_overflow(value, USECS_PER_SEC, &usec))
		lance_timestamp_out_of_range(conv, value, "s");

	return lance_timestamp_datum(conv, usec, value, "s");
}

static Datum
lance_conv_ts_milli(LanceConverter *conv, int64 row)
{
	int64		value = ArrowArrayViewGetIntUnsafe(&conv->view, row);
	int64		usec;

	if (pg_mul_s64_overflow(value, INT64CONST(1000), &usec))
		lance_timestamp_out_of_range(conv, value, "ms");

	return lance_timestamp_datum(conv, usec, value, "ms");
}

static Datum
lance_conv_ts_micro(LanceConverter *conv, int64 row)
{
	int64		value = ArrowArrayViewGetIntUnsafe(&conv->view, row);

	return lance_timestamp_datum(conv, value, value, "us");
}

static Datum
lance_conv_ts_nano(LanceConverter *conv, int64 row)
{
	int64		value = ArrowArrayViewGetIntUnsafe(&conv->view, row);

	/*
	 * PostgreSQL keeps microseconds.  Rounding a nanosecond value that is not
	 * a whole microsecond is precisely a silent loss, so it is an
	 * error instead.  Nothing in test/fixtures reaches this: every timestamp
	 * there is microsecond-aligned on purpose, so the branch is implemented
	 * but is not covered by a test.
	 */
	if (value % 1000 != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("lance_fdw: column \"%s\": nanosecond timestamp " INT64_FORMAT " is not a whole number of microseconds",
						conv->colname, value),
				 errdetail("%s keeps microseconds, and rounding the value would lose information.",
						   format_type_with_typemod(conv->pgtypid,
													conv->pgtypmod))));

	return lance_timestamp_datum(conv, value / 1000, value, "ns");
}

/*
 * decimal128 -> numeric.  The value is a 128-bit two's complement integer plus
 * a scale; nanoarrow renders that as a decimal string and numeric_in parses it
 * under the column's own typmod.  The text form is what keeps the scale Lance
 * stored - 0.0000000000 stays ten digits - and resolve() has already refused
 * any declaration numeric_in would have had to round to.
 */
static Datum
lance_conv_numeric(LanceConverter *conv, int64 row)
{
	struct ArrowDecimal dec;
	struct ArrowBuffer buffer;

	/* 39 digits, a sign and a point fit in this with room to spare */
	char		digits[64];
	int64		size;

	ArrowDecimalInit(&dec, 128, conv->decimal_precision, conv->decimal_scale);
	ArrowArrayViewGetDecimalUnsafe(&conv->view, row, &dec);

	ArrowBufferInit(&buffer);
	if (ArrowDecimalAppendStringToBuffer(&dec, &buffer) != NANOARROW_OK)
	{
		ArrowBufferReset(&buffer);
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("lance_fdw: column \"%s\": could not render a decimal value",
						conv->colname)));
	}

	size = buffer.size_bytes;
	if (size <= 0 || size >= (int64) sizeof(digits))
	{
		ArrowBufferReset(&buffer);
		ereport(ERROR,
				(errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
				 errmsg("lance_fdw: column \"%s\": decimal value needs " INT64_FORMAT " characters, more than a decimal128 can have",
						conv->colname, size)));
	}

	memcpy(digits, buffer.data, (size_t) size);
	digits[size] = '\0';
	ArrowBufferReset(&buffer);

	return DirectFunctionCall3(numeric_in, CStringGetDatum(digits),
							   ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(conv->pgtypmod));
}

/*
 * One PostgreSQL array out of one Arrow list element.  Both list shapes end up
 * here and differ only in how the element window is found: a fixed_size_list
 * has its elements at a fixed stride, a list has an offsets buffer.
 *
 * Elements go through a converter of their own, so a NULL element stays NULL
 * and every element value is converted by exactly the code a scalar column of
 * that type would use.
 */
static Datum
lance_conv_array(LanceConverter *conv, int64 row)
{
	LanceConverter *element = conv->element;
	int64		index = row + conv->view.offset;
	int64		start;
	int64		nelems;
	Datum	   *values;
	bool	   *nulls;
	int			dims[1];
	int			lbs[1];
	int64		i;

	if (conv->arrow_type == NANOARROW_TYPE_FIXED_SIZE_LIST)
	{
		start = index * conv->fixed_size;
		nelems = conv->fixed_size;
	}
	else
	{
		start = ArrowArrayViewListChildOffset(&conv->view, index);
		nelems = ArrowArrayViewListChildOffset(&conv->view, index + 1) - start;
	}

	if (start < 0 || nelems < 0 ||
		nelems > (int64) (MaxAllocSize / sizeof(Datum)))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("lance_fdw: column \"%s\": list of " INT64_FORMAT " elements at offset " INT64_FORMAT " cannot be read as %s",
						conv->colname, nelems, start,
						format_type_with_typemod(conv->pgtypid,
												 conv->pgtypmod))));

	/* An empty list is an empty array, which is not the same as NULL. */
	if (nelems == 0)
		return PointerGetDatum(construct_empty_array(conv->elemtypid));

	values = (Datum *) palloc(sizeof(Datum) * nelems);
	nulls = (bool *) palloc(sizeof(bool) * nelems);

	for (i = 0; i < nelems; i++)
	{
		int64		elemrow = start + i;

		nulls[i] = lance_arrow_converter_is_null(element, elemrow);
		values[i] = nulls[i] ? (Datum) 0 : element->convert(element, elemrow);
	}

	dims[0] = (int) nelems;
	lbs[0] = 1;

	return PointerGetDatum(construct_md_array(values, nulls, 1, dims, lbs,
											  conv->elemtypid, conv->elemlen,
											  conv->elembyval,
											  conv->elemalign));
}

/*
 * One composite value out of one Arrow struct element.
 *
 * The subfields have converters of their own, resolved against the declared
 * composite type's attributes, so each of them is read by exactly the code a
 * column of that type would use and a NULL subfield stays NULL - which is what
 * tells "the whole struct is NULL" (the caller never gets here) apart from "a
 * struct whose fields all happen to be NULL".
 *
 * The C data interface makes a struct's offset apply to its children and
 * nanoarrow does not propagate it, so it is added here, exactly as the list
 * converter does for its element window.
 */
static Datum
lance_conv_record(LanceConverter *conv, int64 row)
{
	int64		index = row + conv->view.offset;
	int			natts = conv->tupdesc->natts;
	Datum	   *values = (Datum *) palloc0(sizeof(Datum) * Max(natts, 1));
	bool	   *nulls = (bool *) palloc(sizeof(bool) * Max(natts, 1));
	HeapTuple	tuple;
	int			i;

	/* An attribute that was dropped from the composite type has no source. */
	memset(nulls, true, sizeof(bool) * Max(natts, 1));

	for (i = 0; i < conv->nfields; i++)
	{
		LanceConverter *sub = &conv->fields[i];
		int			att = conv->fieldatt[i];

		if (lance_arrow_converter_is_null(sub, index))
			continue;

		values[att] = sub->convert(sub, index);
		nulls[att] = false;
	}

	tuple = heap_form_tuple(conv->tupdesc, values, nulls);

	return HeapTupleGetDatum(tuple);
}

/* A value as its own output function prints it. */
static char *
lance_type_output(Oid typid, Datum value)
{
	Oid			outfunc;
	bool		isvarlena;

	getTypeOutputInfo(typid, &outfunc, &isvarlena);

	return OidOutputFunctionCall(outfunc, value);
}

/*
 * The entry window of one map element.  An Arrow map is a list of key/value
 * structs and carries the same int32 offsets buffer, but nanoarrow's list
 * accessor answers -1 for a map, so the buffer is read here - guarded, because
 * a short offsets buffer would otherwise be read past its end.
 */
static int64
lance_map_child_offset(const LanceConverter *conv, int64 i)
{
	const struct ArrowBufferView *offsets = &conv->view.buffer_views[1];

	if (i < 0 || offsets->data.data == NULL ||
		offsets->size_bytes < (int64) ((i + 1) * sizeof(int32)))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("lance_fdw: column \"%s\": Lance returned a map without offset " INT64_FORMAT,
						conv->colname, i)));

	return offsets->data.as_int32[i];
}

/*
 * One jsonb object out of one Arrow map element.
 *
 * The scalar value goes through the converter its Arrow type would get as a
 * column, and only then becomes a JsonbValue, so the epoch arithmetic, the
 * encoding conversion and the range checks are the same code paths as
 * everywhere else rather than a second implementation.
 */
static void
lance_jsonb_scalar(LanceConverter *conv, Oid typid, Datum value, JsonbValue *out)
{
	char	   *rendered;

	switch (typid)
	{
		case BOOLOID:
			out->type = jbvBool;
			out->val.boolean = DatumGetBool(value);
			return;

		case INT2OID:
			out->type = jbvNumeric;
			out->val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric,
																   Int32GetDatum((int32) DatumGetInt16(value))));
			return;

		case INT4OID:
			out->type = jbvNumeric;
			out->val.numeric = DatumGetNumeric(DirectFunctionCall1(int4_numeric,
																   value));
			return;

		case INT8OID:
			out->type = jbvNumeric;
			out->val.numeric = DatumGetNumeric(DirectFunctionCall1(int8_numeric,
																   value));
			return;

		case NUMERICOID:
			out->type = jbvNumeric;
			out->val.numeric = DatumGetNumeric(value);
			return;

		case FLOAT4OID:
		case FLOAT8OID:
			{
				double		d = (typid == FLOAT4OID)
					? (double) DatumGetFloat4(value) : DatumGetFloat8(value);

				/*
				 * JSON has no NaN and no infinity, and jsonb stores numbers as
				 * numeric, so there is nothing to write down.  Refusing the row
				 * is the honest answer; to_jsonb() does the same.
				 */
				if (isnan(d) || isinf(d))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("lance_fdw: column \"%s\": a map value is %s, which jsonb cannot hold",
									conv->colname, isnan(d) ? "NaN" : "an infinity")));

				/*
				 * Through the float's own output function rather than a cast:
				 * float4out prints the shortest digits that read back exactly,
				 * so 0.1 stays 0.1 instead of becoming its double expansion.
				 */
				rendered = lance_type_output(typid, value);
				out->type = jbvNumeric;
				out->val.numeric =
					DatumGetNumeric(DirectFunctionCall3(numeric_in,
														CStringGetDatum(rendered),
														ObjectIdGetDatum(InvalidOid),
														Int32GetDatum(-1)));
				pfree(rendered);
				return;
			}

		case DATEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				/*
				 * ISO 8601, which is what to_json() of the same value produces
				 * and what a JSON consumer expects, rather than the session's
				 * DateStyle.
				 */
				char		buf[MAXDATELEN + 1];

				rendered = JsonEncodeDateTime(buf, value, typid, NULL);
				out->type = jbvString;
				out->val.string.len = strlen(rendered);
				out->val.string.val = pstrdup(rendered);
				return;
			}

		case TEXTOID:
			{
				text	   *t = DatumGetTextPP(value);

				out->type = jbvString;
				out->val.string.len = VARSIZE_ANY_EXHDR(t);
				out->val.string.val = VARDATA_ANY(t);
				return;
			}

		default:
			{
				/*
				 * bytea is the only type that reaches this, and it becomes the
				 * string its output function prints (\x...), which is what a
				 * jsonb has room for.
				 */
				rendered = lance_type_output(typid, value);
				out->type = jbvString;
				out->val.string.len = strlen(rendered);
				out->val.string.val = rendered;
				return;
			}
	}
}

static Datum
lance_conv_map(LanceConverter *conv, int64 row)
{
	LanceConverter *entries = conv->element;
	LanceConverter *keyconv = &entries->fields[0];
	LanceConverter *valconv = &entries->fields[1];
	JsonbParseState *state = NULL;
	JsonbValue *result;
	int64		index = row + conv->view.offset;
	int64		start;
	int64		end;
	int64		i;

	start = lance_map_child_offset(conv, index);
	end = lance_map_child_offset(conv, index + 1);

	if (start < 0 || end < start)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("lance_fdw: column \"%s\": map entries " INT64_FORMAT " to " INT64_FORMAT " are not a range",
						conv->colname, start, end)));

	pushJsonbValue(&state, WJB_BEGIN_OBJECT, NULL);

	for (i = start; i < end; i++)
	{
		int64		entry = i + entries->view.offset;
		JsonbValue	key;
		JsonbValue	value;
		text	   *keytext;

		/*
		 * Arrow says a map key is not nullable and nanoarrow validates that, so
		 * this is a corrupt batch rather than a value to represent.
		 */
		if (lance_arrow_converter_is_null(keyconv, entry))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("lance_fdw: column \"%s\": map entry " INT64_FORMAT " has a NULL key",
							conv->colname, i)));

		keytext = DatumGetTextPP(keyconv->convert(keyconv, entry));
		key.type = jbvString;
		key.val.string.len = VARSIZE_ANY_EXHDR(keytext);
		key.val.string.val = VARDATA_ANY(keytext);
		pushJsonbValue(&state, WJB_KEY, &key);

		if (lance_arrow_converter_is_null(valconv, entry))
			value.type = jbvNull;
		else
			lance_jsonb_scalar(conv, valconv->pgtypid,
							   valconv->convert(valconv, entry), &value);

		pushJsonbValue(&state, WJB_VALUE, &value);
	}

	result = pushJsonbValue(&state, WJB_END_OBJECT, NULL);

	return PointerGetDatum(JsonbValueToJsonb(result));
}

/*
 * The dispatch table.  A row means "this Arrow type may be read into this
 * PostgreSQL type"; the absence of a row is what makes int64 into integer, or
 * utf8 into integer, an error at BeginForeignScan instead of a wrong value
 * later.  Widening within a family is present, narrowing never is.
 *
 * Adding a type is adding rows here plus the function they name.  Nothing else
 * in the scan path knows about types.
 *
 * Two things the table cannot say on its own, because they are not part of the
 * pair it is keyed by, are settled by resolve() below: a timestamp's unit and
 * zone, and how wide a numeric a decimal128 needs.  The list shapes have no
 * rows at all - their rule is their element's rule.
 */
typedef struct LanceConverterRule
{
	enum ArrowType arrow_type;
	Oid			pgtypid;
	LanceConvertFn convert;
} LanceConverterRule;

static const LanceConverterRule lance_converter_rules[] = {
	{NANOARROW_TYPE_BOOL, BOOLOID, lance_conv_bool},

	{NANOARROW_TYPE_INT8, INT2OID, lance_conv_int2},
	{NANOARROW_TYPE_INT8, INT4OID, lance_conv_int4},
	{NANOARROW_TYPE_INT8, INT8OID, lance_conv_int8},

	{NANOARROW_TYPE_INT16, INT2OID, lance_conv_int2},
	{NANOARROW_TYPE_INT16, INT4OID, lance_conv_int4},
	{NANOARROW_TYPE_INT16, INT8OID, lance_conv_int8},

	{NANOARROW_TYPE_INT32, INT4OID, lance_conv_int4},
	{NANOARROW_TYPE_INT32, INT8OID, lance_conv_int8},

	{NANOARROW_TYPE_INT64, INT8OID, lance_conv_int8},

	/* uint8 fits in smallint, uint16 in integer, uint32 in bigint */
	{NANOARROW_TYPE_UINT8, INT2OID, lance_conv_int2},
	{NANOARROW_TYPE_UINT8, INT4OID, lance_conv_int4},
	{NANOARROW_TYPE_UINT8, INT8OID, lance_conv_int8},

	{NANOARROW_TYPE_UINT16, INT4OID, lance_conv_int4},
	{NANOARROW_TYPE_UINT16, INT8OID, lance_conv_int8},

	{NANOARROW_TYPE_UINT32, INT8OID, lance_conv_int8},

	/*
	 * nanoarrow decodes a half float into the double it denotes, so float16
	 * needs no converter of its own; every value of it is exact in real.
	 */
	{NANOARROW_TYPE_HALF_FLOAT, FLOAT4OID, lance_conv_float4},
	{NANOARROW_TYPE_HALF_FLOAT, FLOAT8OID, lance_conv_float8},

	{NANOARROW_TYPE_FLOAT, FLOAT4OID, lance_conv_float4},
	{NANOARROW_TYPE_FLOAT, FLOAT8OID, lance_conv_float8},

	{NANOARROW_TYPE_DOUBLE, FLOAT8OID, lance_conv_float8},

	{NANOARROW_TYPE_STRING, TEXTOID, lance_conv_text},
	{NANOARROW_TYPE_STRING, VARCHAROID, lance_conv_text},
	{NANOARROW_TYPE_LARGE_STRING, TEXTOID, lance_conv_text},
	{NANOARROW_TYPE_LARGE_STRING, VARCHAROID, lance_conv_text},

	{NANOARROW_TYPE_BINARY, BYTEAOID, lance_conv_bytea},
	{NANOARROW_TYPE_LARGE_BINARY, BYTEAOID, lance_conv_bytea},

	{NANOARROW_TYPE_DATE32, DATEOID, lance_conv_date},

	/*
	 * The unit decides which of the four timestamp converters runs and the
	 * zone decides which of the two PostgreSQL types is the right one; both
	 * are read off the field in lance_resolve_timestamp().
	 */
	{NANOARROW_TYPE_TIMESTAMP, TIMESTAMPOID, lance_conv_ts_micro},
	{NANOARROW_TYPE_TIMESTAMP, TIMESTAMPTZOID, lance_conv_ts_micro},

	{NANOARROW_TYPE_DECIMAL128, NUMERICOID, lance_conv_numeric},

	{NANOARROW_TYPE_UNINITIALIZED, InvalidOid, NULL}
};

static LanceConvertFn
lance_converter_lookup(enum ArrowType arrow_type, Oid pgtypid)
{
	const LanceConverterRule *rule;

	for (rule = lance_converter_rules; rule->convert != NULL; rule++)
	{
		if (rule->arrow_type == arrow_type && rule->pgtypid == pgtypid)
			return rule->convert;
	}
	return NULL;
}

static bool
lance_arrow_is_list_type(enum ArrowType arrow_type)
{
	return arrow_type == NANOARROW_TYPE_LIST ||
		arrow_type == NANOARROW_TYPE_FIXED_SIZE_LIST;
}

/*
 * Does this build know how to read the Arrow type at all?  An A-tier type with
 * no rule anywhere is one this build has not implemented yet, which is a
 * different message from one that simply does not fit the declared column.
 *
 * The list, struct and map shapes are named here rather than in the table
 * because they have no rows of their own: a list's rule is its element's, and a
 * struct's is one per subfield against the declared composite type.
 */
static bool
lance_converter_type_known(enum ArrowType arrow_type)
{
	const LanceConverterRule *rule;

	if (lance_arrow_is_list_type(arrow_type) ||
		arrow_type == NANOARROW_TYPE_STRUCT ||
		arrow_type == NANOARROW_TYPE_MAP)
		return true;

	for (rule = lance_converter_rules; rule->convert != NULL; rule++)
	{
		if (rule->arrow_type == arrow_type)
			return true;
	}
	return false;
}

static const char *
lance_arrow_name_or(const struct ArrowSchema *field, const char *fallback)
{
	const char *name = lance_arrow_type_name(field);

	return name != NULL ? name : fallback;
}

/*
 * A length-limited character type would need the length enforced on every
 * value, and silently keeping a longer one is a silent distortion.
 */
static bool
lance_type_length_limited(Oid pgtypid, int32 pgtypmod)
{
	return (pgtypid == VARCHAROID || pgtypid == BPCHAROID) && pgtypmod >= 0;
}

/*
 * Is a decimal128(p,s) readable into the declared numeric?
 *
 * numeric(P,S) keeps S digits after the point and P-S before it, where the
 * Arrow type needs s and p-s; both halves have to fit.  A declaration that is
 * too narrow would have numeric_in round the value away or reject it value by
 * value, and that has to be refused before the scan starts.  Plain numeric
 * constrains nothing and always fits.
 */
static bool
lance_decimal_fits(const struct ArrowSchemaView *view, int32 pgtypmod)
{
	int32		precision;
	int32		scale;

	if (pgtypmod < (int32) VARHDRSZ)
		return true;

	/*
	 * The inverse of the encoding lance_arrow_map_type() writes, with the
	 * scale sign-extended out of its 11-bit field: PostgreSQL 15 and up allow
	 * a numeric to be declared with a negative scale.
	 */
	precision = (int32) (((pgtypmod - VARHDRSZ) >> 16) & 0xffff);
	scale = (int32) (((((pgtypmod - VARHDRSZ) & 0x7ff) ^ 1024) - 1024));

	return scale >= view->decimal_scale &&
		(precision - scale) >= (view->decimal_precision - view->decimal_scale);
}

/*
 * A timestamp's unit and zone are not part of its ArrowType, so the pair the
 * dispatch table is keyed by cannot settle three things on its own:
 *
 *	- a zone means the column is a timestamptz and no zone means it is a
 *	  timestamp; reading either as the other is refused rather than silently
 *	  reinterpreted,
 *	- the declared precision has to hold microseconds, because a timestamp(3)
 *	  would truncate every value PostgreSQL stored in it, and
 *	- the converter itself depends on the unit.
 *
 * NULL means the declaration does not fit, which the caller reports as the one
 * type-mismatch error.
 */
/*
 * The fewest fractional digits a declaration must keep to hold every value of
 * an Arrow timestamp unit exactly.  Seconds and milliseconds need fewer than
 * six: a timestamp(0) holds whole seconds without rounding anything away, and
 * refusing it would refuse a declaration that cannot lose a thing.
 */
static int32
lance_timestamp_min_typmod(enum ArrowTimeUnit unit)
{
	switch (unit)
	{
		case NANOARROW_TIME_UNIT_SECOND:
			return 0;
		case NANOARROW_TIME_UNIT_MILLI:
			return 3;
		case NANOARROW_TIME_UNIT_MICRO:
		case NANOARROW_TIME_UNIT_NANO:
			return LANCE_TIMESTAMP_TYPMOD;
	}

	return LANCE_TIMESTAMP_TYPMOD;
}

static LanceConvertFn
lance_resolve_timestamp(const struct ArrowSchemaView *view, Oid pgtypid,
						int32 pgtypmod)
{
	bool		has_zone = (view->timezone != NULL && view->timezone[0] != '\0');

	if (pgtypid != (has_zone ? TIMESTAMPTZOID : TIMESTAMPOID))
		return NULL;

	/* -1 is "no precision given", which keeps all six digits. */
	if (pgtypmod >= 0 &&
		pgtypmod < lance_timestamp_min_typmod(view->time_unit))
		return NULL;

	switch (view->time_unit)
	{
		case NANOARROW_TIME_UNIT_SECOND:
			return lance_conv_ts_second;
		case NANOARROW_TIME_UNIT_MILLI:
			return lance_conv_ts_milli;
		case NANOARROW_TIME_UNIT_MICRO:
			return lance_conv_ts_micro;
		case NANOARROW_TIME_UNIT_NANO:
			return lance_conv_ts_nano;
	}

	return NULL;
}

/*
 * Pick the converter for a scalar column: the table decides which pairs are
 * allowed at all, and the two types whose rule depends on more than the pair
 * get their say afterwards.  NULL means the declaration does not fit.
 */
static LanceConvertFn
lance_resolve_scalar(const struct ArrowSchemaView *view, Oid pgtypid,
					 int32 pgtypmod)
{
	LanceConvertFn convert = lance_converter_lookup(view->type, pgtypid);

	if (convert == NULL)
		return NULL;

	switch (view->type)
	{
		case NANOARROW_TYPE_TIMESTAMP:
			return lance_resolve_timestamp(view, pgtypid, pgtypmod);
		case NANOARROW_TYPE_DECIMAL128:
			return lance_decimal_fits(view, pgtypmod) ? convert : NULL;
		default:
			return convert;
	}
}

/* The state a converter reads out of the Arrow type it was resolved against. */
static void
lance_converter_set_arrow_state(LanceConverter *conv,
								const struct ArrowSchemaView *view)
{
	conv->arrow_type = view->type;
	conv->decimal_precision = view->decimal_precision;
	conv->decimal_scale = view->decimal_scale;
	conv->fixed_size = view->fixed_size;
}

static void lance_resolve_field(const struct ArrowSchema *field, Oid pgtypid,
								int32 pgtypmod, const char *colname,
								LanceConverter *out);

/* Initialise a subfield's array view; the caller has already hung it off *out. */
static void
lance_resolve_view(const struct ArrowSchema *field, LanceConverter *conv)
{
	struct ArrowError error;

	memset(&error, 0, sizeof(error));
	if (ArrowArrayViewInitFromSchema(&conv->view, field, &error) != NANOARROW_OK)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("lance_fdw: cannot read column \"%s\" of Arrow type \"%s\"",
						conv->colname, lance_arrow_format(field)),
				 errdetail("%s", error.message)));
}

/* A named composite type; bare record is not one, it has no fields yet. */
static bool
lance_type_is_composite(Oid pgtypid)
{
	return pgtypid != RECORDOID && get_typtype(pgtypid) == TYPTYPE_COMPOSITE;
}

/*
 * A struct is read into the composite type the column declares, subfield by
 * subfield in the order the Arrow schema has them.  Each subfield
 * gets a converter of its own, resolved against the attribute it fills, so a
 * struct of a struct or a struct of a list needs no case of its own here.
 *
 * A declaration that is not a composite type at all returns NULL and becomes
 * the caller's one type-mismatch error.  A composite type of the wrong shape
 * gets an error of its own, because "cannot be read as lance_t_c" would leave
 * the user to guess which of its fields is the problem.
 */
static LanceConvertFn
lance_resolve_struct(const struct ArrowSchema *field, Oid pgtypid,
					 int32 pgtypmod, LanceConverter *out)
{
	TupleDesc	tupdesc;
	int			nchildren = (int) field->n_children;
	int			live = 0;
	int			att;
	int			i;

	if (!lance_type_is_composite(pgtypid))
		return NULL;

	tupdesc = lookup_rowtype_tupdesc_copy(pgtypid, pgtypmod);
	out->tupdesc = tupdesc;

	for (i = 0; i < tupdesc->natts; i++)
	{
		if (!TupleDescAttr(tupdesc, i)->attisdropped)
			live++;
	}

	if (live != nchildren)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("lance_fdw: column \"%s\": %s has %d field(s), but the Lance struct has %d",
						out->colname,
						format_type_with_typemod(pgtypid, pgtypmod),
						live, nchildren),
				 errhint("Re-run IMPORT FOREIGN SCHEMA, which builds the composite "
						 "type from the Lance schema.")));

	out->fields = (LanceConverter *) palloc0(sizeof(LanceConverter) *
											 Max(nchildren, 1));
	out->fieldatt = (int *) palloc0(sizeof(int) * Max(nchildren, 1));

	att = 0;
	for (i = 0; i < nchildren; i++)
	{
		struct ArrowSchema *child = field->children[i];
		Form_pg_attribute attr;
		char	   *subname;

		while (att < tupdesc->natts && TupleDescAttr(tupdesc, att)->attisdropped)
			att++;

		attr = TupleDescAttr(tupdesc, att);
		out->fieldatt[i] = att;
		att++;

		/*
		 * Errors below name the subfield by its path under the column, which is
		 * the whole point of resolving them one at a time.
		 */
		subname = psprintf("%s.%s", out->colname,
						   child->name != NULL ? child->name : "?");

		/* Countable before it is resolved, so cleanup reaches a half-built one. */
		out->nfields = i + 1;
		lance_resolve_field(child, attr->atttypid, attr->atttypmod, subname,
							&out->fields[i]);
	}

	return lance_conv_record;
}

/*
 * A map is read as jsonb and nothing else: an object is the only JSON shape a
 * map has, and any other declaration would be a different value.
 *
 * The entries struct gets a converter that is never called - it exists so that
 * the key and the value converters are bound to their arrays by the same code
 * that binds a struct's subfields, and so that the entries array's own offset
 * is available when the entry window is read.
 */
static LanceConvertFn
lance_resolve_map(const struct ArrowSchema *field, Oid pgtypid,
				  LanceConverter *out)
{
	struct ArrowSchema *entries;
	struct ArrowSchemaView entryview;
	struct ArrowError error;
	LanceConverter *entryconv;
	int			i;

	if (pgtypid != JSONBOID)
		return NULL;

	/* map_type() accepted the field, so the entries struct is there. */
	entries = field->children[0];

	memset(&error, 0, sizeof(error));
	if (ArrowSchemaViewInit(&entryview, entries, &error) != NANOARROW_OK)
		return NULL;

	entryconv = (LanceConverter *) palloc0(sizeof(LanceConverter));
	entryconv->colname = out->colname;
	entryconv->pgtypid = pgtypid;
	entryconv->pgtypmod = -1;
	lance_converter_set_arrow_state(entryconv, &entryview);

	/* Reachable by the scan's cleanup before anything below can fail. */
	out->element = entryconv;
	lance_resolve_view(entries, entryconv);

	entryconv->fields = (LanceConverter *) palloc0(sizeof(LanceConverter) * 2);

	for (i = 0; i < 2; i++)
	{
		struct ArrowSchema *child = entries->children[i];
		struct ArrowSchemaView childview;
		Oid			childtypid;
		int32		childtypmod;

		memset(&error, 0, sizeof(error));
		if (ArrowSchemaViewInit(&childview, child, &error) != NANOARROW_OK ||
			!lance_arrow_map_scalar(&childview, &childtypid, &childtypmod))
			return NULL;	/* map_type() has already refused this */

		entryconv->nfields = i + 1;
		lance_resolve_field(child, childtypid, childtypmod, out->colname,
							&entryconv->fields[i]);
	}

	return lance_conv_map;
}

/*
 * A list-shaped column has no row in the dispatch table: its rule is its
 * element's rule.  That is what lets list<int64> be read as bigint[] and
 * refuses it as integer[], with the same widening the element type would get
 * as a column of its own, without a row per (list type, array type) pair.
 *
 * On success *out gains an element converter with a view of its own, which
 * set_array() then points at the child array.  NULL means the declared type is
 * not an array of anything the element can be read as, which the caller
 * reports as the one type-mismatch error.
 */
static LanceConvertFn
lance_resolve_list(const struct ArrowSchema *field,
				   const struct ArrowSchemaView *view, Oid pgtypid,
				   int32 pgtypmod, LanceConverter *out)
{
	Oid			elemtypid = get_element_type(pgtypid);
	struct ArrowSchema *child;
	struct ArrowSchemaView childview;
	struct ArrowError error;
	LanceConverter *element;
	LanceConvertFn elemconvert;

	/*
	 * An array column carries its element's typmod, so numeric(10,2)[] and
	 * varchar(4)[] constrain their elements exactly as the scalar columns do.
	 */
	if (!OidIsValid(elemtypid) || lance_type_length_limited(elemtypid, pgtypmod))
		return NULL;

	/* map_type() accepted the field, so it has exactly one A-tier child. */
	child = field->children[0];

	memset(&error, 0, sizeof(error));
	if (ArrowSchemaViewInit(&childview, child, &error) != NANOARROW_OK)
		return NULL;

	/*
	 * A struct or a map element is resolved by the recursion rather than here,
	 * because it needs subfield converters of its own; the element type has to
	 * be able to hold one before that starts, so that a plain mismatch still
	 * gets the caller's message about the array rather than about the element.
	 */
	if (childview.type == NANOARROW_TYPE_STRUCT ||
		childview.type == NANOARROW_TYPE_MAP)
	{
		if (childview.type == NANOARROW_TYPE_MAP
			? elemtypid != JSONBOID
			: !lance_type_is_composite(elemtypid))
			return NULL;

		element = (LanceConverter *) palloc0(sizeof(LanceConverter));
		out->element = element;
		lance_resolve_field(child, elemtypid, -1, out->colname, element);
	}
	else
	{
		elemconvert = lance_resolve_scalar(&childview, elemtypid, pgtypmod);
		if (elemconvert == NULL)
			return NULL;

		element = (LanceConverter *) palloc0(sizeof(LanceConverter));
		element->convert = elemconvert;

		/* Errors about an element name the column, not the Arrow child's "item". */
		element->colname = out->colname;
		element->pgtypid = elemtypid;
		element->pgtypmod = pgtypmod;
		lance_converter_set_arrow_state(element, &childview);

		/*
		 * Hang the element off *out before its view is initialised: from here on
		 * the scan's cleanup can reach it, so an error below gives back whatever
		 * the half-built view had allocated.
		 */
		out->element = element;
		lance_resolve_view(child, element);
	}

	out->elemtypid = elemtypid;
	get_typlenbyvalalign(elemtypid, &out->elemlen, &out->elembyval,
						 &out->elemalign);

	return lance_conv_array;
}

/*
 * What to tell a user whose declaration does not fit.  A struct has no type to
 * name: the composite type is made by IMPORT FOREIGN SCHEMA out of the table's
 * name and the column's, so there is nothing to point at here.
 */
static const char *
lance_resolve_hint(Oid natural_typid, int32 natural_typmod)
{
	if (natural_typid == RECORDOID)
		return "A Lance struct is read as a composite type; IMPORT FOREIGN SCHEMA "
			"creates one named after the foreign table and the column.";

	if (natural_typid == RECORDARRAYOID)
		return "A Lance list of structs is read as an array of the composite type "
			"IMPORT FOREIGN SCHEMA creates for it.";

	return psprintf("Declare the column %s.",
					format_type_with_typemod(natural_typid, natural_typmod));
}

static void
lance_resolve_field(const struct ArrowSchema *field, Oid pgtypid,
					int32 pgtypmod, const char *colname, LanceConverter *out)
{
	struct ArrowSchemaView view;
	struct ArrowError error;
	Oid			natural_typid;
	int32		natural_typmod;
	bool		is_b_tier;

	memset(out, 0, sizeof(*out));

	out->colname = pstrdup(colname);
	out->pgtypid = pgtypid;
	out->pgtypmod = pgtypmod;

	/*
	 * The tier decision comes first and is independent of what the table
	 * declares: a B-tier column is unreadable whatever the user wrote.
	 * Where the reason is a part of the field - a subfield, a map key - the
	 * detail names it, because the column's own type says "struct" and no more.
	 */
	if (!lance_arrow_map_type(field, &natural_typid, &natural_typmod,
							  &is_b_tier))
	{
		char	   *detail = lance_arrow_b_tier_detail(field);

		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("lance_fdw: column \"%s\": Lance type \"%s\" (%s) is not supported and cannot be read as %s",
						colname, lance_arrow_format(field),
						lance_arrow_name_or(field, "unparsable"),
						format_type_with_typemod(pgtypid, pgtypmod)),
				 detail != NULL ? errdetail("%s", detail) : 0,
				 errhint("Drop the column from the foreign table, or read it "
						 "with a tool that understands the type.")));
	}

	memset(&error, 0, sizeof(error));
	if (ArrowSchemaViewInit(&view, field, &error) != NANOARROW_OK)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("lance_fdw: cannot parse Arrow type \"%s\" of column \"%s\"",
						lance_arrow_format(field), colname),
				 errdetail("%s", error.message)));

	lance_converter_set_arrow_state(out, &view);

	if (lance_arrow_is_list_type(view.type))
		out->convert = lance_resolve_list(field, &view, pgtypid, pgtypmod, out);
	else if (view.type == NANOARROW_TYPE_STRUCT)
		out->convert = lance_resolve_struct(field, pgtypid, pgtypmod, out);
	else if (view.type == NANOARROW_TYPE_MAP)
		out->convert = lance_resolve_map(field, pgtypid, out);
	else
		out->convert = lance_resolve_scalar(&view, pgtypid, pgtypmod);

	/*
	 * Every A-tier type has an implementation, so nothing reaches
	 * this today; it stays as the honest message should map_type() ever gain a
	 * type before the dispatch table does.
	 */
	if (out->convert == NULL && !lance_converter_type_known(view.type))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("lance_fdw: column \"%s\": Lance type \"%s\" (%s) is not supported in this build and cannot be read as %s",
						colname, lance_arrow_format(field),
						lance_arrow_name_or(field, "unparsable"),
						format_type_with_typemod(pgtypid, pgtypmod)),
				 errdetail("This Arrow type maps to %s, but no converter for it "
						   "is compiled in yet.",
						   format_type_with_typemod(natural_typid,
													natural_typmod))));

	if (out->convert == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("lance_fdw: column \"%s\": Lance type \"%s\" (%s) cannot be read as %s",
						colname, lance_arrow_format(field),
						lance_arrow_name_or(field, "unparsable"),
						format_type_with_typemod(pgtypid, pgtypmod)),
				 errhint("%s", lance_resolve_hint(natural_typid, natural_typmod))));

	/*
	 * The same refusal lance_resolve_list() makes for an element, with the
	 * detail a column of its own can carry.
	 */
	if (lance_type_length_limited(pgtypid, pgtypmod))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("lance_fdw: column \"%s\": Lance type \"%s\" (%s) cannot be read as %s",
						colname, lance_arrow_format(field),
						lance_arrow_name_or(field, "unparsable"),
						format_type_with_typemod(pgtypid, pgtypmod)),
				 errdetail("A length limit would have to truncate values."),
				 errhint("Declare the column text.")));

	lance_resolve_view(field, out);
}

void
lance_arrow_resolve_converter(const struct ArrowSchema *field, Oid pgtypid,
							  int32 pgtypmod, LanceConverter *out)
{
	lance_resolve_field(field, pgtypid, pgtypmod,
						(field != NULL && field->name != NULL) ? field->name : "?",
						out);
}

/*
 * Sum the buffers a view can reach.
 *
 * This is memory lance allocated on the Rust side, which palloc knows nothing
 * about; the only reason to measure it is to hand the number to the vmem
 * tracker.  It is a floor rather than the whole truth - lance may hold pages
 * and readahead batches behind these buffers - and a floor measured from the
 * data is worth more here than a guess with a multiplier on it.
 */
static int64
lance_arrow_view_bytes(const struct ArrowArrayView *view)
{
	int64		bytes = 0;
	int			i;
	int64		c;

	if (view == NULL)
		return 0;

	/*
	 * nanoarrow spells "size not known yet" as -1, and a buffer the layout
	 * does not use keeps whatever it was initialised to.  Adding either would
	 * shrink the total, and a total that reached zero would skip the
	 * reservation without a word, so only real sizes are counted.
	 */
	for (i = 0; i < NANOARROW_MAX_FIXED_BUFFERS; i++)
		if (view->buffer_views[i].size_bytes > 0)
			bytes += (int64) view->buffer_views[i].size_bytes;

	/* string_view / binary_view keep their data outside the fixed buffers. */
	for (i = 0; i < view->n_variadic_buffers; i++)
		if (view->variadic_buffer_sizes[i] > 0)
			bytes += view->variadic_buffer_sizes[i];

	for (c = 0; c < view->n_children; c++)
		bytes += lance_arrow_view_bytes(view->children[c]);

	return bytes + lance_arrow_view_bytes(view->dictionary);
}

int64
lance_arrow_converter_bytes(const LanceConverter *conv)
{
	return conv == NULL ? 0 : lance_arrow_view_bytes(&conv->view);
}

void
lance_arrow_converter_set_array(LanceConverter *conv,
								const struct ArrowArray *array)
{
	struct ArrowError error;
	int			i;

	memset(&error, 0, sizeof(error));
	if (ArrowArrayViewSetArray(&conv->view, array, &error) != NANOARROW_OK)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				 errmsg("lance_fdw: column \"%s\": Lance returned a batch this build cannot read",
						conv->colname),
				 errdetail("%s", error.message)));

	/*
	 * A list column's element converter keeps a view of its own so that the
	 * scalar converters work on an element unchanged.  It is pointed at the
	 * child array, whose own offset its accessors then apply.  A map's one child
	 * is the entries struct, which binds the key and the value below.
	 */
	if (conv->element != NULL)
	{
		if (array->n_children != 1 || array->children == NULL ||
			array->children[0] == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("lance_fdw: column \"%s\": Lance returned a list of " INT64_FORMAT " children, expected 1",
							conv->colname, array->n_children)));

		lance_arrow_converter_set_array(conv->element, array->children[0]);
	}

	/* One child array per subfield, in the order the schema had them. */
	if (conv->fields != NULL)
	{
		if (array->n_children != (int64) conv->nfields || array->children == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("lance_fdw: column \"%s\": Lance returned a struct of " INT64_FORMAT " children, expected %d",
							conv->colname, array->n_children, conv->nfields)));

		for (i = 0; i < conv->nfields; i++)
		{
			if (array->children[i] == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
						 errmsg("lance_fdw: column \"%s\": Lance returned no data for subfield %d",
								conv->colname, i + 1)));

			lance_arrow_converter_set_array(&conv->fields[i], array->children[i]);
		}
	}
}

void
lance_arrow_converter_reset(LanceConverter *conv)
{
	int			i;

	/*
	 * A list, struct or map column's views are the ones with something outside
	 * palloc to give back: ArrowArrayViewInitFromSchema allocates a child view
	 * per level, in the parent's own view and again in every subfield
	 * converter's.  The converters themselves are palloc'd and go with the
	 * scan's context, as does a composite type's descriptor.
	 */
	if (conv->element != NULL)
	{
		lance_arrow_converter_reset(conv->element);
		conv->element = NULL;
	}

	for (i = 0; i < conv->nfields; i++)
		lance_arrow_converter_reset(&conv->fields[i]);
	conv->nfields = 0;
	conv->fields = NULL;

	ArrowArrayViewReset(&conv->view);
}

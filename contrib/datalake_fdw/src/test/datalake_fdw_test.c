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
 * datalake_fdw_test.c
 *	  The format layer, reachable from SQL.
 *
 * A data file is written and read by the access method, which is not finished;
 * until it is, there is no way to run the format layer in a real backend, and
 * "it compiles" would be the only thing anyone could say about it.  These two
 * functions are that way in: they write the result of a query to a file and
 * read a file back as rows, so a round trip is an ordinary SQL statement.
 *
 * They are a separate extension because they are not part of what this module
 * offers -- installing datalake_fdw does not put them in the database.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/test/datalake_fdw_test.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <stdlib.h>

#include "catalog/pg_type.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"

#include "am_iceberg/pg_iceberg_guc.h"
#include "am_iceberg/pg_iceberg_options.h"
#include "iceberg_volume_fdw/iceberg_volume_option.h"
#include "common/dl_err.h"
#include "common/file_system_wrapper.h"
#include "format/arrow_builder.h"
#include "format/arrow_decode.h"
#include "format/format.h"

PG_FUNCTION_INFO_V1(datalake_parquet_write);
PG_FUNCTION_INFO_V1(datalake_parquet_read);
PG_FUNCTION_INFO_V1(datalake_storage_write_text);
PG_FUNCTION_INFO_V1(datalake_storage_read_text);
PG_FUNCTION_INFO_V1(datalake_storage_list);
PG_FUNCTION_INFO_V1(datalake_storage_delete);
PG_FUNCTION_INFO_V1(datalake_storage_probe);
PG_FUNCTION_INFO_V1(datalake_storage_register_bad);

extern DlErrCode datalake_test_register_bad_storage_backend(const char *kind);

static DlKeyValue *
storage_kv(FunctionCallInfo fcinfo, int argno, int *nkv)
{
	Datum	  *values;
	bool	  *nulls;
	DlKeyValue *kv;
	int			i;

	*nkv = 0;
	if (PG_ARGISNULL(argno))
		return NULL;

	deconstruct_array(PG_GETARG_ARRAYTYPE_P(argno), TEXTOID, -1, false,
					  TYPALIGN_INT, &values, &nulls, nkv);
	kv = palloc0(*nkv * sizeof(*kv));
	for (i = 0; i < *nkv; i++)
	{
		char	   *equal;

		if (nulls[i])
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("a storage option cannot be null")));
		kv[i].key = TextDatumGetCString(values[i]);
		equal = strchr(kv[i].key, '=');
		if (equal == NULL || equal == kv[i].key)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("storage option must have the form key=value")));
		*equal = '\0';
		kv[i].value = equal + 1;
	}
	return kv;
}

static void
storage_parse_uri(const char *uri, bool leaf, DatalakeLocation *location,
				  char **relative)
{
	char	   *detail = NULL;
	DlErrCode	rc;

	/*
	 * The production parser, dltest included: it accepts any scheme a backend
	 * has registered, so the test backend is addressed exactly the way a
	 * third party's would be.
	 */
	rc = pg_iceberg_parse_location(uri, NULL, NULL, location, &detail);
	if (rc != DL_OK)
	{
		dl_error_set(rc, "parse storage location", NULL, detail);
		dl_error_report(ERROR, rc, "parse storage location");
	}

	*relative = pstrdup("");
	if (leaf)
	{
		char	   *slash = strrchr(location->path_prefix, '/');

		if (slash == NULL || slash[1] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("storage URI must name a file")));
		*relative = pstrdup(slash + 1);
		if (slash == location->path_prefix)
			location->path_prefix = pstrdup(
				strcmp(location->scheme, "s3") == 0 ? "" : "/");
		else
			*slash = '\0';
	}
}

/*
 * Mount whatever the caller named.  A bare path is a local absolute path --
 * which is what the Parquet cases have always passed -- and a URI names a
 * volume's scheme; when a volume is given, its server options and the calling
 * user's credentials are what the backend gets.
 */
static DatalakeFileSystem
storage_open_volume(const char *path_or_uri, const char *volume, bool leaf,
					char **relative)
{
	DatalakeLocation location;
	DatalakeFileSystem fs = NULL;
	DlKeyValue *kv = NULL;
	char	   *uri;
	int			nkv = 0;
	DlErrCode	rc;

	uri = strstr(path_or_uri, "://") != NULL ? pstrdup(path_or_uri) :
		psprintf("file://%s", path_or_uri);

	storage_parse_uri(uri, leaf, &location, relative);

	if (volume != NULL)
	{
		DatalakeLocation volume_location;
		Size		prefix_len;

		iceberg_volume_resolve(volume, GetUserId(), &volume_location, &kv, &nkv);

		/*
		 * A volume's credentials belong to the volume's storage.  Without
		 * this, naming any volume would lend its keys to any bucket the
		 * caller cared to type.
		 */
		prefix_len = strlen(volume_location.path_prefix);
		if (strcmp(location.scheme, volume_location.scheme) != 0 ||
			strcmp(location.authority, volume_location.authority) != 0 ||
			strncmp(location.path_prefix, volume_location.path_prefix,
					prefix_len) != 0 ||
			(location.path_prefix[prefix_len] != '\0' &&
			 location.path_prefix[prefix_len] != '/'))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("\"%s\" is not inside volume \"%s\"",
							path_or_uri, volume)));

		/* The volume says how to reach it; the URI says which object. */
		location.endpoint = volume_location.endpoint;
		location.region = volume_location.region;
	}

	rc = datalake_fs_open(&location, kv, nkv, &fs);
	if (rc != DL_OK)
		dl_error_report(ERROR, rc, "open storage");
	return fs;
}

static DatalakeFileSystem
storage_open_uri(FunctionCallInfo fcinfo, int uri_arg, int kv_arg, bool leaf,
				 char **relative)
{
	DatalakeLocation location;
	DatalakeFileSystem fs = NULL;
	DlKeyValue *kv;
	char	   *uri = text_to_cstring(PG_GETARG_TEXT_PP(uri_arg));
	int			nkv;
	DlErrCode	rc;

	storage_parse_uri(uri, leaf, &location, relative);
	kv = storage_kv(fcinfo, kv_arg, &nkv);
	rc = datalake_fs_open(&location, kv, nkv, &fs);
	if (rc != DL_OK)
		dl_error_report(ERROR, rc, "open storage");
	return fs;
}

/*
 * The SQL declaration and the C function have to agree on the argument list,
 * and nothing checks that they do: a database where the extension was created
 * from an older datalake_fdw_test--1.0.sql hands over fewer arguments than the
 * function reads, and reading one that is not there is a crash.  This turns
 * that into an error naming the fix.
 */
static void
check_nargs(FunctionCallInfo fcinfo, int expected)
{
	if (PG_NARGS() != expected)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("datalake_fdw_test is out of date: the function was declared with %d arguments and expects %d",
						PG_NARGS(), expected),
				 errhint("DROP EXTENSION datalake_fdw_test and CREATE it again.")));
}

static const FormatRoutine *
parquet_routine(void)
{
	const FormatRoutine *routine = GetFormatRoutine("parquet");

	/*
	 * Only reachable from a build that dropped the format, so what it can say
	 * is whatever the registry recorded -- guessing at a reason here would be
	 * a message that outlives the thing it describes.
	 */
	if (routine == NULL)
		dl_error_report(ERROR, DL_ERR_NOT_SUPPORTED, "get_format");

	return routine;
}

/*
 * Hands one batch to the writer.  The batch is consumed either way, so there is
 * nothing left to release when this reports a failure.
 */
static void
write_one_batch(FormatWriter *writer, DlArrowBuilder builder)
{
	struct ArrowArray batch;
	DlErrCode	rc;

	rc = dl_arrow_builder_flush(builder, &batch);
	if (rc != DL_OK)
		dl_error_report(ERROR, rc, "build_batch");

	rc = writer->ops->write_batch(writer, &batch);
	if (rc != DL_OK)
		dl_error_report(ERROR, rc, "write_batch");
}

/*
 * datalake_parquet_write(path, query, row_group_size, compression) -> rows written
 *
 * The rows the query returns are written to `path` as Parquet.  A row group
 * size of zero leaves the format's own default in place; anything else also
 * becomes the number of rows per batch, because a row group is closed at a
 * batch boundary and the option would otherwise be rounded away by a batch size
 * that does not divide by it.  An empty compression name means the format's
 * default: the function is STRICT, so NULL cannot be the way to say that.
 *
 * The columns are given field ids 1..n, which is what a new table's would be.
 */
Datum
datalake_parquet_write(PG_FUNCTION_ARGS)
{
	char	   *path;
	char	   *query;
	int32		row_group_size;
	char	   *compression;
	const FormatRoutine *routine;
	WriterOptions options = {0};
	FormatWriter *volatile open_writer = NULL;
	DlArrowBuilder volatile open_builder = NULL;
	DatalakeFileSystem volatile open_fs = NULL;
	char	   *relative;
	long		batch_rows = iceberg_batch_rows;
	int64		written = 0;
	MemoryContext row_context;

	check_nargs(fcinfo, 5);
	/* Not STRICT, because volume is optional; the rest are not. */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
		PG_RETURN_NULL();
	path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	query = text_to_cstring(PG_GETARG_TEXT_PP(1));
	row_group_size = PG_GETARG_INT32(2);
	compression = text_to_cstring(PG_GETARG_TEXT_PP(3));
	routine = parquet_routine();
	open_fs = storage_open_volume(path,
								  PG_ARGISNULL(4) ? NULL :
								  text_to_cstring(PG_GETARG_TEXT_PP(4)),
								  true, &relative);

	/*
	 * Bounded above as well as below, and by the same number as
	 * iceberg.batch_rows: a row group is held in memory until it is complete,
	 * so an unbounded one asks the writer to buffer the whole result set.
	 */
	if (row_group_size < 0 || row_group_size > DL_MAX_ROW_GROUP_ROWS)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("row group size must be between 0 and %d",
						DL_MAX_ROW_GROUP_ROWS)));

	options.row_group_size = row_group_size;
	options.compression = compression[0] != '\0' ? compression : NULL;
	options.field_ids = NULL;
	if (row_group_size > 0 && row_group_size < batch_rows)
		batch_rows = row_group_size;

	if (SPI_connect() != SPI_OK_CONNECT)
		elog(ERROR, "SPI_connect failed");

	/*
	 * Detoasting a value allocates, and the copy is dead as soon as it has been
	 * appended.  Without a context of its own, a wide table would hold every
	 * copy it ever made until the function returned.
	 */
	row_context = AllocSetContextCreate(CurrentMemoryContext,
										"datalake_parquet_write",
										ALLOCSET_DEFAULT_SIZES);

	PG_TRY();
	{
		SPIPlanPtr	plan;
		Portal		portal;
		TupleDesc	tupdesc = NULL;
		FormatWriter *writer = NULL;
		DlArrowBuilder builder = NULL;
		Datum	   *values = NULL;
		bool	   *nulls = NULL;
		DlErrCode	rc;

		plan = SPI_prepare(query, 0, NULL);
		if (plan == NULL)
			elog(ERROR, "SPI_prepare failed: %s",
				 SPI_result_code_string(SPI_result));

		portal = SPI_cursor_open(NULL, plan, NULL, NULL, true);

		for (;;)
		{
			MemoryContext oldcontext;
			uint64		i;

			SPI_cursor_fetch(portal, true, batch_rows);

			if (SPI_tuptable == NULL)
				elog(ERROR, "the query did not return a result set");

			/*
			 * The descriptor is only available once something has been
			 * fetched, and the writer needs it before the first row can be
			 * appended -- so the file is created here rather than before the
			 * loop.  The copy outlives SPI_freetuptable(), which frees the
			 * descriptor along with the rows it described.
			 */
			if (tupdesc == NULL)
			{
				tupdesc = CreateTupleDescCopy(SPI_tuptable->tupdesc);

				rc = routine->open_writer((DatalakeFileSystem) open_fs, relative,
										  tupdesc, &options, &writer);
				if (rc != DL_OK)
					dl_error_report(ERROR, rc, "open_writer");
				open_writer = writer;

				rc = dl_arrow_builder_open(tupdesc, &builder);
				if (rc != DL_OK)
					dl_error_report(ERROR, rc, "open_builder");
				open_builder = builder;

				values = palloc(tupdesc->natts * sizeof(Datum));
				nulls = palloc(tupdesc->natts * sizeof(bool));
			}

			if (SPI_processed == 0)
				break;

			oldcontext = MemoryContextSwitchTo(row_context);

			for (i = 0; i < SPI_processed; i++)
			{
				HeapTuple	tuple = SPI_tuptable->vals[i];
				int			attno;

				CHECK_FOR_INTERRUPTS();

				for (attno = 0; attno < tupdesc->natts; attno++)
				{
					Form_pg_attribute attr = TupleDescAttr(tupdesc, attno);
					bool		isnull;
					Datum		value = SPI_getbinval(tuple, SPI_tuptable->tupdesc,
													  attno + 1, &isnull);

					/*
					 * The Arrow side runs as C++ and must not allocate, so a
					 * value that is compressed or stored out of line is
					 * expanded here, where failing to do so is an ordinary
					 * error rather than an exception crossing an ABI.
					 */
					if (!isnull && attr->attlen == -1)
						value = PointerGetDatum(PG_DETOAST_DATUM_PACKED(value));

					values[attno] = value;
					nulls[attno] = isnull;
				}

				rc = dl_arrow_builder_append(builder, values, nulls,
											 tupdesc->natts);
				if (rc != DL_OK)
					dl_error_report(ERROR, rc, "append_row");

				written++;
			}

			MemoryContextSwitchTo(oldcontext);
			MemoryContextReset(row_context);

			write_one_batch(writer, builder);
			SPI_freetuptable(SPI_tuptable);
		}

		SPI_cursor_close(portal);

		/*
		 * A query that returned nothing still produces a file, with the schema
		 * and no rows: an empty file is a fact about the query, and a missing
		 * one would be a fact about this function.
		 */
		rc = writer->ops->finish(&writer, NULL);
		open_writer = NULL;		/* consumed, whether or not it succeeded */
		if (rc != DL_OK)
			dl_error_report(ERROR, rc, "finish_writer");

		dl_arrow_builder_close(&builder);
		open_builder = NULL;
	}
	PG_CATCH();
	{
		DlArrowBuilder builder = open_builder;
		FormatWriter *writer = open_writer;
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;

		if (builder != NULL)
			dl_arrow_builder_close(&builder);
		if (writer != NULL)
			writer->ops->abort(&writer);
		datalake_fs_close(&fs);

		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();

	{
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;

		datalake_fs_close(&fs);
	}

	PG_RETURN_INT64(written);
}

/*
 * datalake_parquet_read(path, first_row_group, n_row_groups, field_ids)
 *	  -> setof record
 *
 * The column definition list says what the caller expects the file to hold, and
 * is checked against the file's own schema rather than assumed: reading an
 * Arrow column as the wrong PostgreSQL type would produce values, just not the
 * ones in the file.
 *
 * With an empty field id list the file is read as it is, every column in the
 * file's order, and the definition list has to match it column for column.
 * With one, each entry names the Iceberg field id the corresponding column of
 * the definition list is to be read from, in the way a table's columns are
 * matched to a data file's; an id the file does not have reads as NULL.
 *
 * The row group arguments are the unit a scan is divided at.  Reading 0..0 and
 * then 1..1 has to produce exactly what reading the whole file does, which is
 * the property a scan spread across segments will depend on.
 */
Datum
datalake_parquet_read(PG_FUNCTION_ARGS)
{
	char	   *path;
	ArrayType  *field_id_array;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	const FormatRoutine *routine;
	FormatReader *volatile open_reader = NULL;
	struct ArrowArray *batch = palloc0(sizeof(struct ArrowArray));
	struct ArrowSchema *schema = palloc0(sizeof(struct ArrowSchema));
	Fragment	fragment = {0};
	ProjectionSet projection = {0};
	const ProjectionSet *projection_arg = NULL;
	Datum	   *field_id_datums;
	bool	   *field_id_nulls;
	int			nfield_ids;
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	Datum	   *values;
	bool	   *nulls;
	FormatReader *reader = NULL;
	DatalakeFileSystem volatile open_fs = NULL;
	char	   *relative;
	MemoryContext row_context;
	DlErrCode	rc;

	check_nargs(fcinfo, 5);
	/* Not STRICT, because volume is optional; the rest are not. */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("path, first_row_group, n_row_groups and field_ids are required")));
	path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	field_id_array = PG_GETARG_ARRAYTYPE_P(3);
	routine = parquet_routine();
	open_fs = storage_open_volume(path,
								  PG_ARGISNULL(4) ? NULL :
								  text_to_cstring(PG_GETARG_TEXT_PP(4)),
								  true, &relative);

	fragment.fs = (DatalakeFileSystem) open_fs;
	fragment.path = relative;
	fragment.first_row_group = PG_GETARG_INT32(1);
	fragment.n_row_groups = PG_GETARG_INT32(2);

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	tupdesc = rsinfo->setDesc;
	tupstore = rsinfo->setResult;

	values = palloc(tupdesc->natts * sizeof(Datum));
	nulls = palloc(tupdesc->natts * sizeof(bool));

	deconstruct_array(field_id_array, INT4OID, sizeof(int32), true, TYPALIGN_INT,
					  &field_id_datums, &field_id_nulls, &nfield_ids);
	if (nfield_ids > 0)
	{
		int32	   *field_ids = palloc(nfield_ids * sizeof(int32));
		int			i;

		if (nfield_ids != tupdesc->natts)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("the field id list names %d columns, the column definition list has %d",
							nfield_ids, tupdesc->natts)));

		for (i = 0; i < nfield_ids; i++)
		{
			if (field_id_nulls[i])
				ereport(ERROR,
						(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
						 errmsg("a field id cannot be null")));
			field_ids[i] = DatumGetInt32(field_id_datums[i]);
		}

		projection.field_ids = field_ids;
		projection.nfields = nfield_ids;
		projection_arg = &projection;
	}

	/*
	 * Every text and bytea decoded out of a batch is a copy, and tuplestore
	 * copies it again.  A materialize-mode function is called once, so the
	 * caller's per-tuple context is not reset until it returns -- without a
	 * context of its own, reading a large file would hold a second copy of all
	 * of it until then.
	 */
	row_context = AllocSetContextCreate(CurrentMemoryContext,
										"datalake_parquet_read",
										ALLOCSET_DEFAULT_SIZES);

	rc = routine->open_reader(&fragment, projection_arg, NULL, &reader);
	if (rc != DL_OK)
		dl_error_report(ERROR, rc, "open_reader");
	open_reader = reader;

	PG_TRY();
	{
		for (;;)
		{
			MemoryContext oldcontext;
			bool		eof;
			int64		row;
			int			attno;

			CHECK_FOR_INTERRUPTS();

			rc = reader->ops->next_batch(reader, batch, schema, &eof);
			if (rc != DL_OK)
				dl_error_report(ERROR, rc, "next_batch");
			if (eof)
				break;

			if (schema->n_children != tupdesc->natts)
				ereport(ERROR,
						(errcode(ERRCODE_DATATYPE_MISMATCH),
						 errmsg("the file does not have the number of columns the query expects"),
						 errdetail("The file has %lld, the query expects %d.",
								   (long long) schema->n_children,
								   tupdesc->natts)));

			for (attno = 0; attno < tupdesc->natts; attno++)
			{
				rc = dl_arrow_decode_check(schema->children[attno],
										   TupleDescAttr(tupdesc, attno)->atttypid,
										   TupleDescAttr(tupdesc, attno)->atttypmod);
				if (rc != DL_OK)
					dl_error_report(ERROR, rc, "check_column");
			}

			oldcontext = MemoryContextSwitchTo(row_context);

			for (row = 0; row < batch->length; row++)
			{
				CHECK_FOR_INTERRUPTS();

				for (attno = 0; attno < tupdesc->natts; attno++)
				{
					rc = dl_arrow_decode_value(schema->children[attno],
											   batch->children[attno], row,
											   TupleDescAttr(tupdesc, attno)->atttypid,
											   &values[attno], &nulls[attno]);
					if (rc != DL_OK)
						dl_error_report(ERROR, rc, "decode_value");
				}

				tuplestore_putvalues(tupstore, tupdesc, values, nulls);
			}

			MemoryContextSwitchTo(oldcontext);
			MemoryContextReset(row_context);

			/* Releasing the batch releases the columns under it. */
			batch->release(batch);
			schema->release(schema);
		}

		reader->ops->close(&reader);
		open_reader = NULL;
		MemoryContextDelete(row_context);
	}
	PG_CATCH();
	{
		FormatReader *failed = open_reader;
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;

		if (batch->release != NULL)
			batch->release(batch);
		if (schema->release != NULL)
			schema->release(schema);
		if (failed != NULL)
			failed->ops->close(&failed);
		datalake_fs_close(&fs);

		PG_RE_THROW();
	}
	PG_END_TRY();

	{
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;

		datalake_fs_close(&fs);
	}

	return (Datum) 0;
}

/*
 * The handles an error has to release live in volatile locals, which is what
 * lets PG_CATCH still read them after a longjmp; same shape as the Parquet
 * writer above.  Each is cleared as soon as something else owns it.
 */
Datum
datalake_storage_write_text(PG_FUNCTION_ARGS)
{
	text	   *content;
	char	   *relative;
	DatalakeFileSystem volatile open_fs = NULL;
	DatalakeFile volatile open_file = NULL;
	int64		length;

	check_nargs(fcinfo, 3);
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();
	content = PG_GETARG_TEXT_PP(1);
	length = VARSIZE_ANY_EXHDR(content);
	open_fs = storage_open_uri(fcinfo, 0, 2, true, &relative);

	PG_TRY();
	{
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;
		DatalakeFile file = NULL;
		DlErrCode	rc;

		rc = datalake_file_open(fs, relative, DATALAKE_FILE_WRITE, &file);
		open_file = file;
		if (rc == DL_OK)
			rc = datalake_file_write(file, VARDATA_ANY(content), length);
		if (rc == DL_OK)
		{
			rc = datalake_file_close(&file);
			open_file = file;	/* close consumes the handle */
		}
		if (rc != DL_OK)
			dl_error_report(ERROR, rc, "write storage file");
	}
	PG_CATCH();
	{
		DatalakeFile file = (DatalakeFile) open_file;
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;

		datalake_file_abort(&file);
		datalake_fs_close(&fs);
		PG_RE_THROW();
	}
	PG_END_TRY();

	{
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;

		datalake_fs_close(&fs);
	}
	PG_RETURN_INT64(length);
}

Datum
datalake_storage_read_text(PG_FUNCTION_ARGS)
{
	char	   *relative;
	DatalakeFileSystem volatile open_fs = NULL;
	DatalakeFile volatile open_file = NULL;
	StringInfoData data;

	check_nargs(fcinfo, 2);
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	open_fs = storage_open_uri(fcinfo, 0, 1, true, &relative);
	initStringInfo(&data);
	PG_TRY();
	{
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;
		DatalakeFile file = NULL;
		char		buffer[8192];
		DlErrCode	rc;

		rc = datalake_file_open(fs, relative, DATALAKE_FILE_READ, &file);
		open_file = file;
		while (rc == DL_OK)
		{
			int64		nread;

			rc = datalake_file_read(file, buffer, sizeof(buffer), &nread);
			if (rc != DL_OK || nread == 0)
				break;
			appendBinaryStringInfo(&data, buffer, nread);
		}
		if (rc == DL_OK)
		{
			rc = datalake_file_close(&file);
			open_file = file;	/* close consumes the handle */
		}
		if (rc != DL_OK)
			dl_error_report(ERROR, rc, "read storage file");
	}
	PG_CATCH();
	{
		DatalakeFile file = (DatalakeFile) open_file;
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;

		datalake_file_abort(&file);
		datalake_fs_close(&fs);
		PG_RE_THROW();
	}
	PG_END_TRY();

	{
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;

		datalake_fs_close(&fs);
	}
	PG_RETURN_TEXT_P(cstring_to_text_with_len(data.data, data.len));
}

Datum
datalake_storage_list(PG_FUNCTION_ARGS)
{
	char	   *relative;
	DatalakeFileSystem volatile open_fs = NULL;
	char	 **volatile open_names = NULL;
	int volatile open_nnames = 0;
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	check_nargs(fcinfo, 2);
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	open_fs = storage_open_uri(fcinfo, 0, 1, false, &relative);
	PG_TRY();
	{
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;
		char	  **names = NULL;
		int			nnames = 0;
		int			i;
		DlErrCode	rc;

		rc = datalake_fs_list(fs, relative, &names, &nnames);
		open_names = names;
		open_nnames = nnames;
		if (rc != DL_OK)
			dl_error_report(ERROR, rc, "list storage");
		InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
		for (i = 0; i < nnames; i++)
		{
			Datum		value = CStringGetTextDatum(names[i]);
			bool		isnull = false;

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
							 &value, &isnull);
			free(names[i]);
			names[i] = NULL;	/* so the cleanup path cannot free it twice */
		}
		free(names);
		open_names = NULL;
		open_nnames = 0;
	}
	PG_CATCH();
	{
		char	  **names = (char **) open_names;
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;
		int			i;

		for (i = 0; names != NULL && i < open_nnames; i++)
			free(names[i]);
		free(names);
		datalake_fs_close(&fs);
		PG_RE_THROW();
	}
	PG_END_TRY();

	{
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;

		datalake_fs_close(&fs);
	}
	return (Datum) 0;
}

Datum
datalake_storage_delete(PG_FUNCTION_ARGS)
{
	char	   *relative;
	DatalakeFileSystem volatile open_fs = NULL;

	check_nargs(fcinfo, 2);
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	open_fs = storage_open_uri(fcinfo, 0, 1, true, &relative);

	PG_TRY();
	{
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;
		DlErrCode	rc = datalake_file_delete(fs, relative);

		if (rc != DL_OK)
			dl_error_report(ERROR, rc, "delete storage file");
	}
	PG_CATCH();
	{
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;

		datalake_fs_close(&fs);
		PG_RE_THROW();
	}
	PG_END_TRY();

	{
		DatalakeFileSystem fs = (DatalakeFileSystem) open_fs;

		datalake_fs_close(&fs);
	}
	PG_RETURN_BOOL(true);
}

Datum
datalake_storage_probe(PG_FUNCTION_ARGS)
{
	DatalakeLocation location = {0};
	DatalakeFileSystem fs = NULL;
	char	   *scheme;
	DlErrCode	rc;

	check_nargs(fcinfo, 1);
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	scheme = text_to_cstring(PG_GETARG_TEXT_PP(0));
	location.abi_version = DATALAKE_LOCATION_ABI_VERSION;
	location.scheme = scheme;
	location.authority = pstrdup(strcmp(scheme, "s3") == 0 ? "probe-bucket" : "");
	location.path_prefix = pstrdup("/tmp");
	rc = datalake_fs_open(&location, NULL, 0, &fs);
	if (rc == DL_OK)
	{
		datalake_fs_close(&fs);
		PG_RETURN_TEXT_P(cstring_to_text("supported"));
	}
	PG_RETURN_TEXT_P(cstring_to_text(dl_error_get()->message));
}

Datum
datalake_storage_register_bad(PG_FUNCTION_ARGS)
{
	char	   *kind;
	DlErrCode	rc;

	check_nargs(fcinfo, 1);
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();
	kind = text_to_cstring(PG_GETARG_TEXT_PP(0));
	rc = datalake_test_register_bad_storage_backend(kind);
	if (rc == DL_OK)
		PG_RETURN_TEXT_P(cstring_to_text("accepted"));
	PG_RETURN_TEXT_P(cstring_to_text(dl_error_get()->message));
}

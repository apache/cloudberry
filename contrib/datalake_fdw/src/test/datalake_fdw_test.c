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

#include "executor/spi.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"

#include "am_iceberg/pg_iceberg_guc.h"
#include "common/dl_err.h"
#include "format/arrow_builder.h"
#include "format/arrow_decode.h"
#include "format/format.h"

PG_FUNCTION_INFO_V1(datalake_parquet_write);
PG_FUNCTION_INFO_V1(datalake_parquet_read);

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
 * datalake_parquet_write(path, query, row_group_size) -> rows written
 *
 * The rows the query returns are written to `path` as Parquet.  A row group
 * size of zero leaves the format's own default in place; anything else also
 * becomes the number of rows per batch, because a row group is closed at a
 * batch boundary and the option would otherwise be rounded away by a batch size
 * that does not divide by it.
 */
Datum
datalake_parquet_write(PG_FUNCTION_ARGS)
{
	char	   *path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *query = text_to_cstring(PG_GETARG_TEXT_PP(1));
	int32		row_group_size = PG_GETARG_INT32(2);
	const FormatRoutine *routine = parquet_routine();
	WriterOptions options = {0};
	FormatWriter *volatile open_writer = NULL;
	DlArrowBuilder volatile open_builder = NULL;
	long		batch_rows = iceberg_batch_rows;
	int64		written = 0;
	MemoryContext row_context;

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

				rc = routine->open_writer(path, tupdesc, &options, &writer);
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

		if (builder != NULL)
			dl_arrow_builder_close(&builder);
		if (writer != NULL)
			writer->ops->abort(&writer);

		PG_RE_THROW();
	}
	PG_END_TRY();

	SPI_finish();

	PG_RETURN_INT64(written);
}

/*
 * datalake_parquet_read(path, first_row_group, n_row_groups) -> setof record
 *
 * The column definition list says what the caller expects the file to hold, and
 * is checked against the file's own schema rather than assumed: reading an
 * Arrow column as the wrong PostgreSQL type would produce values, just not the
 * ones in the file.
 *
 * The row group arguments are the unit a scan is divided at.  Reading 0..0 and
 * then 1..1 has to produce exactly what reading the whole file does, which is
 * the property a scan spread across segments will depend on.
 */
Datum
datalake_parquet_read(PG_FUNCTION_ARGS)
{
	char	   *path = text_to_cstring(PG_GETARG_TEXT_PP(0));
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	const FormatRoutine *routine = parquet_routine();
	FormatReader *volatile open_reader = NULL;
	struct ArrowArray *batch = palloc0(sizeof(struct ArrowArray));
	struct ArrowSchema *schema = palloc0(sizeof(struct ArrowSchema));
	Fragment	fragment = {0};
	TupleDesc	tupdesc;
	Tuplestorestate *tupstore;
	Datum	   *values;
	bool	   *nulls;
	FormatReader *reader = NULL;
	MemoryContext row_context;
	DlErrCode	rc;

	fragment.path = path;
	fragment.first_row_group = PG_GETARG_INT32(1);
	fragment.n_row_groups = PG_GETARG_INT32(2);

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	tupdesc = rsinfo->setDesc;
	tupstore = rsinfo->setResult;

	values = palloc(tupdesc->natts * sizeof(Datum));
	nulls = palloc(tupdesc->natts * sizeof(bool));

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

	rc = routine->open_reader(&fragment, NULL, NULL, &reader);
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
										   TupleDescAttr(tupdesc, attno)->atttypid);
				if (rc != DL_OK)
					dl_error_report(ERROR, rc, "check_column");
			}

			oldcontext = MemoryContextSwitchTo(row_context);

			for (row = 0; row < batch->length; row++)
			{
				CHECK_FOR_INTERRUPTS();

				for (attno = 0; attno < tupdesc->natts; attno++)
				{
					rc = dl_arrow_decode_value(batch->children[attno], row,
											   TupleDescAttr(tupdesc, attno)->atttypid,
											   TupleDescAttr(tupdesc, attno)->atttypmod,
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

		if (batch->release != NULL)
			batch->release(batch);
		if (schema->release != NULL)
			schema->release(schema);
		if (failed != NULL)
			failed->ops->close(&failed);

		PG_RE_THROW();
	}
	PG_END_TRY();

	return (Datum) 0;
}

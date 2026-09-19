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
 * arrow_builder.h
 *	  Accumulation of PostgreSQL tuples into an Arrow batch.
 *
 * This is the write half of the boundary the format layer is built on: rows
 * arrive one at a time from an executor, and a data file wants them a column at
 * a time.  Nothing above this knows Arrow, and nothing here knows which format
 * the batch ends up in.
 *
 * postgres.h must be included before this header; the Datum in the append
 * signature is the whole reason a tuple can be handed over without copying it
 * first.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/arrow_builder.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_ARROW_BUILDER_H
#define DL_ARROW_BUILDER_H

#include "common/dl_err.h"
#include "format/format.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct DlArrowBuilderData *DlArrowBuilder;

/*
 * How many rows to accumulate before flushing is the caller's decision, not the
 * builder's -- the caller is the one that also has to decide when to roll to a
 * new file.  iceberg.batch_rows is the setting they take it from.
 */

/*
 * `tupdesc` is a TupleDesc.  It is taken as void * so that this header stays
 * usable from the C++ side without dragging PostgreSQL's headers into it in a
 * particular order; the type is checked by the only two callers there are.
 *
 * The descriptor has to outlive the builder, which is no constraint in
 * practice: the tuples being appended come from it.
 */
extern DlErrCode dl_arrow_builder_open(void *tupdesc, DlArrowBuilder *out);

/*
 * Appends one row.  Varlena values must already be detoasted -- this runs on
 * the C++ side, where a PostgreSQL error would unwind through frames that
 * cannot handle one, so it does not call anything that allocates.
 */
extern DlErrCode dl_arrow_builder_append(DlArrowBuilder builder,
										 const Datum *values,
										 const bool *nulls,
										 int nvalues);

/* Rows accumulated since the last flush. */
extern int64_t dl_arrow_builder_nrows(DlArrowBuilder builder);

/*
 * Hands over what has accumulated and starts a new batch.  The caller owns the
 * exported array and releases it -- or gives it to a writer, which consumes it.
 * Flushing nothing is not an error and produces an empty batch.
 */
extern DlErrCode dl_arrow_builder_flush(DlArrowBuilder builder,
										struct ArrowArray *out);

/* Cleanup entry point: releases the builder and clears the caller's handle. */
extern void dl_arrow_builder_close(DlArrowBuilder *builder);

#ifdef __cplusplus
}
#endif

#endif							/* DL_ARROW_BUILDER_H */

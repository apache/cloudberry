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
 * arrow_decode.h
 *	  PostgreSQL values out of an Arrow batch.
 *
 * It reads the buffers of the Arrow C data interface directly rather than
 * handing them back to Arrow, which keeps the read path free of C++ and makes
 * it a real check on what our own writer exports.
 *
 * postgres.h must be included before this header.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/arrow_decode.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_ARROW_DECODE_H
#define DL_ARROW_DECODE_H

#include "common/dl_err.h"
#include "format/format.h"

/*
 * Whether a column of this Arrow type can be read as this PostgreSQL type.
 * Called once per column per batch: the answer depends only on the schema, and
 * checking it per value would be the same answer several million times.
 *
 * What is accepted is what the type can hold without loss: its own Arrow type,
 * the narrower ones Iceberg lets a column be promoted from -- an int column
 * read as bigint, a float as double precision -- and Arrow's null type, which
 * is what a column the file does not have comes back as.  The modifier is
 * judged by the same rule CREATE TABLE applies, so a varchar(n) is refused
 * here for the same reason it could not have been created.
 *
 * `field` is one child of the batch's schema.
 */
extern DlErrCode dl_arrow_decode_check(const struct ArrowSchema *field,
									   Oid atttypid, int32 atttypmod);

/*
 * One value.  Only valid for a column dl_arrow_decode_check() accepted, which
 * is what lets this trust the buffer layout instead of re-deriving it -- and
 * `field` is how it knows which of the accepted layouts this column has.
 *
 * Values that point at memory -- text, bytea, uuid -- are copied into the
 * current memory context, because the batch is released long before the
 * tuples built from it are done with.  A string is verified to be what the
 * file claims, UTF-8, before it becomes a text: the file is not ours.
 */
extern DlErrCode dl_arrow_decode_value(const struct ArrowSchema *field,
									   const struct ArrowArray *column,
									   int64_t row, Oid atttypid,
									   Datum *value, bool *isnull);

#endif							/* DL_ARROW_DECODE_H */

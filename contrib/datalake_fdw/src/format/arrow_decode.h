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
 * The read half of the boundary the format layer is built on, and the mirror of
 * arrow_builder.h.  This side is C: turning a column into Datums means
 * allocating text and bytea, an allocation can fail, and a failure in
 * PostgreSQL unwinds with longjmp -- which is safe here and would not be if it
 * had to pass through C++ frames on the way out.
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
 * `field` is one child of the batch's schema.
 */
extern DlErrCode dl_arrow_decode_check(const struct ArrowSchema *field,
									   Oid atttypid);

/*
 * One value.  Only valid for a column dl_arrow_decode_check() accepted, which
 * is what lets this trust the buffer layout instead of re-deriving it.
 *
 * Values that point at memory -- text, bytea -- are copied into the current
 * memory context, because the batch is released long before the tuples built
 * from it are done with.
 *
 * `atttypmod` is the modifier the column was declared with, or -1.  A file this
 * module did not write has no idea what it was, so a char(n) in it need not be
 * padded to n and a varchar(n) need not be within n; without applying it, a
 * value that breaks the type's own rules would reach the executor.
 */
extern DlErrCode dl_arrow_decode_value(const struct ArrowArray *column,
									   int64_t row, Oid atttypid,
									   int32 atttypmod,
									   Datum *value, bool *isnull);

#endif							/* DL_ARROW_DECODE_H */

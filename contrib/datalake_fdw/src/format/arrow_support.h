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
 * arrow_support.h
 *	  The type mapping and the error translation shared by the Arrow-facing
 *	  parts of this module.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/arrow_support.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_ARROW_SUPPORT_H
#define DL_ARROW_SUPPORT_H

#include <cstdint>
#include <memory>

#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>

#include "common/dl_err.h"
#include "common/dl_pg_api.h"

extern "C"
{
#include "access/tupdesc.h"
}

#include "format/format_types.h"

/*
 * The metadata key under which Parquet's Arrow bridge carries a column's field
 * id in both directions: a field written with it gets the id in the Parquet
 * schema, and a field read from a Parquet schema that has one carries it here.
 */
#define DL_ARROW_FIELD_ID_KEY "PARQUET:field_id"

/*
 * Turns an Arrow status into this module's error code, recording what Arrow
 * said -- its own class and message are the only thing that makes a failure in
 * a third-party library diagnosable, and the code alone throws them away.
 * `operation` names what was being attempted.  A successful status records
 * nothing and returns DL_OK, so call sites can wrap every Arrow call.
 */
extern DlErrCode DlArrowStatus(const arrow::Status &status, const char *operation);

/*
 * The Arrow type a column of this PostgreSQL type is stored as, or a null
 * pointer when the type has no mapping.  The type alone: whether a modifier
 * makes the column unstorable is dl_format_type_refusal()'s question, and
 * callers report a refusal themselves, because only they know which column it
 * was about.
 */
extern std::shared_ptr<arrow::DataType> DlArrowTypeForPgType(Oid atttypid);

/*
 * The whole descriptor, minus its dropped attributes, each field carrying the
 * Iceberg field id `field_ids` gives for that attribute -- or, when that is
 * null, its 1-based position among the live columns.  Returns a null pointer
 * and records which column was the problem in the error detail: a type or a
 * modifier no data file can hold, or a database encoding other than UTF8,
 * which no data file can hold either.
 */
extern std::shared_ptr<arrow::Schema> DlArrowSchemaFromTupleDesc(TupleDesc tupdesc,
																 const int32_t *field_ids);

/* The field id a field read from a Parquet file carries, or -1 when none. */
extern int32_t DlArrowFieldId(const arrow::Field &field);

#endif							/* DL_ARROW_SUPPORT_H */

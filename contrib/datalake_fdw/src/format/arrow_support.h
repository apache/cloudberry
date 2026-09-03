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
 *	  What every Arrow-facing translation unit in this module needs: how a
 *	  PostgreSQL column type is stored, and how an Arrow failure is reported.
 *
 * The type mapping is in one place because the writer, the builder that feeds
 * it and the reader that decodes what comes back all have to agree on it, and a
 * disagreement between them would show up as wrong values rather than as an
 * error.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/arrow_support.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_ARROW_SUPPORT_H
#define DL_ARROW_SUPPORT_H

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
 * pointer when the type has no mapping yet.  Callers report the refusal
 * themselves, because only they know which column it was about.
 */
extern std::shared_ptr<arrow::DataType> DlArrowTypeForPgType(Oid atttypid);

/*
 * The whole descriptor.  Returns a null pointer and records which column was
 * the problem in the error detail: a type with no mapping, or a dropped column,
 * which has no type to write and which nothing reads a file for yet.
 */
extern std::shared_ptr<arrow::Schema> DlArrowSchemaFromTupleDesc(TupleDesc tupdesc);

#endif							/* DL_ARROW_SUPPORT_H */

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
 * format_registry.c
 *	  Lookup of the reader and writer for a data file format.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/format_registry.c
 *
 *-------------------------------------------------------------------------
 */

#include <stdio.h>
#include <stddef.h>
#include <string.h>

#include "common/dl_err.h"
#include "format/format.h"
#include "format/parquet/parquet_format.h"

/*
 * Parquet is the only format so far.  A name that reaches here came from a
 * table option, so an unknown one is an ordinary mistake and the caller has to
 * be able to say which name it was -- returning a bare NULL would leave every
 * caller to write that message again, and get it wrong differently.  The name
 * goes into the error detail, so a caller that reports DL_ERR_NOT_SUPPORTED
 * gets it without knowing this function exists.
 */
const FormatRoutine *
GetFormatRoutine(const char *format)
{
	char		message[128];

	if (format != NULL && strcmp(format, "parquet") == 0)
		return GetParquetFormatRoutine();

	snprintf(message, sizeof(message),
			 "\"%s\" is not a data file format this build can read or write",
			 format == NULL ? "" : format);
	dl_error_set(DL_ERR_NOT_SUPPORTED, "get_format", NULL, message);

	return NULL;
}

DlErrCode
WrapPositionDeleteFilter(FormatReader *inner, const DeleteFileSet *delete_files,
						 FormatReader **out)
{
	if (out != NULL)
		*out = NULL;
	return DL_ERR_NOT_SUPPORTED;
}

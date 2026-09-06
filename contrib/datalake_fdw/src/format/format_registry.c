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

#include <stddef.h>

#include "format/format.h"

/* No formats in the skeleton; parquet lands in PR-3/4.  Callers must treat
 * NULL as not-supported. */
const FormatRoutine *
GetFormatRoutine(const char *format)
{
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

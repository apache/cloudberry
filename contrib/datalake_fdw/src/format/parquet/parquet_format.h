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
 * parquet_format.h
 *	  The Parquet reader and writer.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/parquet/parquet_format.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_PARQUET_FORMAT_H
#define DL_PARQUET_FORMAT_H

#include "format/format.h"

#ifdef __cplusplus
extern "C"
{
#endif

extern const FormatRoutine *GetParquetFormatRoutine(void);

#ifdef __cplusplus
}
#endif

#endif							/* DL_PARQUET_FORMAT_H */

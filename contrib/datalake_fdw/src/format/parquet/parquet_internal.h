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
 * parquet_internal.h
 *	  What the halves of the Parquet format say to each other.
 *
 * Reading and writing a Parquet file have nothing in common but the name of
 * the format, so they are separate translation units; this is the only thing
 * they share, and parquet_format.cpp is the only other file that needs it.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/parquet/parquet_internal.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_PARQUET_INTERNAL_H
#define DL_PARQUET_INTERNAL_H

#include "format/format.h"

extern DlErrCode parquet_open_reader(const Fragment *fragment,
									 const ProjectionSet *projection,
									 const RowGroupFilterSet *filters,
									 FormatReader **out);

extern DlErrCode parquet_open_writer(const char *path, void *tupdesc,
									 const WriterOptions *options,
									 FormatWriter **out);

#endif							/* DL_PARQUET_INTERNAL_H */

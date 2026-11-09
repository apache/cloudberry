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
 * parquet_format.cpp
 *	  Parquet as a format this module can read and write.
 *
 * Parquet is reached through Arrow rather than through libparquet on its own,
 * because libparquet is written in terms of Arrow's types: linking it already
 * links Arrow, and going around Arrow would mean re-deriving the definition and
 * repetition levels, the four ways a decimal can be stored, and the timestamp
 * unit rules that arrow::parquet already gets right.
 *
 * Nothing here reads or writes anything but a local file yet.  The storage
 * facade in common/file_system_wrapper.h is where object storage arrives, as an
 * arrow::io::RandomAccessFile over it; parquet_read.cpp and parquet_write.cpp
 * are the only files that have to change when it does.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/format/parquet/parquet_format.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "format/parquet/parquet_format.h"
#include "format/parquet/parquet_internal.h"

static const FormatRoutine parquet_format_routine = {
	DL_FORMAT_ABI_VERSION,
	sizeof(FormatRoutine),
	"parquet",
	parquet_open_reader,
	parquet_open_writer
};

extern "C" const FormatRoutine *
GetParquetFormatRoutine(void)
{
	return &parquet_format_routine;
}

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
 * arrow_wrapper.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/arrow_wrapper.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#ifdef VEC_BUILD

// FIXME(jiaqizho): There marco defined in datatime.h
// which include in `cbdb_api.h`. In pax, we always need
// include `cbdb_api.h`.

#undef RESERV
#undef MONTH
#undef YEAR
#undef DAY
#undef JULIAN
#undef TZ
#undef DTZ
#undef DYNTZ
#undef IGNORE_DTF
#undef AMPM
#undef HOUR
#undef MINUTE
#undef SECOND
#undef MILLISECOND
#undef MICROSECOND
#undef IsPowerOf2
#undef Abs

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wunused-but-set-variable"

#include <arrow/array.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_decimal.h>
#include <arrow/array/array_dict.h>
#include <arrow/array/array_nested.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/data.h>
#include <arrow/c/abi.h>
#include <arrow/c/bridge.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/type.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/iterator.h>

#pragma GCC diagnostic pop

#define RESERV 0
#define MONTH 1
#define YEAR 2
#define DAY 3
#define JULIAN 4
#define TZ 5
#define DTZ 6
#define DYNTZ 7
#define IGNORE_DTF 8
#define AMPM 9
#define HOUR 10
#define MINUTE 11
#define SECOND 12
#define MILLISECOND 13
#define MICROSECOND 14

// NOLINTNEXTLINE
#define IsPowerOf2(x) (x > 0 && ((x) & ((x)-1)) == 0)
#define Abs(x) ((x) >= 0 ? (x) : -(x))
namespace arrow {

void ExportArrayRelease(ArrowArray *array);
void ExportArrayRoot(const std::shared_ptr<ArrayData> &data,
                     ArrowArray *export_array);
int FindFieldIndex(
    const std::vector<std::pair<const char *, size_t>> &table_names,
    const std::pair<const char *, size_t> &kname);
std::pair<const char *, size_t> ExtractFieldName(const std::string &name);
}  // namespace arrow

#endif  // VEC_BUILD

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
 * pax_vec_comm.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/vec/pax_vec_comm.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#ifdef VEC_BUILD

#include "comm/bitmap.h"
#include "storage/columns/pax_column.h"
#include "storage/pax_buffer.h"
#include "storage/pax_defined.h"

namespace pax {

void CopyBitmap(const Bitmap8 *bitmap, size_t range_begin, size_t range_lens,
                DataBuffer<char> *null_bits_buffer);

void VarlenaToRawBuffer(char *buffer, size_t buffer_len, char **out_data,
                        size_t *out_len);

void CopyBitmapBuffer(PaxColumn *column,
                      std::shared_ptr<Bitmap8> visibility_map_bitset,
                      size_t group_base_offset, size_t range_begin,
                      size_t range_lens, size_t data_range_lens,
                      size_t out_range_lens, DataBuffer<char> *null_bits_buffer,
                      size_t *out_visable_null_counts);

}  // namespace pax
#endif  // VEC_BUILD

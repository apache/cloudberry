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
 * pax_column_traits.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/columns/pax_column_traits.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/pax_memory.h"
#include "storage/columns/pax_bitpacked_column.h"
#include "storage/columns/pax_bpchar_column.h"
#include "storage/columns/pax_column.h"
#include "storage/columns/pax_encoding_column.h"
#include "storage/columns/pax_encoding_non_fixed_column.h"
#include "storage/columns/pax_numeric_column.h"
#include "storage/columns/pax_vec_bitpacked_column.h"
#include "storage/columns/pax_vec_bpchar_column.h"
#include "storage/columns/pax_vec_column.h"
#include "storage/columns/pax_vec_encoding_column.h"
#include "storage/columns/pax_vec_no_hdr_column.h"
#include "storage/columns/pax_vec_numeric_column.h"

namespace pax::traits {

namespace Impl {

template <typename T>
using CreateFunc = std::function<std::unique_ptr<T>(uint32)>;

template <typename T>
using CreateFunc2 = std::function<std::unique_ptr<T>(uint32, uint32)>;

template <typename T>
static std::unique_ptr<T> CreateImpl(uint32 cap) {
  auto t = std::make_unique<T>(cap);
  return t;
}

template <typename T>
static std::unique_ptr<T> CreateImpl2(uint32 data_cap, uint32 offsets_cap) {
  auto t = std::make_unique<T>(data_cap, offsets_cap);
  return t;
}

template <typename T>
using CreateEncodingFunc = std::function<std::unique_ptr<T>(
    uint32, const PaxEncoder::EncodingOption &)>;

template <typename T>
using CreateDecodingFunc = std::function<std::unique_ptr<T>(
    uint32, const PaxDecoder::DecodingOption &)>;

template <typename T>
using CreateEncodingFunc2 = std::function<std::unique_ptr<T>(
    uint32, uint32, const PaxEncoder::EncodingOption &)>;

template <typename T>
using CreateDecodingFunc2 = std::function<std::unique_ptr<T>(
    uint32, uint32, const PaxDecoder::DecodingOption &)>;

template <typename T>
static std::unique_ptr<T> CreateEncodingImpl(
    uint32 cap, const PaxEncoder::EncodingOption &encoding_opt) {
  return std::make_unique<T>(cap, encoding_opt);
}

template <typename T>
static std::unique_ptr<T> CreateDecodingImpl(
    uint32 cap, const PaxDecoder::DecodingOption &decoding_opt) {
  return std::make_unique<T>(cap, decoding_opt);
}

template <typename T>
static std::unique_ptr<T> CreateEncodingImpl2(
    uint32 cap, uint32 length_cap,
    const PaxEncoder::EncodingOption &encoding_opt) {
  return std::make_unique<T>(cap, length_cap, encoding_opt);
}

template <typename T>
static std::unique_ptr<T> CreateDecodingImpl2(
    uint32 cap, uint32 length_cap,
    const PaxDecoder::DecodingOption &decoding_opt) {
  return std::make_unique<T>(cap, length_cap, decoding_opt);
}

}  // namespace Impl

template <template <typename> class T, typename N>
struct ColumnCreateTraits {};

template <class T>
struct ColumnCreateTraits2 {};

#define TRAITS_DECL(_class, _type)                 \
  template <>                                      \
  struct ColumnCreateTraits<_class, _type> {       \
    static Impl::CreateFunc<_class<_type>> create; \
  }

#define TRAITS_DECL2(_class)                 \
  template <>                                \
  struct ColumnCreateTraits2<_class> {       \
    static Impl::CreateFunc2<_class> create; \
  }

TRAITS_DECL(PaxCommColumn, int8);
TRAITS_DECL(PaxCommColumn, int16);
TRAITS_DECL(PaxCommColumn, int32);
TRAITS_DECL(PaxCommColumn, int64);
TRAITS_DECL(PaxCommColumn, float);
TRAITS_DECL(PaxCommColumn, double);

TRAITS_DECL(PaxVecCommColumn, int8);
TRAITS_DECL(PaxVecCommColumn, int16);
TRAITS_DECL(PaxVecCommColumn, int32);
TRAITS_DECL(PaxVecCommColumn, int64);
TRAITS_DECL(PaxVecCommColumn, float);
TRAITS_DECL(PaxVecCommColumn, double);

TRAITS_DECL2(PaxNonFixedColumn);
TRAITS_DECL2(PaxVecNonFixedColumn);

template <template <typename> class T, typename N>
struct ColumnOptCreateTraits {};

template <class T>
struct ColumnOptCreateTraits2 {};

#define TRAITS_OPT_DECL(_class, _type)                              \
  template <>                                                       \
  struct ColumnOptCreateTraits<_class, _type> {                     \
    static Impl::CreateEncodingFunc<_class<_type>> create_encoding; \
    static Impl::CreateDecodingFunc<_class<_type>> create_decoding; \
  }

#define TRAITS_OPT_DECL_NO_TYPE(_class)                      \
  template <>                                                \
  struct ColumnOptCreateTraits2<_class> {                    \
    static Impl::CreateEncodingFunc<_class> create_encoding; \
    static Impl::CreateDecodingFunc<_class> create_decoding; \
  }

#define TRAITS_OPT_DECL2(_class)                              \
  template <>                                                 \
  struct ColumnOptCreateTraits2<_class> {                     \
    static Impl::CreateEncodingFunc2<_class> create_encoding; \
    static Impl::CreateDecodingFunc2<_class> create_decoding; \
  }

TRAITS_OPT_DECL(PaxEncodingColumn, int8);
TRAITS_OPT_DECL(PaxEncodingColumn, int16);
TRAITS_OPT_DECL(PaxEncodingColumn, int32);
TRAITS_OPT_DECL(PaxEncodingColumn, int64);

TRAITS_OPT_DECL(PaxVecEncodingColumn, int8);
TRAITS_OPT_DECL(PaxVecEncodingColumn, int16);
TRAITS_OPT_DECL(PaxVecEncodingColumn, int32);
TRAITS_OPT_DECL(PaxVecEncodingColumn, int64);

TRAITS_OPT_DECL2(PaxNonFixedEncodingColumn);
TRAITS_OPT_DECL2(PaxVecNonFixedEncodingColumn);
TRAITS_OPT_DECL_NO_TYPE(PaxShortNumericColumn);
TRAITS_OPT_DECL2(PaxPgNumericColumn);
TRAITS_OPT_DECL2(PaxBpCharColumn);
TRAITS_OPT_DECL2(PaxVecBpCharColumn);
TRAITS_OPT_DECL2(PaxVecNoHdrColumn);
TRAITS_OPT_DECL_NO_TYPE(PaxBitPackedColumn);
TRAITS_OPT_DECL_NO_TYPE(PaxVecBitPackedColumn);

}  // namespace pax::traits

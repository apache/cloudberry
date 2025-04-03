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
 * pax_toast.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/toast/pax_toast.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "comm/cbdb_api.h"

#include "comm/byte_buffer.h"
#include "comm/guc.h"
#include "comm/pax_memory.h"

namespace pax {

typedef struct pax_varatt_external {
  int32 va_rawsize;  /* Original data size (doesn't includes header) */
  uint32 va_extinfo; /* External saved size (without header) and
                      * compression method */
  uint64 va_extogsz;
  uint64 va_extoffs;
  uint64 va_extsize;
} pax_varatt_external;

static_assert(sizeof(pax_varatt_external) == 32, "illegal structure");

#define PAX_VARATT_EXTERNAL_CMID(PTR)                       \
  (ToastCompressionId)((PaxExtGetDatum(PTR))->va_extinfo >> \
                       VARLENA_EXTSIZE_BITS)

#define PAX_VARATT_EXTERNAL_ORIGIN_SIZE(PTR) ((PaxExtGetDatum(PTR))->va_extogsz)

#define PAX_VARATT_EXTERNAL_OFFSET(PTR) ((PaxExtGetDatum(PTR))->va_extoffs)

#define PAX_VARATT_EXTERNAL_SIZE(PTR) ((PaxExtGetDatum(PTR))->va_extsize)

typedef struct pax_varatt_external_ref {
  pax_varatt_external ext;

  char *data_ref;
  size_t data_size;
} pax_varatt_external_ref;

class ExternalToastValue : public MemoryObject {
 public:
  explicit ExternalToastValue(size_t size);
  explicit ExternalToastValue(ByteBuffer &&tmp);
  explicit ExternalToastValue(ExternalToastValue &&tmp);
  ExternalToastValue(const ExternalToastValue &) = delete;

  ~ExternalToastValue() = default;

  ExternalToastValue &operator=(const ExternalToastValue &) = delete;
  ExternalToastValue &operator=(ExternalToastValue &&tmp) {
    if (this == &tmp) return *this;
    buffer_ = std::move(tmp.buffer_);

    return *this;
  };

  Datum GetDatum() { return PointerGetDatum(Addr()); };
  void *Addr() { return buffer_.Addr(); }
  size_t Size() { return buffer_.Size(); }

 private:
  ByteBuffer buffer_;
};

#define PaxExtGetDatum(PTR) (pax_varatt_external *)(PTR)
#define PaxExtRefGetDatum(PTR) (pax_varatt_external_ref *)(PTR)

// pax only accepts compressed and external toast
#define VARATT_IS_PAX_EXTERNAL_TOAST(PTR) \
  (VARATT_IS_EXTERNAL(PTR) && VARTAG_EXTERNAL(PTR) == VARTAG_CUSTOM)

#define VARATT_IS_PAX_SUPPORT_TOAST(PTR) \
  (VARATT_IS_PAX_EXTERNAL_TOAST(PTR) || VARATT_IS_COMPRESSED(PTR))

#define PAX_VARSIZE_ANY(PTR)                                       \
  (VARATT_IS_PAX_EXTERNAL_TOAST(PTR) ? sizeof(pax_varatt_external) \
                                     : VARSIZE_ANY(PTR))

#define PAX_EXTERNAL_TOAST_SET_SIZE_AND_COMPRESS_METHOD(            \
    PTR, origin_len, compress_len, cm_method)                       \
  do {                                                              \
    Assert((origin_len) > 0 && (uint64)(origin_len) <= UINT64_MAX); \
    Assert((cm_method) == TOAST_PGLZ_COMPRESSION_ID ||              \
           (cm_method) == TOAST_LZ4_COMPRESSION_ID ||               \
           (cm_method) == TOAST_INVALID_COMPRESSION_ID);            \
    (PaxExtGetDatum(PTR))->va_extinfo =                             \
        (0) | ((uint32)(cm_method) << VARLENA_EXTSIZE_BITS);        \
    (PaxExtGetDatum(PTR))->va_extogsz = origin_len;                 \
    (PaxExtGetDatum(PTR))->va_extsize = compress_len;               \
  } while (0)

#define PAX_VARATT_EXTERNAL_SET_OFFSET(PTR, offs) \
  do {                                            \
    Assert(VARATT_IS_PAX_EXTERNAL_TOAST(PTR));    \
    (PaxExtGetDatum(PTR))->va_extoffs = offs;     \
  } while (0)

#define VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST(PTR) \
  (pax_enable_toast &&                            \
   VARSIZE_ANY_EXHDR(PTR) >= (uint32)pax_min_size_of_compress_toast)

#define VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST_BY_SIZE(SIZE) \
  (pax_enable_toast && SIZE >= (uint32)pax_min_size_of_compress_toast)

#define VARATT_CAN_MAKE_PAX_EXTERNAL_TOAST(PTR) \
  (pax_enable_toast &&                          \
   VARSIZE_ANY_EXHDR(PTR) >= (uint32)pax_min_size_of_external_toast)

#define VARATT_CAN_MAKE_PAX_EXTERNAL_TOAST_BY_SIZE(SIZE) \
  (pax_enable_toast && SIZE >= (uint32)pax_min_size_of_external_toast)

// make pax toast
std::pair<Datum, std::shared_ptr<MemoryObject>> pax_make_toast(
    Datum d, char storage_type);

// raw size of pax toast
size_t pax_toast_raw_size(Datum d);
size_t pax_toast_hdr_size(Datum d);

// detoast pax toast
size_t pax_detoast_raw(Datum d, char *dst_buff, size_t dst_size,
                       char *ext_buff = nullptr, size_t ext_buff_size = 0);
std::pair<Datum, std::shared_ptr<MemoryObject>> pax_detoast(
    Datum d, char *ext_buff = nullptr, size_t ext_buff_size = 0);

// free pax toast
void pax_free_toast(Datum d);

extern struct varlena *pg_detoast_exp_short(struct varlena *vl);

}  // namespace pax

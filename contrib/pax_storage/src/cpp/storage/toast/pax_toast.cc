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
 * pax_toast.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/toast/pax_toast.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/toast/pax_toast.h"

#include "comm/fmt.h"
#include "comm/pax_memory.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_compress.h"

#ifdef USE_LZ4
#include <lz4.h>
#endif

namespace pax {

class ExternalToastData : public MemoryObject {
 public:
  ExternalToastData(
      std::shared_ptr<struct pax_varatt_external_ref> external_ref,
      ByteBuffer &&buffer)
      : ref_(std::move(external_ref)), buffer_(std::move(buffer)) {}
  ExternalToastData(const ExternalToastData &) = delete;
  ExternalToastData &operator=(const ExternalToastData &) = delete;
  ExternalToastData &operator=(ExternalToastData &&tmp) {
    if (this != &tmp) {
      ref_ = std::move(tmp.ref_);
      buffer_ = std::move(tmp.buffer_);
    }
    return *this;
  }

  // Free both objects in destructor function
  ~ExternalToastData() = default;

 private:
  std::shared_ptr<struct pax_varatt_external_ref> ref_;
  ByteBuffer buffer_;
};

static ByteBuffer pax_pglz_compress_datum_without_hdr(
    const struct varlena *value) {
  int32 valsize, len;
  PgLZCompressor compressor;

  valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

  if (valsize < PGLZ_strategy_default->min_input_size ||
      valsize > PGLZ_strategy_default->max_input_size)
    return ByteBuffer();

  len = compressor.GetCompressBound(valsize);

  ByteBuffer buffer(len, len);
  len = compressor.Compress(buffer.Addr(), len, VARDATA_ANY(value), valsize,
                            0 /* level has no effect*/);
  if (compressor.IsError(len)) {
    return ByteBuffer();
  }
  buffer.SetSize(len);
  return buffer;
}

static ByteBuffer pax_pglz_compress_datum(const struct varlena *value) {
  int32 valsize, len;
  struct varlena *tmp = nullptr;
  PgLZCompressor compressor;

  valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

  // No point in wasting a alloc cycle if value size is outside the allowed
  // range for compression.
  if (valsize < PGLZ_strategy_default->min_input_size ||
      valsize > PGLZ_strategy_default->max_input_size)
    return ByteBuffer();

  len = compressor.GetCompressBound(valsize);

  // Figure out the maximum possible size of the pglz output, add the bytes
  // that will be needed for varlena overhead, and allocate that amount.

  ByteBuffer buffer(len + VARHDRSZ_COMPRESSED, len + VARHDRSZ_COMPRESSED);
  tmp = reinterpret_cast<struct varlena *>(buffer.Addr());
  len = compressor.Compress((char *)tmp + VARHDRSZ_COMPRESSED, len,
                            VARDATA_ANY(value), valsize,
                            0 /* level has no effect*/);
  if (compressor.IsError(len)) {
    return ByteBuffer();
  }
  buffer.SetSize(len + VARHDRSZ_COMPRESSED);
  SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);

  return buffer;
}

static ByteBuffer pax_lz4_compress_datum_without_hdr(
    const struct varlena *value) {
#ifndef USE_LZ4
  CBDB_RAISE(cbdb::CException::kExTypeToastNoLZ4Support);
#else
  int32 valsize;
  int32 len;
  int32 max_size;
  PaxLZ4Compressor compressor;

  valsize = VARSIZE_ANY_EXHDR(value);
  // Figure out the maximum possible size of the LZ4 output, add the bytes
  // that will be needed for varlena overhead, and allocate that amount.
  max_size = compressor.GetCompressBound(valsize);

  ByteBuffer buffer(max_size);
  len = compressor.Compress(buffer.Addr(), max_size, VARDATA_ANY(value),
                            valsize, 0 /* level has no effect*/);
  if (compressor.IsError(len) || len > valsize) {
    return ByteBuffer();
  }
  buffer.SetSize(len);
  return buffer;
#endif
}

static ByteBuffer pax_lz4_compress_datum(const struct varlena *value) {
#ifndef USE_LZ4
  CBDB_RAISE(cbdb::CException::kExTypeToastNoLZ4Support);
#else
  int32 valsize;
  int32 len;
  int32 max_size;
  void *tmp;
  PaxLZ4Compressor compressor;

  valsize = VARSIZE_ANY_EXHDR(value);
  // Figure out the maximum possible size of the LZ4 output, add the bytes
  // that will be needed for varlena overhead, and allocate that amount.
  max_size = compressor.GetCompressBound(valsize);

  ByteBuffer buffer(max_size + VARHDRSZ_COMPRESSED);
  tmp = buffer.Addr();
  len = compressor.Compress((char *)tmp + VARHDRSZ_COMPRESSED, max_size,
                            VARDATA_ANY(value), valsize,
                            0 /* level has no effect*/);

  // Should allow current datum compress failed
  // data is incompressible so just free the memory and return NULL
  if (compressor.IsError(len) || len > valsize) {
    return ByteBuffer();
  }

  buffer.SetSize(len + VARHDRSZ_COMPRESSED);
  SET_VARSIZE_COMPRESSED(tmp, len + VARHDRSZ_COMPRESSED);

  return buffer;
#endif
}

static std::pair<Datum, std::shared_ptr<MemoryObject>>
pax_make_compressed_toast(Datum value,
                          char cmethod = InvalidCompressionMethod) {
  ByteBuffer buffer;
  struct varlena *tmp = nullptr;
  uint32 valsize;
  ToastCompressionId cmid = TOAST_INVALID_COMPRESSION_ID;

  if (!VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST(value))
    return {PointerGetDatum(nullptr), nullptr};

  Assert(!VARATT_IS_EXTERNAL(DatumGetPointer(value)));
  Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));

  valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

  // If the compression method is not valid, use the current default
  if (!CompressionMethodIsValid(cmethod)) {
    cmethod = default_toast_compression;
  }

  // Call appropriate compression routine for the compression method.
  switch (cmethod) {
    case TOAST_PGLZ_COMPRESSION:
      buffer = pax_pglz_compress_datum((const struct varlena *)value);
      cmid = TOAST_PGLZ_COMPRESSION_ID;
      break;
    case TOAST_LZ4_COMPRESSION:
      buffer = pax_lz4_compress_datum((const struct varlena *)value);
      cmid = TOAST_LZ4_COMPRESSION_ID;
      break;
    default:
      CBDB_RAISE(cbdb::CException::kExTypeToastInvalidCompressType,
                 fmt("Invalid toast compress method [cmethod=%c]", cmethod));
  }

  if (buffer.Empty()) return {PointerGetDatum(nullptr), nullptr};

  // We recheck the actual size even if compression reports success, because
  // it might be satisfied with having saved as little as one byte in the
  // compressed data --- which could turn into a net loss once you consider
  // header and alignment padding.  Worst case, the compressed format might
  // require three padding bytes (plus header, which is included in
  // VARSIZE(tmp)), whereas the uncompressed format would take only one
  // header byte and no padding if the value is short enough.  So we insist
  // on a savings of more than 2 bytes to ensure we have a gain.
  tmp = reinterpret_cast<struct varlena *>(buffer.Addr());
  if (VARSIZE(tmp) < valsize - 2) {
    // successful compression
    Assert(cmid != TOAST_INVALID_COMPRESSION_ID);
    TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD(tmp, valsize, cmid);
    return {PointerGetDatum(buffer.Addr()),
            std::make_shared<ExternalToastValue>(std::move(buffer))};
  } else {
    // incompressible data
    return {PointerGetDatum(nullptr), nullptr};
  }
}

static std::pair<ByteBuffer, ToastCompressionId> pax_do_compressed_raw(
    Datum value, char cmethod) {
  ToastCompressionId cmid = TOAST_INVALID_COMPRESSION_ID;
  ByteBuffer buffer;
  int32 valsize;

  valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

  // If the compression method is not valid, use the current default
  if (!CompressionMethodIsValid(cmethod)) {
    cmethod = default_toast_compression;
  }

  // Call appropriate compression routine for the compression method.
  // should allow no compress here
  switch (cmethod) {
    case TOAST_PGLZ_COMPRESSION:
      buffer =
          pax_pglz_compress_datum_without_hdr((const struct varlena *)value);
      cmid = TOAST_PGLZ_COMPRESSION_ID;
      break;
    case TOAST_LZ4_COMPRESSION:
      buffer =
          pax_lz4_compress_datum_without_hdr((const struct varlena *)value);
      cmid = TOAST_LZ4_COMPRESSION_ID;
      break;
    default:
      break;
  }

  // compress failed
  if (buffer.Empty()) {
    goto no_compress;
  }

  if (buffer.Size() >= (uint32)(valsize)-2) {
    // incompressible data
    goto no_compress;
  }

  Assert(cmid != TOAST_INVALID_COMPRESSION_ID);
  return std::pair<ByteBuffer, ToastCompressionId>{std::move(buffer), cmid};

no_compress:
  return std::pair<ByteBuffer, ToastCompressionId>{
      ByteBuffer(), TOAST_INVALID_COMPRESSION_ID};
}

static std::pair<Datum, std::shared_ptr<MemoryObject>> pax_make_external_toast(
    Datum value, bool need_compress) {
  ToastCompressionId cmid = TOAST_INVALID_COMPRESSION_ID;
  pax_varatt_external_ref *varatt_ref;
  ByteBuffer buffer;
  int32 valsize;
  char cmethod = InvalidCompressionMethod;

  Assert(!VARATT_IS_EXTERNAL(DatumGetPointer(value)));
  Assert(!VARATT_IS_COMPRESSED(DatumGetPointer(value)));

  valsize = VARSIZE_ANY_EXHDR(DatumGetPointer(value));

  if (!VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST(value))
    return {PointerGetDatum(nullptr), nullptr};

  if (need_compress) {
    std::tie(buffer, cmid) = pax_do_compressed_raw(value, cmethod);
  }

  AssertImply(!buffer.Empty(), cmid != TOAST_INVALID_COMPRESSION_ID);
  AssertImply(buffer.Empty(), cmid == TOAST_INVALID_COMPRESSION_ID);

  auto obj_ref = std::make_shared<pax_varatt_external_ref>();
  varatt_ref = obj_ref.get();
  PAX_EXTERNAL_TOAST_SET_SIZE_AND_COMPRESS_METHOD(
      varatt_ref, valsize,
      (cmid != TOAST_INVALID_COMPRESSION_ID) ? buffer.Size()
                                             : VARSIZE_ANY_EXHDR(value),
      cmid);

  if (cmid != TOAST_INVALID_COMPRESSION_ID) {
    varatt_ref->data_ref = reinterpret_cast<char *>(buffer.Addr());
    varatt_ref->data_size = buffer.Size();
  } else {
    varatt_ref->data_ref = VARDATA_ANY(value);
    varatt_ref->data_size = VARSIZE_ANY_EXHDR(value);
  }

  SET_VARTAG_EXTERNAL(varatt_ref, VARTAG_CUSTOM);
  return {PointerGetDatum(varatt_ref),
          std::make_shared<ExternalToastData>(obj_ref, std::move(buffer))};
}

std::pair<Datum, std::shared_ptr<MemoryObject>> pax_make_toast(
    Datum d, char storage_type) {
  std::shared_ptr<MemoryObject> mobj;
  Datum result = d;

  if (!pax_enable_toast) {
    return {d, nullptr};
  }

  switch (storage_type) {
    case TYPSTORAGE_PLAIN: {
      // do nothing
      break;
    }
    case TYPSTORAGE_EXTENDED: {
      if (VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST(d) &&
          !VARATT_CAN_MAKE_PAX_EXTERNAL_TOAST(d)) {
        std::tie(result, mobj) = pax_make_compressed_toast(PointerGetDatum(d));
      } else if (VARATT_CAN_MAKE_PAX_EXTERNAL_TOAST(d)) {
        std::tie(result, mobj) =
            pax_make_external_toast(PointerGetDatum(d), true);
      }
      break;
    }
    case TYPSTORAGE_EXTERNAL: {
      if (VARATT_CAN_MAKE_PAX_EXTERNAL_TOAST(d)) {
        // should not make compress toast here
        std::tie(result, mobj) =
            pax_make_external_toast(PointerGetDatum(d), false);
      }
      break;
    }
    case TYPSTORAGE_MAIN: {
      if (VARATT_CAN_MAKE_PAX_COMPRESSED_TOAST(d)) {
        std::tie(result, mobj) = pax_make_compressed_toast(PointerGetDatum(d));
      }

      break;
    }
    default: {
      Assert(false);
    }
  }

  return {result, mobj};
}

size_t pax_toast_raw_size(Datum d) {
  if (!VARATT_IS_PAX_SUPPORT_TOAST(d)) return 0;

  if (VARATT_IS_PAX_EXTERNAL_TOAST(d)) {
    return (PaxExtGetDatum(d))->va_extogsz;
  } else if (VARATT_IS_COMPRESSED(d)) {
    return VARDATA_COMPRESSED_GET_EXTSIZE(d);
  }

  pg_unreachable();
}

size_t pax_toast_hdr_size(Datum d) {
  if (!VARATT_IS_PAX_SUPPORT_TOAST(d)) return 0;

  if (VARATT_IS_PAX_EXTERNAL_TOAST(d)) {
    return sizeof(pax_varatt_external);
  } else if (VARATT_IS_COMPRESSED(d)) {
    return VARHDRSZ_COMPRESSED;
  }

  pg_unreachable();
}

size_t pax_decompress_buffer(ToastCompressionId cmid, char *dst_buff,
                             size_t dst_cap, char *src_buff,
                             size_t src_buff_size) {
  switch (cmid) {
    case TOAST_PGLZ_COMPRESSION_ID: {
      size_t rawsize;
      PgLZCompressor compressor;
      // decompress the data
      rawsize =
          compressor.Decompress(dst_buff, dst_cap, src_buff, src_buff_size);
      if (compressor.IsError(rawsize)) {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeToastPGLZError,
                   fmt("Toast PGLZ decompress failed, %s",
                       compressor.ErrorName(rawsize)));
      }

      return rawsize;
    }
    case TOAST_LZ4_COMPRESSION_ID: {
#ifndef USE_LZ4
      CBDB_RAISE(cbdb::CException::kExTypeToastNoLZ4Support);
#else
      size_t rawsize;
      PaxLZ4Compressor compressor;

      rawsize =
          compressor.Decompress(dst_buff, dst_cap, src_buff, src_buff_size);
      if (compressor.IsError(rawsize)) {
        CBDB_RAISE(cbdb::CException::ExType::kExTypeToastLZ4Error,
                   fmt("Toast LZ4 decompress failed, %s",
                       compressor.ErrorName(rawsize)));
      }

      return rawsize;
#endif
    }
    case TOAST_INVALID_COMPRESSION_ID: {
      Assert(dst_cap >= src_buff_size);
      memcpy(dst_buff, src_buff, src_buff_size);
      return src_buff_size;
    }
    default:
      Assert(false);
  }
  pg_unreachable();
}

size_t pax_decompress_datum(Datum d, char *dst_buff, size_t dst_cap) {
  ToastCompressionId cmid;

  Assert(VARATT_IS_COMPRESSED(d));
  Assert(dst_buff);
  // Fetch the compression method id stored in the compression header and
  // decompress the data using the appropriate decompression routine.
  cmid = (ToastCompressionId)(TOAST_COMPRESS_METHOD(d));
  return pax_decompress_buffer(cmid, dst_buff, dst_cap,
                               (char *)d + VARHDRSZ_COMPRESSED,
                               VARSIZE(d) - VARHDRSZ_COMPRESSED);
}

size_t pax_detoast_raw(Datum d, char *dst_buff, size_t dst_cap, char *ext_buff,
                       size_t ext_buff_size) {
  size_t decompress_size = 0;
  if (VARATT_IS_COMPRESSED(d)) {
    auto compress_toast_extsize = VARDATA_COMPRESSED_GET_EXTSIZE(d);
    CBDB_CHECK(dst_cap >= compress_toast_extsize,
               cbdb::CException::ExType::kExTypeOutOfRange,
               fmt("Fail to detoast raw buffer [record size=%u, dst cap=%lu]",
                   compress_toast_extsize, dst_cap));
    // only external toast exist invalid compress toast
    Assert((ToastCompressionId)(TOAST_COMPRESS_METHOD(d)) !=
           TOAST_INVALID_COMPRESSION_ID);

    decompress_size = pax_decompress_datum(d, dst_buff, compress_toast_extsize);
    Assert(decompress_size <= dst_cap);
  } else if (VARATT_IS_PAX_EXTERNAL_TOAST(d)) {
    Assert(ext_buff);
    Assert(ext_buff_size > 0);
    auto offset = PAX_VARATT_EXTERNAL_OFFSET(d);
    auto raw_size = PAX_VARATT_EXTERNAL_SIZE(d);
    auto origin_size = PAX_VARATT_EXTERNAL_ORIGIN_SIZE(d);

    CBDB_CHECK(dst_cap >= origin_size && offset + raw_size <= ext_buff_size,
               cbdb::CException::ExType::kExTypeOutOfRange,
               fmt("Fail to detoast raw buffer [dst cap=%lu, origin size=%lu, "
                   "off=%lu, raw size=%lu, external buff size=%lu]",
                   dst_cap, origin_size, offset, raw_size, ext_buff_size));

    decompress_size =
        pax_decompress_buffer(PAX_VARATT_EXTERNAL_CMID(d), dst_buff,
                              origin_size, ext_buff + offset, raw_size);
  }

  return decompress_size;
}

std::pair<Datum, std::unique_ptr<MemoryObject>> pax_detoast(
    Datum d, char *ext_buff, size_t ext_buff_size) {
  std::unique_ptr<ExternalToastValue> value;

  if (VARATT_IS_COMPRESSED(d)) {
    char *result;
    size_t raw_size = VARDATA_COMPRESSED_GET_EXTSIZE(d);

    value = std::make_unique<ExternalToastValue>(raw_size + VARHDRSZ);
    result = reinterpret_cast<char *>(value->Addr());
    // only external toast exist invalid compress toast
    Assert((ToastCompressionId)(TOAST_COMPRESS_METHOD(d)) !=
           TOAST_INVALID_COMPRESSION_ID);

    auto pg_attribute_unused() decompress_size =
        pax_decompress_datum(d, result + VARHDRSZ, raw_size);
    Assert(decompress_size == raw_size);

    SET_VARSIZE(result, raw_size + VARHDRSZ);

    return std::pair<Datum, std::unique_ptr<MemoryObject>>{
        PointerGetDatum(result), std::move(value)};
  } else if (VARATT_IS_PAX_EXTERNAL_TOAST(d)) {
    char *result;
    Assert(ext_buff);
    Assert(ext_buff_size > 0);
    auto offset = PAX_VARATT_EXTERNAL_OFFSET(d);
    auto raw_size = PAX_VARATT_EXTERNAL_SIZE(d);
    auto origin_size = PAX_VARATT_EXTERNAL_ORIGIN_SIZE(d);

    CBDB_CHECK(offset + raw_size <= ext_buff_size,
               cbdb::CException::ExType::kExTypeOutOfRange,
               fmt("Fail to detoast [dst off=%lu, raw size=%lu, external "
                   "buff size=%lu]",
                   offset, raw_size, ext_buff_size));

    value = std::make_unique<ExternalToastValue>(origin_size + VARHDRSZ);

    result = reinterpret_cast<char *>(value->Addr());
    auto pg_attribute_unused() decompress_size =
        pax_decompress_buffer(PAX_VARATT_EXTERNAL_CMID(d), result + VARHDRSZ,
                              origin_size, ext_buff + offset, raw_size);

    Assert(decompress_size == origin_size);
    SET_VARSIZE(result, origin_size + VARHDRSZ);
    return std::pair<Datum, std::unique_ptr<MemoryObject>>{
        PointerGetDatum(result), std::move(value)};
  }

  return std::pair<Datum, std::unique_ptr<MemoryObject>>{d, nullptr};
}

ExternalToastValue::ExternalToastValue(size_t size)
    : buffer_(ByteBuffer(size, size)) {}
ExternalToastValue::ExternalToastValue(ByteBuffer &&buffer)
    : buffer_(std::move(buffer)) {}

struct varlena *pg_detoast_exp_short(struct varlena *vl) {
  if (VARATT_IS_COMPRESSED(vl) || VARATT_IS_EXTERNAL(vl)) {
    return cbdb::PgDeToastDatum(vl);
  }

  return vl;
}
}  // namespace pax

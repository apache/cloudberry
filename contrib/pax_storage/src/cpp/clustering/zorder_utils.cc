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
 * zorder_utils.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/zorder_utils.cc
 *
 *-------------------------------------------------------------------------
 */

#include "clustering/zorder_utils.h"

#include "comm/cbdb_api.h"

#include "comm/cbdb_wrappers.h"

#define LONG_MIN_VALUE 0x8000000000000000L
namespace paxc {

// should match the behavior in the datum_to_bytes function
bool support_zorder_type(Oid type) {
  switch (type) {
    case BOOLOID:
    case CHAROID:
    case INT2OID:
    case INT4OID:
    case INT8OID:
    case FLOAT4OID:
    case FLOAT8OID:
    case VARCHAROID:
    case BPCHAROID:
    case TEXTOID:
    case BYTEAOID:
    case DATEOID:
      return true;
    default:
      return false;
  }
}
}  // namespace paxc

namespace pax {

static inline void int8_to_bytes(int64 val, char *result) {
  uint64 temp = val;
  temp ^= (1L << 63);

  for (int i = N_BYTES - 1; i > 0; i--) {
    result[i] = (uint8)(temp & 0xFF);
    temp >>= 8;
  }
  result[0] = (uint8)(temp & 0xFF);
}

static inline void float8_to_bytes(float8 val, char *result) {
  int64 lval;

  memcpy((char *)&lval, (char *)&val, sizeof(lval));

  // If lval is a positive number, the sign bit is 0, then xor_value=
  // (0x00000000|0x80000000), only the sign bit is changed for positive
  // numbers. If lval is a negative number, the sign bit is 1, then xor_value=
  // (0xffffffff|0x80000000), all bits are changed for negative
  // numbers.
  lval ^= ((lval >> 63) | LONG_MIN_VALUE);

  for (int i = N_BYTES - 1; i > 0; i--) {
    result[i] = (uint8)(lval & 0xFF);
    lval >>= 8;
  }
  result[0] = (uint8)(lval & 0xFF);
}

static inline void float4_to_bytes(float4 val, char *result) {
  float8_to_bytes(float8(val), result);
}

static inline void varchar_to_bytes(char *val, int length, char *result) {
  length = length >= N_BYTES ? N_BYTES : length;
  // result has filled with 0
  memcpy(result, val, length);
}

static inline void null_to_bytes(void *result) { memset(result, 0, N_BYTES); }

void datum_to_bytes(Datum datum, Oid type, bool isnull, char *result) {
  if (isnull) {
    null_to_bytes(result);
    return;
  }

  memset(result, 0, N_BYTES);
  switch (type) {
    case BOOLOID:
      int8_to_bytes(cbdb::DatumToBool(datum) ? 1 : 0, result);
      break;
    case CHAROID:
      int8_to_bytes((int64)cbdb::DatumToInt8(datum), result);
      break;
    case INT2OID:
      int8_to_bytes((int64)cbdb::DatumToInt16(datum), result);
      break;
    case INT4OID:
    case DATEOID:
      int8_to_bytes((int64)cbdb::DatumToInt32(datum), result);
      break;
    case INT8OID:
      int8_to_bytes(cbdb::DatumToInt64(datum), result);
      break;
    case FLOAT4OID:
      float4_to_bytes(cbdb::DatumToFloat4(datum), result);
      break;
    case FLOAT8OID:
      float8_to_bytes(cbdb::DatumToFloat8(datum), result);
      break;
    case VARCHAROID:
    case BPCHAROID:
    case TEXTOID:
    case BYTEAOID: {
      struct varlena *var_data = PG_DETOAST_DATUM_PACKED(datum);
      varchar_to_bytes(VARDATA_ANY(var_data), VARSIZE_ANY_EXHDR(var_data),
                       result);
      break;
    }
    default:
      Assert(false);
      elog(ERROR, "unsupported data type %d for zorder", type);
  }
}

int bytes_compare(const char *a, const char *b, int ncolumns) {
  for (int i = 0; i < ncolumns * N_BYTES; i++) {
    int32 a_num = (int32)(a[i] & 0xFF);
    int32 b_num = (int32)(b[i] & 0xFF);

    if (a_num < b_num) return -1;
    if (a_num > b_num) return 1;
  }

  return 0;
}

// Set the bit at the given position in the destination byte to the value of the
// source bit.
static inline void SetBit(uint8 *dst, int dst_pos, uint8 src, int src_pos) {
  *dst |= (((src >> (7 - src_pos)) & 0x1) << (7 - dst_pos));
}

void interleave_bits(const char *src, char *result, int ncolumns) {
  for (int bit = 0; bit < N_BYTES * 8; bit++) {
    int bytePos = bit / 8;  // current byte position of each column
    int bitPos = bit % 8;   // bit position in current byte

    for (int i = 0; i < ncolumns; i++) {
      int bitPosOut = bit * ncolumns + i;  // current bit position in result
      int bytePosOut = bitPosOut / 8;      // current byte position in result
      int curBitPosOut = bitPosOut % 8;  // current bit position in current byte

      SetBit((uint8 *)&result[bytePosOut], curBitPosOut,
             src[i * N_BYTES + bytePos], bitPos);
    }
  }
}

Datum bytes_to_zorder_datum(char *buffer, int ncolumns) {
  Datum zorder_value;
  int value_len = ncolumns * N_BYTES;

  // FIXME(gongxun): varattrib_1b is better?
  zorder_value =
      PointerGetDatum(cbdb::Palloc0(TYPEALIGN(N_BYTES, value_len + VARHDRSZ)));
  interleave_bits(buffer, VARDATA(zorder_value), ncolumns);
  SET_VARSIZE(zorder_value, value_len + VARHDRSZ);
  return zorder_value;
}
}  // namespace pax

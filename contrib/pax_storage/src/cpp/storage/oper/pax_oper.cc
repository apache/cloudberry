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
 * pax_oper.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/oper/pax_oper.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/oper/pax_oper.h"

#include "comm/cbdb_wrappers.h"
#include "comm/pax_memory.h"
#include "exceptions/CException.h"

namespace pax {

namespace boolop {

// oper(bool, bool)
static bool BoolLT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const bool *)l) < *((const bool *)r));
}

static bool BoolLE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const bool *)l) <= *((const bool *)r));
}

static bool BoolEQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const bool *)l) == *((const bool *)r));
}

static bool BoolGE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const bool *)l) >= *((const bool *)r));
}

static bool BoolGT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const bool *)l) > *((const bool *)r));
}

}  // namespace boolop

namespace charop {

// oper(char, char)
static bool CharLT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const uint8 *)l) < *((const uint8 *)r));
}

static bool CharLE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const uint8 *)l) <= *((const uint8 *)r));
}

static bool CharEQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const uint8 *)l) == *((const uint8 *)r));
}

static bool CharGE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const uint8 *)l) >= *((const uint8 *)r));
}

static bool CharGT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const uint8 *)l) > *((const uint8 *)r));
}

}  // namespace charop

namespace bitop {

static inline int32 bit_cmp(const VarBit *arg1, const VarBit *arg2) {
  int bitlen1, bytelen1, bitlen2, bytelen2;
  int32 cmp;

  bytelen1 = VARBITBYTES(arg1);
  bytelen2 = VARBITBYTES(arg2);

  cmp = memcmp(VARBITS(arg1), VARBITS(arg2), Min(bytelen1, bytelen2));
  if (cmp == 0) {
    bitlen1 = VARBITLEN(arg1);
    bitlen2 = VARBITLEN(arg2);
    if (bitlen1 != bitlen2) cmp = (bitlen1 < bitlen2) ? -1 : 1;
  }
  return cmp;
}

static bool BitLT(const void *l, const void *r, Oid /*collation*/) {
  return (bit_cmp(*(const VarBit **)l, *(const VarBit **)r) < 0);
}

static bool BitLE(const void *l, const void *r, Oid /*collation*/) {
  return (bit_cmp(*(const VarBit **)l, *(const VarBit **)r) <= 0);
}

static bool BitEQ(const void *l, const void *r, Oid /*collation*/) {
  const VarBit *arg1 = *(const VarBit **)l;
  const VarBit *arg2 = *(const VarBit **)r;

  // fast path for different-length inputs
  if (VARBITLEN(arg1) != VARBITLEN(arg2)) return false;

  return (bit_cmp(arg1, arg2) == 0);
}

static bool BitGE(const void *l, const void *r, Oid /*collation*/) {
  return (bit_cmp(*(const VarBit **)l, *(const VarBit **)r) >= 0);
}

static bool BitGT(const void *l, const void *r, Oid /*collation*/) {
  return (bit_cmp(*(const VarBit **)l, *(const VarBit **)r) > 0);
}

}  // namespace bitop

namespace byteaop {

static bool ByteaLT(const void *l, const void *r, Oid /*collation*/) {
  const bytea *arg1 = *(const bytea **)l;
  const bytea *arg2 = *(const bytea **)r;
  int len1, len2;
  int cmp;

  len1 = VARSIZE_ANY_EXHDR(arg1);
  len2 = VARSIZE_ANY_EXHDR(arg2);

  cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

  return (cmp < 0) || ((cmp == 0) && (len1 < len2));
}

static bool ByteaLE(const void *l, const void *r, Oid /*collation*/) {
  const bytea *arg1 = *(const bytea **)l;
  const bytea *arg2 = *(const bytea **)r;
  int len1, len2;
  int cmp;

  len1 = VARSIZE_ANY_EXHDR(arg1);
  len2 = VARSIZE_ANY_EXHDR(arg2);

  cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

  return (cmp < 0) || ((cmp == 0) && (len1 <= len2));
}

static bool ByteaEQ(const void *l, const void *r, Oid /*collation*/) {
  const bytea *arg1 = *(const bytea **)l;
  const bytea *arg2 = *(const bytea **)r;
  Size len1, len2;

  len1 = VARSIZE_ANY_EXHDR(arg1);
  len2 = VARSIZE_ANY_EXHDR(arg2);
  if (len1 != len2) return false;

  return (memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), len1) == 0);
}

static bool ByteaGE(const void *l, const void *r, Oid /*collation*/) {
  const bytea *arg1 = *(const bytea **)l;
  const bytea *arg2 = *(const bytea **)r;
  int len1, len2;
  int cmp;

  len1 = VARSIZE_ANY_EXHDR(arg1);
  len2 = VARSIZE_ANY_EXHDR(arg2);

  cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));

  return (cmp > 0) || ((cmp == 0) && (len1 >= len2));
}

static bool ByteaGT(const void *l, const void *r, Oid /*collation*/) {
  const bytea *arg1 = *(const bytea **)l;
  const bytea *arg2 = *(const bytea **)r;
  int len1, len2;
  int cmp;

  len1 = VARSIZE_ANY_EXHDR(arg1);
  len2 = VARSIZE_ANY_EXHDR(arg2);

  cmp = memcmp(VARDATA_ANY(arg1), VARDATA_ANY(arg2), Min(len1, len2));
  return (cmp > 0) || ((cmp == 0) && (len1 > len2));
}

}  // namespace byteaop

namespace int2op {

// oper(int2, int2)
static bool Int2LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) < *((const int16 *)r));
}

static bool Int2LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) <= *((const int16 *)r));
}

static bool Int2EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) == *((const int16 *)r));
}

static bool Int2GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) >= *((const int16 *)r));
}

static bool Int2GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) > *((const int16 *)r));
}

// oper(int2, int4)
static bool Int24LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) < *((const int32 *)r));
}

static bool Int24LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) <= *((const int32 *)r));
}

static bool Int24EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) == *((const int32 *)r));
}

static bool Int24GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) >= *((const int32 *)r));
}

static bool Int24GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) > *((const int32 *)r));
}

// oper(int2, int8)
static bool Int28LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) < *((const int64 *)r));
}

static bool Int28LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) <= *((const int64 *)r));
}

static bool Int28EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) == *((const int64 *)r));
}

static bool Int28GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) >= *((const int64 *)r));
}

static bool Int28GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int16 *)l) > *((const int64 *)r));
}

}  // namespace int2op

namespace int4op {

// oper(int4, int4)
static bool Int4LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) < *((const int32 *)r));
}

static bool Int4LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) <= *((const int32 *)r));
}

static bool Int4EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) == *((const int32 *)r));
}

static bool Int4GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) >= *((const int32 *)r));
}

static bool Int4GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) > *((const int32 *)r));
}

// oper(int4, int8)
static bool Int48LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) < *((const int64 *)r));
}

static bool Int48LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) <= *((const int64 *)r));
}

static bool Int48EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) == *((const int64 *)r));
}

static bool Int48GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) >= *((const int64 *)r));
}

static bool Int48GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) > *((const int64 *)r));
}

// oper(int4, int2)
static bool Int42LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) < *((const int16 *)r));
}

static bool Int42LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) <= *((const int16 *)r));
}

static bool Int42EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) == *((const int16 *)r));
}

static bool Int42GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) >= *((const int16 *)r));
}

static bool Int42GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int32 *)l) > *((const int16 *)r));
}

}  // namespace int4op

namespace int8op {

// oper(int8, int8)
static bool Int8LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) < *((const int64 *)r));
}

static bool Int8LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) <= *((const int64 *)r));
}

static bool Int8EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) == *((const int64 *)r));
}

static bool Int8GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) >= *((const int64 *)r));
}

static bool Int8GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) > *((const int64 *)r));
}

// oper(int8, int4)
static bool Int84LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) < *((const int32 *)r));
}

static bool Int84LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) <= *((const int32 *)r));
}

static bool Int84EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) == *((const int32 *)r));
}

static bool Int84GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) >= *((const int32 *)r));
}

static bool Int84GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) > *((const int32 *)r));
}

// oper(int8, int2)
static bool Int82LT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) < *((const int16 *)r));
}

static bool Int82LE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) <= *((const int16 *)r));
}

static bool Int82EQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) == *((const int16 *)r));
}

static bool Int82GE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) >= *((const int16 *)r));
}

static bool Int82GT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const int64 *)l) > *((const int16 *)r));
}

}  // namespace int8op

namespace float4op {

static bool Float4LT(const void *l, const void *r, Oid /*collation*/) {
  float4 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float4 *)r);

  return !isnan(val1) && (isnan(val2) || val1 < val2);
}

static bool Float4LE(const void *l, const void *r, Oid /*collation*/) {
  float4 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float4 *)r);

  return isnan(val2) || (!isnan(val1) && val1 <= val2);
}

static bool Float4EQ(const void *l, const void *r, Oid /*collation*/) {
  float4 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float4 *)r);

  return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

static bool Float4GE(const void *l, const void *r, Oid /*collation*/) {
  float4 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float4 *)r);

  return isnan(val1) || (!isnan(val2) && val1 >= val2);
}

static bool Float4GT(const void *l, const void *r, Oid /*collation*/) {
  float4 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float4 *)r);

  return !isnan(val2) && (isnan(val1) || val1 > val2);
}

static bool Float48LT(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float8 *)r);

  return !isnan(val1) && (isnan(val2) || val1 < val2);
}

static bool Float48LE(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float8 *)r);

  return isnan(val2) || (!isnan(val1) && val1 <= val2);
}

static bool Float48EQ(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float8 *)r);

  return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

static bool Float48GE(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float8 *)r);

  return isnan(val1) || (!isnan(val2) && val1 >= val2);
}

static bool Float48GT(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float4 *)l);
  val2 = *((const float8 *)r);

  return !isnan(val2) && (isnan(val1) || val1 > val2);
}

}  // namespace float4op

namespace float8op {

static bool Float8LT(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float8 *)r);

  return !isnan(val1) && (isnan(val2) || val1 < val2);
}

static bool Float8LE(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float8 *)r);

  return isnan(val2) || (!isnan(val1) && val1 <= val2);
}

static bool Float8EQ(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float8 *)r);

  return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

static bool Float8GE(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float8 *)r);

  return isnan(val1) || (!isnan(val2) && val1 >= val2);
}

static bool Float8GT(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float8 *)r);

  return !isnan(val2) && (isnan(val1) || val1 > val2);
}

static bool Float84LT(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float4 *)r);

  return !isnan(val1) && (isnan(val2) || val1 < val2);
}

static bool Float84LE(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float4 *)r);

  return isnan(val2) || (!isnan(val1) && val1 <= val2);
}

static bool Float84EQ(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float4 *)r);

  return isnan(val1) ? isnan(val2) : !isnan(val2) && val1 == val2;
}

static bool Float84GE(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float4 *)r);

  return isnan(val1) || (!isnan(val2) && val1 >= val2);
}

static bool Float84GT(const void *l, const void *r, Oid /*collation*/) {
  float8 val1, val2;
  val1 = *((const float8 *)l);
  val2 = *((const float4 *)r);

  return !isnan(val2) && (isnan(val1) || val1 > val2);
}

}  // namespace float8op

namespace textop {
#define TEXTBUFLEN 1024

static inline bool LocaleIsC(Oid collation) {
  if (collation == C_COLLATION_OID || collation == POSIX_COLLATION_OID) {
    return 1;
  }

  /*
   * If we're asked about the default collation, we have to inquire of the C
   * library.  Cache the result so we only have to compute it once.
   */
  if (collation == DEFAULT_COLLATION_OID) {
    static int result = -1;
    char *localeptr;
    if (result != -1) {
      return (bool)result;
    }

    localeptr = setlocale(LC_COLLATE, NULL);
    CBDB_CHECK(localeptr, cbdb::CException::ExType::kExTypeCError,
               fmt("Invalid locale, fail to `setlocale`, errno: %d", errno));

    if (strcmp(localeptr, "C") == 0 ||  // cut line
        strcmp(localeptr, "POSIX") == 0) {
      result = 1;
    } else {
      result = 0;
    }

    return (bool)result;
  } else {
    return false;
  }
}

static inline int VarstrCmp(const char *arg1, int len1, const char *arg2,
                            int len2, Oid collid) {
  int rc;

  CBDB_CHECK(OidIsValid(collid), cbdb::CException::ExType::kExTypeLogicError,
             fmt("[collid=%u] not support", collid));
  if (LocaleIsC(collid)) {
    rc = memcmp(arg1, arg2, Min(len1, len2));
    if ((rc == 0) && (len1 != len2)) rc = (len1 < len2) ? -1 : 1;
  } else if (collid == DEFAULT_COLLATION_OID) {
    char a1buf[TEXTBUFLEN];
    char a2buf[TEXTBUFLEN];
    char *a1p, *a2p;
    if (len1 == len2 && memcmp(arg1, arg2, len1) == 0) return 0;

    a1p = (len1 >= TEXTBUFLEN) ? ::pax::PAX_ALLOC<char>(len1 + 1) : a1buf;
    a2p = (len2 >= TEXTBUFLEN) ? ::pax::PAX_ALLOC<char>(len2 + 1) : a2buf;

    memcpy(a1p, arg1, len1);
    a1p[len1] = '\0';
    memcpy(a2p, arg2, len2);
    a2p[len2] = '\0';

    rc = strcoll(a1p, a2p);

    if (rc == 0) rc = strcmp(a1p, a2p);

    if (a1p != a1buf) ::pax::PAX_FREE(a1p);

    if (a2p != a2buf) ::pax::PAX_FREE(a2p);
  } else {
    // not support special provider
    CBDB_RAISE(cbdb::CException::ExType::kExTypeUnImplements);
  }

  return rc;
}


static inline int BcTruelen(const BpChar *arg) {
  return bpchartruelen(VARDATA_ANY(arg), VARSIZE_ANY_EXHDR(arg));
}

static inline int TextCmp(const text *arg1, const text *arg2, Oid collid) {
  // safe to direct call toast_raw_datum_size
  return VarstrCmp(VARDATA_ANY(arg1), VARSIZE_ANY_EXHDR(arg1),
                   VARDATA_ANY(arg2), VARSIZE_ANY_EXHDR(arg2), collid);
}

static inline int TextBpcharCmp(const text *arg1, const BpChar *arg2, Oid collid) {
  return VarstrCmp(VARDATA_ANY(arg1), VARSIZE_ANY_EXHDR(arg1),
                   VARDATA_ANY(arg2), BcTruelen(arg2), collid);
}

static bool TextLT(const void *l, const void *r, Oid collation) {
  return TextCmp(*(const text **)l, *(const text **)r, collation) < 0;
}

static bool TextLE(const void *l, const void *r, Oid collation) {
  return TextCmp(*(const text **)l, *(const text **)r, collation) <= 0;
}

static bool TextEQ(const void *l, const void *r, Oid collation) {
  return TextCmp(*(const text **)l, *(const text **)r, collation) == 0;
}

static bool TextGE(const void *l, const void *r, Oid collation) {
  return TextCmp(*(const text **)l, *(const text **)r, collation) >= 0;
}

static bool TextGT(const void *l, const void *r, Oid collation) {
  return TextCmp(*(const text **)l, *(const text **)r, collation) > 0;
}


// oper(text, bpchar)
static bool TextBpcharLT(const void *l, const void *r, Oid collation) {
  return TextBpcharCmp(*(const text **)l, *(const BpChar **)r, collation) < 0;
}

static bool TextBpcharLE(const void *l, const void *r, Oid collation) {
  return TextBpcharCmp(*(const text **)l, *(const BpChar **)r, collation) <= 0;
}

static bool TextBpcharEQ(const void *l, const void *r, Oid collation) {
  return TextBpcharCmp(*(const text **)l, *(const BpChar **)r, collation) == 0;
}

static bool TextBpcharGE(const void *l, const void *r, Oid collation) {
  return TextBpcharCmp(*(const text **)l, *(const BpChar **)r, collation) >= 0;
}

static bool TextBpcharGT(const void *l, const void *r, Oid collation) {
  return TextBpcharCmp(*(const text **)l, *(const BpChar **)r, collation) > 0;
}

static bool BpCharLT(const void *l, const void *r, Oid collation) {
  const BpChar *lbpchar = *(const BpChar **)l;
  const BpChar *rbpchar = *(const BpChar **)r;

  return VarstrCmp(VARDATA_ANY(lbpchar), BcTruelen(lbpchar),
                   VARDATA_ANY(rbpchar), BcTruelen(rbpchar), collation) < 0;
}

static bool BpCharLE(const void *l, const void *r, Oid collation) {
  const BpChar *lbpchar = *(const BpChar **)l;
  const BpChar *rbpchar = *(const BpChar **)r;

  return VarstrCmp(VARDATA_ANY(lbpchar), BcTruelen(lbpchar),
                   VARDATA_ANY(rbpchar), BcTruelen(rbpchar), collation) <= 0;
}

static bool BpCharEQ(const void *l, const void *r, Oid collation) {
  const BpChar *lbpchar = *(const BpChar **)l;
  const BpChar *rbpchar = *(const BpChar **)r;

  return VarstrCmp(VARDATA_ANY(lbpchar), BcTruelen(lbpchar),
                   VARDATA_ANY(rbpchar), BcTruelen(rbpchar), collation) == 0;
}

static bool BpCharGE(const void *l, const void *r, Oid collation) {
  const BpChar *lbpchar = *(const BpChar **)l;
  const BpChar *rbpchar = *(const BpChar **)r;

  return VarstrCmp(VARDATA_ANY(lbpchar), BcTruelen(lbpchar),
                   VARDATA_ANY(rbpchar), BcTruelen(rbpchar), collation) >= 0;
}

static bool BpCharGT(const void *l, const void *r, Oid collation) {
  const BpChar *lbpchar = *(const BpChar **)l;
  const BpChar *rbpchar = *(const BpChar **)r;

  return VarstrCmp(VARDATA_ANY(lbpchar), BcTruelen(lbpchar),
                   VARDATA_ANY(rbpchar), BcTruelen(rbpchar), collation) > 0;
}

// oper(bpchar, text)
static bool BpcharTextLT(const void *l, const void *r, Oid collation) {
  return TextBpcharCmp(*(const text **)r, *(const BpChar **)l, collation) > 0;
}

static bool BpcharTextLE(const void *l, const void *r, Oid collation) {
  return TextBpcharCmp(*(const text **)r, *(const BpChar **)l, collation) >= 0;
}

static bool BpcharTextEQ(const void *l, const void *r, Oid collation) {
  return TextBpcharCmp(*(const text **)r, *(const BpChar **)l, collation) == 0;
}

static bool BpcharTextGE(const void *l, const void *r, Oid collation) {
  return TextBpcharCmp(*(const text **)r, *(const BpChar **)l, collation) <= 0;
}

static bool BpcharTextGT(const void *l, const void *r, Oid collation) {
  return TextBpcharCmp(*(const text **)r, *(const BpChar **)l, collation) < 0;
}


static int name_cmp(const Name arg1, const Name arg2, Oid collid) {
  /* Fast path for common case used in system catalogs */
  if (collid == C_COLLATION_OID)
    return strncmp(NameStr(*arg1), NameStr(*arg2), NAMEDATALEN);

  /* Else rely on the varstr infrastructure */
  return varstr_cmp(NameStr(*arg1), strlen(NameStr(*arg1)), NameStr(*arg2),
                    strlen(NameStr(*arg2)), collid);
}

static bool NameLT(const void *l, const void *r, Oid collid) {
  return name_cmp(*(const Name *)l, *(const Name *)r, collid) < 0;
}

static bool NameLE(const void *l, const void *r, Oid collid) {
  return name_cmp(*(const Name *)l, *(const Name *)r, collid) <= 0;
}

static bool NameEQ(const void *l, const void *r, Oid collid) {
  return name_cmp(*(const Name *)l, *(const Name *)r, collid) == 0;
}

static bool NameGE(const void *l, const void *r, Oid collid) {
  return name_cmp(*(const Name *)l, *(const Name *)r, collid) >= 0;
}

static bool NameGT(const void *l, const void *r, Oid collid) {
  return name_cmp(*(const Name *)l, *(const Name *)r, collid) > 0;
}

}  // namespace textop

namespace numericop {

static bool NumericLT(const void *l, const void *r, Oid collation) {
  // safe to direct call pg function
  return cmp_numerics(*((const Numeric *)l), *((const Numeric *)r)) < 0;
}

static bool NumericLE(const void *l, const void *r, Oid collation) {
  return cmp_numerics(*((const Numeric *)l), *((const Numeric *)r)) <= 0;
}

static bool NumericEQ(const void *l, const void *r, Oid collation) {
  return cmp_numerics(*((const Numeric *)l), *((const Numeric *)r)) == 0;
}

static bool NumericGE(const void *l, const void *r, Oid collation) {
  return cmp_numerics(*((const Numeric *)l), *((const Numeric *)r)) >= 0;
}

static bool NumericGT(const void *l, const void *r, Oid collation) {
  return cmp_numerics(*((const Numeric *)l), *((const Numeric *)r)) > 0;
}

}  // namespace numericop

namespace timeop {

// oper(date, date)
static bool DateLT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const DateADT *)l) < *((const DateADT *)r));
}

static bool DateLE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const DateADT *)l) <= *((const DateADT *)r));
}

static bool DateEQ(const void *l, const void *r, Oid /*collation*/) {
  return (*((const DateADT *)l) == *((const DateADT *)r));
}

static bool DateGE(const void *l, const void *r, Oid /*collation*/) {
  return (*((const DateADT *)l) >= *((const DateADT *)r));
}

static bool DateGT(const void *l, const void *r, Oid /*collation*/) {
  return (*((const DateADT *)l) > *((const DateADT *)r));
}

static Timestamp
date2timestamp_opt_overflow(DateADT dateVal, int *overflow)
{
	Timestamp	result;

	if (overflow)
		*overflow = 0;

	if (DATE_IS_NOBEGIN(dateVal))
		TIMESTAMP_NOBEGIN(result);
	else if (DATE_IS_NOEND(dateVal))
		TIMESTAMP_NOEND(result);
	else
	{
		// Since dates have the same minimum values as timestamps, only upper
		// boundary need be checked for overflow.
		if (dateVal >= (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE))
		{
			if (overflow)
			{
				*overflow = 1;
				TIMESTAMP_NOEND(result);
				return result;
			}
			else
			{
        CBDB_RAISE(cbdb::CException::ExType::kExTypeValueOverflow,
               "date out of range for timestamp");
			}
		}

		// date is days since 2000, timestamp is microseconds since same...
		result = dateVal * USECS_PER_DAY;
	}

	return result;
}

static int
timestamp_cmp_internal(Timestamp dt1, Timestamp dt2)
{
	return (dt1 < dt2) ? -1 : ((dt1 > dt2) ? 1 : 0);
}

static int32
date_cmp_timestamp_internal(DateADT dateVal, Timestamp dt2)
{
	Timestamp	dt1;
	int			overflow;

	dt1 = date2timestamp_opt_overflow(dateVal, &overflow);
	if (overflow > 0)
	{
		/* dt1 is larger than any finite timestamp, but less than infinity */
		return TIMESTAMP_IS_NOEND(dt2) ? -1 : +1;
	}
	Assert(overflow == 0);		/* -1 case cannot occur */

	return timestamp_cmp_internal(dt1, dt2);
}


// oper(date, timestamp)
static bool DateTimestampLT(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamp_internal(*((const DateADT *)l), *((const Timestamp *)r)) < 0;
}

static bool DateTimestampLE(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamp_internal(*((const DateADT *)l), *((const Timestamp *)r)) <= 0;
}

static bool DateTimestampEQ(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamp_internal(*((const DateADT *)l), *((const Timestamp *)r)) == 0;
}

static bool DateTimestampGE(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamp_internal(*((const DateADT *)l), *((const Timestamp *)r)) >= 0;
}

static bool DateTimestampGT(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamp_internal(*((const DateADT *)l), *((const Timestamp *)r)) > 0;
}

TimestampTz
date2timestamptz_opt_overflow(DateADT dateVal, int *overflow)
{
	TimestampTz result;
	struct pg_tm tt,
			   *tm = &tt;
	int			tz;

	if (overflow)
		*overflow = 0;

	if (DATE_IS_NOBEGIN(dateVal))
		TIMESTAMP_NOBEGIN(result);
	else if (DATE_IS_NOEND(dateVal))
		TIMESTAMP_NOEND(result);
	else
	{
		/*
		 * Since dates have the same minimum values as timestamps, only upper
		 * boundary need be checked for overflow.
		 */
		if (dateVal >= (TIMESTAMP_END_JULIAN - POSTGRES_EPOCH_JDATE))
		{
			if (overflow)
			{
				*overflow = 1;
				TIMESTAMP_NOEND(result);
				return result;
			}
			else
			{
				CBDB_RAISE(cbdb::CException::ExType::kExTypeValueOverflow,
               "date out of range for timestamptz");
			}
		}

		j2date(dateVal + POSTGRES_EPOCH_JDATE,
			   &(tm->tm_year), &(tm->tm_mon), &(tm->tm_mday));
		tm->tm_hour = 0;
		tm->tm_min = 0;
		tm->tm_sec = 0;
    // safe to call 
		tz = DetermineTimeZoneOffset(tm, session_timezone); 

		result = dateVal * USECS_PER_DAY + tz * USECS_PER_SEC;

		
		 // Since it is possible to go beyond allowed timestamptz range because
		 // of time zone, check for allowed timestamp range after adding tz.
		if (!IS_VALID_TIMESTAMP(result))
		{
			if (overflow)
			{
				if (result < MIN_TIMESTAMP)
				{
					*overflow = -1;
					TIMESTAMP_NOBEGIN(result);
				}
				else
				{
					*overflow = 1;
					TIMESTAMP_NOEND(result);
				}
			}
			else
			{
				CBDB_RAISE(cbdb::CException::ExType::kExTypeValueOverflow,
               "date out of range for timestamptz");
			}
		}
	}

	return result;
}

static int32
date_cmp_timestamptz_internal(DateADT dateVal, TimestampTz dt2)
{
	TimestampTz dt1;
	int			overflow;

	dt1 = date2timestamptz_opt_overflow(dateVal, &overflow);
	if (overflow > 0)
	{
		/* dt1 is larger than any finite timestamp, but less than infinity */
		return TIMESTAMP_IS_NOEND(dt2) ? -1 : +1;
	}
	if (overflow < 0)
	{
		/* dt1 is less than any finite timestamp, but more than -infinity */
		return TIMESTAMP_IS_NOBEGIN(dt2) ? +1 : -1;
	}

	return timestamptz_cmp_internal(dt1, dt2);
}

// oper(date, timestamptz)
static bool DateTimestampTzLT(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamptz_internal(*((const DateADT *)l), *((const TimestampTz *)r)) < 0;
}

static bool DateTimestampTzLE(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamptz_internal(*((const DateADT *)l), *((const TimestampTz *)r)) <= 0;
}

static bool DateTimestampTzEQ(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamptz_internal(*((const DateADT *)l), *((const TimestampTz *)r)) == 0;
}

static bool DateTimestampTzGE(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamptz_internal(*((const DateADT *)l), *((const TimestampTz *)r)) >= 0;
}

static bool DateTimestampTzGT(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamptz_internal(*((const DateADT *)l), *((const TimestampTz *)r)) > 0;
}

static bool TimeStampLT(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_internal(*((const Timestamp *)l),
                                *((const Timestamp *)r)) < 0;
}

static bool TimeStampLE(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_internal(*((const Timestamp *)l),
                                *((const Timestamp *)r)) <= 0;
}

static bool TimeStampEQ(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_internal(*((const Timestamp *)l),
                                *((const Timestamp *)r)) == 0;
}

static bool TimeStampGE(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_internal(*((const Timestamp *)l),
                                *((const Timestamp *)r)) >= 0;
}

static bool TimeStampGT(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_internal(*((const Timestamp *)l),
                                *((const Timestamp *)r)) > 0;
}

static Timestamp
dt2local(Timestamp dt, int tz)
{
	dt -= (tz * USECS_PER_SEC);
	return dt;
}

static TimestampTz
timestamp2timestamptz_opt_overflow(Timestamp timestamp, int *overflow)
{
	TimestampTz result;
	struct pg_tm tt,
			   *tm = &tt;
	fsec_t		fsec = 0;
	int			tz;

	if (overflow)
		*overflow = 0;

	if (TIMESTAMP_NOT_FINITE(timestamp))
		return timestamp;

	// We don't expect this to fail, but check it pro forma 
	if (timestamp2tm(timestamp, NULL, tm, &fsec, NULL, NULL) == 0) // safe to call 
	{
		tz = DetermineTimeZoneOffset(tm, session_timezone); // safe to call 

		result = dt2local(timestamp, -tz);

		if (IS_VALID_TIMESTAMP(result))
		{
			return result;
		}
		else if (overflow)
		{
			if (result < MIN_TIMESTAMP)
			{
				*overflow = -1;
				TIMESTAMP_NOBEGIN(result);
			}
			else
			{
				*overflow = 1;
				TIMESTAMP_NOEND(result);
			}
			return result;
		}
	}

  CBDB_RAISE(cbdb::CException::ExType::kExTypeValueOverflow,
               "timestamp out of range");
	return 0;
}

static int32
timestamp_cmp_timestamptz_internal(Timestamp timestampVal, TimestampTz dt2)
{
	TimestampTz dt1;
	int			overflow;

	dt1 = timestamp2timestamptz_opt_overflow(timestampVal, &overflow);
	if (overflow > 0)
	{
		/* dt1 is larger than any finite timestamp, but less than infinity */
		return TIMESTAMP_IS_NOEND(dt2) ? -1 : +1;
	}
	if (overflow < 0)
	{
		/* dt1 is less than any finite timestamp, but more than -infinity */
		return TIMESTAMP_IS_NOBEGIN(dt2) ? +1 : -1;
	}

	return timestamptz_cmp_internal(dt1, dt2);
}


// oper(timestamp, timestamptz)
static bool TimeStampTimeStampTzLT(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_timestamptz_internal(*((const Timestamp *)l),
                                *((const TimestampTz *)r)) < 0;
}

static bool TimeStampTimeStampTzLE(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_timestamptz_internal(*((const Timestamp *)l),
                                *((const TimestampTz *)r)) <= 0;
}

static bool TimeStampTimeStampTzEQ(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_timestamptz_internal(*((const Timestamp *)l),
                                *((const TimestampTz *)r)) == 0;
}

static bool TimeStampTimeStampTzGE(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_timestamptz_internal(*((const Timestamp *)l),
                                *((const TimestampTz *)r)) >= 0;
}

static bool TimeStampTimeStampTzGT(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_timestamptz_internal(*((const Timestamp *)l),
                                *((const TimestampTz *)r)) > 0;
}

// oper(timestamp, timestamptz)
static bool TimeStampDateLT(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamp_internal(*((const DateADT *)r), *((const Timestamp *)l)) > 0;
}

static bool TimeStampDateLE(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamp_internal(*((const DateADT *)r), *((const Timestamp *)l)) >= 0;
}

static bool TimeStampDateEQ(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamp_internal(*((const DateADT *)r), *((const Timestamp *)l)) == 0;
}

static bool TimeStampDateGE(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamp_internal(*((const DateADT *)r), *((const Timestamp *)l)) <= 0;
}

static bool TimeStampDateGT(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamp_internal(*((const DateADT *)r), *((const Timestamp *)l)) < 0;
}

// oper(timestamptz, timestamp)
static bool TimeStampTzTimeStampLT(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_timestamptz_internal(*((const Timestamp *)r), 
    *((const TimestampTz *)l)) > 0;
}

static bool TimeStampTzTimeStampLE(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_timestamptz_internal(*((const Timestamp *)r), 
    *((const TimestampTz *)l)) >= 0;
}

static bool TimeStampTzTimeStampEQ(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_timestamptz_internal(*((const Timestamp *)r), 
    *((const TimestampTz *)l)) == 0;
}

static bool TimeStampTzTimeStampGE(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_timestamptz_internal(*((const Timestamp *)r), 
    *((const TimestampTz *)l)) <= 0;
}

static bool TimeStampTzTimeStampGT(const void *l, const void *r, Oid /*collation*/) {
  return timestamp_cmp_timestamptz_internal(*((const Timestamp *)r), 
    *((const TimestampTz *)l)) < 0;
}

// oper(timestamptz, date)
static bool TimeStampTzDateLT(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamptz_internal(*((const DateADT *)r), *((const TimestampTz *)l)) > 0;
}

static bool TimeStampTzDateLE(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamptz_internal(*((const DateADT *)r), *((const TimestampTz *)l)) >= 0;
}

static bool TimeStampTzDateEQ(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamptz_internal(*((const DateADT *)r), *((const TimestampTz *)l)) == 0;
}

static bool TimeStampTzDateGE(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamptz_internal(*((const DateADT *)r), *((const TimestampTz *)l)) <= 0;
}

static bool TimeStampTzDateGT(const void *l, const void *r, Oid /*collation*/) {
  return date_cmp_timestamptz_internal(*((const DateADT *)r), *((const TimestampTz *)l)) < 0;
}


static bool TimeLT(const void *l, const void *r, Oid /*collation*/) {
  return *((const TimeADT *)l) < *((const TimeADT *)r);
}

static bool TimeLE(const void *l, const void *r, Oid /*collation*/) {
  return *((const TimeADT *)l) <= *((const TimeADT *)r);
}

static bool TimeEQ(const void *l, const void *r, Oid /*collation*/) {
  return *((const TimeADT *)l) == *((const TimeADT *)r);
}

static bool TimeGE(const void *l, const void *r, Oid /*collation*/) {
  return *((const TimeADT *)l) >= *((const TimeADT *)r);
}

static bool TimeGT(const void *l, const void *r, Oid /*collation*/) {
  return *((const TimeADT *)l) > *((const TimeADT *)r);
}

static inline int timetz_cmp(const TimeTzADT *time1, const TimeTzADT *time2) {
  TimeOffset t1, t2;

  // Primary sort is by true (GMT-equivalent) time
  t1 = time1->time + (time1->zone * USECS_PER_SEC);
  t2 = time2->time + (time2->zone * USECS_PER_SEC);

  if (t1 > t2) return 1;
  if (t1 < t2) return -1;

  // If same GMT time, sort by timezone; we only want to say that two
  // timetz's are equal if both the time and zone parts are equal.
  if (time1->zone > time2->zone) return 1;
  if (time1->zone < time2->zone) return -1;

  return 0;
}

static bool TimeTzLT(const void *l, const void *r, Oid /*collation*/) {
  return timetz_cmp(*((const TimeTzADT **)l), *((const TimeTzADT **)r)) < 0;
}

static bool TimeTzLE(const void *l, const void *r, Oid /*collation*/) {
  return timetz_cmp(*((const TimeTzADT **)l), *((const TimeTzADT **)r)) <= 0;
}

static bool TimeTzEQ(const void *l, const void *r, Oid /*collation*/) {
  return timetz_cmp(*((const TimeTzADT **)l), *((const TimeTzADT **)r)) == 0;
}

static bool TimeTzGE(const void *l, const void *r, Oid /*collation*/) {
  return timetz_cmp(*((const TimeTzADT **)l), *((const TimeTzADT **)r)) >= 0;
}

static bool TimeTzGT(const void *l, const void *r, Oid /*collation*/) {
  return timetz_cmp(*((const TimeTzADT **)l), *((const TimeTzADT **)r)) > 0;
}

}  // namespace timeop

namespace oidop {

static bool OidLT(const void *l, const void *r, Oid /*collation*/) {
  return *(const Oid *)l < *(const Oid *)r;
}

static bool OidLE(const void *l, const void *r, Oid /*collation*/) {
  return *(const Oid *)l <= *(const Oid *)r;
}

static bool OidEQ(const void *l, const void *r, Oid /*collation*/) {
  return *(const Oid *)l == *(const Oid *)r;
}

static bool OidGE(const void *l, const void *r, Oid /*collation*/) {
  return *(const Oid *)l >= *(const Oid *)r;
}

static bool OidGT(const void *l, const void *r, Oid /*collation*/) {
  return *(const Oid *)l > *(const Oid *)r;
}

}  // namespace oidop

namespace inetop {

static inline int32 network_cmp_internal(const inet *a1, const inet *a2) {
  if (ip_family(a1) == ip_family(a2)) {
    int order;

    order = bitncmp(ip_addr(a1), ip_addr(a2), Min(ip_bits(a1), ip_bits(a2)));
    if (order != 0) return order;
    order = ((int)ip_bits(a1)) - ((int)ip_bits(a2));
    if (order != 0) return order;
    return bitncmp(ip_addr(a1), ip_addr(a2), ip_maxbits(a1));
  }

  return ip_family(a1) - ip_family(a2);
}

static bool InetLT(const void *l, const void *r, Oid /*collation*/) {
  return network_cmp_internal(*(const inet **)l, *(const inet **)r) < 0;
}

static bool InetLE(const void *l, const void *r, Oid /*collation*/) {
  return network_cmp_internal(*(const inet **)l, *(const inet **)r) <= 0;
}

static bool InetEQ(const void *l, const void *r, Oid /*collation*/) {
  return network_cmp_internal(*(const inet **)l, *(const inet **)r) == 0;
}

static bool InetGE(const void *l, const void *r, Oid /*collation*/) {
  return network_cmp_internal(*(const inet **)l, *(const inet **)r) >= 0;
}

static bool InetGT(const void *l, const void *r, Oid /*collation*/) {
  return network_cmp_internal(*(const inet **)l, *(const inet **)r) > 0;
}

}  // namespace inetop

namespace intervalop {

static inline INT128 interval_cmp_value(const Interval *interval) {
  INT128 span;
  int64 dayfraction;
  int64 days;

  /*
   * Separate time field into days and dayfraction, then add the month and
   * day fields to the days part.  We cannot overflow int64 days here.
   */
  dayfraction = interval->time % USECS_PER_DAY;
  days = interval->time / USECS_PER_DAY;
  days += interval->month * INT64CONST(30);
  days += interval->day;

  /* Widen dayfraction to 128 bits */
  span = int64_to_int128(dayfraction);

  /* Scale up days to microseconds, forming a 128-bit product */
  int128_add_int64_mul_int64(&span, days, USECS_PER_DAY);

  return span;
}

static inline int interval_cmp(const Interval *interval1,
                               const Interval *interval2) {
  INT128 span1 = interval_cmp_value(interval1);
  INT128 span2 = interval_cmp_value(interval2);

  return int128_compare(span1, span2);
}

static bool IntervalLT(const void *l, const void *r, Oid /*collation*/) {
  const Interval *interval1 = *(const Interval **)l;
  const Interval *interval2 = *(const Interval **)r;

  return interval_cmp(interval1, interval2) < 0;
}

static bool IntervalLE(const void *l, const void *r, Oid /*collation*/) {
  return interval_cmp(*(const Interval **)l, *(const Interval **)r) <= 0;
}

static bool IntervalEQ(const void *l, const void *r, Oid /*collation*/) {
  return interval_cmp(*(const Interval **)l, *(const Interval **)r) == 0;
}

static bool IntervalGE(const void *l, const void *r, Oid /*collation*/) {
  return interval_cmp(*(const Interval **)l, *(const Interval **)r) >= 0;
}

static bool IntervalGT(const void *l, const void *r, Oid /*collation*/) {
  return interval_cmp(*(const Interval **)l, *(const Interval **)r) > 0;
}

}  // namespace intervalop

namespace macaddrop {

#define hibits(addr) \
  ((unsigned long)(((addr)->a << 16) | ((addr)->b << 8) | ((addr)->c)))

#define lobits(addr) \
  ((unsigned long)(((addr)->d << 16) | ((addr)->e << 8) | ((addr)->f)))

static inline int macaddr_cmp(const macaddr *a1, const macaddr *a2) {
  if (hibits(a1) < hibits(a2))
    return -1;
  else if (hibits(a1) > hibits(a2))
    return 1;
  else if (lobits(a1) < lobits(a2))
    return -1;
  else if (lobits(a1) > lobits(a2))
    return 1;
  else
    return 0;
}

static bool MACaddrLT(const void *l, const void *r, Oid /*collation*/) {
  return macaddr_cmp(*(const macaddr **)l, *(const macaddr **)r) < 0;
}

static bool MACaddrLE(const void *l, const void *r, Oid /*collation*/) {
  return macaddr_cmp(*(const macaddr **)l, *(const macaddr **)r) <= 0;
}

static bool MACaddrEQ(const void *l, const void *r, Oid /*collation*/) {
  return macaddr_cmp(*(const macaddr **)l, *(const macaddr **)r) == 0;
}

static bool MACaddrGE(const void *l, const void *r, Oid /*collation*/) {
  return macaddr_cmp(*(const macaddr **)l, *(const macaddr **)r) >= 0;
}

static bool MACaddrGT(const void *l, const void *r, Oid /*collation*/) {
  return macaddr_cmp(*(const macaddr **)l, *(const macaddr **)r) > 0;
}

#undef hibits
#undef lobits

#define hibits8(addr)                                                         \
  ((unsigned long)(((addr)->a << 24) | ((addr)->b << 16) | ((addr)->c << 8) | \
                   ((addr)->d)))

#define lobits8(addr)                                                         \
  ((unsigned long)(((addr)->e << 24) | ((addr)->f << 16) | ((addr)->g << 8) | \
                   ((addr)->h)))

static inline int macaddr8_cmp(const macaddr8 *a1, const macaddr8 *a2) {
  if (hibits8(a1) < hibits8(a2))
    return -1;
  else if (hibits8(a1) > hibits8(a2))
    return 1;
  else if (lobits8(a1) < lobits8(a2))
    return -1;
  else if (lobits8(a1) > lobits8(a2))
    return 1;
  else
    return 0;
}

static bool MACaddr8LT(const void *l, const void *r, Oid /*collation*/) {
  return macaddr8_cmp(*(const macaddr8 **)l, *(const macaddr8 **)r) < 0;
}

static bool MACaddr8LE(const void *l, const void *r, Oid /*collation*/) {
  return macaddr8_cmp(*(const macaddr8 **)l, *(const macaddr8 **)r) <= 0;
}

static bool MACaddr8EQ(const void *l, const void *r, Oid /*collation*/) {
  return macaddr8_cmp(*(const macaddr8 **)l, *(const macaddr8 **)r) == 0;
}

static bool MACaddr8GE(const void *l, const void *r, Oid /*collation*/) {
  return macaddr8_cmp(*(const macaddr8 **)l, *(const macaddr8 **)r) >= 0;
}

static bool MACaddr8GT(const void *l, const void *r, Oid /*collation*/) {
  return macaddr8_cmp(*(const macaddr8 **)l, *(const macaddr8 **)r) > 0;
}

#undef hibits8
#undef lobits8

}  // namespace macaddrop

namespace cashop {

static bool CashLT(const void *l, const void *r, Oid /*collation*/) {
  return *(const Cash *)l < *(const Cash *)r;
}

static bool CashLE(const void *l, const void *r, Oid /*collation*/) {
  return *(const Cash *)l <= *(const Cash *)r;
}

static bool CashEQ(const void *l, const void *r, Oid /*collation*/) {
  return *(const Cash *)l == *(const Cash *)r;
}

static bool CashGE(const void *l, const void *r, Oid /*collation*/) {
  return *(const Cash *)l >= *(const Cash *)r;
}

static bool CashGT(const void *l, const void *r, Oid /*collation*/) {
  return *(const Cash *)l > *(const Cash *)r;
}

}  // namespace cashop

namespace uuidop {

static inline int uuid_internal_cmp(const pg_uuid_t *arg1,
                                    const pg_uuid_t *arg2) {
  return memcmp(arg1->data, arg2->data, UUID_LEN);
}

static bool UUIDLT(const void *l, const void *r, Oid /*collation*/) {
  return uuid_internal_cmp(*(const pg_uuid_t **)l, *(const pg_uuid_t **)r) < 0;
}

static bool UUIDLE(const void *l, const void *r, Oid /*collation*/) {
  return uuid_internal_cmp(*(const pg_uuid_t **)l, *(const pg_uuid_t **)r) <= 0;
}

static bool UUIDEQ(const void *l, const void *r, Oid /*collation*/) {
  return uuid_internal_cmp(*(const pg_uuid_t **)l, *(const pg_uuid_t **)r) == 0;
}

static bool UUIDGE(const void *l, const void *r, Oid /*collation*/) {
  return uuid_internal_cmp(*(const pg_uuid_t **)l, *(const pg_uuid_t **)r) >= 0;
}

static bool UUIDGT(const void *l, const void *r, Oid /*collation*/) {
  return uuid_internal_cmp(*(const pg_uuid_t **)l, *(const pg_uuid_t **)r) > 0;
}

}  // namespace uuidop

namespace geoop {

static inline float8 float8_mi(const float8 val1, const float8 val2) {
  float8 result;

  result = val1 - val2;
  if (unlikely(isinf(result)) && !isinf(val1) && !isinf(val2))
    CBDB_RAISE(cbdb::CException::ExType::kExTypeValueOverflow,
               "the left/right or mi(left,right) value is inf.");

  return result;
}

static inline float8 float8_mul(const float8 val1, const float8 val2) {
  float8 result;

  result = val1 * val2;
  if (unlikely(isinf(result)) && !isinf(val1) && !isinf(val2))
    CBDB_RAISE(cbdb::CException::ExType::kExTypeValueOverflow,
               "the left/right or mul(left,right) value is inf.");
  if (unlikely(result == 0.0) && val1 != 0.0 && val2 != 0.0)
    CBDB_RAISE(cbdb::CException::ExType::kExTypeValueOverflow,
               "the left/right or mul(left,right) value is 0.");

  return result;
}

static inline float8 box_wd(const BOX *box) {
  return float8_mi(box->high.x, box->low.x);
}

static inline float8 box_ht(const BOX *box) {
  return float8_mi(box->high.y, box->low.y);
}

static inline float8 box_ar(const BOX *box) {
  return float8_mul(box_wd(box), box_ht(box));
}

static inline float8 get_float8_infinity(void) {
#ifdef INFINITY
  /* C99 standard way */
  return (float8)INFINITY;
#else

  /*
   * On some platforms, HUGE_VAL is an infinity, elsewhere it's just the
   * largest normal float8.  We assume forcing an overflow will get us a
   * true infinity.
   */
  return (float8)(HUGE_VAL * HUGE_VAL);
#endif
}

static inline float8 get_float8_nan(void) {
  /* (float8) NAN doesn't work on some NetBSD/MIPS releases */
#if defined(NAN) && !(defined(__NetBSD__) && defined(__mips__))
  /* C99 standard way */
  return (float8)NAN;
#else
  /* Assume we can get a NaN via zero divide */
  return (float8)(0.0 / 0.0);
#endif
}

// Determine the hypotenuse.
//
// If required, x and y are swapped to make x the larger number. The
// traditional formula of x^2+y^2 is rearranged to factor x outside the
// sqrt. This allows computation of the hypotenuse for significantly
// larger values, and with a higher precision than when using the naive
// formula.  In particular, this cannot overflow unless the final result
// would be out-of-range.
//
// sqrt( x^2 + y^2 ) = sqrt( x^2( 1 + y^2/x^2) )
//					 = x * sqrt( 1 + y^2/x^2 )
//					 = x * sqrt( 1 + y/x * y/x )
//
// It is expected that this routine will eventually be replaced with the
// C99 hypot() function.
//
// This implementation conforms to IEEE Std 1003.1 and GLIBC, in that the
// case of hypot(inf,nan) results in INF, and not NAN.
static float8 hypot(float8 x, float8 y) {
  float8 yx, result;

  if (isinf(x) || isinf(y)) return get_float8_infinity();

  if (isnan(x) || isnan(y)) return get_float8_nan();

  /* Else, drop any minus signs */
  x = fabs(x);
  y = fabs(y);

  /* Swap x and y if needed to make x the larger one */
  if (x < y) {
    float8 temp = x;

    x = y;
    y = temp;
  }

  /*
   * If y is zero, the hypotenuse is x.  This test saves a few cycles in
   * such cases, but more importantly it also protects against
   * divide-by-zero errors, since now x >= y.
   */
  if (y == 0.0) return x;

  /* Determine the hypotenuse */
  yx = y / x;
  result = x * sqrt(1.0 + (yx * yx));

  if (unlikely(isinf(result)))
    CBDB_RAISE(cbdb::CException::ExType::kExTypeValueOverflow,
               "the hypot is inf.");
  if (unlikely(result == 0.0))
    CBDB_RAISE(cbdb::CException::ExType::kExTypeValueOverflow,
               "the hypot is 0.");

  return result;
}

static inline float8 point_dt(const Point *pt1, const Point *pt2) {
  return hypot(float8_mi(pt1->x, pt2->x), float8_mi(pt1->y, pt2->y));
}

static float8 circle_ar(const CIRCLE *circle) {
  return float8_mul(float8_mul(circle->radius, circle->radius), M_PI);
}

static bool BoxLT(const void *l, const void *r, Oid /*collation*/) {
  return FPlt(box_ar(*(const BOX **)l), box_ar(*(const BOX **)r));
}

static bool BoxLE(const void *l, const void *r, Oid /*collation*/) {
  return FPle(box_ar(*(const BOX **)l), box_ar(*(const BOX **)r));
}

static bool BoxEQ(const void *l, const void *r, Oid /*collation*/) {
  return FPeq(box_ar(*(const BOX **)l), box_ar(*(const BOX **)r));
}

static bool BoxGE(const void *l, const void *r, Oid /*collation*/) {
  return FPge(box_ar(*(const BOX **)l), box_ar(*(const BOX **)r));
}

static bool BoxGT(const void *l, const void *r, Oid /*collation*/) {
  return FPgt(box_ar(*(const BOX **)l), box_ar(*(const BOX **)r));
}

static bool LSEGLT(const void *l, const void *r, Oid /*collation*/) {
  const LSEG *l1 = *(const LSEG **)l;
  const LSEG *l2 = *(const LSEG **)r;

  return FPlt(point_dt(&l1->p[0], &l1->p[1]), point_dt(&l2->p[0], &l2->p[1]));
}

static bool LSEGLE(const void *l, const void *r, Oid /*collation*/) {
  const LSEG *l1 = *(const LSEG **)l;
  const LSEG *l2 = *(const LSEG **)r;

  return FPle(point_dt(&l1->p[0], &l1->p[1]), point_dt(&l2->p[0], &l2->p[1]));
}

static bool LSEGEQ(const void *l, const void *r, Oid /*collation*/) {
  const LSEG *l1 = *(const LSEG **)l;
  const LSEG *l2 = *(const LSEG **)r;

  return FPeq(point_dt(&l1->p[0], &l1->p[1]), point_dt(&l2->p[0], &l2->p[1]));
}

static bool LSEGGE(const void *l, const void *r, Oid /*collation*/) {
  const LSEG *l1 = *(const LSEG **)l;
  const LSEG *l2 = *(const LSEG **)r;

  return FPge(point_dt(&l1->p[0], &l1->p[1]), point_dt(&l2->p[0], &l2->p[1]));
}

static bool LSEGGT(const void *l, const void *r, Oid /*collation*/) {
  const LSEG *l1 = *(const LSEG **)l;
  const LSEG *l2 = *(const LSEG **)r;

  return FPgt(point_dt(&l1->p[0], &l1->p[1]), point_dt(&l2->p[0], &l2->p[1]));
}

static bool CircleLT(const void *l, const void *r, Oid /*collation*/) {
  return FPlt(circle_ar(*(const CIRCLE **)l), circle_ar(*(const CIRCLE **)r));
}

static bool CircleLE(const void *l, const void *r, Oid /*collation*/) {
  return FPle(circle_ar(*(const CIRCLE **)l), circle_ar(*(const CIRCLE **)r));
}

static bool CircleEQ(const void *l, const void *r, Oid /*collation*/) {
  return FPeq(circle_ar(*(const CIRCLE **)l), circle_ar(*(const CIRCLE **)r));
}

static bool CircleGE(const void *l, const void *r, Oid /*collation*/) {
  return FPge(circle_ar(*(const CIRCLE **)l), circle_ar(*(const CIRCLE **)r));
}

static bool CircleGT(const void *l, const void *r, Oid /*collation*/) {
  return FPgt(circle_ar(*(const CIRCLE **)l), circle_ar(*(const CIRCLE **)r));
}

static bool PathLT(const void *l, const void *r, Oid /*collation*/) {
  const PATH *p1 = *(const PATH **)l;
  const PATH *p2 = *(const PATH **)r;

  return p1->npts < p2->npts;
}

static bool PathLE(const void *l, const void *r, Oid /*collation*/) {
  const PATH *p1 = *(const PATH **)l;
  const PATH *p2 = *(const PATH **)r;

  return p1->npts <= p2->npts;
}

static bool PathEQ(const void *l, const void *r, Oid /*collation*/) {
  const PATH *p1 = *(const PATH **)l;
  const PATH *p2 = *(const PATH **)r;

  return p1->npts == p2->npts;
}

static bool PathGE(const void *l, const void *r, Oid /*collation*/) {
  const PATH *p1 = *(const PATH **)l;
  const PATH *p2 = *(const PATH **)r;

  return p1->npts >= p2->npts;
}

static bool PathGT(const void *l, const void *r, Oid /*collation*/) {
  const PATH *p1 = *(const PATH **)l;
  const PATH *p2 = *(const PATH **)r;

  return p1->npts > p2->npts;
}

}  // namespace geoop

#define INIT_MIN_MAX_OPER(L_OID, R_OID, SN, FUNC) \
  { {L_OID, R_OID, SN}, FUNC }

// Below SQL to get the all pg support operators
// Notice that: don't use the `oprname = '='` to filter the operators.
// select oprcode, pg_type.typname from pg_operator join pg_type on
// pg_operator.oprleft=pg_type.oid where
// pg_operator.oprleft=pg_operator.oprright and oprname = '<' order by typname;
std::map<OperMinMaxKey, OperMinMaxFunc> min_max_opers = {
    // oper(bool, bool)
    INIT_MIN_MAX_OPER(BOOLOID, BOOLOID, BTLessStrategyNumber, boolop::BoolLT),
    INIT_MIN_MAX_OPER(BOOLOID, BOOLOID, BTLessEqualStrategyNumber,
                      boolop::BoolLE),
    INIT_MIN_MAX_OPER(BOOLOID, BOOLOID, BTEqualStrategyNumber, boolop::BoolEQ),
    INIT_MIN_MAX_OPER(BOOLOID, BOOLOID, BTGreaterEqualStrategyNumber,
                      boolop::BoolGE),
    INIT_MIN_MAX_OPER(BOOLOID, BOOLOID, BTGreaterStrategyNumber,
                      boolop::BoolGT),

    // oper(char, char)
    INIT_MIN_MAX_OPER(CHAROID, CHAROID, BTLessStrategyNumber, charop::CharLT),
    INIT_MIN_MAX_OPER(CHAROID, CHAROID, BTLessEqualStrategyNumber,
                      charop::CharLE),
    INIT_MIN_MAX_OPER(CHAROID, CHAROID, BTEqualStrategyNumber, charop::CharEQ),
    INIT_MIN_MAX_OPER(CHAROID, CHAROID, BTGreaterEqualStrategyNumber,
                      charop::CharGE),
    INIT_MIN_MAX_OPER(CHAROID, CHAROID, BTGreaterStrategyNumber,
                      charop::CharGT),

    // oper(int2, int2)
    INIT_MIN_MAX_OPER(INT2OID, INT2OID, BTLessStrategyNumber, int2op::Int2LT),
    INIT_MIN_MAX_OPER(INT2OID, INT2OID, BTLessEqualStrategyNumber,
                      int2op::Int2LE),
    INIT_MIN_MAX_OPER(INT2OID, INT2OID, BTEqualStrategyNumber, int2op::Int2EQ),
    INIT_MIN_MAX_OPER(INT2OID, INT2OID, BTGreaterEqualStrategyNumber,
                      int2op::Int2GE),
    INIT_MIN_MAX_OPER(INT2OID, INT2OID, BTGreaterStrategyNumber,
                      int2op::Int2GT),

    // oper(int2, int4)
    INIT_MIN_MAX_OPER(INT2OID, INT4OID, BTLessStrategyNumber, int2op::Int24LT),
    INIT_MIN_MAX_OPER(INT2OID, INT4OID, BTLessEqualStrategyNumber,
                      int2op::Int24LE),
    INIT_MIN_MAX_OPER(INT2OID, INT4OID, BTEqualStrategyNumber, int2op::Int24EQ),
    INIT_MIN_MAX_OPER(INT2OID, INT4OID, BTGreaterEqualStrategyNumber,
                      int2op::Int24GE),
    INIT_MIN_MAX_OPER(INT2OID, INT4OID, BTGreaterStrategyNumber,
                      int2op::Int24GT),

    // oper(int2, int8)
    INIT_MIN_MAX_OPER(INT2OID, INT8OID, BTLessStrategyNumber, int2op::Int28LT),
    INIT_MIN_MAX_OPER(INT2OID, INT8OID, BTLessEqualStrategyNumber,
                      int2op::Int28LE),
    INIT_MIN_MAX_OPER(INT2OID, INT8OID, BTEqualStrategyNumber, int2op::Int28EQ),
    INIT_MIN_MAX_OPER(INT2OID, INT8OID, BTGreaterEqualStrategyNumber,
                      int2op::Int28GE),
    INIT_MIN_MAX_OPER(INT2OID, INT8OID, BTGreaterStrategyNumber,
                      int2op::Int28GT),

    // oper(int4, int4)
    INIT_MIN_MAX_OPER(INT4OID, INT4OID, BTLessStrategyNumber, int4op::Int4LT),
    INIT_MIN_MAX_OPER(INT4OID, INT4OID, BTLessEqualStrategyNumber,
                      int4op::Int4LE),
    INIT_MIN_MAX_OPER(INT4OID, INT4OID, BTEqualStrategyNumber, int4op::Int4EQ),
    INIT_MIN_MAX_OPER(INT4OID, INT4OID, BTGreaterEqualStrategyNumber,
                      int4op::Int4GE),
    INIT_MIN_MAX_OPER(INT4OID, INT4OID, BTGreaterStrategyNumber,
                      int4op::Int4GT),

    // oper(int4, int8)
    INIT_MIN_MAX_OPER(INT4OID, INT8OID, BTLessStrategyNumber, int4op::Int48LT),
    INIT_MIN_MAX_OPER(INT4OID, INT8OID, BTLessEqualStrategyNumber,
                      int4op::Int48LE),
    INIT_MIN_MAX_OPER(INT4OID, INT8OID, BTEqualStrategyNumber, int4op::Int48EQ),
    INIT_MIN_MAX_OPER(INT4OID, INT8OID, BTGreaterEqualStrategyNumber,
                      int4op::Int48GE),
    INIT_MIN_MAX_OPER(INT4OID, INT8OID, BTGreaterStrategyNumber,
                      int4op::Int48GT),

    // oper(int4, int2)
    INIT_MIN_MAX_OPER(INT4OID, INT2OID, BTLessStrategyNumber, int4op::Int42LT),
    INIT_MIN_MAX_OPER(INT4OID, INT2OID, BTLessEqualStrategyNumber,
                      int4op::Int42LE),
    INIT_MIN_MAX_OPER(INT4OID, INT2OID, BTEqualStrategyNumber, int4op::Int42EQ),
    INIT_MIN_MAX_OPER(INT4OID, INT2OID, BTGreaterEqualStrategyNumber,
                      int4op::Int42GE),
    INIT_MIN_MAX_OPER(INT4OID, INT2OID, BTGreaterStrategyNumber,
                      int4op::Int42GT),

    // oper(int8, int8)
    INIT_MIN_MAX_OPER(INT8OID, INT8OID, BTLessStrategyNumber, int8op::Int8LT),
    INIT_MIN_MAX_OPER(INT8OID, INT8OID, BTLessEqualStrategyNumber,
                      int8op::Int8LE),
    INIT_MIN_MAX_OPER(INT8OID, INT8OID, BTEqualStrategyNumber, int8op::Int8EQ),
    INIT_MIN_MAX_OPER(INT8OID, INT8OID, BTGreaterEqualStrategyNumber,
                      int8op::Int8GE),
    INIT_MIN_MAX_OPER(INT8OID, INT8OID, BTGreaterStrategyNumber,
                      int8op::Int8GT),

    // oper(int8, int4)
    INIT_MIN_MAX_OPER(INT8OID, INT4OID, BTLessStrategyNumber, int8op::Int84LT),
    INIT_MIN_MAX_OPER(INT8OID, INT4OID, BTLessEqualStrategyNumber,
                      int8op::Int84LE),
    INIT_MIN_MAX_OPER(INT8OID, INT4OID, BTEqualStrategyNumber, int8op::Int84EQ),
    INIT_MIN_MAX_OPER(INT8OID, INT4OID, BTGreaterEqualStrategyNumber,
                      int8op::Int84GE),
    INIT_MIN_MAX_OPER(INT8OID, INT4OID, BTGreaterStrategyNumber,
                      int8op::Int84GT),

    // oper(int8, int2)
    INIT_MIN_MAX_OPER(INT8OID, INT2OID, BTLessStrategyNumber, int8op::Int82LT),
    INIT_MIN_MAX_OPER(INT8OID, INT2OID, BTLessEqualStrategyNumber,
                      int8op::Int82LE),
    INIT_MIN_MAX_OPER(INT8OID, INT2OID, BTEqualStrategyNumber, int8op::Int82EQ),
    INIT_MIN_MAX_OPER(INT8OID, INT2OID, BTGreaterEqualStrategyNumber,
                      int8op::Int82GE),
    INIT_MIN_MAX_OPER(INT8OID, INT2OID, BTGreaterStrategyNumber,
                      int8op::Int82GT),

    // oper(numeric, numeric)
    INIT_MIN_MAX_OPER(NUMERICOID, NUMERICOID, BTLessStrategyNumber,
                      numericop::NumericLT),
    INIT_MIN_MAX_OPER(NUMERICOID, NUMERICOID, BTLessEqualStrategyNumber,
                      numericop::NumericLE),
    INIT_MIN_MAX_OPER(NUMERICOID, NUMERICOID, BTEqualStrategyNumber,
                      numericop::NumericEQ),
    INIT_MIN_MAX_OPER(NUMERICOID, NUMERICOID, BTGreaterEqualStrategyNumber,
                      numericop::NumericGE),
    INIT_MIN_MAX_OPER(NUMERICOID, NUMERICOID, BTGreaterStrategyNumber,
                      numericop::NumericGT),

    // oper(float4, float4)
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT4OID, BTLessStrategyNumber,
                      float4op::Float4LT),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT4OID, BTLessEqualStrategyNumber,
                      float4op::Float4LE),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT4OID, BTEqualStrategyNumber,
                      float4op::Float4EQ),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT4OID, BTGreaterEqualStrategyNumber,
                      float4op::Float4GE),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT4OID, BTGreaterStrategyNumber,
                      float4op::Float4GT),

    // oper(float4, float8)
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT8OID, BTLessStrategyNumber,
                      float4op::Float48LT),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT8OID, BTLessEqualStrategyNumber,
                      float4op::Float48LE),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT8OID, BTEqualStrategyNumber,
                      float4op::Float48EQ),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT8OID, BTGreaterEqualStrategyNumber,
                      float4op::Float48GE),
    INIT_MIN_MAX_OPER(FLOAT4OID, FLOAT8OID, BTGreaterStrategyNumber,
                      float4op::Float48GT),

    // oper(float8, float8)
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT8OID, BTLessStrategyNumber,
                      float8op::Float8LT),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT8OID, BTLessEqualStrategyNumber,
                      float8op::Float8LE),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT8OID, BTEqualStrategyNumber,
                      float8op::Float8EQ),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT8OID, BTGreaterEqualStrategyNumber,
                      float8op::Float8GE),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT8OID, BTGreaterStrategyNumber,
                      float8op::Float8GT),

    // oper(float8, float4)
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT4OID, BTLessStrategyNumber,
                      float8op::Float84LT),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT4OID, BTLessEqualStrategyNumber,
                      float8op::Float84LE),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT4OID, BTEqualStrategyNumber,
                      float8op::Float84EQ),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT4OID, BTGreaterEqualStrategyNumber,
                      float8op::Float84GE),
    INIT_MIN_MAX_OPER(FLOAT8OID, FLOAT4OID, BTGreaterStrategyNumber,
                      float8op::Float84GT),

    // oper(text, text)
    INIT_MIN_MAX_OPER(TEXTOID, TEXTOID, BTLessStrategyNumber, textop::TextLT),
    INIT_MIN_MAX_OPER(TEXTOID, TEXTOID, BTLessEqualStrategyNumber,
                      textop::TextLE),
    INIT_MIN_MAX_OPER(TEXTOID, TEXTOID, BTEqualStrategyNumber, textop::TextEQ),
    INIT_MIN_MAX_OPER(TEXTOID, TEXTOID, BTGreaterEqualStrategyNumber,
                      textop::TextGE),
    INIT_MIN_MAX_OPER(TEXTOID, TEXTOID, BTGreaterStrategyNumber,
                      textop::TextGT),

    // oper(text, varchar)
    INIT_MIN_MAX_OPER(TEXTOID, VARCHAROID, BTLessStrategyNumber,
                      textop::TextLT),
    INIT_MIN_MAX_OPER(TEXTOID, VARCHAROID, BTLessEqualStrategyNumber,
                      textop::TextLE),
    INIT_MIN_MAX_OPER(TEXTOID, VARCHAROID, BTEqualStrategyNumber,
                      textop::TextEQ),
    INIT_MIN_MAX_OPER(TEXTOID, VARCHAROID, BTGreaterEqualStrategyNumber,
                      textop::TextGE),
    INIT_MIN_MAX_OPER(TEXTOID, VARCHAROID, BTGreaterStrategyNumber,
                      textop::TextGT),

    // oper(text, bpchar)
    INIT_MIN_MAX_OPER(TEXTOID, BPCHAROID, BTLessStrategyNumber,
                      textop::TextBpcharLT),
    INIT_MIN_MAX_OPER(TEXTOID, BPCHAROID, BTLessEqualStrategyNumber,
                      textop::TextBpcharLE),
    INIT_MIN_MAX_OPER(TEXTOID, BPCHAROID, BTEqualStrategyNumber,
                      textop::TextBpcharEQ),
    INIT_MIN_MAX_OPER(TEXTOID, BPCHAROID, BTGreaterEqualStrategyNumber,
                      textop::TextBpcharGE),
    INIT_MIN_MAX_OPER(TEXTOID, BPCHAROID, BTGreaterStrategyNumber,
                      textop::TextBpcharGT),

    // oper(bpchar, bpchar)
    INIT_MIN_MAX_OPER(BPCHAROID, BPCHAROID, BTLessStrategyNumber,
                      textop::BpCharLT),
    INIT_MIN_MAX_OPER(BPCHAROID, BPCHAROID, BTLessEqualStrategyNumber,
                      textop::BpCharLE),
    INIT_MIN_MAX_OPER(BPCHAROID, BPCHAROID, BTEqualStrategyNumber,
                      textop::BpCharEQ),
    INIT_MIN_MAX_OPER(BPCHAROID, BPCHAROID, BTGreaterEqualStrategyNumber,
                      textop::BpCharGE),
    INIT_MIN_MAX_OPER(BPCHAROID, BPCHAROID, BTGreaterStrategyNumber,
                      textop::BpCharGT),
    
    // oper(bpchar, text)
    INIT_MIN_MAX_OPER(BPCHAROID, TEXTOID, BTLessStrategyNumber,
                      textop::BpcharTextLT),
    INIT_MIN_MAX_OPER(BPCHAROID, TEXTOID, BTLessEqualStrategyNumber,
                      textop::BpcharTextLE),
    INIT_MIN_MAX_OPER(BPCHAROID, TEXTOID, BTEqualStrategyNumber,
                      textop::BpcharTextEQ),
    INIT_MIN_MAX_OPER(BPCHAROID, TEXTOID, BTGreaterEqualStrategyNumber,
                      textop::BpcharTextGE),
    INIT_MIN_MAX_OPER(BPCHAROID, TEXTOID, BTGreaterStrategyNumber,
                      textop::BpcharTextGT),

    // oper(bpchar, varchar)
    INIT_MIN_MAX_OPER(BPCHAROID, VARCHAROID, BTLessStrategyNumber,
                      textop::BpcharTextLT),
    INIT_MIN_MAX_OPER(BPCHAROID, VARCHAROID, BTLessEqualStrategyNumber,
                      textop::BpcharTextLE),
    INIT_MIN_MAX_OPER(BPCHAROID, VARCHAROID, BTEqualStrategyNumber,
                      textop::BpcharTextEQ),
    INIT_MIN_MAX_OPER(BPCHAROID, VARCHAROID, BTGreaterEqualStrategyNumber,
                      textop::BpcharTextGE),
    INIT_MIN_MAX_OPER(BPCHAROID, VARCHAROID, BTGreaterStrategyNumber,
                      textop::BpcharTextGT),

    // oper(varchar, varchar)
    INIT_MIN_MAX_OPER(VARCHAROID, VARCHAROID, BTLessStrategyNumber,
                      textop::TextLT),
    INIT_MIN_MAX_OPER(VARCHAROID, VARCHAROID, BTLessEqualStrategyNumber,
                      textop::TextLE),
    INIT_MIN_MAX_OPER(VARCHAROID, VARCHAROID, BTEqualStrategyNumber,
                      textop::TextEQ),
    INIT_MIN_MAX_OPER(VARCHAROID, VARCHAROID, BTGreaterEqualStrategyNumber,
                      textop::TextGE),
    INIT_MIN_MAX_OPER(VARCHAROID, VARCHAROID, BTGreaterStrategyNumber,
                      textop::TextGT),

    // oper(varchar, text)
    INIT_MIN_MAX_OPER(VARCHAROID, TEXTOID, BTLessStrategyNumber,
                      textop::TextLT),
    INIT_MIN_MAX_OPER(VARCHAROID, TEXTOID, BTLessEqualStrategyNumber,
                      textop::TextLE),
    INIT_MIN_MAX_OPER(VARCHAROID, TEXTOID, BTEqualStrategyNumber,
                      textop::TextEQ),
    INIT_MIN_MAX_OPER(VARCHAROID, TEXTOID, BTGreaterEqualStrategyNumber,
                      textop::TextGE),
    INIT_MIN_MAX_OPER(VARCHAROID, TEXTOID, BTGreaterStrategyNumber,
                      textop::TextGT),

    // oper(varchar, bpchar)
    INIT_MIN_MAX_OPER(VARCHAROID, BPCHAROID, BTLessStrategyNumber,
                      textop::TextBpcharLT),
    INIT_MIN_MAX_OPER(VARCHAROID, BPCHAROID, BTLessEqualStrategyNumber,
                      textop::TextBpcharLE),
    INIT_MIN_MAX_OPER(VARCHAROID, BPCHAROID, BTEqualStrategyNumber,
                      textop::TextBpcharEQ),
    INIT_MIN_MAX_OPER(VARCHAROID, BPCHAROID, BTGreaterEqualStrategyNumber,
                      textop::TextBpcharGE),
    INIT_MIN_MAX_OPER(VARCHAROID, BPCHAROID, BTGreaterStrategyNumber,
                      textop::TextBpcharGT),

    // oper(name,name)
    INIT_MIN_MAX_OPER(NAMEOID, NAMEOID, BTLessStrategyNumber, textop::NameLT),
    INIT_MIN_MAX_OPER(NAMEOID, NAMEOID, BTLessEqualStrategyNumber,
                      textop::NameLE),
    INIT_MIN_MAX_OPER(NAMEOID, NAMEOID, BTEqualStrategyNumber, textop::NameEQ),
    INIT_MIN_MAX_OPER(NAMEOID, NAMEOID, BTGreaterEqualStrategyNumber,
                      textop::NameGE),
    INIT_MIN_MAX_OPER(NAMEOID, NAMEOID, BTGreaterStrategyNumber,
                      textop::NameGT),

    // oper(date, date)
    INIT_MIN_MAX_OPER(DATEOID, DATEOID, BTLessStrategyNumber, timeop::DateLT),
    INIT_MIN_MAX_OPER(DATEOID, DATEOID, BTLessEqualStrategyNumber,
                      timeop::DateLE),
    INIT_MIN_MAX_OPER(DATEOID, DATEOID, BTEqualStrategyNumber, timeop::DateEQ),
    INIT_MIN_MAX_OPER(DATEOID, DATEOID, BTGreaterEqualStrategyNumber,
                      timeop::DateGE),
    INIT_MIN_MAX_OPER(DATEOID, DATEOID, BTGreaterStrategyNumber,
                      timeop::DateGT),

    // oper(date, timestamp)
    INIT_MIN_MAX_OPER(DATEOID, TIMESTAMPOID, BTLessStrategyNumber, timeop::DateTimestampLT),
    INIT_MIN_MAX_OPER(DATEOID, TIMESTAMPOID, BTLessEqualStrategyNumber,
                      timeop::DateTimestampLE),
    INIT_MIN_MAX_OPER(DATEOID, TIMESTAMPOID, BTEqualStrategyNumber, timeop::DateTimestampEQ),
    INIT_MIN_MAX_OPER(DATEOID, TIMESTAMPOID, BTGreaterEqualStrategyNumber,
                      timeop::DateTimestampGE),
    INIT_MIN_MAX_OPER(DATEOID, TIMESTAMPOID, BTGreaterStrategyNumber,
                      timeop::DateTimestampGT),

    // oper(date, timestamptz)
    INIT_MIN_MAX_OPER(DATEOID, TIMESTAMPTZOID, BTLessStrategyNumber, timeop::DateTimestampTzLT),
    INIT_MIN_MAX_OPER(DATEOID, TIMESTAMPTZOID, BTLessEqualStrategyNumber,
                      timeop::DateTimestampTzLE),
    INIT_MIN_MAX_OPER(DATEOID, TIMESTAMPTZOID, BTEqualStrategyNumber, timeop::DateTimestampTzEQ),
    INIT_MIN_MAX_OPER(DATEOID, TIMESTAMPTZOID, BTGreaterEqualStrategyNumber,
                      timeop::DateTimestampTzGE),
    INIT_MIN_MAX_OPER(DATEOID, TIMESTAMPTZOID, BTGreaterStrategyNumber,
                      timeop::DateTimestampTzGT),

    // oper(timestamp,timestamp)
    INIT_MIN_MAX_OPER(TIMESTAMPOID, TIMESTAMPOID, BTLessStrategyNumber,
                      timeop::TimeStampLT),
    INIT_MIN_MAX_OPER(TIMESTAMPOID, TIMESTAMPOID, BTLessEqualStrategyNumber,
                      timeop::TimeStampLE),
    INIT_MIN_MAX_OPER(TIMESTAMPOID, TIMESTAMPOID, BTEqualStrategyNumber,
                      timeop::TimeStampEQ),
    INIT_MIN_MAX_OPER(TIMESTAMPOID, TIMESTAMPOID, BTGreaterEqualStrategyNumber,
                      timeop::TimeStampGE),
    INIT_MIN_MAX_OPER(TIMESTAMPOID, TIMESTAMPOID, BTGreaterStrategyNumber,
                      timeop::TimeStampGT),

    // oper(timestamp,timestamptz)
    INIT_MIN_MAX_OPER(TIMESTAMPOID, TIMESTAMPTZOID, BTLessStrategyNumber,
                      timeop::TimeStampTimeStampTzLT),
    INIT_MIN_MAX_OPER(TIMESTAMPOID, TIMESTAMPTZOID, BTLessEqualStrategyNumber,
                      timeop::TimeStampTimeStampTzLE),
    INIT_MIN_MAX_OPER(TIMESTAMPOID, TIMESTAMPTZOID, BTEqualStrategyNumber,
                      timeop::TimeStampTimeStampTzEQ),
    INIT_MIN_MAX_OPER(TIMESTAMPOID, TIMESTAMPTZOID, BTGreaterEqualStrategyNumber,
                      timeop::TimeStampTimeStampTzGE),
    INIT_MIN_MAX_OPER(TIMESTAMPOID, TIMESTAMPTZOID, BTGreaterStrategyNumber,
                      timeop::TimeStampTimeStampTzGT),

    // oper(timestamp,date)
    INIT_MIN_MAX_OPER(TIMESTAMPOID, DATEOID, BTLessStrategyNumber,
                      timeop::TimeStampDateLT),
    INIT_MIN_MAX_OPER(TIMESTAMPOID, DATEOID, BTLessEqualStrategyNumber,
                      timeop::TimeStampDateLE),
    INIT_MIN_MAX_OPER(TIMESTAMPOID, DATEOID, BTEqualStrategyNumber,
                      timeop::TimeStampDateEQ),
    INIT_MIN_MAX_OPER(TIMESTAMPOID, DATEOID, BTGreaterEqualStrategyNumber,
                      timeop::TimeStampDateGE),
    INIT_MIN_MAX_OPER(TIMESTAMPOID, DATEOID, BTGreaterStrategyNumber,
                      timeop::TimeStampDateGT),

    // oper(timestamptzoid,timestamptzoid)
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, TIMESTAMPTZOID, BTLessStrategyNumber,
                      timeop::TimeStampLT),
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, TIMESTAMPTZOID, BTLessEqualStrategyNumber,
                      timeop::TimeStampLE),
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, TIMESTAMPTZOID, BTEqualStrategyNumber,
                      timeop::TimeStampEQ),
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, TIMESTAMPTZOID,
                      BTGreaterEqualStrategyNumber, timeop::TimeStampGE),
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, TIMESTAMPTZOID, BTGreaterStrategyNumber,
                      timeop::TimeStampGT),

    // oper(timestamptzoid,timestampoid)
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, TIMESTAMPOID, BTLessStrategyNumber,
                      timeop::TimeStampTzTimeStampLT),
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, TIMESTAMPOID, BTLessEqualStrategyNumber,
                      timeop::TimeStampTzTimeStampLE),
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, TIMESTAMPOID, BTEqualStrategyNumber,
                      timeop::TimeStampTzTimeStampEQ),
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, TIMESTAMPOID,
                      BTGreaterEqualStrategyNumber, timeop::TimeStampTzTimeStampGE),
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, TIMESTAMPOID, BTGreaterStrategyNumber,
                      timeop::TimeStampTzTimeStampGT),

    // oper(timestamptzoid,dateoid)
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, DATEOID, BTLessStrategyNumber,
                      timeop::TimeStampTzDateLT),
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, DATEOID, BTLessEqualStrategyNumber,
                      timeop::TimeStampTzDateLE),
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, DATEOID, BTEqualStrategyNumber,
                      timeop::TimeStampTzDateEQ),
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, DATEOID,
                      BTGreaterEqualStrategyNumber, timeop::TimeStampTzDateGE),
    INIT_MIN_MAX_OPER(TIMESTAMPTZOID, DATEOID, BTGreaterStrategyNumber,
                      timeop::TimeStampTzDateGT),

    // oper(time,time)
    INIT_MIN_MAX_OPER(TIMEOID, TIMEOID, BTLessStrategyNumber, timeop::TimeLT),
    INIT_MIN_MAX_OPER(TIMEOID, TIMEOID, BTLessEqualStrategyNumber,
                      timeop::TimeLE),
    INIT_MIN_MAX_OPER(TIMEOID, TIMEOID, BTEqualStrategyNumber, timeop::TimeEQ),
    INIT_MIN_MAX_OPER(TIMEOID, TIMEOID, BTGreaterEqualStrategyNumber,
                      timeop::TimeGE),
    INIT_MIN_MAX_OPER(TIMEOID, TIMEOID, BTGreaterStrategyNumber,
                      timeop::TimeGT),

    // oper(time,timetz)
    INIT_MIN_MAX_OPER(TIMEOID, TIMETZOID, BTLessStrategyNumber, timeop::TimeTzLT),
    INIT_MIN_MAX_OPER(TIMEOID, TIMETZOID, BTLessEqualStrategyNumber,
                      timeop::TimeTzLE),
    INIT_MIN_MAX_OPER(TIMEOID, TIMETZOID, BTEqualStrategyNumber, timeop::TimeTzEQ),
    INIT_MIN_MAX_OPER(TIMEOID, TIMETZOID, BTGreaterEqualStrategyNumber,
                      timeop::TimeTzGE),
    INIT_MIN_MAX_OPER(TIMEOID, TIMETZOID, BTGreaterStrategyNumber,
                      timeop::TimeTzGT),

    // oper(timetz,timetz)
    INIT_MIN_MAX_OPER(TIMETZOID, TIMETZOID, BTLessStrategyNumber,
                      timeop::TimeTzLT),
    INIT_MIN_MAX_OPER(TIMETZOID, TIMETZOID, BTLessEqualStrategyNumber,
                      timeop::TimeTzLE),
    INIT_MIN_MAX_OPER(TIMETZOID, TIMETZOID, BTEqualStrategyNumber,
                      timeop::TimeTzEQ),
    INIT_MIN_MAX_OPER(TIMETZOID, TIMETZOID, BTGreaterEqualStrategyNumber,
                      timeop::TimeTzGE),
    INIT_MIN_MAX_OPER(TIMETZOID, TIMETZOID, BTGreaterStrategyNumber,
                      timeop::TimeTzGT),

    // oper(timetz, time)
    INIT_MIN_MAX_OPER(TIMETZOID, TIMEOID, BTLessStrategyNumber,
                      timeop::TimeTzLT),
    INIT_MIN_MAX_OPER(TIMETZOID, TIMEOID, BTLessEqualStrategyNumber,
                      timeop::TimeTzLE),
    INIT_MIN_MAX_OPER(TIMETZOID, TIMEOID, BTEqualStrategyNumber,
                      timeop::TimeTzEQ),
    INIT_MIN_MAX_OPER(TIMETZOID, TIMEOID, BTGreaterEqualStrategyNumber,
                      timeop::TimeTzGE),
    INIT_MIN_MAX_OPER(TIMETZOID, TIMEOID, BTGreaterStrategyNumber,
                      timeop::TimeTzGT),

    // oper(intervaloid,intervaloid)
    INIT_MIN_MAX_OPER(INTERVALOID, INTERVALOID, BTLessStrategyNumber,
                      intervalop::IntervalLT),
    INIT_MIN_MAX_OPER(INTERVALOID, INTERVALOID, BTLessEqualStrategyNumber,
                      intervalop::IntervalLE),
    INIT_MIN_MAX_OPER(INTERVALOID, INTERVALOID, BTEqualStrategyNumber,
                      intervalop::IntervalEQ),
    INIT_MIN_MAX_OPER(INTERVALOID, INTERVALOID, BTGreaterEqualStrategyNumber,
                      intervalop::IntervalGE),
    INIT_MIN_MAX_OPER(INTERVALOID, INTERVALOID, BTGreaterStrategyNumber,
                      intervalop::IntervalGT),

    // oper(bitoid,bitoid)
    INIT_MIN_MAX_OPER(BITOID, BITOID, BTLessStrategyNumber, bitop::BitLT),
    INIT_MIN_MAX_OPER(BITOID, BITOID, BTLessEqualStrategyNumber, bitop::BitLE),
    INIT_MIN_MAX_OPER(BITOID, BITOID, BTEqualStrategyNumber, bitop::BitEQ),
    INIT_MIN_MAX_OPER(BITOID, BITOID, BTGreaterEqualStrategyNumber,
                      bitop::BitGE),
    INIT_MIN_MAX_OPER(BITOID, BITOID, BTGreaterStrategyNumber, bitop::BitGT),

    // oper(bitoid, varbitoid)
    INIT_MIN_MAX_OPER(BITOID, VARBITOID, BTLessStrategyNumber, bitop::BitLT),
    INIT_MIN_MAX_OPER(BITOID, VARBITOID, BTLessEqualStrategyNumber,
                      bitop::BitLE),
    INIT_MIN_MAX_OPER(BITOID, VARBITOID, BTEqualStrategyNumber,
                      bitop::BitEQ),
    INIT_MIN_MAX_OPER(BITOID, VARBITOID, BTGreaterEqualStrategyNumber,
                      bitop::BitGE),
    INIT_MIN_MAX_OPER(BITOID, VARBITOID, BTGreaterStrategyNumber,
                      bitop::BitGT),

    // oper(varbitoid,varbitoid)
    INIT_MIN_MAX_OPER(VARBITOID, VARBITOID, BTLessStrategyNumber, bitop::BitLT),
    INIT_MIN_MAX_OPER(VARBITOID, VARBITOID, BTLessEqualStrategyNumber,
                      bitop::BitLE),
    INIT_MIN_MAX_OPER(VARBITOID, VARBITOID, BTEqualStrategyNumber,
                      bitop::BitEQ),
    INIT_MIN_MAX_OPER(VARBITOID, VARBITOID, BTGreaterEqualStrategyNumber,
                      bitop::BitGE),
    INIT_MIN_MAX_OPER(VARBITOID, VARBITOID, BTGreaterStrategyNumber,
                      bitop::BitGT),

    // oper(varbitoid,bitoid)
    INIT_MIN_MAX_OPER(VARBITOID, BITOID, BTLessStrategyNumber, bitop::BitLT),
    INIT_MIN_MAX_OPER(VARBITOID, BITOID, BTLessEqualStrategyNumber,
                      bitop::BitLE),
    INIT_MIN_MAX_OPER(VARBITOID, BITOID, BTEqualStrategyNumber,
                      bitop::BitEQ),
    INIT_MIN_MAX_OPER(VARBITOID, BITOID, BTGreaterEqualStrategyNumber,
                      bitop::BitGE),
    INIT_MIN_MAX_OPER(VARBITOID, BITOID, BTGreaterStrategyNumber,
                      bitop::BitGT),

    // oper(byteaoid,byteaoid)
    INIT_MIN_MAX_OPER(BYTEAOID, BYTEAOID, BTLessStrategyNumber,
                      byteaop::ByteaLT),
    INIT_MIN_MAX_OPER(BYTEAOID, BYTEAOID, BTLessEqualStrategyNumber,
                      byteaop::ByteaLE),
    INIT_MIN_MAX_OPER(BYTEAOID, BYTEAOID, BTEqualStrategyNumber,
                      byteaop::ByteaEQ),
    INIT_MIN_MAX_OPER(BYTEAOID, BYTEAOID, BTGreaterEqualStrategyNumber,
                      byteaop::ByteaGE),
    INIT_MIN_MAX_OPER(BYTEAOID, BYTEAOID, BTGreaterStrategyNumber,
                      byteaop::ByteaGT),

    // oper(oidoid,oidoid)
    INIT_MIN_MAX_OPER(OIDOID, OIDOID, BTLessStrategyNumber, oidop::OidLT),
    INIT_MIN_MAX_OPER(OIDOID, OIDOID, BTLessEqualStrategyNumber, oidop::OidLE),
    INIT_MIN_MAX_OPER(OIDOID, OIDOID, BTEqualStrategyNumber, oidop::OidEQ),
    INIT_MIN_MAX_OPER(OIDOID, OIDOID, BTGreaterEqualStrategyNumber,
                      oidop::OidGE),
    INIT_MIN_MAX_OPER(OIDOID, OIDOID, BTGreaterStrategyNumber, oidop::OidGT),

    // oper(inetoid,inetoid)
    INIT_MIN_MAX_OPER(INETOID, INETOID, BTLessStrategyNumber, inetop::InetLT),
    INIT_MIN_MAX_OPER(INETOID, INETOID, BTLessEqualStrategyNumber,
                      inetop::InetLE),
    INIT_MIN_MAX_OPER(INETOID, INETOID, BTEqualStrategyNumber, inetop::InetEQ),
    INIT_MIN_MAX_OPER(INETOID, INETOID, BTGreaterEqualStrategyNumber,
                      inetop::InetGE),
    INIT_MIN_MAX_OPER(INETOID, INETOID, BTGreaterStrategyNumber,
                      inetop::InetGT),

    // oper(macaddroid,macaddroid)
    INIT_MIN_MAX_OPER(MACADDROID, MACADDROID, BTLessStrategyNumber,
                      macaddrop::MACaddrLT),
    INIT_MIN_MAX_OPER(MACADDROID, MACADDROID, BTLessEqualStrategyNumber,
                      macaddrop::MACaddrLE),
    INIT_MIN_MAX_OPER(MACADDROID, MACADDROID, BTEqualStrategyNumber,
                      macaddrop::MACaddrEQ),
    INIT_MIN_MAX_OPER(MACADDROID, MACADDROID, BTGreaterEqualStrategyNumber,
                      macaddrop::MACaddrGE),
    INIT_MIN_MAX_OPER(MACADDROID, MACADDROID, BTGreaterStrategyNumber,
                      macaddrop::MACaddrGT),

    // oper(macaddr8oid,macaddr8oid)
    INIT_MIN_MAX_OPER(MACADDR8OID, MACADDR8OID, BTLessStrategyNumber,
                      macaddrop::MACaddr8LT),
    INIT_MIN_MAX_OPER(MACADDR8OID, MACADDR8OID, BTLessEqualStrategyNumber,
                      macaddrop::MACaddr8LE),
    INIT_MIN_MAX_OPER(MACADDR8OID, MACADDR8OID, BTEqualStrategyNumber,
                      macaddrop::MACaddr8EQ),
    INIT_MIN_MAX_OPER(MACADDR8OID, MACADDR8OID, BTGreaterEqualStrategyNumber,
                      macaddrop::MACaddr8GE),
    INIT_MIN_MAX_OPER(MACADDR8OID, MACADDR8OID, BTGreaterStrategyNumber,
                      macaddrop::MACaddr8GT),

    // oper(cashoid,cashoid)
    INIT_MIN_MAX_OPER(CASHOID, CASHOID, BTLessStrategyNumber, cashop::CashLT),
    INIT_MIN_MAX_OPER(CASHOID, CASHOID, BTLessEqualStrategyNumber,
                      cashop::CashLE),
    INIT_MIN_MAX_OPER(CASHOID, CASHOID, BTEqualStrategyNumber, cashop::CashEQ),
    INIT_MIN_MAX_OPER(CASHOID, CASHOID, BTGreaterEqualStrategyNumber,
                      cashop::CashGE),
    INIT_MIN_MAX_OPER(CASHOID, CASHOID, BTGreaterStrategyNumber,
                      cashop::CashGT),

    // oper(uuidoid,uuidoid)
    INIT_MIN_MAX_OPER(UUIDOID, UUIDOID, BTLessStrategyNumber, uuidop::UUIDLT),
    INIT_MIN_MAX_OPER(UUIDOID, UUIDOID, BTLessEqualStrategyNumber,
                      uuidop::UUIDLE),
    INIT_MIN_MAX_OPER(UUIDOID, UUIDOID, BTEqualStrategyNumber, uuidop::UUIDEQ),
    INIT_MIN_MAX_OPER(UUIDOID, UUIDOID, BTGreaterEqualStrategyNumber,
                      uuidop::UUIDGE),
    INIT_MIN_MAX_OPER(UUIDOID, UUIDOID, BTGreaterStrategyNumber,
                      uuidop::UUIDGT),

    // oper(boxoid,boxoid)
    INIT_MIN_MAX_OPER(BOXOID, BOXOID, BTLessStrategyNumber, geoop::BoxLT),
    INIT_MIN_MAX_OPER(BOXOID, BOXOID, BTLessEqualStrategyNumber, geoop::BoxLE),
    INIT_MIN_MAX_OPER(BOXOID, BOXOID, BTEqualStrategyNumber, geoop::BoxEQ),
    INIT_MIN_MAX_OPER(BOXOID, BOXOID, BTGreaterEqualStrategyNumber,
                      geoop::BoxGE),
    INIT_MIN_MAX_OPER(BOXOID, BOXOID, BTGreaterStrategyNumber, geoop::BoxGT),

    // oper(lsegoid,lsegoid)
    INIT_MIN_MAX_OPER(LSEGOID, LSEGOID, BTLessStrategyNumber, geoop::LSEGLT),
    INIT_MIN_MAX_OPER(LSEGOID, LSEGOID, BTLessEqualStrategyNumber,
                      geoop::LSEGLE),
    INIT_MIN_MAX_OPER(LSEGOID, LSEGOID, BTEqualStrategyNumber, geoop::LSEGEQ),
    INIT_MIN_MAX_OPER(LSEGOID, LSEGOID, BTGreaterEqualStrategyNumber,
                      geoop::LSEGGE),
    INIT_MIN_MAX_OPER(LSEGOID, LSEGOID, BTGreaterStrategyNumber, geoop::LSEGGT),

    // oper(circleoid,circleoid)
    INIT_MIN_MAX_OPER(CIRCLEOID, CIRCLEOID, BTLessStrategyNumber,
                      geoop::CircleLT),
    INIT_MIN_MAX_OPER(CIRCLEOID, CIRCLEOID, BTLessEqualStrategyNumber,
                      geoop::CircleLE),
    INIT_MIN_MAX_OPER(CIRCLEOID, CIRCLEOID, BTEqualStrategyNumber,
                      geoop::CircleEQ),
    INIT_MIN_MAX_OPER(CIRCLEOID, CIRCLEOID, BTGreaterEqualStrategyNumber,
                      geoop::CircleGE),
    INIT_MIN_MAX_OPER(CIRCLEOID, CIRCLEOID, BTGreaterStrategyNumber,
                      geoop::CircleGT),

    // oper(pathoid,pathoid)
    INIT_MIN_MAX_OPER(PATHOID, PATHOID, BTLessStrategyNumber, geoop::PathLT),
    INIT_MIN_MAX_OPER(PATHOID, PATHOID, BTLessEqualStrategyNumber,
                      geoop::PathLE),
    INIT_MIN_MAX_OPER(PATHOID, PATHOID, BTEqualStrategyNumber, geoop::PathEQ),
    INIT_MIN_MAX_OPER(PATHOID, PATHOID, BTGreaterEqualStrategyNumber,
                      geoop::PathGE),
    INIT_MIN_MAX_OPER(PATHOID, PATHOID, BTGreaterStrategyNumber, geoop::PathGT),
};

}  // namespace pax

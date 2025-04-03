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
 * vec_numeric.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/vec_numeric.cc
 *
 *-------------------------------------------------------------------------
 */

#include "comm/vec_numeric.h"

#include <string.h>

#include "comm/cbdb_wrappers.h"

#define NUMERIC_SHORT_HEADER_SIZE 2

// The vec numeric defined

static const int64 digits_base[] = {1LL, 10LL, 100LL, 1000LL, 10000LL};

#define VEC_SHORT_NUMERIC_SCALE_MASK             \
  (((1ULL << VEC_SHORT_NUMERIC_SCALE_WIDTH) - 1) \
   << VEC_SHORT_NUMERIC_HIGH_DATA_WIDTH)
#define VEC_SHORT_NUMERIC_SCALE_SHIFT VEC_SHORT_NUMERIC_HIGH_DATA_WIDTH
#define VEC_SHORT_NUMERIC_HIGH_MASK ((1LL << VEC_SHORT_NUMERIC_SCALE_SHIFT) - 1)

#define VEC_SHORT_NUMERIC_SHORT_DSCALE(n_high) \
  (((n_high)&VEC_SHORT_NUMERIC_SCALE_MASK) >> VEC_SHORT_NUMERIC_SCALE_SHIFT)

#define VEC_SHORT_NUMERIC_SHORT_SIGN(n_high) \
  (1 | (n_high << VEC_SHORT_NUMERIC_HEADER_WIDTH >> 63))

#define VEC_SHORT_NUMERIC_IS_INF(n_high) ((n_high >> 62) & 1)

#define VEC_SHORT_NUMERIC_IS_SPECIAL(n_high) ((n_high >> 62) != 0)
#define VEC_NUMERIC_HIGH_BYTES_REMOVE_HEADER_MASK 0x00FFFFFFFFFFFFFF

#ifdef HAVE_INT128

/*
 * Convert 128 bit integer to numeric with scale.
 */
static void int128_to_numericvar_with_scale(int128 val, NumericVar *var,
                                            bool neg, int scale) {
  NumericDigit *ptr;
  int ndigits = 0;
  int dweight, weight, nweight = 0;
  int offset, scale_padding;
  bool padding_done;
  uint128 temp;

  var->sign = neg ? NUMERIC_NEG : NUMERIC_POS;
  var->dscale = scale;
  if (val == 0) {
    var->ndigits = 0;
    var->weight = 0;
    return;
  }

  /* int128 can require at most 39 decimal digits; add one for safety */
  alloc_numeric_var(var, 40 / DEC_DIGITS);
  Assert(val > 0);

  temp = val;
  while (temp != 0) {
    temp /= 10;
    nweight++;
  }

  dweight = nweight - scale - 1;

  ptr = var->digits + var->ndigits;

  if (dweight >= 0)
    weight = (dweight + 1 + DEC_DIGITS - 1) / DEC_DIGITS - 1;
  else
    weight = -((-dweight - 1) / DEC_DIGITS + 1);

  /*
   * offset is the number of decimal zeroes to insert before
   * the first given digit to have a correctly aligned first NBASE digit.
   *
   * scale_padding determines the number of decimal places to complement.
   */
  offset = (weight + 1) * DEC_DIGITS - (dweight + 1);
  scale_padding = (offset + nweight) % DEC_DIGITS;
  padding_done = scale_padding == 0;

  while (val > 0) {
    ptr--;
    ndigits++;

    if (!padding_done) {
      int16 pow_padding = 10;
      int16 pow_padding_remain = 10;
      for (int i = 1; i < scale_padding; i++) {
        pow_padding *= 10;
      }

      for (int i = 1; i < (DEC_DIGITS - scale_padding); i++) {
        pow_padding_remain *= 10;
      }

      temp = val / pow_padding;
      *ptr = (val - (temp * pow_padding)) * pow_padding_remain;
      val = temp;

      padding_done = true;
    } else {
      temp = val / NBASE;
      *ptr = (val - (temp * NBASE));
      val = temp;
    }
  }

  var->digits = ptr;
  var->ndigits = ndigits;
  var->weight = weight;
}

static Numeric int128_to_numeric(int128 val, int scale, bool neg, bool nan,
                                 bool pinf, bool ninf) {
  Numeric res;
  NumericVar result;

  /* Make sure the parameter passed in is already a positive integer */
  Assert(val >= 0);
  AssertImply(nan, !(pinf || ninf));
  AssertImply(pinf, !(nan || ninf));
  AssertImply(ninf, !(nan || pinf));

  if (nan) {
    res = make_nan_numeric_result();
  } else if (pinf) {
    res = make_pinf_numeric_result();
  } else if (ninf) {
    res = make_ninf_numeric_result();
  } else {
    init_numeric_var(&result);

    int128_to_numericvar_with_scale(val, &result, neg, scale);

    res = make_numeric_result(&result);

    free_numeric_var(&result);
  }

  return res;
}
#endif

static inline int64 high_bits_with_header(uint64 scale, int64 high_val) {
  uint64 header = (scale << VEC_SHORT_NUMERIC_SCALE_SHIFT);
  return header | (VEC_SHORT_NUMERIC_HIGH_MASK & high_val);
}

static inline int128 vec_combine_to_int128(const int64 *n_low,
                                           const int64 *n_high) {
  int128 val;
  const uint64 n_low_val = *n_low;
  const int64 n_high_val = *n_high;
  int sign;

  val = ((int128)(n_high_val & (VEC_NUMERIC_HIGH_BYTES_REMOVE_HEADER_MASK))
         << 64);
  val |= n_low_val;
  sign = VEC_SHORT_NUMERIC_SHORT_SIGN(n_high_val);
  if (sign < 0) {
    static int128 mask = 0;
    if (mask == 0) {
      mask = ((int128)VEC_NUMERIC_HIGH_BYTES_REMOVE_HEADER_MASK << 64) |
             0xFFFFFFFFFFFFFFFF;
    }
    val = -val;
    val &= mask;
  }

  return val;
}

static inline void vec_numeric_special(Numeric num, int64 *n_low,
                                       int64 *n_high) {
  if (NUMERIC_IS_PINF(num)) {
    *n_low = INT64_MAX;
    *n_high = (1LL << 62) | ((1LL << (VEC_SHORT_NUMERIC_SCALE_SHIFT - 1)) - 1);
  } else if (NUMERIC_IS_NINF(num)) {
    *n_low = INT64_MIN;
    *n_high = (3LL << 62) | ((1LL << VEC_SHORT_NUMERIC_SCALE_SHIFT) - 1);
  } else {
    *n_low = INT64_MIN;
    *n_high = (2LL << 62) | ((1LL << VEC_SHORT_NUMERIC_SCALE_SHIFT) - 1);
  }
}

void pg_short_numeric_to_vec_short_numeric(Numeric num, int num_len,
                                           int64 *n_low, int64 *n_high) {
  int scale, weight, ndigits;
  NumericDigit *digits;
  bool neg;
  int i = 0;
  int128 frac_val = 0, val = 0;

  if (NUMERIC_IS_SPECIAL(num)) {
    vec_numeric_special(num, n_low, n_high);
    return;
  }

  scale = NUMERIC_DSCALE(num);
  weight = NUMERIC_WEIGHT(num);
  digits = NUMERIC_DIGITS(num);
  ndigits = (num_len - NUMERIC_SHORT_HEADER_SIZE) / sizeof(NumericDigit);
  neg = (NUMERIC_SIGN(num) == NUMERIC_NEG);

  // integer part
  if (weight >= 0 && ndigits > 0) {
    val = digits[0];
    for (i = 1; i <= weight; i++) {
      val *= NBASE;
      if (i < ndigits) {
        val += digits[i];
      }
    }
    for (int i = 0; i < scale; i++) val *= 10;
  }
  int frac_offset = weight >= 0 ? 0 : (abs(weight) - 1) * 4;
  // fractional part
  while (frac_offset < scale) {
    frac_val = frac_val * NBASE;
    frac_offset += DEC_DIGITS;
    if (i < ndigits) frac_val += digits[i];
    if (frac_offset > scale) {
      Assert((DEC_DIGITS - scale % DEC_DIGITS) < (int)lengthof(digits_base));
      frac_val = frac_val / digits_base[DEC_DIGITS - scale % DEC_DIGITS];
    }
    i++;
  }
  val += frac_val;
  val = neg ? -val : val;
  *n_low = (uint64)val;
  *n_high = high_bits_with_header(scale, val >> 64);
}

Datum vec_short_numeric_to_datum(const int64 *n_low, const int64 *n_high) {
#ifdef HAVE_INT128
  const int64 n_high_val = *n_high;
  int64 sign;
  int128 val;
  int32 scale = VEC_SHORT_NUMERIC_SHORT_DSCALE(n_high_val);
  bool is_nan = false, is_inf = false, is_ninf = false;

  val = vec_combine_to_int128(n_low, n_high);
  sign = VEC_SHORT_NUMERIC_SHORT_SIGN(n_high_val);

  if (VEC_SHORT_NUMERIC_IS_SPECIAL(n_high_val) ||  //
      scale < -VEC_SHORT_NUMERIC_MAX_SCALE ||
      scale > VEC_SHORT_NUMERIC_MAX_SCALE) {
    if (VEC_SHORT_NUMERIC_IS_INF(n_high_val) &&
        VEC_SHORT_NUMERIC_SHORT_SIGN(n_high_val) > 0) {
      is_inf = true;
    } else if (VEC_SHORT_NUMERIC_IS_INF(n_high_val) &&
               VEC_SHORT_NUMERIC_SHORT_SIGN(n_high_val) < 0) {
      is_ninf = true;
    } else {
      is_nan = true;
    }
  }
  return cbdb::NumericToDatum(
        int128_to_numeric(val, VEC_SHORT_NUMERIC_SHORT_DSCALE(n_high_val),
                          sign < 0, is_nan, is_inf, is_ninf));
#else
  Assert(false);
  return (Datum)0;
#endif
}

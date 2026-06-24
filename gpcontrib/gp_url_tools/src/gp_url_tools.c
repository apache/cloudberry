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
 * gp_url_tools.c
 *
 * IDENTIFICATION
 *	  gpcontrib/gp_url_tools/src/gp_url_tools.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "fmgr.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(encode_url);
PG_FUNCTION_INFO_V1(decode_url);
PG_FUNCTION_INFO_V1(encode_uri);
PG_FUNCTION_INFO_V1(decode_uri);

static const unsigned int utf16_low[2] = {0xD800, 0xDC00};
static const unsigned int utf16_high[2] = {0xDBFF, 0xDFFF};
static const unsigned int utf16_decode = 0x03FF;
static const unsigned int utf16_decode_base = 0x10000;
static const int utf8_with_percent_length = 3;          // Example: '%20
static const int utf16_with_percent_length = 6;         // Example: '%u0430'
static const int utf16_surrogate_pair_length = 12;      // Example: '%uD800%uDC00'
static const int utf16_second_codepoint_offset = 8;     // '%uD800%uDC00' => ('%uD800%u'.lenght == 8)
static const int utf16_past_first_codepoint_offset = 6; // '%uD800%uDC00' => ('%uD800'.lenght == 6)

static unsigned char hex_char_to_value(char c) {
    if ('0' <= c && c <= '9')
        return c - '0';
    if ('A' <= c && c <= 'F')
        return c - 'A' + 10;
    if ('a' <= c && c <= 'f')
        return c - 'a' + 10;
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("invalid hexadecimal digit: \"%c\"", c)));
    pg_unreachable();
}

static bool allowed_character(const char c, const char *unreserved_special) {
    return ('0' <= c && c <= '9') || ('A' <= c && c <= 'Z') ||
           ('a' <= c && c <= 'z') || (strchr(unreserved_special, c) != NULL);
}

static char *write_character(char *output, const char c) {
    *output = c;
    return ++output;
}

static void valid_encoding_length(char *current, char *end, int length) {
    Assert(current + length <= end);
}


static text *encode(text *input, const char *unreserved_special) {
    int input_length, output_length;
    text *output;
    char *cinput, *coutput, *current, *cend;

    // Convert input data for processing
    cinput = text_to_cstring(input);
    input_length = strlen(cinput);
    /*
     * Worst case: every input byte becomes '%XX' (3 output chars).
     * The +1 accounts for the null terminator
     */
    output_length = 3 * input_length + 1;
    coutput = palloc(sizeof(*coutput) * output_length);
    current = coutput;
    cend = coutput + output_length;

    for (int i = 0; i < input_length; ++i) {
        if (allowed_character(cinput[i], unreserved_special)) {
            // Allowed character => copy it into result string
            valid_encoding_length(current, cend, 1);
            current = write_character(current, cinput[i]);
        } else {
            // Percent-encode byte as '%XX'
            valid_encoding_length(current, cend, 3);
            current += sprintf(current, "%%%02X", (unsigned char)cinput[i]);
        }
    }
    // Terminate result string
    valid_encoding_length(current, cend, 1);
    current = write_character(current, '\0');

    output = cstring_to_text(coutput);
    pfree(coutput);
    return output;
}

static bool valid_utf16(unsigned int byte, int byte_num) {
    return utf16_low[byte_num] <= byte && byte <= utf16_high[byte_num];
}

static unsigned int decode_utf16_pair(unsigned int bytes[2]) {
    Assert(valid_utf16(bytes[0], 0));
    Assert(valid_utf16(bytes[1], 1));

    return (utf16_decode_base + ((bytes[0] & utf16_decode) << 10) +
            (bytes[1] & utf16_decode));
}

/*
 * Check whether the sequence starts with a percent-encoded UTF-8 byte (%XX).
 *
 * A UTF-8 percent-encoded byte starts with '%' followed by exactly two hex
 * digits (e.g. "%20", "%D0").  This is distinguished from a UTF-16 sequence
 * which starts with '%u' or '%U' (e.g. "%uD83D").
 *
 * Requires at least 3 characters: '%' + 2 hex digits.
 */
static bool is_utf8(const char *sequence, int length) {
    return utf8_with_percent_length <= length && sequence[0] == '%' &&
           sequence[1] != 'u' && sequence[1] != 'U';
}

/*
 * Check whether the sequence starts with a legacy percent-encoded UTF-16 unit
 * ('%uXXXX' or '%UXXXX'). Requires at least 6 characters: '%u' + 4 hex digits.
 */
static bool is_utf16(const char *sequence, int length) {
    return utf16_with_percent_length <= length && sequence[0] == '%' &&
           (sequence[1] == 'u' || sequence[1] == 'U');
}

static void fetch_utf16(unsigned int *byte, const char *input) {
    for (int i = 0; i < 4; ++i)
        *byte = ((*byte) << 4) | hex_char_to_value(input[i]);
}

static text *decode(text *input, const char *unreserved_special) {
    int input_length;
    text *output;
    char *cinput, *coutput, *current;

    cinput = text_to_cstring(input);
    input_length = strlen(cinput);
    coutput = palloc(sizeof(*coutput) * (input_length + 1));
    current = coutput;

    for (int i = 0; i < input_length;) {
        if (cinput[i] == '%') {
            // Special character => start process '%XX' sequence of chars
            if (is_utf16(cinput + i, input_length - i)) {
                unsigned int result = 0;
                unsigned int bytes[2] = {};
                unsigned char buffer[10] = {};

                fetch_utf16(bytes, cinput + i + 2);

                if (valid_utf16(bytes[0], 0)) {
                    if (input_length - i < utf16_surrogate_pair_length) {
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                             errmsg("invalid sequence: not enough characters "
                                    "to decode UTF-16 symbol from %d position",
                                    i)));
                    }

                    fetch_utf16(bytes + 1,
                                cinput + i + utf16_second_codepoint_offset);
                    if (!valid_utf16(bytes[1], 1)) {
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                             errmsg("invalid UTF-16 byte: characters from %d "
                                    "position define invalid UTF-16 symbol",
                                    i + utf16_past_first_codepoint_offset)));
                    }

                    result = decode_utf16_pair(bytes);
                    i += utf16_surrogate_pair_length;
                } else {
                    result = bytes[0];
                    i += utf16_with_percent_length;
                }

                unicode_to_utf8((pg_wchar)result, buffer);
                memcpy(current, buffer, pg_utf_mblen(buffer));
                current += pg_utf_mblen(buffer);
            } else if (is_utf8(cinput + i, input_length - i)) {
                current =
                    write_character(current, (hex_char_to_value(cinput[i + 1]) << 4) |
                                                 hex_char_to_value(cinput[i + 2]));
                i += 3;
            } else {
                // '%' starts a special sequence, but there are not enough
                // characters left to decode it => error 'incorrect sequence of tokens'
                ereport(ERROR,
                        (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                         errmsg("invalid sequence: not enough characters to "
                                "decode any UTF-typed symbol from %d position",
                                i)));
            }
        } else if (allowed_character(cinput[i], unreserved_special)) {
            // Copy an unescaped character that is allowed
            current = write_character(current, cinput[i]);
            i += 1;
        } else {
            ereport(ERROR, (errcode(ERRCODE_CHARACTER_NOT_IN_REPERTOIRE),
                            errmsg("disallowed characters in URL: \"%c\"",
                                   cinput[i])));
        }
    }
    current = write_character(current, '\0');

    output = cstring_to_text(coutput);
    pfree(coutput);
    return output;
}

static const char *url_unreserved_special = ".-~_";

Datum encode_url(PG_FUNCTION_ARGS) {
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    PG_RETURN_TEXT_P(encode(PG_GETARG_TEXT_PP(0), url_unreserved_special));
}

Datum decode_url(PG_FUNCTION_ARGS) {
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    PG_RETURN_TEXT_P(decode(PG_GETARG_TEXT_PP(0), url_unreserved_special));
}

static const char *uri_unreserved_special = "-_.!~*'();/?:@&=+$,#";

Datum encode_uri(PG_FUNCTION_ARGS) {
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    PG_RETURN_TEXT_P(encode(PG_GETARG_TEXT_PP(0), uri_unreserved_special));
}

Datum decode_uri(PG_FUNCTION_ARGS) {
    if (PG_ARGISNULL(0))
        PG_RETURN_NULL();
    PG_RETURN_TEXT_P(decode(PG_GETARG_TEXT_PP(0), uri_unreserved_special));
}

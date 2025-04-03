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
 * zorder_utils.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/clustering/zorder_utils.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"
#define N_BYTES 8
namespace paxc {

// should match the behavior in the datum_to_bytes function
bool support_zorder_type(Oid type);
};  // namespace paxc

namespace pax {

// Convert several types to byte representations which could be compared
// lexicographically.
void datum_to_bytes(Datum datum, Oid type, bool isnull, char *result);

int bytes_compare(const char *a, const char *b, int ncolumns);

Datum bytes_to_zorder_datum(char *buffer, int ncolumns);

void interleave_bits(const char *src, char *result, int ncolumns);

}  // namespace pax
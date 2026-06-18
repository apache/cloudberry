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
 * proto_wrappers.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/proto/proto_wrappers.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

// The libproto defined `FATAL` inside as a marco linker
#undef FATAL
// PG defines several macros (Min/Max in c.h, IsPowerOf2 in xlog_internal.h)
// whose names collide with abseil identifiers reached through protobuf
// headers. Suppress while including, restore the PG definitions after.
#undef Min
#undef Max
#undef IsPowerOf2
#include "storage/proto/micro_partition_stats.pb.h"
#include "storage/proto/orc_proto.pb.h"
#include "storage/proto/pax.pb.h"
#define FATAL 22
#define Min(x, y) ((x) < (y) ? (x) : (y))
#define Max(x, y) ((x) > (y) ? (x) : (y))
#define IsPowerOf2(x) (x > 0 && ((x) & ((x)-1)) == 0)

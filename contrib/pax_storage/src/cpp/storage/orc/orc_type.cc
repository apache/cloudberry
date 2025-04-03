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
 * orc_type.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/orc/orc_type.cc
 *
 *-------------------------------------------------------------------------
 */

#include "storage/orc/orc_type.h"

#include "comm/cbdb_api.h"

#include "comm/fmt.h"
#include "exceptions/CException.h"
#include "storage/orc/porc.h"

namespace pax {
pax::porc::proto::Type_Kind ConvertPgTypeToPorcType(FormData_pg_attribute *attr,
                                                    bool is_vec) {
  pax::porc::proto::Type_Kind type;
  if (attr->attbyval) {
    switch (attr->attlen) {
      case 1:
        if (attr->atttypid == BOOLOID) {
          type = pax::porc::proto::Type_Kind::Type_Kind_BOOLEAN;
        } else {
          type = pax::porc::proto::Type_Kind::Type_Kind_BYTE;
        }
        break;
      case 2:
        type = pax::porc::proto::Type_Kind::Type_Kind_SHORT;
        break;
      case 4:
        type = pax::porc::proto::Type_Kind::Type_Kind_INT;
        break;
      case 8:
        type = pax::porc::proto::Type_Kind::Type_Kind_LONG;
        break;
      default:
        CBDB_RAISE(cbdb::CException::kExTypeInvalid,
                   fmt("Invalid attribute [attlen=%d]", attr->attlen));
    }
  } else {
    Assert(attr->attlen > 0 || attr->attlen == -1);
    if (attr->atttypid == NUMERICOID) {
      type = is_vec ? pax::porc::proto::Type_Kind::Type_Kind_VECDECIMAL
                    : pax::porc::proto::Type_Kind::Type_Kind_DECIMAL;
    } else if (attr->atttypid == BPCHAROID) {
      type = is_vec ? pax::porc::proto::Type_Kind::Type_Kind_VECBPCHAR
                    : pax::porc::proto::Type_Kind::Type_Kind_BPCHAR;

    } else if (is_vec && attr->attlen > 0) {
      type = pax::porc::proto::Type_Kind::Type_Kind_VECNOHEADER;
    } else {
      type = pax::porc::proto::Type_Kind::Type_Kind_STRING;
    }
  }
  return type;
}
}  // namespace pax

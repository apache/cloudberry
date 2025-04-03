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
 * pax_fastsequence.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/catalog/pax_fastsequence.h
 *
 *-------------------------------------------------------------------------
 */

//-------------------------------------------------------------------------
// Cloudberry Database
// Copyright (c) 2023, HashData Technology Limited.
// pax_fastsequence.h
// provide a system table maintaining a light-weight fast sequence number for a
// unique object.
//
// IDENTIFICATION
//	    src/catalog/pax_fastsequence.h
// Author: Tony Ying
//--------------------------------------------------------------------------

#pragma once
#include "comm/cbdb_api.h"

#define ANUM_PG_PAX_FAST_SEQUENCE_OBJID 1
#define ANUM_PG_PAX_FAST_SEQUENCE_LASTSEQUENCE 2
#define NATTS_PG_PAX_FAST_SEQUENCE_TABLES 2

// CREATE:  initialize seqno by INSERT, no tuple exists before
// INPLACE: inplace update when grow the seqno or non-transactional truncate
// UPDATE:  transactional truncate, needs to preserve the old seqno
//          after rollback
#define FASTSEQUENCE_INIT_TYPE_CREATE 'C'
#define FASTSEQUENCE_INIT_TYPE_INPLACE 'I'
#define FASTSEQUENCE_INIT_TYPE_UPDATE 'U'

#ifdef __cplusplus
extern "C" {
#endif

void CPaxInitializeFastSequenceEntry(Oid objid, char init_type,
                                     int32 fast_seq);
int32 CPaxGetFastSequences(Oid objid, bool increase);
char *CPaxGetFastSequencesName(Oid oid, bool missing_ok);

#ifdef __cplusplus
}
#endif

#ifdef __cplusplus
namespace cbdb {
int32 CPaxGetFastSequences(Oid objid, bool increase = true);
}
#endif

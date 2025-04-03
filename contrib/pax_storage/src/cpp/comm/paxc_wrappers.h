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
 * paxc_wrappers.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/paxc_wrappers.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

namespace paxc {
// pax file operation, will refactor it later
void DeletePaxDirectoryPath(const char *dirname, bool delete_topleveldir);
void MakedirRecursive(const char *path);
char *BuildPaxDirectoryPath(RelFileNode rd_node, BackendId rd_backend);
bool PGGetOperatorNo(Oid opno, NameData *oprname, Oid *oprleft, Oid *oprright,
                     FmgrInfo *finfo);
bool PGGetOperator(const char *operatorName, Oid operatorNamespace,
                   Oid leftObjectId, Oid rightObjectId, Oid *opno,
                   FmgrInfo *finfo);
bool PGGetAddOperator(Oid atttypid, Oid subtype, Oid namespc, Oid *resulttype,
                      FmgrInfo *finfo);
bool PGGetProc(Oid procoid, FmgrInfo *finfo);

bool PGGetAggInfo(const char *procedure, Oid atttypid, Oid *prorettype,
                  Oid *transtype, FmgrInfo *trans_finfo, FmgrInfo *final_finfo,
                  bool *final_func_exist, bool *agginitval_isnull);
bool SumAGGGetProcinfo(Oid atttypid, Oid *prorettype, Oid *transtype,
                       FmgrInfo *trans_finfo, FmgrInfo *final_finfo,
                       bool *final_func_exist, FmgrInfo *add_finfo);
Datum SumFuncCall(FmgrInfo *flinfo, AggState *state, Datum arg1, Datum arg2);
bool IsDfsTablespaceById(Oid spcId);

bool NeedWAL(Relation rel);

typedef struct PaxFileNodePendingDelete {
  char relkind;
  char *relativePath;
} PaxFileNodePendingDelete;

typedef struct PendingRelDeletePaxFile {
  PendingRelDelete reldelete;        /* base pending delete */
  PaxFileNodePendingDelete filenode; /* relation that may need to be
                                      * deleted */
} PendingRelDeletePaxFile;

SMgrRelation PaxRelationCreateStorage(RelFileNode rnode, Relation rel);


}  // namespace paxc

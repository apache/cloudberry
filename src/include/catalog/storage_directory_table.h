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
 * storage_directory_table.h
 *	  Storage manipulation for directory table.
 *
 * IDENTIFICATION
 *	  src/include/catalog/storage_directory_table.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STORAGE_DIRECTORY_TABLE_H
#define STORAGE_DIRECTORY_TABLE_H

#include "utils/relcache.h"

extern bool allow_dml_directory_table;

extern void UFileAddPendingDelete(Relation rel, Oid spcId, char *relativePath, bool atCommit);
extern void DirectoryTableDropStorage(Oid relid);

#endif	/* STORAGE_DIRECTORY_TABLE_H */

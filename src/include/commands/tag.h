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
 * tag.h
 *		Tag management commands (create/drop/alter tag).
 *
 * src/include/commands/tag.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef TAG_H
#define TAG_H
#include "catalog/objectaddress.h"

extern Oid CreateTag(CreateTagStmt *stmt);
extern ObjectAddress AlterTag(AlterTagStmt *stmt);
extern void DropTag(DropTagStmt *stmt);
extern void AddTagDescriptions(List *tags, Oid databaseid, Oid classid, Oid objid, char *objname);
extern void AlterTagDescriptions(List *tags, Oid databaseid, Oid classid, Oid objid, char *objname);
extern void UnsetTagDescriptions(List *tags, Oid databaseid, Oid classid, Oid objid, char *objname);
extern void DeleteTagDescriptions(Oid databaseid, Oid classid, Oid objid);
extern Oid get_tag_oid(const char *tagname, bool missing_ok);
extern ObjectAddress RenameTag(const char *oldname, const char *newname);
extern char *TagGetNameByOid(Oid tagid, bool missing_ok);

#endif							/* TAG_H */
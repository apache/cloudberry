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
 * taskcmds.h
 *      prototypes for taskcmds.c.
 *
 * IDENTIFICATION
 *		src/include/commands/taskcmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef TASKCMDS_H
#define TASKCMDS_H

#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"

extern ObjectAddress DefineTask(ParseState *pstate, CreateTaskStmt *stmt);

extern ObjectAddress AlterTask(ParseState *pstate, AlterTaskStmt *stmt);

extern ObjectAddress DropTask(ParseState *pstate, DropTaskStmt *stmt);

extern void RemoveTaskById(Oid jobid);

#endif                  /* TASKCMDS_H */

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
 * lance_scan.h
 *	  The executor side of lance_fdw: everything between BeginForeignScan and
 *	  EndForeignScan.
 *
 * IDENTIFICATION
 *	  src/lance_scan.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LANCE_SCAN_H
#define LANCE_SCAN_H

#include "lance_fdw.h"

#include "commands/explain.h"
#include "nodes/execnodes.h"

extern void lance_scan_begin(ForeignScanState *node, int eflags);
extern TupleTableSlot *lance_scan_next(ForeignScanState *node);
extern void lance_scan_rescan(ForeignScanState *node);
extern void lance_scan_end(ForeignScanState *node);
extern void lance_scan_explain(ForeignScanState *node, ExplainState *es);

#endif							/* LANCE_SCAN_H */

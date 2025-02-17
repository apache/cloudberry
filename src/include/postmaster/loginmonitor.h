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
 * loginmonitor.h
 *	  header file for integrated loginmonitor daemon
 *
 * src/include/postmaster/loginmonitor.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGINMONITOR_H
#define LOGINMONITOR_H

#include "storage/block.h"

extern int StartLoginMonitorLauncher(void);
extern int StartLoginMonitorWorker(void);
extern Size LoginMonitorShmemSize(void);
extern void LoginMonitorShmemInit(void);
extern void SendLoginFailedSignal(const char *curr_user_name);
extern bool IsLoginMonitorWorkerProcess(void);
extern bool IsLoginMonitorLauncherProcess(void);
extern void HandleLoginFailed(void);
extern void LoginMonitorWorkerFailed(void);

#define IsAnyLoginMonitorProcess() \
	(IsLoginMonitorLauncherProcess() || IsLoginMonitorWorkerProcess())

extern int login_monitor_max_processes;
#endif							/* LOGINMONITOR_H */

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
 * meta_engine_init.c
 *	  Registration of the metadata engine this build provides.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/meta/meta_engine_init.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "common/dl_err.h"
#include "meta/engine_stub/stub_engine.h"
#include "meta/meta_engine_init.h"

/*
 * Called from _PG_init (extensible.c, later task); agent and builtin engines
 * join in later PRs. SQL-visible NOTICE behavior is covered by skel-regress;
 * this skeleton has no separate C test harness.
 */
void
DatalakeRegisterMetaEngines(void)
{
	DlErrCode	rc;

	rc = RegisterStubMetaEngine();
	if (rc != DL_OK)
		elog(ERROR, "datalake_fdw: failed to register stub meta engine: %s",
			 dl_err_message(rc));
}

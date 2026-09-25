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
 * storage_backend_register.h
 *	  Order-independent registration helper for storage plugins.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/storage_backend_register.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef STORAGE_BACKEND_REGISTER_H
#define STORAGE_BACKEND_REGISTER_H

/* Arrow headers must precede PostgreSQL headers in a C++ translation unit. */
#include "storage_backend.h"

extern "C"
{
#include "postgres.h"

#include "fmgr.h"
#include "utils/elog.h"
}

/*
 * Register a backend, whatever order the libraries were loaded in.
 *
 * Loading the extension is PostgreSQL's job, and PostgreSQL reports a missing
 * library or symbol by raising an error, which unwinds with longjmp -- through
 * this plugin's C++ frames, skipping their destructors.  So the load happens
 * inside PG_TRY and comes back as a value instead.
 */
static inline DlErrCode
datalake_storage_register(const DatalakeStorageBackend *backend)
{
	typedef DlErrCode (*dl_register_fn) (const DatalakeStorageBackend *);
	volatile DlErrCode rc = DL_ERR_INTERNAL;

	PG_TRY();
	{
		dl_register_fn fn = reinterpret_cast<dl_register_fn>(
			load_external_function("$libdir/datalake_fdw",
								   "datalake_register_storage_backend",
								   true, NULL));

		rc = fn(backend);
	}
	PG_CATCH();
	{
		/*
		 * The caller is a plugin's _PG_init, which is entitled to decide for
		 * itself whether it can carry on; what it must not get is an unwind
		 * through its own frames.
		 */
		FlushErrorState();
		rc = DL_ERR_NOT_SUPPORTED;
	}
	PG_END_TRY();

	return rc;
}

#endif							/* STORAGE_BACKEND_REGISTER_H */

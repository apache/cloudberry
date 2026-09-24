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
 * backend_registry.cpp
 *	  Process-local registry shared with storage backend plugins.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/backend_registry.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include <arrow/memory_pool.h>
#include <arrow/util/config.h>

#include "common/storage_backend.h"
#include "common/dl_pg_api.h"

extern "C"
{
#include "fmgr.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/memutils.h"
}

#include "common/backend_registry.h"
#include "common/dl_wrappers.h"

#define DL_STORAGE_RENDEZVOUS_NAME "datalake_storage_registry_v1"
#define DL_STORAGE_V1_MIN_SIZE \
	(offsetof(DatalakeStorageBackend, finalize) + \
	 sizeof(((DatalakeStorageBackend *) 0)->finalize))

typedef struct DatalakeStorageBackendEntry
{
	const DatalakeStorageBackend *backend;
	bool		initialized;
	struct DatalakeStorageBackendEntry *next;
} DatalakeStorageBackendEntry;

typedef struct DatalakeStorageRegistry
{
	DatalakeStorageBackendEntry *backends;
	void	   *wrappers;			/* reserved for a future wrapper chain */
	bool		finalizer_registered;
} DatalakeStorageRegistry;

extern DlErrCode datalake_register_local_backend(void);

static DatalakeStorageRegistry *
storage_registry(bool create)
{
	void	  **slot = NULL;

	/* Both calls can allocate and therefore must not longjmp through C++. */
	DL_WRAP_START;
	{
		slot = find_rendezvous_variable(DL_STORAGE_RENDEZVOUS_NAME);
		if (create && *slot == NULL)
			*slot = MemoryContextAllocZero(TopMemoryContext,
										   sizeof(DatalakeStorageRegistry));
	}
	DL_WRAP_END;

	return slot == NULL ? NULL :
		static_cast<DatalakeStorageRegistry *>(*slot);
}

static void
storage_backends_finalize(int code, Datum arg)
{
	DatalakeStorageRegistry *registry = NULL;
	DatalakeStorageBackendEntry *entry;

	(void) code;
	(void) arg;

	try
	{
		registry = storage_registry(false);
	}
	catch (...)
	{
		/* Process exit is a cleanup boundary: never throw or ereport here. */
		return;
	}

	if (registry == NULL)
		return;

	for (entry = registry->backends; entry != NULL; entry = entry->next)
	{
		if (entry->initialized && entry->backend->finalize != NULL)
		{
			try
			{
				DL_WRAP_START;
				{
					elog(DEBUG1, "datalake_fdw: finalizing storage backend \"%s\"",
						 entry->backend->uri_scheme);
				}
				DL_WRAP_END;
				entry->backend->finalize();
			}
			catch (...)
			{
				/* One plugin must not prevent the remaining finalizers. */
			}
		}
		entry->initialized = false;
	}
}

static void
set_registration_error(const char *message)
{
	dl_error_set(DL_ERR_INVALID_OPTION, "register storage backend", NULL,
				 message);
}

extern "C" __attribute__((visibility("default"))) DlErrCode
datalake_register_storage_backend(const DatalakeStorageBackend *backend)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_ABI_GUARD_BEGIN
	{
		DatalakeStorageRegistry *registry;
		DatalakeStorageBackendEntry *entry;
		char		message[DL_ERR_MSG_LEN];

		dl_error_reset();
		if (backend == NULL)
		{
			set_registration_error("expected a non-null storage backend, got null");
			rc = DL_ERR_INVALID_OPTION;
		}
		else if (backend->abi_version != DL_STORAGE_ABI_VERSION)
		{
			snprintf(message, sizeof(message),
					 "storage backend ABI version mismatch: expected %u, got %u",
					 DL_STORAGE_ABI_VERSION, backend->abi_version);
			set_registration_error(message);
			rc = DL_ERR_INVALID_OPTION;
		}
		else if (backend->struct_size < DL_STORAGE_V1_MIN_SIZE)
		{
			snprintf(message, sizeof(message),
					 "storage backend struct size mismatch: expected at least %zu, got %u",
					 (size_t) DL_STORAGE_V1_MIN_SIZE, backend->struct_size);
			set_registration_error(message);
			rc = DL_ERR_INVALID_OPTION;
		}
		else if (backend->arrow_version == NULL ||
				 strcmp(backend->arrow_version, ARROW_VERSION_STRING) != 0)
		{
			snprintf(message, sizeof(message),
					 "storage backend Arrow version mismatch: expected \"%s\", got \"%s\"",
					 ARROW_VERSION_STRING,
					 backend->arrow_version == NULL ? "(null)" : backend->arrow_version);
			set_registration_error(message);
			rc = DL_ERR_INVALID_OPTION;
		}
		else if (backend->abi_fingerprint == NULL ||
				 strcmp(backend->abi_fingerprint,
						DL_STORAGE_ABI_FINGERPRINT) != 0)
		{
			snprintf(message, sizeof(message),
					 "storage backend ABI fingerprint mismatch: expected \"%s\", got \"%s\"",
					 DL_STORAGE_ABI_FINGERPRINT,
					 backend->abi_fingerprint == NULL ?
					 "(null)" : backend->abi_fingerprint);
			set_registration_error(message);
			rc = DL_ERR_INVALID_OPTION;
		}
		else if (backend->uri_scheme == NULL || backend->uri_scheme[0] == '\0' ||
				 backend->mount == NULL)
		{
			set_registration_error("expected a scheme and mount function, got an incomplete storage backend");
			rc = DL_ERR_INVALID_OPTION;
		}
		else
		{
			registry = storage_registry(true);
			for (entry = registry->backends; entry != NULL; entry = entry->next)
			{
				if (strcmp(entry->backend->uri_scheme, backend->uri_scheme) == 0)
					break;
			}

			if (entry != NULL)
			{
				snprintf(message, sizeof(message),
						 "storage backend scheme expected to be unique, got duplicate \"%s\"",
						 backend->uri_scheme);
				dl_error_set(DL_ERR_ALREADY_EXISTS,
							 "register storage backend", NULL, message);
				rc = DL_ERR_ALREADY_EXISTS;
			}
			else
			{
				DL_WRAP_START;
				{
					entry = static_cast<DatalakeStorageBackendEntry *>(
						MemoryContextAllocZero(TopMemoryContext, sizeof(*entry)));
				}
				DL_WRAP_END;
				entry->backend = backend;
				entry->next = registry->backends;
				registry->backends = entry;
				rc = DL_OK;
			}
		}
	}
	DL_ABI_GUARD_END(rc, "register_storage_backend");

	return rc;
}

const DatalakeStorageBackend *
datalake_lookup_storage_backend(const char *scheme)
{
	DatalakeStorageRegistry *registry = storage_registry(false);
	DatalakeStorageBackendEntry *entry;

	if (registry == NULL || scheme == NULL)
		return NULL;

	for (entry = registry->backends; entry != NULL; entry = entry->next)
	{
		if (strcmp(entry->backend->uri_scheme, scheme) == 0)
			return entry->backend;
	}
	return NULL;
}

arrow::Status
datalake_initialize_storage_backend(const DatalakeStorageBackend *backend)
{
	DatalakeStorageRegistry *registry = storage_registry(false);
	DatalakeStorageBackendEntry *entry;

	if (registry == NULL || backend == NULL)
		return arrow::Status::Invalid("storage backend is not registered");

	for (entry = registry->backends; entry != NULL; entry = entry->next)
	{
		if (entry->backend != backend)
			continue;
		if (entry->initialized)
			return arrow::Status::OK();

		arrow::Status status = arrow::Status::OK();

		Assert(MyProcPid != PostmasterPid);
		if (!registry->finalizer_registered)
		{
			DL_WRAP_START;
			{
				on_proc_exit(storage_backends_finalize, (Datum) 0);
			}
			DL_WRAP_END;
			registry->finalizer_registered = true;
		}

		if (backend->initialize != NULL)
			status = backend->initialize();

		if (status.ok())
			entry->initialized = true;
		return status;
	}

	return arrow::Status::Invalid("storage backend is not registered");
}

extern "C" bool
datalake_storage_scheme_registered(const char *scheme)
{
	bool		found = false;

	/* Asked from C, on a path that must not throw. */
	try
	{
		found = datalake_lookup_storage_backend(scheme) != NULL;
	}
	catch (...)
	{
		found = false;
	}
	return found;
}

extern "C" void
datalake_register_storage_backends(void)
{
	DlErrCode	rc = DL_ERR_INTERNAL;

	DL_TRY
	{
		rc = datalake_register_local_backend();
	}
	DL_CATCH_END();

	if (rc != DL_OK && rc != DL_ERR_ALREADY_EXISTS)
		dl_error_report(ERROR, rc, "register file storage backend");
}

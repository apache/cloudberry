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
 * backend_registry.h
 *	  Internal access to the storage backend registry.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/backend_registry.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef BACKEND_REGISTRY_H
#define BACKEND_REGISTRY_H

#ifdef __cplusplus
#include "common/storage_backend.h"
extern const DatalakeStorageBackend *datalake_lookup_storage_backend(
	const char *scheme);
extern arrow::Status datalake_initialize_storage_backend(
	const DatalakeStorageBackend *backend);
#endif

#ifdef __cplusplus
extern "C"
{
#endif

extern void datalake_register_storage_backends(void);

/*
 * Whether anything can reach this scheme.  The location parser asks, so that
 * a volume may name any storage a backend has registered rather than only the
 * two this module ships.
 */
extern bool datalake_storage_scheme_registered(const char *scheme);

#ifdef __cplusplus
}
#endif

#endif							/* BACKEND_REGISTRY_H */

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
 * datalake_location.h
 *	  The canonical form of a lake table storage location.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/datalake_location.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DATALAKE_LOCATION_H
#define DATALAKE_LOCATION_H

#include <stdint.h>

/* Canonical, versioned location form. URIs are parsed ONCE (options layer);
 * every backend receives only this struct and must never re-parse URIs. */
typedef struct DatalakeLocation
{
	uint32_t	abi_version;     /* = DATALAKE_LOCATION_ABI_VERSION */
	char	   *scheme;          /* v1 whitelist: "s3" | "file" */
	char	   *authority;       /* s3: bucket (validated); file: empty */
	char	   *path_prefix;     /* normalized: starts with '/', no trailing '/'
								 * except the file root itself */
	char	   *endpoint;        /* optional, may be NULL */
	char	   *region;          /* optional, may be NULL */
} DatalakeLocation;
#define DATALAKE_LOCATION_ABI_VERSION 1

/* Join paths as full = path_prefix + "/" + relative; relative never starts
 * with '/'. */

#endif						/* DATALAKE_LOCATION_H */

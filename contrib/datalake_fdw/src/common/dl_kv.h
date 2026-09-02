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
 * dl_kv.h
 *	  A configuration pair as options and mappings deliver it.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/dl_kv.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_KV_H
#define DL_KV_H

/*
 * A configuration pair as it arrives from a foreign server, a user mapping or
 * a table option.  It lives in common/ because both the storage layer and the
 * metadata layer consume such pairs, and neither should have to include the
 * other's headers to name the type.
 */
typedef struct DlKeyValue
{
	char	   *key;
	char	   *value;
} DlKeyValue;

#endif							/* DL_KV_H */

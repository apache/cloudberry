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
 * dl_resource.h
 *	  Cleanups that happen even when nothing calls them.
 *
 * A reader or a writer holds an operating system file descriptor and memory
 * from an allocator that is not PostgreSQL's.  Neither is reclaimed by
 * transaction abort, so anything that owns one has to be released by name --
 * and a caller that raises an error before it reaches its own cleanup would
 * never get to.  Registering here makes the resource owner do it instead, and
 * makes a caller that simply forgot a warning rather than a descriptor that is
 * gone until the backend exits.
 *
 * This is the shape PAX uses (contrib/pax_storage/src/cpp/comm/pax_resource.cc):
 * a callback registered once, and a list of what to release keyed by the owner
 * that was current when it was remembered.  PostgreSQL 16 also has a typed
 * resource-kind API, which this server does not carry.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/dl_resource.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DL_RESOURCE_H
#define DL_RESOURCE_H

#ifdef __cplusplus
extern "C"
{
#endif

/*
 * Runs during transaction abort, so the same rules as any other cleanup path:
 * noexcept, and it must not raise.
 */
typedef void (*DlResourceRelease) (void *arg);

/* Installs the callback.  Called once, from _PG_init. */
extern void dl_resource_init(void);

/*
 * Remembers that `release(arg)` has to happen before the current resource owner
 * goes away.  Returns false only when it could not record that, which the
 * caller has to treat as a failure to acquire the resource at all -- releasing
 * it itself and reporting -- because nothing else is going to.
 */
extern bool dl_resource_remember(DlResourceRelease release, void *arg);

/*
 * Drops that record, for the ordinary path where the caller releases the
 * resource itself.  Silent when there is nothing to drop: a cleanup that runs
 * twice reaches this the second time.
 */
extern void dl_resource_forget(DlResourceRelease release, void *arg);

#ifdef __cplusplus
}
#endif

#endif							/* DL_RESOURCE_H */

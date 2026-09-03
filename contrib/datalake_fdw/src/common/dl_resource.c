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
 * dl_resource.c
 *	  Cleanups that happen even when nothing calls them.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/common/dl_resource.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <stdlib.h>

#include "storage/ipc.h"
#include "utils/resowner.h"

#include "common/dl_resource.h"

typedef struct DlResourceEntry
{
	struct DlResourceEntry *next;
	ResourceOwner owner;
	DlResourceRelease release;
	void	   *arg;
} DlResourceEntry;

/*
 * There are a handful of these at a time -- one per open data file -- so a list
 * walked linearly is the whole structure needed.
 */
static DlResourceEntry *dl_resources;

/*
 * malloc rather than palloc: this outlives the memory context that was current
 * when it was remembered, by construction, and is walked while the transaction
 * is being torn down.
 */
static void
dl_resource_release_callback(ResourceReleasePhase phase, bool isCommit,
							 bool isTopLevel, void *arg)
{
	DlResourceEntry **link;

	/*
	 * After locks, so that anything the release path might touch is still
	 * usable.  Nothing to do while the process is exiting: the descriptors go
	 * with it, and running C++ destructors on the way out is a way to turn an
	 * exit into a crash.
	 */
	if (phase != RESOURCE_RELEASE_AFTER_LOCKS || proc_exit_inprogress)
		return;

	link = &dl_resources;
	while (*link != NULL)
	{
		DlResourceEntry *entry = *link;

		if (entry->owner != CurrentResourceOwner)
		{
			link = &entry->next;
			continue;
		}

		/*
		 * Reaching here on a commit means the owner released nothing: the
		 * statement finished and left a file open.  The resource is still
		 * cleaned up, but quietly doing so would hide the bug that let it
		 * happen, which is the same call PostgreSQL's own resource owners make.
		 */
		if (isCommit)
			elog(WARNING, "datalake_fdw leaked a resource: %p", entry->arg);

		*link = entry->next;
		entry->release(entry->arg);
		free(entry);
	}
}

void
dl_resource_init(void)
{
	RegisterResourceReleaseCallback(dl_resource_release_callback, NULL);
}

bool
dl_resource_remember(DlResourceRelease release, void *arg)
{
	DlResourceEntry *entry = malloc(sizeof(DlResourceEntry));

	if (entry == NULL)
		return false;

	entry->owner = CurrentResourceOwner;
	entry->release = release;
	entry->arg = arg;
	entry->next = dl_resources;
	dl_resources = entry;

	return true;
}

void
dl_resource_forget(DlResourceRelease release, void *arg)
{
	DlResourceEntry **link = &dl_resources;

	while (*link != NULL)
	{
		DlResourceEntry *entry = *link;

		if (entry->release == release && entry->arg == arg)
		{
			*link = entry->next;
			free(entry);
			return;
		}

		link = &entry->next;
	}
}

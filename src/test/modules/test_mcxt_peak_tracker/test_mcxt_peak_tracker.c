/*--------------------------------------------------------------------------
 *
 * test_mcxt_peak_tracker.c
 *		Test correctness of memory context accounting.
 *
 * Copyright (c) 2022-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_mcxt_peak_tracker/test_mcxt_peak_tracker.c
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "c.h"
#include "fmgr.h"
#include "utils/memutils.h"
#include <assert.h>

#ifndef USE_ASSERT_CHECKING
#undef Assert
#define Assert(condition) assert(condition)
#endif

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(check_reparent);
PG_FUNCTION_INFO_V1(check_peak_usage_propagation);
PG_FUNCTION_INFO_V1(check_alloc_and_free_for_context_types);

#define ASetNew(parent, name) \
	AllocSetContextCreate(parent, name, \
		ALLOCSET_DEFAULT_MINSIZE, \
		ALLOCSET_DEFAULT_INITSIZE, \
		ALLOCSET_DEFAULT_INITSIZE)


/* test helpers */
static void
start_peak_usage_and_validate(MemoryContext cxt)
{
	Assert(MemoryContextStartPeakUsageTracking(cxt));
	Assert(cxt->mem_allocated > 0);
	Assert(cxt->trackerGroup != NULL);
	Assert(cxt->trackerGroup->currentUsage == cxt->mem_allocated);
	Assert(cxt->trackerGroup->peakUsage == cxt->trackerGroup->currentUsage);
	Assert(cxt->trackerGroup->owner == cxt);
}

static void
check_freed_aset_chunk(MemoryContextPeakTrackerGroup group)
{
#ifdef CLOBBER_FREED_MEMORY
	char		buff[sizeof(MemoryContextPeakTrackerGroupData)];
	/*
	 * after postgres 15 there was changes at memory contexts and beginning
	 * of chunk after "freeing" points to the freelist
	 */
	int link_offset = 0;
#if PG_VERSION_NUM >= 160000
	link_offset = sizeof(void *);
#endif
	memset(buff, 0x7f, sizeof(buff));
	Assert(
		memcmp(
			buff + link_offset,
			(char *) group + link_offset,
			sizeof(buff) - link_offset
		) == 0
	);
#endif
}

static void
check_alloc_and_free(MemoryContext cxt)
{
	MemoryContextPeakTrackerGroup cxtGroup;
	Size		startMem = 0;
	Size		prevCurrent = 0;
	Size		peakVal = 0;
	void	   *ptrSmall,
			   *ptrBig1,
			   *ptrBig2;

	start_peak_usage_and_validate(cxt);

	/* setup */
	cxtGroup = cxt->trackerGroup;
	startMem = cxt->mem_allocated;
	prevCurrent = cxt->trackerGroup->currentUsage;

	/* small size allocation won't trigger new block allocation */
	ptrSmall = MemoryContextAlloc(cxt, 24);
	Assert(prevCurrent == cxtGroup->currentUsage);
	Assert(cxtGroup->currentUsage == cxtGroup->peakUsage);
	prevCurrent = cxtGroup->currentUsage;

	/* check allocation for "big" block */
	ptrBig1 = MemoryContextAlloc(cxt, ALLOCSET_DEFAULT_INITSIZE);
	Assert(prevCurrent < cxtGroup->currentUsage);
	Assert(cxtGroup->currentUsage == cxtGroup->peakUsage);
	prevCurrent = cxtGroup->currentUsage;

	/*
	 * check allocation for "bigger" block so further pfree ASet would remove
	 * it
	 */
	ptrBig2 = MemoryContextAlloc(cxt, ALLOCSET_DEFAULT_INITSIZE * 2);
	Assert(prevCurrent < cxtGroup->currentUsage);
	Assert(cxtGroup->currentUsage == cxtGroup->peakUsage);
	prevCurrent = cxtGroup->currentUsage;
	peakVal = cxtGroup->currentUsage;

	/* check pfree  */
	pfree(ptrBig2);
	Assert(prevCurrent >= cxtGroup->currentUsage);
	if (cxt->type == T_AllocSetContext) /* decrease */
		Assert(prevCurrent > cxtGroup->currentUsage);

	prevCurrent = cxtGroup->currentUsage;

	/* check reset */
	MemoryContextReset(cxt);
	Assert(cxt->trackerGroup == cxtGroup);	/* check group doesn't change
											 * after reset */
	Assert(prevCurrent > cxtGroup->currentUsage);
	Assert(startMem == cxtGroup->currentUsage);
	Assert(peakVal == cxtGroup->peakUsage);

	(void) ptrSmall;
	(void) ptrBig1;
}


/* tests */
Datum
check_peak_usage_propagation(PG_FUNCTION_ARGS)
{
	MemoryContext top,
				mid,
				leaf;
	MemoryContextPeakTrackerGroup topGroup;
	Size		prevPeak = 0;

	/* create top level tracker group ctx */
	top = ASetNew(NULL, "top");
	start_peak_usage_and_validate(top);

	topGroup = top->trackerGroup;
	prevPeak = top->trackerGroup->currentUsage;

	/*
	 * crete child context with new tracker group and check that mem usage
	 * propagated
	 */
	mid = ASetNew(top, "mid");
	start_peak_usage_and_validate(mid);
	Assert(prevPeak + mid->mem_allocated == topGroup->currentUsage);
	Assert(topGroup->currentUsage == topGroup->peakUsage);
	prevPeak = topGroup->currentUsage;

	/*
	 * crete leaf cxt with new tracker group and check that mem usage
	 * propagated
	 */
	leaf = GenerationContextCreate(mid, "leaf", ALLOCSET_DEFAULT_SIZES);
	start_peak_usage_and_validate(leaf);
	Assert(prevPeak + leaf->mem_allocated == topGroup->currentUsage);
	Assert(topGroup->currentUsage == topGroup->peakUsage);
	prevPeak = topGroup->currentUsage;

	/* check that allocation on leaf cxt allocation propagated */
	MemoryContextAlloc(leaf, 12000);
	Assert(prevPeak < topGroup->currentUsage);
	Assert(topGroup->currentUsage == topGroup->peakUsage);
	prevPeak = topGroup->currentUsage;

	/*
	 * reset the middle ctx (leaf would be deleted), and check the decrement
	 * propagated
	 */
	MemoryContextReset(mid);
	Assert(mid->trackerGroup->owner == mid);	/* dummy check */
	Assert(topGroup->peakUsage == prevPeak);
	Assert(topGroup->currentUsage == top->mem_allocated + mid->mem_allocated);
	Assert(topGroup->currentUsage < topGroup->peakUsage);
	Assert(mid->trackerGroup->peakUsage < topGroup->peakUsage);

	MemoryContextDelete(top);
	PG_RETURN_BOOL(true);
}

Datum
check_alloc_and_free_for_context_types(PG_FUNCTION_ARGS)
{
	MemoryContext aset = ASetNew(NULL, "aset");
	MemoryContext gen = GenerationContextCreate(NULL, "generation", ALLOCSET_DEFAULT_SIZES);

	check_alloc_and_free(aset);
	check_alloc_and_free(gen);

	MemoryContextDelete(aset);
	MemoryContextDelete(gen);
	PG_RETURN_BOOL(true);
}

Datum
check_reparent(PG_FUNCTION_ARGS)
{
	/*
	 * reparent non group owner context tree without tracker group to NULL
	 * won't create group
	 */
	{
		MemoryContext root = ASetNew(NULL, "root");
		MemoryContext leaf = ASetNew(root, "leaf");

		Assert(root->trackerGroup == NULL);
		MemoryContextReparentPeakUsage(root, NULL);

		/* check that group wasn't created */
		Assert(root->trackerGroup == NULL);
		Assert(leaf->trackerGroup == NULL);
		MemoryContextDelete(root);
	}

	/*
	 * reparent non group owner context tree to other context without group -
	 * unpins context tree from old group and triggers usage calcultion
	 */
	{
		MemoryContext newRoot = ASetNew(NULL, "new root");
		MemoryContext root = ASetNew(NULL, "root");
		MemoryContext mid,
					leaf;

		start_peak_usage_and_validate(root);

		mid = ASetNew(root, "mid");
		leaf = ASetNew(mid, "leaf");
		Assert(root->trackerGroup == mid->trackerGroup);
		Assert(root->trackerGroup == leaf->trackerGroup);
		Assert(root->trackerGroup->currentUsage ==
			   root->mem_allocated + leaf->mem_allocated + mid->mem_allocated);


		MemoryContextReparentPeakUsage(mid, newRoot);
		Assert(mid->trackerGroup == NULL);
		Assert(leaf->trackerGroup == NULL);
		Assert(root->trackerGroup->currentUsage == root->mem_allocated);

		MemoryContextDelete(root);
		MemoryContextDelete(newRoot);
	}

	/* reparent to the same group - nothing changes */
	{
		MemoryContext root = ASetNew(NULL, "root");
		MemoryContext leaf1,
					leaf2;
		MemoryContextPeakTrackerGroup gr;
		MemoryContextPeakTrackerGroupData grData;

		/* setup */
		start_peak_usage_and_validate(root);
		gr = root->trackerGroup;
		leaf1 = ASetNew(root, "leaf1");
		leaf2 = ASetNew(root, "leaf2");
		grData = *gr;			/* store theby value */

		/* check that after reparenting noting changes */
		MemoryContextReparentPeakUsage(root, root);
		MemoryContextReparentPeakUsage(leaf1, leaf2);
		MemoryContextReparentPeakUsage(leaf1, root);
		Assert(root->trackerGroup == gr);
		Assert(leaf1->trackerGroup == gr);
		Assert(leaf2->trackerGroup == gr);
		Assert(memcmp(&grData, gr, sizeof(grData)) == 0);

		MemoryContextDelete(root);
	}

	/*
	 * reparent non group owner context with group owner (and not) descendants
	 * to new group must perform mem usage recalculation and relink
	 * descendants
	 */
	{
		MemoryContext oldRoot = ASetNew(NULL, "oldRoot");
		MemoryContext newRoot = ASetNew(NULL, "newRoot");
		MemoryContext toReparent,
					cxt1,
					cxt2,
					leaf1,
					leaf2;
		MemoryContextPeakTrackerGroup cxt2Group;
		Size		reparentSize = 0;

		/* setup */
		start_peak_usage_and_validate(newRoot);
		start_peak_usage_and_validate(oldRoot);

		/* init non group owner descendants */
		toReparent = ASetNew(oldRoot, "toReparent");
		cxt1 = ASetNew(toReparent, "cxt1");
		leaf1 = ASetNew(cxt1, "leaf1");

		/* init tracker group owner descendant and it's childs */
		cxt2 = ASetNew(toReparent, "cxt2");
		start_peak_usage_and_validate(cxt2);
		cxt2Group = cxt2->trackerGroup;
		leaf2 = ASetNew(cxt2, "leaf2");

		reparentSize = toReparent->mem_allocated + cxt1->mem_allocated +
			leaf1->mem_allocated + cxt2->trackerGroup->currentUsage;
		Assert(toReparent->trackerGroup == oldRoot->trackerGroup);
		Assert(toReparent->trackerGroup == cxt1->trackerGroup);
		Assert(toReparent->trackerGroup == cxt2->trackerGroup->parentGroup);
		Assert(oldRoot->trackerGroup->currentUsage == oldRoot->mem_allocated + reparentSize);

		MemoryContextReparentPeakUsage(toReparent, newRoot);
		/* check that trackerGroup is changed for non group owner descendants */
		Assert(toReparent->trackerGroup == newRoot->trackerGroup);
		Assert(cxt1->trackerGroup == newRoot->trackerGroup);
		Assert(leaf1->trackerGroup == newRoot->trackerGroup);

		/* check that parentGrpoup is only changed for group owner descendants */
		Assert(cxt2->trackerGroup == cxt2Group);
		Assert(leaf2->trackerGroup == cxt2Group);
		Assert(cxt2->trackerGroup->parentGroup == newRoot->trackerGroup);

		/* check recalculation */
		Assert(newRoot->trackerGroup->currentUsage == newRoot->mem_allocated + reparentSize);
		Assert(oldRoot->trackerGroup->currentUsage == oldRoot->mem_allocated);

		MemoryContextDelete(oldRoot);
		MemoryContextDelete(newRoot);
	}

	/*
	 * reparent group owner context to other parent must recreate group and
	 * relink context childs
	 */
	{
		MemoryContext toReparent = ASetNew(NULL, "toReparent");
		MemoryContext newRoot = ASetNew(NULL, "newRoot");
		MemoryContext cxt1,
					cxt2,
					leaf;
		MemoryContextPeakTrackerGroup oldGroup,
					cxt2Group;
		MemoryContextPeakTrackerGroupData oldGroupData;

		/* setup */
		start_peak_usage_and_validate(toReparent);
		start_peak_usage_and_validate(newRoot);

		cxt1 = ASetNew(toReparent, "cxt1");
		cxt2 = ASetNew(toReparent, "cxt2");
		start_peak_usage_and_validate(cxt2);
		cxt2Group = cxt2->trackerGroup;
		leaf = ASetNew(cxt2, "leaf");

		Assert(toReparent->trackerGroup == cxt1->trackerGroup);
		Assert(toReparent->trackerGroup == cxt2->trackerGroup->parentGroup);

		oldGroup = toReparent->trackerGroup;
		oldGroupData = *oldGroup;

		MemoryContextReparentPeakUsage(toReparent, newRoot);

		/* check that new group was created */
		Assert(oldGroup != toReparent->trackerGroup);
		Assert(toReparent->trackerGroup->owner == toReparent);
		/* check the group is linked to new parent */
		Assert(toReparent->trackerGroup->parentGroup == newRoot->trackerGroup);
		Assert(oldGroupData.currentUsage == toReparent->trackerGroup->currentUsage);
		check_freed_aset_chunk(oldGroup);

		/* check descendants relinked correctly  */
		Assert(cxt1->trackerGroup == toReparent->trackerGroup);
		Assert(cxt2->trackerGroup->parentGroup == toReparent->trackerGroup);
		/* check that nested group doesn't changed links */
		Assert(cxt2->trackerGroup == cxt2Group);
		Assert(leaf->trackerGroup == cxt2Group);

		/*
		 * delete reparent firstly, because only tracker group was reparented
		 * not context itself
		 */
		MemoryContextDelete(toReparent);
		MemoryContextDelete(newRoot);
	}


	/*
	 * reparent context tree (non group owner) to existing group triggers only
	 * usage calculation
	 */
	{
		MemoryContext newRoot = ASetNew(NULL, "new root");
		MemoryContext oldRoot = ASetNew(NULL, "old root");
		MemoryContextPeakTrackerGroup oldGroup,
					newGroup;
		MemoryContext mid,
					leaf;
		Size		diff;

		start_peak_usage_and_validate(newRoot);
		start_peak_usage_and_validate(oldRoot);
		oldGroup = oldRoot->trackerGroup;
		newGroup = newRoot->trackerGroup;
		mid = ASetNew(oldRoot, "mid");
		leaf = ASetNew(mid, "leaf");
		Assert(mid->trackerGroup->owner == oldRoot);

		/* allocated diff after reparent */
		diff = mid->mem_allocated + leaf->mem_allocated;
		MemoryContextReparentPeakUsage(mid, newRoot);
		/* check that group changed without new groups creation */
		Assert(newGroup == newRoot->trackerGroup);
		Assert(newGroup == mid->trackerGroup);
		Assert(newGroup == leaf->trackerGroup);
		Assert(newGroup->currentUsage == newRoot->mem_allocated + diff);
		Assert(oldGroup == oldRoot->trackerGroup);
		Assert(oldGroup->currentUsage == oldRoot->mem_allocated);

		MemoryContextDelete(oldRoot);
		MemoryContextDelete(newRoot);
	}

	/*
	 * real scenario: reparent the context with a group to another context and
	 * remove old parent
	 */
	{
		MemoryContext shortLived = ASetNew(NULL, "short");
		MemoryContext cache = ASetNew(NULL, "cache");
		MemoryContext cxt;
		MemoryContextPeakTrackerGroup oldCxtGroup;

		start_peak_usage_and_validate(shortLived);

		cxt = ASetNew(shortLived, "cxt");
		start_peak_usage_and_validate(cxt);
		oldCxtGroup = cxt->trackerGroup;

		/* check that context is linked to parent group */
		Assert(cxt->trackerGroup->parentGroup == shortLived->trackerGroup);
		Assert(shortLived->trackerGroup->currentUsage ==
			   shortLived->mem_allocated + cxt->mem_allocated);

		oldCxtGroup = cxt->trackerGroup;

		MemoryContextSetParent(cxt, cache);
		/* check used mem decreased */
		Assert(shortLived->trackerGroup->currentUsage == shortLived->mem_allocated);
		/* check that reparented context changed group */
		Assert(cxt->trackerGroup != oldCxtGroup);
		Assert(cxt->trackerGroup->parentGroup == NULL);
		Assert(cxt->trackerGroup->currentUsage == cxt->mem_allocated);

		MemoryContextDelete(shortLived);

		check_freed_aset_chunk(oldCxtGroup);

		MemoryContextDelete(cache);
	}

	PG_RETURN_BOOL(true);
}

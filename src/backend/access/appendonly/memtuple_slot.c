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
 * memtuple_slot.c
 *	  TupleTableSlot implementation for MemTuples.
 *
 * Append-only row tables store tuples in the MemTuple format.  Unlike heap
 * and minimal tuples a MemTuple is not self describing: a MemTupleBinding
 * is required to locate its attributes.  That binding lives here in the
 * slot, which is what lets the slot hand out attributes on demand instead
 * of forcing the scan to expand every column of every tuple up front.
 *
 * Attribute offsets are precomputed in the binding, so each attribute is
 * fetched independently in constant time.  tts_nvalid therefore doubles as
 * the resume point for getsomeattrs, and no separate deforming cursor (the
 * "off" field heap and minimal tuple slots carry) is needed.
 *
 * IDENTIFICATION
 *	    src/backend/access/appendonly/memtuple_slot.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "cdb/cdbvars.h"
#include "foreign/foreign.h"
#include "access/memtup.h"
#include "executor/tuptable.h"

/*
 * Return the binding used to interpret this slot's tuple, creating one from
 * the slot's tuple descriptor if the caller did not supply one.  A binding
 * built here is owned by the slot and released with it.
 */
static MemTupleBinding *
tts_memtuple_bind(MemTupleTableSlot *mslot)
{
	if (mslot->mt_bind == NULL)
	{
		MemoryContext oldcontext;

		oldcontext = MemoryContextSwitchTo(mslot->base.tts_mcxt);
		mslot->mt_bind = create_memtuple_binding(mslot->base.tts_tupleDescriptor);
		MemoryContextSwitchTo(oldcontext);

		mslot->own_bind = true;
	}

	return mslot->mt_bind;
}

static void
tts_memtuple_init(TupleTableSlot *slot)
{
	MemTupleTableSlot *mslot = (MemTupleTableSlot *) slot;

	mslot->tuple = NULL;
	mslot->mt_bind = NULL;
	mslot->own_bind = false;
}

static void
tts_memtuple_release(TupleTableSlot *slot)
{
	MemTupleTableSlot *mslot = (MemTupleTableSlot *) slot;

	if (mslot->own_bind && mslot->mt_bind != NULL)
		destroy_memtuple_binding(mslot->mt_bind);

	mslot->mt_bind = NULL;
	mslot->own_bind = false;
}

static void
tts_memtuple_clear(TupleTableSlot *slot)
{
	MemTupleTableSlot *mslot = (MemTupleTableSlot *) slot;

	if (TTS_SHOULDFREE(slot))
	{
		pfree(mslot->tuple);
		slot->tts_flags &= ~TTS_FLAG_SHOULDFREE;
	}

	slot->tts_nvalid = 0;
	slot->tts_flags |= TTS_FLAG_EMPTY;
	ItemPointerSetInvalid(&slot->tts_tid);
	mslot->tuple = NULL;
}

static void
tts_memtuple_getsomeattrs(TupleTableSlot *slot, int natts)
{
	MemTupleTableSlot *mslot = (MemTupleTableSlot *) slot;
	MemTupleBinding *mt_bind;

	Assert(!TTS_EMPTY(slot));
	Assert(mslot->tuple != NULL);
	Assert(natts <= slot->tts_tupleDescriptor->natts);

	mt_bind = tts_memtuple_bind(mslot);

	/* Resume where the previous call stopped; each fetch is independent. */
	memtuple_deform_range(mslot->tuple, mt_bind, slot->tts_nvalid, natts,
						  slot->tts_values, slot->tts_isnull);

	slot->tts_nvalid = natts;
}

static Datum
tts_memtuple_getsysattr(TupleTableSlot *slot, int attnum, bool *isnull)
{
	Assert(!TTS_EMPTY(slot));

	/*
	 * A memtuple carries no system attributes, but append-optimized relations
	 * are still queried for gp_segment_id and gp_foreign_server, which are
	 * properties of the segment rather than of the tuple.
	 */
	if (attnum == GpSegmentIdAttributeNumber)
	{
		*isnull = false;

		return Int32GetDatum(GpIdentity.segindex);
	}
	else if (attnum == GpForeignServerAttributeNumber)
	{
		*isnull = false;

		return ObjectIdGetDatum(GetForeignServerSegByRelid(slot->tts_tableOid));
	}

	elog(ERROR, "memtuple table slot does not have system attributes");

	return 0;					/* silence compiler warnings */
}

static void
tts_memtuple_materialize(TupleTableSlot *slot)
{
	MemTupleTableSlot *mslot = (MemTupleTableSlot *) slot;
	MemoryContext oldcontext;

	Assert(!TTS_EMPTY(slot));

	/* Nothing outside the slot is left to depend on. */
	if (TTS_SHOULDFREE(slot) && mslot->own_bind)
		return;

	oldcontext = MemoryContextSwitchTo(slot->tts_mcxt);

	/*
	 * A borrowed binding belongs to the scan, which frees it in
	 * AppendOnlyExecutorReadBlock_Finish(), so the tuple would become
	 * unreadable once the scan ends.  Build one of our own, which is
	 * equivalent because both are derived from this tuple descriptor.
	 */
	if (!mslot->own_bind)
	{
		mslot->mt_bind = create_memtuple_binding(slot->tts_tupleDescriptor);
		mslot->own_bind = true;
	}

	if (!TTS_SHOULDFREE(slot))
	{
		if (mslot->tuple == NULL)
			mslot->tuple = memtuple_form(mslot->mt_bind,
										 slot->tts_values,
										 slot->tts_isnull);
		else
			mslot->tuple = memtuple_copy(mslot->tuple);

		slot->tts_flags |= TTS_FLAG_SHOULDFREE;

		/*
		 * Already deformed by-reference attributes point into the tuple we
		 * just replaced, so they have to be fetched again from the copy.
		 */
		slot->tts_nvalid = 0;
	}

	MemoryContextSwitchTo(oldcontext);
}

static void
tts_memtuple_copyslot(TupleTableSlot *dstslot, TupleTableSlot *srcslot)
{
	TupleDesc	srcdesc = srcslot->tts_tupleDescriptor;

	Assert(srcdesc->natts <= dstslot->tts_tupleDescriptor->natts);

	tts_memtuple_clear(dstslot);

	slot_getallattrs(srcslot);

	for (int natt = 0; natt < srcdesc->natts; natt++)
	{
		dstslot->tts_values[natt] = srcslot->tts_values[natt];
		dstslot->tts_isnull[natt] = srcslot->tts_isnull[natt];
	}

	dstslot->tts_nvalid = srcdesc->natts;
	dstslot->tts_flags &= ~TTS_FLAG_EMPTY;

	/* Build a tuple of our own so we do not depend on the source's memory. */
	tts_memtuple_materialize(dstslot);
}

static HeapTuple
tts_memtuple_copy_heap_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	return heap_form_tuple(slot->tts_tupleDescriptor,
						   slot->tts_values,
						   slot->tts_isnull);
}

static MinimalTuple
tts_memtuple_copy_minimal_tuple(TupleTableSlot *slot)
{
	Assert(!TTS_EMPTY(slot));

	slot_getallattrs(slot);

	return heap_form_minimal_tuple(slot->tts_tupleDescriptor,
								   slot->tts_values,
								   slot->tts_isnull);
}

const TupleTableSlotOps TTSOpsMemTuple = {
	.base_slot_size = sizeof(MemTupleTableSlot),
	.init = tts_memtuple_init,
	.release = tts_memtuple_release,
	.clear = tts_memtuple_clear,
	.getsomeattrs = tts_memtuple_getsomeattrs,
	.getsysattr = tts_memtuple_getsysattr,
	.materialize = tts_memtuple_materialize,
	.copyslot = tts_memtuple_copyslot,

	/* A memtuple table slot can not "own" a heap or minimal tuple. */
	.get_heap_tuple = NULL,
	.get_minimal_tuple = NULL,
	.copy_heap_tuple = tts_memtuple_copy_heap_tuple,
	.copy_minimal_tuple = tts_memtuple_copy_minimal_tuple
};

/* --------------------------------
 *		ExecStoreMemTuple
 *
 *		Store a MemTuple into a TTSOpsMemTuple slot.  Nothing is deformed
 *		here; attributes are fetched from the tuple as the executor asks for
 *		them.  'mt_bind' describes the tuple and must stay valid for as long
 *		as the tuple does.  If 'shouldFree' is true the slot takes ownership
 *		of the tuple and frees it when cleared.
 * --------------------------------
 */
TupleTableSlot *
ExecStoreMemTuple(MemTuple mtup,
				  MemTupleBinding *mt_bind,
				  TupleTableSlot *slot,
				  bool shouldFree)
{
	MemTupleTableSlot *mslot = (MemTupleTableSlot *) slot;

	Assert(mtup != NULL);
	Assert(mt_bind != NULL);
	Assert(slot != NULL);
	Assert(slot->tts_tupleDescriptor != NULL);
	Assert(TTS_IS_MEMTUPLE(slot));

	tts_memtuple_clear(slot);

	Assert(!TTS_SHOULDFREE(slot));
	Assert(TTS_EMPTY(slot));

	slot->tts_flags &= ~TTS_FLAG_EMPTY;
	slot->tts_nvalid = 0;
	mslot->tuple = mtup;

	/*
	 * Borrow the scan's binding while the slot has none of its own.  Once the
	 * slot owns one it keeps it: both describe the same tuple descriptor, and
	 * the slot's outlives the scan.
	 */
	if (!mslot->own_bind)
		mslot->mt_bind = mt_bind;

	if (shouldFree)
		slot->tts_flags |= TTS_FLAG_SHOULDFREE;

	return slot;
}

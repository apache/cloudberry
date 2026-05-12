/*-------------------------------------------------------------------------
 *
 * valid.h
 *	  POSTGRES tuple qualification validity definitions.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/valid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef VALID_H
#define VALID_H

#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/tupdesc.h"

/*
 *		HeapKeyTest
 *
 *		Test a heap tuple to see if it satisfies a scan key.
 */
static inline bool
HeapKeyTest(HeapTuple tuple, TupleDesc tupdesc, int nkeys, ScanKey keys)
{
	int			cur_nkeys = nkeys;
	ScanKey		cur_key = keys;

	for (; cur_nkeys--; cur_key++)
	{
		Datum		atp;
		bool		isnull;
		Datum		test;

		if (cur_key->sk_flags & SK_ISNULL)
			return false;

		atp = heap_getattr(tuple, cur_key->sk_attno, tupdesc, &isnull);

		if (isnull)
			return false;

		test = FunctionCall2Coll(&cur_key->sk_func,
								 cur_key->sk_collation,
								 atp, cur_key->sk_argument);

		if (!DatumGetBool(test))
			return false;
	}

	return true;
}

/*
 *		HeapKeyTestUsingSlot
 *
 *		Same as HeapKeyTest but pass a slot instead of a heap tuple.
 *		Eventually we may want to get rid of HeapKeyTest all together
 *		and instead only do the test through a slot, which should be
 *		faster.
 */
#define HeapKeyTestUsingSlot(slot, \
							 nkeys, \
							 keys, \
							 result) \
do \
{ \
	/* Use underscores to protect the variables passed in as parameters */ \
	int			__cur_nkeys = (nkeys); \
	ScanKey		__cur_keys = (keys); \
		\
	(result) = true; /* may change */ \
	for (; __cur_nkeys--; __cur_keys++) \
	{ \
		Datum	__atp; \
		bool	__isnull; \
		Datum	__test; \
\
		if (__cur_keys->sk_flags & SK_ISNULL) \
		{ \
			(result) = false; \
			break; \
		} \
\
		__atp = slot_getattr(slot, \
							 __cur_keys->sk_attno, \
							 &__isnull); \
\
		 if (__isnull) \
		 { \
			 (result) = false; \
				 break; \
		 } \
\
		 __test = FunctionCall2(&__cur_keys->sk_func, \
								__atp, __cur_keys->sk_argument); \
\
		if (!DatumGetBool(__test)) \
		{ \
			(result) = false; \
				break; \
		} \
	} \
} while (0)

#endif							/* VALID_H */

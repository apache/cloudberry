/*-------------------------------------------------------------------------
 *
 * xid8.h
 *	  Header file for the "xid8" ADT.
 *
 * Copyright (c) 2020-2021, PostgreSQL Global Development Group
 *
 * src/include/utils/xid8.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef XID8_H
#define XID8_H

#include "access/transam.h"

#define DatumGetFullTransactionId(X) (FullTransactionIdFromU64(DatumGetUInt64(X)))
#define FullTransactionIdGetDatum(X) (UInt64GetDatum(U64FromFullTransactionId(X)))
#define PG_GETARG_FULLTRANSACTIONID(X) DatumGetFullTransactionId(PG_GETARG_DATUM(X))
#define PG_RETURN_FULLTRANSACTIONID(X) return FullTransactionIdGetDatum(X)

/*
 * Snapshot containing FullTransactionIds.
 * Plugins like zombodb need the pg_snapshot structure, so move it to this header file.
 */
typedef struct
{
	/*
	 * 4-byte length hdr, should not be touched directly.
	 *
	 * Explicit embedding is ok as we want always correct alignment anyway.
	 */
	int32		__varsz;

	uint32		nxip;			/* number of fxids in xip array */
	FullTransactionId xmin;
	FullTransactionId xmax;
	/* in-progress fxids, xmin <= xip[i] < xmax: */
	FullTransactionId xip[FLEXIBLE_ARRAY_MEMBER];
} pg_snapshot;

/*
 * deserialize for zombodb.
 */ 
extern pg_snapshot*  deserialize_snapshot(const char *str);

#endif							/* XID8_H */

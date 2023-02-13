/*-------------------------------------------------------------------------
 *
 * mdb.c
 *	  mdb routines
 *
 * Portions Copyright (c) 1996-2022, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/mdb.c
 *
 *-------------------------------------------------------------------------
 */


#include "postgres.h"
#include "fmgr.h"
#include "utils/fmgrprotos.h"

/*
 * mdb_admin_enabled
 *		Check that mdb locale patch is enabled
 */
Datum
mdb_locale_enabled(PG_FUNCTION_ARGS)
{
	bool res;

#if USE_MDBLOCALES
	res = true;
#else
	res = false;
#endif

	PG_RETURN_BOOL(res);
}

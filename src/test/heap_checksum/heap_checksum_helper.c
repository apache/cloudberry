/*-------------------------------------------------------------------------
*
* heap_checksum_helper.c
*
* DENTIFICATION
*	src/test/heap_checksum/heap_checksum_helper.c
*--------------------------------------------------------------------------
*/
#include "postgres.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "access/xlog.h"
#include "access/xlogutils.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

Datum invalidate_buffers(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(invalidate_buffers);
Datum
invalidate_buffers(PG_FUNCTION_ARGS)
{
	SMgrRelation reln = (SMgrRelation) palloc(sizeof(SMgrRelationData));
	RelFileLocatorBackend  rnodebackend;
	ForkNumber forknum = MAIN_FORKNUM;
	BlockNumber firstDelBlock = 0;

	rnodebackend.locator.spcOid = PG_GETARG_OID(0);
	rnodebackend.locator.dbOid  = PG_GETARG_OID(1);
	rnodebackend.locator.relNumber = PG_GETARG_OID(2);

	rnodebackend.backend = InvalidBackendId; /* not temporary/local */
	Assert(!InRecovery); /* can't be used in recovery mode */
	reln->smgr_rlocator = rnodebackend;

	DropRelFileNodeBuffers(reln, &forknum, 1, &firstDelBlock);
	pfree(reln);

	PG_RETURN_BOOL(true);
}


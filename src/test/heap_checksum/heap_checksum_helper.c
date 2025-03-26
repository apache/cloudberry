/*-------------------------------------------------------------------------
*
* heap_checksum_helper.c
*
* DENTIFICATION
*	src/test/heap_checksum/heap_checksum_helper.c
*--------------------------------------------------------------------------
*/
#include "c.h"
#include "postgres.h"
#include "funcapi.h"
#include "nodes/pg_list.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "access/xlog.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

Datum invalidate_buffers(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(invalidate_buffers);
Datum
invalidate_buffers(PG_FUNCTION_ARGS)
{
	SMgrRelation reln = (SMgrRelation) palloc(sizeof(SMgrRelationData));
	RelFileNodeBackend  rnodebackend;
	ForkNumber forknum = MAIN_FORKNUM;
	BlockNumber firstDelBlock = 0;

	rnodebackend.node.spcNode = PG_GETARG_OID(0);
	rnodebackend.node.dbNode  = PG_GETARG_OID(1);
	rnodebackend.node.relNode = PG_GETARG_OID(2);

	rnodebackend.backend = InvalidBackendId; /* not temporary/local */
	Assert(!InRecovery); /* can't be used in recovery mode */
	reln->smgr_rnode = rnodebackend;

	DropRelFileNodeBuffers(reln, &forknum, 1, &firstDelBlock);
	pfree(reln);

	PG_RETURN_BOOL(true);
}


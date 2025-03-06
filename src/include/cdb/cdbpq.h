/*-------------------------------------------------------------------------
 *
 * cdbpq.h
 *
 * IDENTIFICATION
 *		src/include/cdb/cdbpq.h
 *-------------------------------------------------------------------------
 */

#ifndef _CDBPQ_H_
#define _CDBPQ_H_

#include "postgres_ext.h"
#include "libpq-int.h"

/* Interface for dispatching stmts from QD to qExecs.
 *
 * See cdbtm.h for valid flag values.
 */
extern int PQsendGpQuery_shared(PGconn       *conn,
								 char         *query,
								 int          query_len,
								 bool         nonblock);
extern PGcmdQueueEntry *pqAllocCmdQueueEntry(PGconn *conn);
extern void pqRecycleCmdQueueEntry(PGconn *conn, PGcmdQueueEntry *entry);
extern void pqAppendCmdQueueEntry(PGconn *conn, PGcmdQueueEntry *entry);

/*
 * Handle extend protocol aside from upstream.
 */
extern bool HandleExtendProtocol(PGconn *conn);

#endif

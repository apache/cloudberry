/*-------------------------------------------------------------------------
 *
 * dirtablecmds.h
 *	  prototypes for dirtablecmds.c.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/dirtablecmds.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef DIRTABLECMDS_H
#define DIRTABLECMDS_H

#include "catalog/objectaddress.h"
#include "nodes/params.h"
#include "parser/parse_node.h"

Oid GetDirectoryTableTablespace(CreateDirectoryTableStmt *stmt);
void CreateDirectoryTable(CreateDirectoryTableStmt *stmt, Oid relId, Oid tablespaceId);

#endif //DIRTABLECMDS_H

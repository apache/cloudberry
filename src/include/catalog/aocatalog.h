/*-------------------------------------------------------------------------
 *
 * aocatalog.h
 *
 * Helper function to support the creation of
 * append-only auxiliary relation as block directories and visimaps.
 *
 * Copyright (c) 2013-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/aocatalog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef AOCATALOG_H
#define AOCATALOG_H

#include "catalog/heap.h"
#include "catalog/index.h"

/*
 * Convenient function to get the index of the AO/CO auxiliary tables.
 * Currently, this is only used for aoblkdir and aovisimap relations
 * which have one and only one index. Asserting the same in order to
 * make sure the caller knows what they are doing.
 *
 * GPDB_PORT: taken from upstream c16943a6e1, which cbdb does not carry (that
 * commit drops pg_appendonly.visimapidxid/blkdiridxid, a catalog change well
 * outside the scope of this feature).  The helper itself only walks the
 * relation's index list, so it is safe to have on its own.
 */
static inline Oid
AppendonlyGetAuxIndex(Relation rel)
{
	List	   *oids;

	oids = RelationGetIndexList(rel);
	Assert(oids->length == 1);

	return linitial_oid(oids);
}

extern bool CreateAOAuxiliaryTable(Relation rel,
								   const char *auxiliaryNamePrefix,
								   char relkind,
								   TupleDesc tupledesc,
								   IndexInfo  *indexInfo,
								   List *indexColNames,
								   Oid	*classObjectId,
								   int16 *coloptions);

extern bool IsAppendonlyMetadataRelkind(const char relkind);

extern void NewRelationCreateAOAuxTables(Oid relOid, bool createBlkDir);
#endif   /* AOCATALOG_H */

/*-------------------------------------------------------------------------
 *
 * catalog.h
 *	  prototypes for functions in backend/catalog/catalog.c
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/catalog.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CATALOG_H
#define CATALOG_H

#include "catalog/pg_class.h"
#include "storage/relfilelocator.h"
#include "utils/relcache.h"

/*
 * This file is used to store internal configuration information specific to a
 * server that's not same between primary and mirror pair. For example it
 * stores gp_dbid, which is different for primary and mirror pair, even if
 * contentid is same for them. This file is not copied over during
 * pg_basebackup and pg_rewind to mirror from primary.
 */
#define GP_INTERNAL_AUTO_CONF_FILE_NAME "internal.auto.conf"

extern bool IsSystemRelation(Relation relation);
extern bool IsToastRelation(Relation relation);
extern bool IsCatalogRelation(Relation relation);
extern bool IsInplaceUpdateRelation(Relation relation);

extern bool IsSystemClass(Oid relid, Form_pg_class reltuple);
extern bool IsToastClass(Form_pg_class reltuple);
extern bool IsSystemClassByRelid(Oid relid);

extern bool IsCatalogRelationOid(Oid relid);
extern bool IsInplaceUpdateOid(Oid relid);

extern bool IsCatalogNamespace(Oid namespaceId);
extern bool IsToastNamespace(Oid namespaceId);
extern bool IsAoSegmentNamespace(Oid namespaceId);
extern bool IsExtAuxNamespace(Oid namespaceId);

extern bool IsReservedName(const char *name);
extern bool IsReservedGpName(const char *name);
extern char* GetReservedPrefix(const char *name);

extern bool IsSharedRelation(Oid relationId);

extern Oid GetNewOidWithIndex(Relation relation, Oid indexId,
							  AttrNumber oidcolumn);

extern void reldir_and_filename(RelFileLocator rnode, BackendId backend, ForkNumber forknum,
					char **dir, char **filename);
extern char *aorelpathbackend(RelFileLocator node, BackendId backend, int32 segno);

extern bool system_relation_modified;

extern bool IsPinnedObject(Oid classId, Oid objectId);

extern Oid	GetNewOidWithIndex(Relation relation, Oid indexId,
							   AttrNumber oidcolumn);
extern RelFileNumber GetNewRelFileNumber(Oid reltablespace,
										 Relation pg_class,
										 char relpersistence);

#endif							/* CATALOG_H */

/*-------------------------------------------------------------------------
 *
 * laketablecmds.h
 *	  prototypes for laketablecmds.c.
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/commands/laketablecmds.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LAKETABLECMDS_H
#define LAKETABLECMDS_H

#include "catalog/pg_lake_table.h"
#include "nodes/parsenodes.h"
#include "utils/guc.h"
#include "utils/rel.h"

/*
 * Name of the table access method lake tables are created with.  The
 * kernel only provides the DDL scaffolding; the access method itself is
 * provided by a datalake extension.
 */
#define ICEBERG_TABLE_AM_NAME	"iceberg"

/* GUC variables */
extern char *iceberg_default_catalog;
extern char *iceberg_default_volume;

/* GUC check hooks */
extern bool check_iceberg_default_catalog(char **newval, void **extra, GucSource source);
extern bool check_iceberg_default_volume(char **newval, void **extra, GucSource source);

/* Functions to get default values */
extern const char *GetDefaultIcebergCatalog(void);
extern const char *GetDefaultIcebergVolume(void);

/* Lake table management */
extern Oid	GetIcebergTableAmOid(bool missing_ok);
extern bool RelationIsIcebergTable(Relation rel);
extern void ValidateLakeTableOptions(CreateLakeTableStmt *stmt);
extern void CreateLakeTable(CreateLakeTableStmt *stmt, Oid relId);
extern void RemoveLakeTableEntry(Oid relid);

#endif							/* LAKETABLECMDS_H */

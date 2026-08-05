/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * laketablecmds.h
 *	  prototypes for laketablecmds.c.
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
extern bool RelationIsLakeTable(Relation rel);
extern void ValidateLakeTableStmt(CreateLakeTableStmt *stmt);
extern void CreateLakeTable(CreateLakeTableStmt *stmt, Oid relId);
extern void RemoveLakeTableEntry(Oid relid);

#endif							/* LAKETABLECMDS_H */

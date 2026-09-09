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
 * pg_iceberg_options.h
 *	  How a lake table names its catalog and volume, and how that is read back.
 *
 * A relation using this access method holds no data locally: its metadata lives
 * in an external Iceberg catalog and its files live on external storage.  The
 * mapping from the relation to the two foreign servers that describe those
 * places is therefore part of the table definition, and every operation on the
 * table starts by reading it.
 *
 * pg_iceberg_get_table_info() is that read.  It is the only place that knows
 * where the mapping is stored -- here, the relation's own reloptions -- so the
 * storage can be reconsidered later without touching a single caller.
 *
 * Its signature and result types are those of the reference implementation: the
 * existing implementation of this feature that this work derives from and is
 * meant to replace, which keeps the same mapping in a system catalog of its own
 * that an extension cannot add.  The difference stops inside this function, and
 * code written against either one compiles against the other.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_options.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_ICEBERG_OPTIONS_H
#define PG_ICEBERG_OPTIONS_H

#ifdef __cplusplus
/*
 * IcebergTableOptions.namespace carries the reference implementation's field
 * name, which is a keyword in C++.  Keeping the name is what lets that code
 * move here unchanged; the cost is that this header is C-only.  A C++ layer
 * that needs the mapping should be given an accessor rather than this struct.
 */
#error "pg_iceberg_options.h is C-only; see the comment above this #error"
#endif

#include "postgres.h"

#include "common/datalake_location.h"
#include "common/dl_err.h"
#include "meta/iceberg_meta_engine.h"
#include "utils/relcache.h"

/*
 * Reference implementation: IcebergTableOptions.
 *
 * Deferred, names kept for the port: autovacuum_enabled, compression,
 * compression_level -- all of them write-path options.
 */
typedef struct IcebergTableOptions
{
	char	   *catalog;		/* catalog name within the external catalog */
	char	   *namespace;		/* namespace within the external catalog */
	char	   *table;			/* table name within the external catalog */
	char	   *location;		/* optional location override, NULL when the
								 * volume's own base path applies */

	/*
	 * Whether DROP TABLE should delete the lake data as well.  No counterpart
	 * in the reference implementation, which deletes it unconditionally; here
	 * the default is not to, so that dropping a reference cannot destroy data
	 * another reader still expects to find.
	 */
	bool		purge_on_drop;
} IcebergTableOptions;

/*
 * Reference implementation: IcebergTableInfo.
 *
 * The reference implementation distinguishes a catalog object from the server
 * hosting it, and likewise for volumes.  An extension cannot add catalog or
 * volume objects to the system catalogs, so here a server names exactly one of
 * each and catalog_name/volume_name fall back to the server name.  Keeping all
 * four fields means a caller that reads either one still gets a usable answer.
 *
 * Fields below the marker have no counterpart in the reference implementation.
 * They stay at the end so that the shared prefix keeps its layout.
 */
typedef struct IcebergTableInfo
{
	char	   *catalog_name;
	char	   *catalog_server_name;
	char	   *volume_name;
	char	   *volume_server_name;
	IcebergTableOptions *opts;

	/* --- extension-only, keep last --- */
	Oid			catalog_srvid;	/* for USAGE checks and dependency records */
	Oid			volume_srvid;
	DatalakeLocation volume_location;	/* parsed once from the volume server */
	MetaKv	   *catalog_props;	/* catalog server options, non-secret only */
	int			n_catalog_props;
} IcebergTableInfo;

extern void pg_iceberg_register_reloptions(void);
extern bytea *pg_iceberg_amoptions(Datum reloptions, char relkind,
								   bool validate);

/*
 * Reference implementation: pg_iceberg_get_table_info().  Errors out when relid
 * is not a lake table with a resolvable mapping.
 *
 * The _rel variant is what this tree calls: reading reloptions needs the
 * relation anyway, so a caller holding one should not pay for a second open.
 * Callers reached from object access hooks must use it -- see the note in
 * pg_iceberg_options.c.
 */
extern IcebergTableInfo *pg_iceberg_get_table_info(Oid relid);
extern IcebergTableInfo *pg_iceberg_get_table_info_rel(Relation rel);
extern void pg_iceberg_free_table_info(IcebergTableInfo *info);

/*
 * Record the dependency rows that make the mapping durable.  Named after the
 * reference implementation's catalog-side equivalent so that the two lifecycles
 * read alike.  There is no removal counterpart: both the reloptions and these
 * rows belong to the relation, so the delete that removes it removes them too.
 */
extern void InsertLakeTableEntry(Oid relid, const IcebergTableInfo *info);

/* OID of this extension's access method, InvalidOid when it does not exist. */
extern Oid pg_iceberg_am_oid(void);

extern MetaKv *pg_iceberg_resolve_credentials(Oid serverid, Oid auth_userid,
											  int *n_props);
extern void pg_iceberg_check_server_usage(Oid serverid);
extern DlErrCode pg_iceberg_parse_location(const char *uri,
										   const char *endpoint,
										   const char *region,
										   DatalakeLocation *out,
										   char **errdetail);
extern Oid pg_iceberg_catalog_fdw_oid(bool missing_ok);
extern Oid pg_iceberg_volume_fdw_oid(bool missing_ok);

#endif							/* PG_ICEBERG_OPTIONS_H */

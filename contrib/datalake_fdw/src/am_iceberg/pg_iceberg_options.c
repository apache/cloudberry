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
 * pg_iceberg_options.c
 *	  How a lake table names its catalog and volume, and how that is read back.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_options.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/relation.h"
#include "access/reloptions.h"
#include "am_iceberg/pg_iceberg_options.h"
#include "catalog/dependency.h"
#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "iceberg_catalog_fdw/iceberg_catalog_option.h"
#include "iceberg_volume_fdw/iceberg_volume_option.h"
#include "miscadmin.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

typedef struct IcebergRelOptions
{
	int32		vl_len_;
	int			catalog_off;
	int			volume_off;
	int			fileformat_off;
	bool		purge_on_drop;
} IcebergRelOptions;

static relopt_kind iceberg_relopt_kind;

static char *iceberg_relopt_string(IcebergRelOptions *opts, int off);
static MetaKv *defelems_to_kvs(List *options, int *n_props);
static DlErrCode invalid_location(char **errdetail, char *detail);
static bool s3_bucket_alnum(char ch);
static bool s3_bucket_char(char ch);

/*
 * Register a private reloption kind for iceberg table access-method options.
 */
void
pg_iceberg_register_reloptions(void)
{
	if (iceberg_relopt_kind != 0)
		return;

	iceberg_relopt_kind = add_reloption_kind();

	add_string_reloption(iceberg_relopt_kind, "catalog",
						 "iceberg catalog foreign server name", "", NULL,
						 AccessExclusiveLock);
	add_string_reloption(iceberg_relopt_kind, "volume",
						 "iceberg volume foreign server name", "", NULL,
						 AccessExclusiveLock);
	add_string_reloption(iceberg_relopt_kind, "fileformat",
						 "iceberg data file format", "parquet", NULL,
						 AccessExclusiveLock);

	/*
	 * Dropping the table means dropping this database's reference to it; the
	 * data belongs to the lake and stays there.  Deleting it as well has to be
	 * asked for, and the answer belongs to the table rather than to a session:
	 * a setting could make the same DROP destroy data or not depending on who
	 * typed it, and a reloption travels with the table into a dump.
	 */
	add_bool_reloption(iceberg_relopt_kind, "purge_on_drop",
					   "delete the lake data when the table is dropped",
					   false, AccessExclusiveLock);
}

bytea *
pg_iceberg_amoptions(Datum reloptions, char relkind, bool validate)
{
	static const relopt_parse_elt tab[] = {
		{"catalog", RELOPT_TYPE_STRING,
		 offsetof(IcebergRelOptions, catalog_off)},
		{"volume", RELOPT_TYPE_STRING,
		 offsetof(IcebergRelOptions, volume_off)},
		{"fileformat", RELOPT_TYPE_STRING,
		 offsetof(IcebergRelOptions, fileformat_off)},
		{"purge_on_drop", RELOPT_TYPE_BOOL,
		 offsetof(IcebergRelOptions, purge_on_drop)}
	};

	Assert(iceberg_relopt_kind != 0);

	/*
	 * The option set does not depend on relkind; the same mapping applies to
	 * every relation kind that can carry this access method.
	 */

	/*
	 * Whether the named servers exist is deliberately not checked here:
	 * amoptions runs in relcache and utility contexts where such lookups are
	 * unsafe or premature.  The use points check instead.
	 */
	return (bytea *) build_reloptions(reloptions, validate,
									 iceberg_relopt_kind,
									 sizeof(IcebergRelOptions),
									 tab, lengthof(tab));
}

/*
 * rd_options belongs to the relcache.  Every returned string is copied into
 * the caller's current memory context; callers must never retain a pointer
 * into rd_options itself.
 */
static char *
iceberg_relopt_string(IcebergRelOptions *opts, int off)
{
	if (opts == NULL || off == 0)
		return NULL;

	return pstrdup(((char *) opts) + off);
}

static MetaKv *
defelems_to_kvs(List *options, int *n_props)
{
	MetaKv	   *props;
	ListCell   *lc;
	int			i = 0;
	int			count = list_length(options);

	*n_props = count;
	if (count == 0)
		return NULL;

	props = palloc0(sizeof(MetaKv) * count);
	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		props[i].key = pstrdup(def->defname);
		props[i].value = pstrdup(defGetString(def));
		i++;
	}

	return props;
}

Oid
pg_iceberg_catalog_fdw_oid(bool missing_ok)
{
	ForeignDataWrapper *fdw;

	fdw = GetForeignDataWrapperByName("iceberg_catalog_fdw", missing_ok);
	return fdw == NULL ? InvalidOid : fdw->fdwid;
}

Oid
pg_iceberg_volume_fdw_oid(bool missing_ok)
{
	ForeignDataWrapper *fdw;

	fdw = GetForeignDataWrapperByName("iceberg_volume_fdw", missing_ok);
	return fdw == NULL ? InvalidOid : fdw->fdwid;
}

/*
 * OID of this extension's access method, or InvalidOid while it does not
 * exist.
 *
 * Missing-ok because the AM is absent while CREATE EXTENSION is still
 * installing it.  The result is deliberately not cached: DROP EXTENSION
 * followed by CREATE EXTENSION produces a new OID, and a stale cached one
 * would make callers silently treat lake tables as ordinary relations.  The
 * lookup is syscache-backed.
 */
Oid
pg_iceberg_am_oid(void)
{
	return get_table_am_oid("iceberg", true);
}

IcebergTableInfo *
pg_iceberg_get_table_info_rel(Relation rel)
{
	IcebergRelOptions *opts;
	IcebergTableInfo *info;
	IcebergCatalogOptions *catalog_options;
	IcebergVolumeOptions *volume_options;
	ForeignServer *catalog_server;
	ForeignServer *volume_server;
	char	   *catalog_server_name;
	char	   *volume_server_name;
	char	   *parse_detail = NULL;
	DlErrCode	parse_result;
	Oid			amoid;

	/*
	 * rd_options is only an IcebergRelOptions when this access method put it
	 * there.  Every other access method has its own layout -- a heap's
	 * StdRdOptions would present fillfactor and toast_tuple_target where the
	 * string offsets are read below, and pstrdup() would then run off the
	 * allocation.  The callers in this module check the access method before
	 * getting here, but this function is reachable from anywhere, so it cannot
	 * rely on that.
	 */
	amoid = pg_iceberg_am_oid();
	if (!OidIsValid(amoid) || rel->rd_rel->relam != amoid)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("\"%s\" is not an iceberg table",
						RelationGetRelationName(rel))));

	opts = (IcebergRelOptions *) rel->rd_options;

	catalog_server_name = iceberg_relopt_string(opts,
											   opts == NULL ? 0 : opts->catalog_off);
	volume_server_name = iceberg_relopt_string(opts,
											   opts == NULL ? 0 : opts->volume_off);

	/*
	 * fileformat is a valid option and is persisted, but nothing consumes it
	 * until the format layer exists, so the result does not carry it.
	 */

	if (catalog_server_name == NULL || catalog_server_name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("iceberg table \"%s\" has no catalog binding",
						RelationGetRelationName(rel)),
				 errhint("Specify WITH (catalog = '...', volume = '...'), or set "
						 "iceberg.default_catalog and iceberg.default_volume "
						 "before CREATE TABLE.")));

	if (volume_server_name == NULL || volume_server_name[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("iceberg table \"%s\" has no volume binding",
						RelationGetRelationName(rel)),
				 errhint("Specify WITH (catalog = '...', volume = '...'), or set "
						 "iceberg.default_catalog and iceberg.default_volume "
						 "before CREATE TABLE.")));

	catalog_server = GetForeignServerByName(catalog_server_name, false);
	if (catalog_server->fdwid != pg_iceberg_catalog_fdw_oid(false))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("server \"%s\" is not an iceberg catalog server",
						catalog_server_name)));

	volume_server = GetForeignServerByName(volume_server_name, false);
	if (volume_server->fdwid != pg_iceberg_volume_fdw_oid(false))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("server \"%s\" is not an iceberg volume server",
						volume_server_name)));

	catalog_options = get_iceberg_catalog_options(catalog_server);
	volume_options = get_iceberg_volume_options(volume_server);

	info = palloc0(sizeof(IcebergTableInfo));
	info->catalog_name = pstrdup(catalog_options->foreign_catalog.catalog_name);
	info->catalog_server_name = catalog_server_name;
	info->volume_name = pstrdup(volume_server->servername);
	info->volume_server_name = volume_server_name;

	info->opts = palloc0(sizeof(IcebergTableOptions));

	/*
	 * Copied rather than aliased to info->catalog_name: the two fields are
	 * released independently.
	 */
	info->opts->catalog = pstrdup(info->catalog_name);
	info->opts->namespace = get_namespace_name(RelationGetNamespace(rel));
	info->opts->table = pstrdup(RelationGetRelationName(rel));
	info->opts->location = NULL;
	info->opts->purge_on_drop = opts != NULL && opts->purge_on_drop;

	info->catalog_srvid = catalog_server->serverid;
	info->volume_srvid = volume_server->serverid;
	info->catalog_props = defelems_to_kvs(catalog_server->options,
										 &info->n_catalog_props);

	parse_result = pg_iceberg_parse_location(volume_options->foreign_volume.base_path,
											 volume_options->volume_server.endpoint,
											 volume_options->volume_server.region,
											 &info->volume_location,
											 &parse_detail);
	if (parse_result != DL_OK)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("iceberg volume server \"%s\" has an invalid %s",
						volume_server_name,
						DATALAKE_ICEBERG_VOLUME_BASE_PATH),
				 errdetail("%s", parse_detail)));

	/*
	 * Credential resolution intentionally does not happen here.  Stub and
	 * DDL-only paths must be able to describe a table with zero credentials.
	 */
	return info;
}

/*
 * Same result from a relation OID.
 *
 * Not for use from an object access hook: while a relation is being dropped its
 * relcache entry may already be gone, and a hook has to decide what to do about
 * that rather than error out.  Those callers open the relation themselves and
 * use pg_iceberg_get_table_info_rel().
 */
IcebergTableInfo *
pg_iceberg_get_table_info(Oid relid)
{
	Relation	rel;
	IcebergTableInfo *info;

	rel = try_relation_open(relid, AccessShareLock, false);
	if (rel == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("lake table entry not found for relation %u", relid)));

	info = pg_iceberg_get_table_info_rel(rel);
	relation_close(rel, AccessShareLock);

	return info;
}

void
pg_iceberg_free_table_info(IcebergTableInfo *info)
{
	int			i;

	if (info == NULL)
		return;

	if (info->catalog_name)
		pfree(info->catalog_name);
	if (info->catalog_server_name)
		pfree(info->catalog_server_name);
	if (info->volume_name)
		pfree(info->volume_name);
	if (info->volume_server_name)
		pfree(info->volume_server_name);

	if (info->opts)
	{
		if (info->opts->catalog)
			pfree(info->opts->catalog);
		if (info->opts->namespace)
			pfree(info->opts->namespace);
		if (info->opts->table)
			pfree(info->opts->table);
		if (info->opts->location)
			pfree(info->opts->location);
		pfree(info->opts);
	}

	/*
	 * The parsed location and the property array are allocated by this module
	 * too; a destructor that released only the names would let a statement
	 * resolving many tables accumulate the rest until its context is reset.
	 */
	if (info->volume_location.scheme)
		pfree(info->volume_location.scheme);
	if (info->volume_location.authority)
		pfree(info->volume_location.authority);
	if (info->volume_location.path_prefix)
		pfree(info->volume_location.path_prefix);
	if (info->volume_location.endpoint)
		pfree(info->volume_location.endpoint);
	if (info->volume_location.region)
		pfree(info->volume_location.region);

	for (i = 0; i < info->n_catalog_props; i++)
	{
		if (info->catalog_props[i].key)
			pfree(info->catalog_props[i].key);
		if (info->catalog_props[i].value)
			pfree(info->catalog_props[i].value);
	}
	if (info->catalog_props)
		pfree(info->catalog_props);

	pfree(info);
}

/*
 * Make the mapping enforceable by recording what the table depends on.
 *
 * The mapping itself needs no insertion: it is part of the relation, written by
 * the CREATE TABLE that produced it.  What has to be added is the pair of
 * dependency rows that stop either server from being dropped out from under the
 * table, and that carry it along when one is dropped with CASCADE.  Recorded on
 * the dispatcher and on every segment, so that DROP SERVER is refused locally on
 * whichever node first evaluates it.
 *
 * There is deliberately no RemoveLakeTableEntry() counterpart: both the
 * reloptions and these rows belong to the relation, so they are removed by the
 * same delete that removes it.
 */
void
InsertLakeTableEntry(Oid relid, const IcebergTableInfo *info)
{
	ObjectAddress table;
	ObjectAddress server;

	ObjectAddressSet(table, RelationRelationId, relid);

	ObjectAddressSet(server, ForeignServerRelationId, info->catalog_srvid);
	recordDependencyOn(&table, &server, DEPENDENCY_NORMAL);

	ObjectAddressSet(server, ForeignServerRelationId, info->volume_srvid);
	recordDependencyOn(&table, &server, DEPENDENCY_NORMAL);
}

MetaKv *
pg_iceberg_resolve_credentials(Oid serverid, Oid auth_userid, int *n_props)
{
	HeapTuple	tuple;
	Datum		options_datum;
	bool		isnull;
	List	   *options;
	MetaKv	   *props;

	Assert(n_props != NULL);
	*n_props = 0;

	/*
	 * GetUserMapping() cannot be used here because it ereports when neither a
	 * user-specific nor PUBLIC mapping exists.  User mappings are optional in
	 * v1, so perform the same two syscache probes with missing-ok semantics.
	 */
	tuple = SearchSysCache2(USERMAPPINGUSERSERVER,
							ObjectIdGetDatum(auth_userid),
							ObjectIdGetDatum(serverid));
	if (!HeapTupleIsValid(tuple) && OidIsValid(auth_userid))
		tuple = SearchSysCache2(USERMAPPINGUSERSERVER,
								ObjectIdGetDatum(InvalidOid),
								ObjectIdGetDatum(serverid));

	if (!HeapTupleIsValid(tuple))
		return NULL;

	options_datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tuple,
									Anum_pg_user_mapping_umoptions,
									&isnull);
	if (isnull)
	{
		ReleaseSysCache(tuple);
		return NULL;
	}

	options = untransformRelOptions(options_datum);
	props = defelems_to_kvs(options, n_props);
	ReleaseSysCache(tuple);

	return props;
}

void
pg_iceberg_check_server_usage(Oid serverid)
{
	AclResult	aclresult;

	aclresult = object_aclcheck(ForeignServerRelationId, serverid,
								GetUserId(), ACL_USAGE);
	if (aclresult == ACLCHECK_NO_PRIV)
		aclcheck_error(aclresult, OBJECT_FOREIGN_SERVER,
					   GetForeignServer(serverid)->servername);
}

static DlErrCode
invalid_location(char **errdetail, char *detail)
{
	if (errdetail != NULL)
		*errdetail = detail;
	else
		pfree(detail);

	return DL_ERR_INVALID_OPTION;
}

static bool
s3_bucket_alnum(char ch)
{
	return (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9');
}

static bool
s3_bucket_char(char ch)
{
	return s3_bucket_alnum(ch) || ch == '.' || ch == '-';
}

DlErrCode
pg_iceberg_parse_location(const char *uri, const char *endpoint,
						  const char *region, DatalakeLocation *out,
						  char **errdetail)
{
	const char *scheme_end;
	const char *authority_start;
	const char *path_start;
	Size		scheme_len;
	Size		authority_len;
	Size		path_len;
	bool		is_s3;
	Size		i;

	Assert(out != NULL);
	memset(out, 0, sizeof(*out));
	if (errdetail != NULL)
		*errdetail = NULL;

	if (uri == NULL)
		return invalid_location(errdetail,
								pstrdup("location URI is null"));

	scheme_end = strstr(uri, "://");
	if (scheme_end == NULL)
		return invalid_location(errdetail,
								psprintf("location URI \"%s\" is missing \"://\"",
										 uri));

	scheme_len = scheme_end - uri;
	is_s3 = scheme_len == strlen(DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_S3) &&
		strncmp(uri, DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_S3, scheme_len) == 0;
	if (!is_s3 &&
		!(scheme_len == strlen(DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_HDFS) &&
		  strncmp(uri, DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_HDFS, scheme_len) == 0))
		return invalid_location(errdetail,
								psprintf("location URI \"%s\" has unsupported scheme; expected %s or %s",
										 uri,
										 DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_S3,
										 DATALAKE_ICEBERG_VOLUME_SERVER_TYPE_HDFS));

	if (strchr(uri, '?') != NULL)
		return invalid_location(errdetail,
								psprintf("location URI \"%s\" must not contain a query",
										 uri));
	if (strchr(uri, '#') != NULL)
		return invalid_location(errdetail,
								psprintf("location URI \"%s\" must not contain a fragment",
										 uri));

	authority_start = scheme_end + 3;
	path_start = strchr(authority_start, '/');
	authority_len = path_start == NULL ?
		strlen(authority_start) : (Size) (path_start - authority_start);

	if (authority_len == 0)
		return invalid_location(errdetail,
								psprintf("location URI \"%s\" has an empty authority",
										 uri));
	if (memchr(authority_start, '@', authority_len) != NULL)
		return invalid_location(errdetail,
								psprintf("location URI \"%s\" must not contain userinfo",
										 uri));

	if (is_s3)
	{
		if (authority_len < 3 || authority_len > 63)
			return invalid_location(errdetail,
									psprintf("s3 bucket in location URI \"%s\" must be 3 to 63 characters",
											 uri));
		if (!s3_bucket_alnum(authority_start[0]) ||
			!s3_bucket_alnum(authority_start[authority_len - 1]))
			return invalid_location(errdetail,
									psprintf("s3 bucket in location URI \"%s\" must start and end with a lowercase letter or digit",
											 uri));
		for (i = 0; i < authority_len; i++)
		{
			if (!s3_bucket_char(authority_start[i]))
				return invalid_location(errdetail,
										psprintf("s3 bucket in location URI \"%s\" contains an invalid character",
												 uri));
		}
	}

	path_len = path_start == NULL ? 0 : strlen(path_start);
	while (path_len > 0 && path_start[path_len - 1] == '/')
		path_len--;

	out->schema_version = DATALAKE_LOCATION_SCHEMA_VERSION;
	out->scheme = pnstrdup(uri, scheme_len);
	out->authority = pnstrdup(authority_start, authority_len);
	out->path_prefix = path_len == 0 ?
		pstrdup("") : pnstrdup(path_start, path_len);
	out->endpoint = endpoint == NULL ? NULL : pstrdup(endpoint);
	out->region = region == NULL ? NULL : pstrdup(region);

	return DL_OK;
}

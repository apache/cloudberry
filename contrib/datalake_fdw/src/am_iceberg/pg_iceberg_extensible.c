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
 * pg_iceberg_extensible.c
 *	  Extension initialization and the Iceberg utility guard.
 *
 * IDENTIFICATION
 *	  contrib/datalake_fdw/src/am_iceberg/pg_iceberg_extensible.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/tableam.h"
#include "am_iceberg/pg_iceberg_ddl.h"
#include "am_iceberg/pg_iceberg_guc.h"
#include "am_iceberg/pg_iceberg_options.h"
#include "am_iceberg/pg_iceberg_reject.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_depend.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_namespace.h"
#include "cdb/cdbvars.h"
#include "commands/defrem.h"
#include "common/backend_registry.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "meta/iceberg_meta_engine.h"
#include "meta/meta_engine_init.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "storage/lmgr.h"
#include "tcop/utility.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;

static ProcessUtility_hook_type prev_ProcessUtility_hook;

static bool iceberg_is_effective_am(const char *accessMethod);
static Oid get_rel_relam(Oid relid);
static bool relid_is_iceberg(Oid relid);
static bool server_referenced_by_iceberg(Oid srvid);
static bool rangevar_is_iceberg(RangeVar *relation);
static const char *string_object_name(Node *object);
static Oid lock_object_by_name(Oid classid,
							   Oid (*lookup) (const char *name, bool missing_ok),
							   const char *name, LOCKMODE lockmode);
static bool locked_server_referenced_by_iceberg(const char *servername);
static bool utility_drop_targets_iceberg(DropStmt *stmt);
static bool alter_table_targets_iceberg_am(AlterTableStmt *stmt);
static bool alter_table_is_owner_only(AlterTableStmt *stmt);
static bool database_has_iceberg_table(void);
static bool schema_has_iceberg_table(const char *schemaname);
static bool server_belongs_to_module(const char *servername);
static bool is_module_fdw_name(const char *fdwname);
static const char *find_reloption(List *options, const char *name);
static void lock_create_servers(Oid catalog_srvid, Oid volume_srvid,
								LOCKMODE lockmode);
static void unlock_create_servers(Oid catalog_srvid, Oid volume_srvid,
								  LOCKMODE lockmode);
static void validate_create_binding(const char *catalog_name,
									const char *volume_name);
static void prepare_iceberg_create(CreateStmt *stmt);
static void reject_utility_mode_ddl(const char *subject) pg_attribute_noreturn();
static void reject_targeted_operation(const char *operation);
static void pg_iceberg_ProcessUtility(PlannedStmt *pstmt,
									 const char *queryString,
									 bool readOnlyTree,
									 ProcessUtilityContext context,
									 ParamListInfo params,
									 QueryEnvironment *queryEnv,
									 DestReceiver *dest,
									 QueryCompletion *qc);

/*
 * Resolve an omitted access method exactly as core CREATE TABLE does.  Keep
 * this as the single source of truth for every relation-creating node handled
 * by the hook.
 */
static bool
iceberg_is_effective_am(const char *accessMethod)
{
	if (accessMethod != NULL)
		return strcmp(accessMethod, "iceberg") == 0;
	return default_table_access_method != NULL &&
		strcmp(default_table_access_method, "iceberg") == 0;
}

/*
 * This tree has no lsyscache get_rel_relam() helper, so provide the same
 * missing-ok syscache lookup locally.
 */
static Oid
get_rel_relam(Oid relid)
{
	HeapTuple	tuple;
	Oid			relam = InvalidOid;

	tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	if (HeapTupleIsValid(tuple))
	{
		relam = ((Form_pg_class) GETSTRUCT(tuple))->relam;
		ReleaseSysCache(tuple);
	}

	return relam;
}

static bool
relid_is_iceberg(Oid relid)
{
	Oid			iceberg_am_oid;

	/*
	 * Look the OID up every time instead of caching it: DROP EXTENSION
	 * followed by CREATE EXTENSION hands out a new OID, and a cached one would
	 * make these guards silently stop matching.  The lookup is syscache-backed,
	 * and it must be missing-ok because the predicate is consulted for
	 * arbitrary relations before the extension exists.
	 */
	iceberg_am_oid = get_table_am_oid("iceberg", true);

	return OidIsValid(iceberg_am_oid) && OidIsValid(relid) &&
		get_rel_relam(relid) == iceberg_am_oid;
}

static bool
server_referenced_by_iceberg(Oid srvid)
{
	Relation	depend_rel;
	ScanKeyData keys[2];
	SysScanDesc scan;
	HeapTuple	tuple;
	bool		referenced = false;

	if (!OidIsValid(srvid))
		return false;

	depend_rel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&keys[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ForeignServerRelationId));
	ScanKeyInit(&keys[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(srvid));

	scan = systable_beginscan(depend_rel, DependReferenceIndexId, true,
							  NULL, lengthof(keys), keys);
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Form_pg_depend dependency = (Form_pg_depend) GETSTRUCT(tuple);

		if (dependency->classid == RelationRelationId &&
			relid_is_iceberg(dependency->objid))
		{
			referenced = true;
			break;
		}
	}

	systable_endscan(scan);
	table_close(depend_rel, AccessShareLock);

	return referenced;
}

static bool
rangevar_is_iceberg(RangeVar *relation)
{
	Oid			relid;

	if (relation == NULL)
		return false;

	relid = RangeVarGetRelid(relation, AccessShareLock, true);
	return OidIsValid(relid) && relid_is_iceberg(relid);
}

static const char *
string_object_name(Node *object)
{
	if (object != NULL && IsA(object, String))
		return strVal(object);
	return NULL;
}

/*
 * Resolve a name to an object OID and lock that object exclusively, so that a
 * guard can decide about it without racing the statement it is guarding.
 *
 * The re-resolution is what makes this correct rather than merely locked.
 * Acquiring the lock can mean waiting, and the transactions waited for are free
 * to rename this object away and give its name to a different one.  A guard
 * that skipped the recheck would then hold a lock on an object the statement no
 * longer names, and would scan it instead of the object about to be changed.
 * This is the lookup-lock-recheck loop PostgreSQL applies to relations for the
 * same reason.
 *
 * Returns InvalidOid when the name resolves to nothing, holding no lock.
 */
static Oid
lock_object_by_name(Oid classid,
					Oid (*lookup) (const char *name, bool missing_ok),
					const char *name, LOCKMODE lockmode)
{
	for (;;)
	{
		Oid			objectid = lookup(name, true);

		if (!OidIsValid(objectid))
			return InvalidOid;

		LockDatabaseObject(classid, objectid, 0, lockmode);

		if (lookup(name, true) == objectid)
			return objectid;

		UnlockDatabaseObject(classid, objectid, 0, lockmode);
	}
}

/*
 * The exclusive counterpart of the share lock CREATE takes on the servers it
 * binds to.  ALTER statements name one server, so ascending-OID order is
 * trivial; preserve that ordering if a future statement form locks more than
 * one.  Lock before scanning so the dependency decision cannot race CREATE.
 */
static bool
locked_server_referenced_by_iceberg(const char *servername)
{
	Oid			srvid;

	if (servername == NULL)
		return false;

	srvid = lock_object_by_name(ForeignServerRelationId,
								get_foreign_server_oid, servername,
								AccessExclusiveLock);
	if (!OidIsValid(srvid))
		return false;

	return server_referenced_by_iceberg(srvid);
}

static bool
utility_drop_targets_iceberg(DropStmt *stmt)
{
	ListCell   *lc;

	if (stmt->removeType == OBJECT_TABLE)
	{
		foreach(lc, stmt->objects)
		{
			RangeVar   *relation =
				makeRangeVarFromNameList((List *) lfirst(lc));

			if (rangevar_is_iceberg(relation))
				return true;
		}
	}
	else if (stmt->removeType == OBJECT_FOREIGN_SERVER)
	{
		foreach(lc, stmt->objects)
		{
			Node	   *object = (Node *) lfirst(lc);

			if (locked_server_referenced_by_iceberg(
					string_object_name(object)))
				return true;
		}
	}

	return false;
}

/*
 * Does this statement convert its target into a lake table?
 */
static bool
alter_table_targets_iceberg_am(AlterTableStmt *stmt)
{
	ListCell   *lc;

	foreach(lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		if (cmd->subtype == AT_SetAccessMethod &&
			iceberg_is_effective_am(cmd->name))
			return true;
	}

	return false;
}

/*
 * Does any lake table exist in this database?
 *
 * Used by the statements that name no relation at all, where there is nothing
 * to match against and the only safe answer is to refuse if such a table could
 * be reached.
 */
static bool
database_has_iceberg_table(void)
{
	Relation	class_rel;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple	tuple;
	Oid			iceberg_am_oid;
	bool		found = false;

	iceberg_am_oid = get_table_am_oid("iceberg", true);
	if (!OidIsValid(iceberg_am_oid))
		return false;

	class_rel = table_open(RelationRelationId, AccessShareLock);
	ScanKeyInit(&key,
				Anum_pg_class_relam,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(iceberg_am_oid));
	scan = systable_beginscan(class_rel, InvalidOid, false, NULL, 1, &key);
	if (HeapTupleIsValid(tuple = systable_getnext(scan)))
		found = true;
	systable_endscan(scan);
	table_close(class_rel, AccessShareLock);

	return found;
}

/*
 * Does the named schema contain a lake table?
 *
 * A lake table's namespace is not just where it lives locally: it is the
 * namespace this module reports to the metadata engine.  Renaming the schema
 * would therefore silently repoint the table at a different external namespace,
 * leaving whatever it named before behind.  The check is scoped to schemas that
 * actually contain one, so renaming any other schema stays unaffected.
 */
static bool
schema_has_iceberg_table(const char *schemaname)
{
	Relation	class_rel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tuple;
	Oid			iceberg_am_oid;
	Oid			namespace_oid;
	bool		found = false;

	if (schemaname == NULL)
		return false;

	iceberg_am_oid = get_table_am_oid("iceberg", true);
	if (!OidIsValid(iceberg_am_oid))
		return false;

	/*
	 * The exclusive counterpart of the share lock CREATE takes on its target
	 * namespace, same as the server guard: without it a concurrent CREATE could
	 * add a lake table to this schema after the scan below and before the
	 * rename runs, and that table would then live in the renamed schema while
	 * the metadata engine had already been told the old name.
	 */
	namespace_oid = lock_object_by_name(NamespaceRelationId,
										get_namespace_oid, schemaname,
										AccessExclusiveLock);
	if (!OidIsValid(namespace_oid))
		return false;

	class_rel = table_open(RelationRelationId, AccessShareLock);
	ScanKeyInit(&key[0],
				Anum_pg_class_relam,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(iceberg_am_oid));
	ScanKeyInit(&key[1],
				Anum_pg_class_relnamespace,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(namespace_oid));
	scan = systable_beginscan(class_rel, InvalidOid, false, NULL, 2, key);
	if (HeapTupleIsValid(tuple = systable_getnext(scan)))
		found = true;
	systable_endscan(scan);
	table_close(class_rel, AccessShareLock);

	return found;
}

/*
 * Does this ALTER TABLE do nothing but change the owner?
 *
 * Every other form is refused while the access method is unfinished, but this
 * one has to go through: pg_dump writes ALTER TABLE ... OWNER TO for every
 * table it dumps, so refusing it means refusing to restore a dump this module
 * produced.  Ownership is local catalog state -- it cannot reach the external
 * table or change what the mapping resolves to -- so letting it through costs
 * nothing that the refusal was protecting.
 */
static bool
alter_table_is_owner_only(AlterTableStmt *stmt)
{
	ListCell   *lc;

	if (stmt->cmds == NIL)
		return false;

	foreach(lc, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(lc);

		if (cmd->subtype != AT_ChangeOwner)
			return false;
	}

	return true;
}

/*
 * Is this a server belonging to one of this module's wrappers?
 *
 * Distinct from "referenced by a lake table": a server can be mutated before
 * any table names it, and that is exactly the window in which the coordinator
 * and a segment can be left holding different options for the same server name.
 */
static bool
server_belongs_to_module(const char *servername)
{
	ForeignServer *server;
	Oid			catalog_fdw;
	Oid			volume_fdw;

	if (servername == NULL)
		return false;

	server = GetForeignServerByName(servername, true);
	if (server == NULL)
		return false;

	catalog_fdw = pg_iceberg_catalog_fdw_oid(true);
	volume_fdw = pg_iceberg_volume_fdw_oid(true);

	return (OidIsValid(catalog_fdw) && server->fdwid == catalog_fdw) ||
		(OidIsValid(volume_fdw) && server->fdwid == volume_fdw);
}

/*
 * Is this one of the two wrappers this extension registers?
 *
 * Mappings name their servers, and a server is resolved back to its wrapper by
 * the wrapper's name.  Renaming one therefore breaks every mapping lookup at
 * once -- including on the DROP path, which then cannot tell the metadata engine
 * anything.  Refused whether or not a table exists yet, because the breakage is
 * in the lookup rather than in any particular table.
 */
static bool
is_module_fdw_name(const char *fdwname)
{
	return fdwname != NULL &&
		(strcmp(fdwname, "iceberg_catalog_fdw") == 0 ||
		 strcmp(fdwname, "iceberg_volume_fdw") == 0);
}

static const char *
find_reloption(List *options, const char *name)
{
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (strcmp(def->defname, name) == 0)
			return defGetString(def);
	}

	return NULL;
}

static void
lock_create_servers(Oid catalog_srvid, Oid volume_srvid, LOCKMODE lockmode)
{
	/*
	 * Ascending OID order, so that two CREATEs naming the same pair in opposite
	 * order cannot deadlock.  ALTER-side server guards take the
	 * AccessExclusiveLock counterpart before scanning pg_depend.
	 */
	if (catalog_srvid < volume_srvid)
	{
		LockDatabaseObject(ForeignServerRelationId, catalog_srvid, 0, lockmode);
		LockDatabaseObject(ForeignServerRelationId, volume_srvid, 0, lockmode);
	}
	else if (volume_srvid < catalog_srvid)
	{
		LockDatabaseObject(ForeignServerRelationId, volume_srvid, 0, lockmode);
		LockDatabaseObject(ForeignServerRelationId, catalog_srvid, 0, lockmode);
	}
	else
		LockDatabaseObject(ForeignServerRelationId, catalog_srvid, 0, lockmode);
}

static void
unlock_create_servers(Oid catalog_srvid, Oid volume_srvid, LOCKMODE lockmode)
{
	UnlockDatabaseObject(ForeignServerRelationId, catalog_srvid, 0, lockmode);
	if (volume_srvid != catalog_srvid)
		UnlockDatabaseObject(ForeignServerRelationId, volume_srvid, 0, lockmode);
}

static void
validate_create_binding(const char *catalog_name, const char *volume_name)
{
	ForeignServer *catalog_server;
	ForeignServer *volume_server;

	/*
	 * Resolve, lock, and only then read what was locked -- the same
	 * lookup-lock-recheck the guards use, for the same reason.  Two things go
	 * wrong without it: the name can come to denote a different server while
	 * this backend waits for the lock, so the checks below would describe a
	 * server the statement no longer names; and a concurrent ALTER SERVER that
	 * commits during that wait leaves any copy fetched beforehand stale, so the
	 * definitive read has to happen afterwards.
	 */
	for (;;)
	{
		Oid			catalog_srvid = get_foreign_server_oid(catalog_name, false);
		Oid			volume_srvid = get_foreign_server_oid(volume_name, false);

		lock_create_servers(catalog_srvid, volume_srvid, AccessShareLock);

		if (get_foreign_server_oid(catalog_name, true) == catalog_srvid &&
			get_foreign_server_oid(volume_name, true) == volume_srvid)
		{
			catalog_server = GetForeignServer(catalog_srvid);
			volume_server = GetForeignServer(volume_srvid);
			break;
		}

		unlock_create_servers(catalog_srvid, volume_srvid, AccessShareLock);
	}

	if (catalog_server->fdwid != pg_iceberg_catalog_fdw_oid(false))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("server \"%s\" is not an iceberg catalog server",
						catalog_name)));
	if (volume_server->fdwid != pg_iceberg_volume_fdw_oid(false))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
				 errmsg("server \"%s\" is not an iceberg volume server",
						volume_name)));

	pg_iceberg_check_server_usage(catalog_server->serverid);
	pg_iceberg_check_server_usage(volume_server->serverid);
}

static void
prepare_iceberg_create(CreateStmt *stmt)
{
	DistributedBy *distributed_by;
	const char *catalog_name;
	const char *volume_name;

	if (stmt->accessMethod == NULL)
		stmt->accessMethod = pstrdup("iceberg");

	/*
	 * A lake table is distributed randomly: rows live outside PostgreSQL, so no
	 * local key can describe where they are.  That policy is injected below.
	 *
	 * A QE receives the policy the QD injected and transformed, including the
	 * resolved segment count, so it accepts exactly that shape.
	 *
	 * On the dispatcher, an explicit clause is accepted only when it asks for
	 * what would have been injected anyway.  Refusing every clause looks
	 * stricter but is wrong in one case that matters: pg_dump writes
	 * DISTRIBUTED RANDOMLY into the CREATE TABLE it emits, so refusing it means
	 * refusing to restore a dump this module produced.  A clause naming columns
	 * still cannot be honoured and is refused with the syntax the user wrote.
	 */
	if (stmt->distributedBy != NULL)
	{
		if (stmt->distributedBy->ptype != POLICYTYPE_PARTITIONED)
			pg_iceberg_not_supported(
				stmt->distributedBy->ptype == POLICYTYPE_REPLICATED ?
				"DISTRIBUTED REPLICATED" : "this distribution policy");
		if (stmt->distributedBy->keyCols != NIL)
			pg_iceberg_not_supported("DISTRIBUTED BY");
	}
	else if (Gp_role == GP_ROLE_EXECUTE)
		pg_iceberg_not_supported("DISTRIBUTED BY");
	if (stmt->partspec != NULL || stmt->partbound != NULL)
		pg_iceberg_not_supported("partitioned tables");
	if (stmt->inhRelations != NIL)
		pg_iceberg_not_supported("INHERITS");
	if (stmt->ofTypename != NULL)
		pg_iceberg_not_supported("typed tables (OF type)");
	if (stmt->relation->relpersistence == RELPERSISTENCE_TEMP)
		pg_iceberg_not_supported("TEMP tables");
	if (stmt->relation->relpersistence == RELPERSISTENCE_UNLOGGED)
		pg_iceberg_not_supported("UNLOGGED tables");
	if (stmt->oncommit != ONCOMMIT_NOOP)
		pg_iceberg_not_supported("ON COMMIT");
	if (stmt->tablespacename != NULL)
		pg_iceberg_not_supported("TABLESPACE");

	if (Gp_role != GP_ROLE_EXECUTE)
	{
		distributed_by = makeNode(DistributedBy);
		distributed_by->ptype = POLICYTYPE_PARTITIONED;
		distributed_by->numsegments = -1;
		distributed_by->keyCols = NIL;
		stmt->distributedBy = distributed_by;
	}

	catalog_name = find_reloption(stmt->options, "catalog");
	if (catalog_name == NULL)
	{
		if (iceberg_default_catalog == NULL ||
			iceberg_default_catalog[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("no catalog specified"),
					 errhint("Specify WITH (catalog = '...') or SET iceberg.default_catalog.")));

		stmt->options = lappend(stmt->options,
								makeDefElem("catalog",
											(Node *) makeString(
												pstrdup(iceberg_default_catalog)),
											-1));
		catalog_name = iceberg_default_catalog;
	}

	volume_name = find_reloption(stmt->options, "volume");
	if (volume_name == NULL)
	{
		if (iceberg_default_volume == NULL ||
			iceberg_default_volume[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_TABLE_DEFINITION),
					 errmsg("no volume specified"),
					 errhint("Specify WITH (volume = '...') or SET iceberg.default_volume.")));

		stmt->options = lappend(stmt->options,
								makeDefElem("volume",
											(Node *) makeString(
												pstrdup(iceberg_default_volume)),
											-1));
		volume_name = iceberg_default_volume;
	}

	/*
	 * This makes the QD fail before dispatch; QEs repeat the checks against
	 * their local catalog copies.
	 */
	validate_create_binding(catalog_name, volume_name);
}

static void
reject_utility_mode_ddl(const char *subject)
{
	/*
	 * A utility-mode backend would create, alter, or drop only local catalog
	 * state, without dispatch.  That would break the invariant that every node
	 * agrees about a lake table's mapping and about the servers it names.
	 */
	pg_iceberg_not_supported(psprintf("utility-mode DDL on %s", subject));
}

static void
reject_targeted_operation(const char *operation)
{
	if (Gp_role == GP_ROLE_UTILITY)
		reject_utility_mode_ddl("iceberg tables");
	pg_iceberg_not_supported(operation);
}

static void
pg_iceberg_ProcessUtility(PlannedStmt *pstmt,
						  const char *queryString,
						  bool readOnlyTree,
						  ProcessUtilityContext context,
						  ParamListInfo params,
						  QueryEnvironment *queryEnv,
						  DestReceiver *dest,
						  QueryCompletion *qc)
{
	Node	   *parsetree = pstmt->utilityStmt;

	switch (nodeTag(parsetree))
	{
		case T_CreateStmt:
			{
				CreateStmt *stmt = (CreateStmt *) parsetree;

				if (iceberg_is_effective_am(stmt->accessMethod))
				{
					if (Gp_role == GP_ROLE_UTILITY)
						reject_utility_mode_ddl("iceberg tables");

					/*
					 * Every successful Iceberg CREATE is mutated.  Preserve a
					 * protected parse tree by replacing the PlannedStmt and
					 * mutating only its copy.
					 */
					if (readOnlyTree)
					{
						pstmt = copyObject(pstmt);
						readOnlyTree = false;
						parsetree = pstmt->utilityStmt;
						stmt = (CreateStmt *) parsetree;
					}
					prepare_iceberg_create(stmt);
				}
			}
			break;

		case T_CreateTableAsStmt:
			{
				CreateTableAsStmt *stmt = (CreateTableAsStmt *) parsetree;

				if (stmt->into != NULL &&
					iceberg_is_effective_am(stmt->into->accessMethod))
					reject_targeted_operation(
						"CREATE TABLE AS / CREATE MATERIALIZED VIEW");
			}
			break;

		case T_AlterTableStmt:
			{
				AlterTableStmt *stmt = (AlterTableStmt *) parsetree;

				if (rangevar_is_iceberg(stmt->relation))
				{
					/*
					 * Utility mode is refused for every form, including the one
					 * accepted below: core does not dispatch a utility-mode
					 * ALTER TABLE, so an owner change made there would land on
					 * the connected node alone and leave the catalogs
					 * disagreeing about who owns the table.
					 */
					if (Gp_role == GP_ROLE_UTILITY)
						reject_utility_mode_ddl("iceberg tables");

					if (!alter_table_is_owner_only(stmt))
						reject_targeted_operation(
							"ALTER TABLE on iceberg tables");
				}

				/*
				 * Converting some other table INTO a lake table has to be
				 * refused here as well, and the guard above does not see it:
				 * the relation is still a heap when the statement arrives.
				 * Left alone, the rewrite would reach the metadata engine
				 * through the transient relation it builds -- under a
				 * generated name, with none of the checks CREATE TABLE makes,
				 * including whether the user may use the servers at all.
				 */
				if (alter_table_targets_iceberg_am(stmt))
					reject_targeted_operation(
						"ALTER TABLE ... SET ACCESS METHOD iceberg");
			}
			break;

		case T_RenameStmt:
			{
				RenameStmt *stmt = (RenameStmt *) parsetree;

				if ((stmt->renameType == OBJECT_TABLE ||
					 stmt->renameType == OBJECT_COLUMN) &&
					rangevar_is_iceberg(stmt->relation))
					reject_targeted_operation("RENAME on iceberg tables");
				if (stmt->renameType == OBJECT_FOREIGN_SERVER &&
					locked_server_referenced_by_iceberg(
						string_object_name(stmt->object)))
					reject_targeted_operation(
						"RENAME on servers referenced by iceberg tables");
				if (stmt->renameType == OBJECT_SCHEMA &&
					schema_has_iceberg_table(stmt->subname))
					reject_targeted_operation(
						"RENAME on schemas containing iceberg tables");
				if (stmt->renameType == OBJECT_FDW &&
					is_module_fdw_name(string_object_name(stmt->object)))
					reject_targeted_operation(
						"RENAME on the iceberg foreign-data wrappers");
			}
			break;

		case T_AlterObjectSchemaStmt:
			{
				AlterObjectSchemaStmt *stmt =
					(AlterObjectSchemaStmt *) parsetree;

				if (stmt->objectType == OBJECT_TABLE &&
					rangevar_is_iceberg(stmt->relation))
					reject_targeted_operation(
						"SET SCHEMA on iceberg tables");
			}
			break;

		case T_AlterOwnerStmt:
			{
				AlterOwnerStmt *stmt = (AlterOwnerStmt *) parsetree;

				if (stmt->objectType == OBJECT_TABLE &&
					rangevar_is_iceberg(stmt->relation))
					reject_targeted_operation(
						"ALTER OWNER on iceberg tables");
				if (stmt->objectType == OBJECT_FOREIGN_SERVER &&
					locked_server_referenced_by_iceberg(
						string_object_name(stmt->object)))
					reject_targeted_operation(
						"ALTER OWNER on servers referenced by iceberg tables");
			}
			break;

		case T_AlterForeignServerStmt:
			{
				AlterForeignServerStmt *stmt =
					(AlterForeignServerStmt *) parsetree;

				/*
				 * Utility mode first, and for any server of ours rather than
				 * only for one a table already names.  Core does not dispatch a
				 * utility-mode ALTER SERVER, so the options would change on the
				 * connected node alone; a table created afterwards would then
				 * resolve the same server name to different options depending on
				 * which node resolved it, and nothing downstream would notice.
				 * The window is before any dependency exists, which is precisely
				 * what the reference check below cannot see.
				 */
				if (Gp_role == GP_ROLE_UTILITY &&
					server_belongs_to_module(stmt->servername))
					reject_utility_mode_ddl("iceberg servers");

				/* Both VERSION and OPTIONS forms use this parse node. */
				if (locked_server_referenced_by_iceberg(stmt->servername))
					reject_targeted_operation(
						"ALTER SERVER on servers referenced by iceberg tables");
			}
			break;

		case T_VacuumStmt:
			/*
			 * Plain VACUUM and ANALYZE are no-ops for iceberg tables and stay
			 * allowed, but VACUUM FULL must be refused here rather than in the
			 * table AM: rewriting a relation first creates a transient one,
			 * which reaches OAT_POST_CREATE and makes the metadata engine
			 * create a table in the remote catalog.  The subsequent
			 * relation_copy_for_cluster error rolls back the local catalog,
			 * yet the remote side would keep an orphan behind.
			 */
			{
				VacuumStmt *stmt = (VacuumStmt *) parsetree;
				ListCell   *lc;
				bool		is_full = false;

				foreach(lc, stmt->options)
				{
					DefElem    *def = (DefElem *) lfirst(lc);

					if (strcmp(def->defname, "full") == 0)
						is_full = defGetBoolean(def);
				}

				if (is_full)
				{
					/*
					 * A database-wide VACUUM FULL names no relation, so there
					 * is nothing to match: refuse it outright while any lake
					 * table exists, rather than let it reach one and rewrite
					 * it.
					 */
					if (stmt->rels == NIL)
					{
						if (database_has_iceberg_table())
							reject_targeted_operation(
								"VACUUM FULL while iceberg tables exist");
					}
					else
					{
						foreach(lc, stmt->rels)
						{
							VacuumRelation *vrel = (VacuumRelation *) lfirst(lc);

							if (rangevar_is_iceberg(vrel->relation))
								reject_targeted_operation(
									"VACUUM FULL on iceberg tables");
						}
					}
				}
			}
			break;

		case T_DropStmt:
			/*
			 * Plain DROP TABLE is supported outside utility mode; OAT_DROP
			 * performs the engine call.  DROP SERVER protection otherwise
			 * comes from the dependencies recorded on every node.
			 */
			if (Gp_role == GP_ROLE_UTILITY &&
				utility_drop_targets_iceberg((DropStmt *) parsetree))
				reject_utility_mode_ddl("iceberg tables");
			break;

		default:
			break;
	}

	if (prev_ProcessUtility_hook)
		(*prev_ProcessUtility_hook) (pstmt, queryString, readOnlyTree,
									context, params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv, dest, qc);
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("datalake_fdw must be loaded via shared_preload_libraries"),
				 errhint("Add \"datalake_fdw\" to shared_preload_libraries and restart the server.")));

	pg_iceberg_define_gucs();
	pg_iceberg_register_reloptions();
	DatalakeRegisterMetaEngines();
	datalake_register_storage_backends();

	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = pg_iceberg_ProcessUtility;

	pg_iceberg_prev_object_access_hook = object_access_hook;
	object_access_hook = pg_iceberg_object_access;
}

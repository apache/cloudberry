/*-------------------------------------------------------------------------
 *
 * mdb_admin.c
 *	  Provisioning of the mdb_admin privilege role
 *
 *	  gpcontrib/gp_toolkit/mdb_admin.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/oid_dispatch.h"
#include "catalog/pg_authid.h"
#include "commands/user.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/syscache.h"

/* Name of the mdb_admin role; its OID is MDB_ADMIN_ROLEID (see acl.h). */
#define MDB_ADMIN_ROLE_NAME	"mdb_admin"

PG_FUNCTION_INFO_V1(pg_create_mdb_admin_role);

/*
 * Create the mdb_admin role with its fixed OID (MDB_ADMIN_ROLEID, 8067).
 *
 * The core privilege checks identify mdb_admin by this fixed OID (see acl.c
 * and resgroupcmds.c), so the role must always be created with it.  On a
 * Cloudberry cluster the OID is dispatched to the segments so the role ends
 * up with the same OID everywhere.  Returns the new role's OID.
 */
Datum
pg_create_mdb_admin_role(PG_FUNCTION_ARGS)
{
	CreateRoleStmt stmt;
	List	   *options = NIL;
	Oid			roleid;

	/*
	 * Only a superuser may establish the mdb_admin privilege role.  Otherwise
	 * a CREATEROLE user could drop mdb_admin and re-create it (CreateRole only
	 * requires CREATEROLE), taking ownership of the fixed-OID role and
	 * granting the capability to itself.
	 */
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be superuser to create the mdb_admin role")));

	/* Check if a role with the fixed OID already exists. */
	if (SearchSysCacheExists1(AUTHOID, ObjectIdGetDatum(MDB_ADMIN_ROLEID)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("role with OID %u already exists", MDB_ADMIN_ROLEID)));

	/* Check if a role named "mdb_admin" already exists. */
	if (SearchSysCacheExists1(AUTHNAME, CStringGetDatum(MDB_ADMIN_ROLE_NAME)))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("role \"%s\" already exists", MDB_ADMIN_ROLE_NAME)));

	/* Build options for CreateRole: connection limit = 0. */
	options = list_make1(makeDefElem("connectionlimit",
									 (Node *) makeInteger(0), -1));

	/* Prepare the CreateRoleStmt. */
	memset(&stmt, 0, sizeof(stmt));
	stmt.type = T_CreateRoleStmt;
	stmt.stmt_type = ROLESTMT_ROLE;
	stmt.role = MDB_ADMIN_ROLE_NAME;
	stmt.options = options;

	/*
	 * Request the fixed OID for the role.  GetNewOidForAuthId() consumes and
	 * clears this override.
	 */
	next_aux_pg_authid_oid = MDB_ADMIN_ROLEID;

	roleid = CreateRole(NULL, &stmt);

	PG_RETURN_OID(roleid);
}

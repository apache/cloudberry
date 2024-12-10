/*-------------------------------------------------------------------------
 *
 * pg_password_history.h
 *	  definition of the "password history" system catalog (pg_password_history)
 *
 *
 * Copyright (c) 2023, Cloudberry Database, HashData Technology Limited.
 *
 * src/include/catalog/pg_password_history.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PASSWORD_HISTORY_H
#define PG_PASSWORD_HISTORY_H

#include "catalog/genbki.h"
#include "catalog/pg_password_history_d.h"

/* ----------------
 *		pg_password_history definition.  cpp turns this into
 *		typedef struct FormData_pg_password_history
 * ----------------
 */
CATALOG(pg_password_history,10141,PasswordHistoryRelationId) BKI_SHARED_RELATION BKI_ROWTYPE_OID(10142,PasswordHistoryRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	Oid		passhistroleid BKI_FORCE_NOT_NULL;	/* oid of role */
#ifdef CATALOG_VARLEN
	timestamptz	passhistpasswordsetat BKI_FORCE_NOT_NULL;	/* password set time */
	text		passhistpassword BKI_FORCE_NOT_NULL;	/* the history password */
#endif
} FormData_pg_password_history;

/* ----------------
 *		Form_pg_password_history corresponds to a pointer to a tuple with
 *		the format of pg_password_history relation.
 * ----------------
 */
typedef FormData_pg_password_history *Form_pg_password_history;

DECLARE_TOAST(pg_password_history, 10143, 10144);
#define PgPasswordHistoryToastTable 10143
#define PgPasswordHistoryToastIndex 10144

DECLARE_UNIQUE_INDEX(pg_password_history_role_password_index, 10145, on pg_password_history using btree(passhistroleid oid_ops, passhistpassword text_ops));
#define PasswordHistoryRolePasswordIndexId	10145
DECLARE_INDEX(pg_password_history_role_passwordsetat_index, 10146, on pg_password_history using btree(passhistroleid oid_ops, passhistpasswordsetat timestamptz_ops));
#define PasswordHistoryRolePasswordsetatIndexId	10146

#endif							/* PG_PASSWORD_HISTORY_H */

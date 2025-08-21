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
 * pg_password_history.h
 *	  definition of the "password history" system catalog (pg_password_history)
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
CATALOG(pg_password_history,10141,PasswordHistoryRelationId) BKI_SHARED_RELATION BKI_ROWTYPE_OID(11144,PasswordHistoryRelation_Rowtype_Id) BKI_SCHEMA_MACRO
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

DECLARE_UNIQUE_INDEX(pg_password_history_role_password_index, 10145, PasswordHistoryRolePasswordIndexId, on pg_password_history using btree(passhistroleid oid_ops, passhistpassword text_ops));
DECLARE_INDEX(pg_password_history_role_passwordsetat_index, 10146, PasswordHistoryRolePasswordsetatIndexId, on pg_password_history using btree(passhistroleid oid_ops, passhistpasswordsetat timestamptz_ops));

#endif							/* PG_PASSWORD_HISTORY_H */

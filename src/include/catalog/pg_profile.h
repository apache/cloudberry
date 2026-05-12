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
 * pg_profile.h
 *	  definition of the "profile" system catalog (pg_profile)
 *
 * src/include/catalog/pg_profile.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PROFILE_H
#define PG_PROFILE_H

#include "catalog/objectaddress.h"
#include "catalog/genbki.h"
#include "catalog/pg_profile_d.h"
#include "parser/parse_node.h"

/* ----------------
 *		pg_profile definition.  cpp turns this into
 *		typedef struct FormData_pg_profile
 * ----------------
 */
CATALOG(pg_profile,10135,ProfileRelationId) BKI_SHARED_RELATION BKI_ROWTYPE_OID(11145,ProfileRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	Oid		oid BKI_FORCE_NOT_NULL;		/* oid */
	NameData	prfname BKI_FORCE_NOT_NULL;	/* name of profile */
	int32		prffailedloginattempts BKI_DEFAULT(-1) BKI_FORCE_NOT_NULL;	/* the number of failed login attempts */
	int32		prfpasswordlocktime BKI_DEFAULT(-1) BKI_FORCE_NOT_NULL;	/* the locking period */
	int32		prfpasswordlifetime BKI_DEFAULT(-1) BKI_FORCE_NOT_NULL;	/* the number of days that the current password is valid and usable */
	int32		prfpasswordgracetime BKI_DEFAULT(-1) BKI_FORCE_NOT_NULL;	/* the number of days an old password can still be used */
	int32		prfpasswordreusetime BKI_DEFAULT(-1) BKI_FORCE_NOT_NULL;	/* the number of days a user must wait before reusing a password */
	int32		prfpasswordreusemax BKI_DEFAULT(-1) BKI_FORCE_NOT_NULL;	/* the number of password changes that must occur before a password can be reused */
	int32		prfpasswordallowhashed BKI_DEFAULT(-1) BKI_FORCE_NOT_NULL;	/* whether an hash encrypted password is allowed to be used or not */
	Oid		prfpasswordverifyfuncdb BKI_FORCE_NULL;	/* verify function for database when login */
	Oid		prfpasswordverifyfunc BKI_FORCE_NULL;	/* verify function when login */
} FormData_pg_profile;

/* ----------------
 *		Form_pg_profile corresponds to a pointer to a tuple with
 *		the format of pg_profile relation.
 * ----------------
 */
typedef FormData_pg_profile *Form_pg_profile;

DECLARE_UNIQUE_INDEX(profile_name_index, 10137, ProfilePrfnameIndexId, on pg_profile using btree(prfname name_ops));
DECLARE_UNIQUE_INDEX(profile_oid_index, 10138, ProfileOidIndexId, on pg_profile using btree(oid oid_ops));
DECLARE_INDEX(profile_password_verify_function_index, 10139, ProfileVerifyFunctionIndexId, on pg_profile using btree(prfpasswordverifyfuncdb oid_ops, prfpasswordverifyfunc oid_ops));

#define DefaultProfileOID	10140

#define PROFILE_UNLIMITED	-2
#define PROFILE_DEFAULT		-1
#define PROFILE_MAX_VALID	9999

extern char *
ProfileGetNameByOid(Oid profileOid, bool noerr);
extern Oid
get_profile_oid(const char *prfname, bool missing_ok);
extern ObjectAddress
rename_profile(char *oldname, char *newname);
extern Oid
CreateProfile(ParseState *pstate, CreateProfileStmt *stmt);
extern Oid
AlterProfile(AlterProfileStmt *stmt);
extern void
DropProfile(DropProfileStmt *stmt);
extern int32
tranformProfileValueToNormal(int32 profile_val, int attoff);
#endif							/* PG_PROFILE_H */

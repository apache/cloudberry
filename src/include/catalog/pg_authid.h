/*-------------------------------------------------------------------------
 *
 * pg_authid.h
 *	  definition of the "authorization identifier" system catalog (pg_authid)
 *
 *	  pg_shadow and pg_group are now publicly accessible views on pg_authid.
 *
 *
 * Portions Copyright (c) 2006-2010, Greenplum inc.
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_authid.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_AUTHID_H
#define PG_AUTHID_H

#include "catalog/genbki.h"
#include "catalog/pg_authid_d.h"

/* ----------------
 *		pg_authid definition.  cpp turns this into
 *		typedef struct FormData_pg_authid
 * ----------------
 */
CATALOG(pg_authid,1260,AuthIdRelationId) BKI_SHARED_RELATION BKI_ROWTYPE_OID(2842,AuthIdRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	Oid			oid;			/* oid */
	NameData	rolname;		/* name of role */
	bool		rolsuper;		/* read this field via superuser() only! */
	bool		rolinherit;		/* inherit privileges from other roles? */
	bool		rolcreaterole;	/* allowed to create more roles? */
	bool		rolcreatedb;	/* allowed to create databases? */
	bool		rolcanlogin;	/* allowed to log in as session user? */
	bool		rolreplication; /* role used for streaming replication */
	bool		rolbypassrls;	/* bypasses row-level security? */
	int32		rolconnlimit;	/* max connections allowed (-1=no limit) */
	bool		rolenableprofile BKI_DEFAULT(f) BKI_FORCE_NOT_NULL;	/* whether user can use profile */

	/* remaining fields may be null; use heap_getattr to read them! */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		rolpassword;	/* password, if any */
	timestamptz rolvaliduntil;	/* password expiration time, if any */

	/*
	 * GP added fields
	 */

	Oid	 	rolprofile BKI_DEFAULT(10140) BKI_FORCE_NOT_NULL;	/* name of profile */

	int16		rolaccountstatus BKI_DEFAULT(0) BKI_FORCE_NOT_NULL;	/* status of account */

	int32		rolfailedlogins BKI_DEFAULT(0) BKI_FORCE_NOT_NULL;	/* number of failed logins */

	timestamptz	rolpasswordsetat BKI_DEFAULT(_null_);			/* password set time, if any */

	timestamptz	rollockdate BKI_DEFAULT(_null_) BKI_FORCE_NULL;		/* account lock time, if any */

	timestamptz	rolpasswordexpire BKI_DEFAULT(_null_) BKI_FORCE_NULL;	/* account password expire time, if any */

	/* ID of resource queue for this role */
	Oid			rolresqueue BKI_DEFAULT(6055);

	/* allowed to create readable gpfdist tbl?  */
	bool		rolcreaterextgpfd BKI_DEFAULT(f);

	/* allowed to create readable http tbl?  */
	bool		rolcreaterexthttp BKI_DEFAULT(f);

	/* allowed to create writable gpfdist tbl?  */
	bool		rolcreatewextgpfd BKI_DEFAULT(f);

	/* ID of resource group for this role  */
	Oid			rolresgroup BKI_DEFAULT(6438);
#endif
} FormData_pg_authid;

typedef enum
{
    ROLE_ACCOUNT_STATUS_OPEN,
    ROLE_ACCOUNT_STATUS_LOCKED_TIMED,
    ROLE_ACCOUNT_STATUS_LOCKED,
    ROLE_ACCOUNT_STATUS_EXPIRED_GRACE,
    ROLE_ACCOUNT_STATUS_EXPIRED
} ROLE_ACCOUNT_STATUS;

/* GPDB added foreign key definitions for gpcheckcat. */
FOREIGN_KEY(rolresqueue REFERENCES pg_resqueue(oid));
FOREIGN_KEY(rolresgroup REFERENCES pg_resgroup(oid));

/* ----------------
 *		Form_pg_authid corresponds to a pointer to a tuple with
 *		the format of pg_authid relation.
 * ----------------
 */
typedef FormData_pg_authid *Form_pg_authid;

DECLARE_TOAST(pg_authid, 4175, 4176);
#define PgAuthidToastTable 4175
#define PgAuthidToastIndex 4176

DECLARE_UNIQUE_INDEX(pg_authid_rolname_index, 2676, on pg_authid using btree(rolname name_ops));
#define AuthIdRolnameIndexId	2676
DECLARE_UNIQUE_INDEX_PKEY(pg_authid_oid_index, 2677, on pg_authid using btree(oid oid_ops));
#define AuthIdOidIndexId	2677

#endif							/* PG_AUTHID_H */

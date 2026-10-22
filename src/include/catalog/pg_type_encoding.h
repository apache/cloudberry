/*-------------------------------------------------------------------------
 *
 * pg_type_encoding.h
 *	  some where to stash type ENCODING () clauses
 *
 * Portions Copyright (c) EMC, 2011
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/pg_type_encoding.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_TYPE_ENCODING_H
#define PG_TYPE_ENCODING_H

#include "catalog/genbki.h"
#include "catalog/pg_type_encoding_d.h"

/* ----------------
 *		pg_type_encoding definition.  cpp turns this into
 *		typedef struct FormData_pg_type_encoding
 * ----------------
 */
CATALOG(pg_type_encoding,6421,TypeEncodingRelationId)
{
	Oid		typid;			
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text	typoptions[1];	
#endif
} FormData_pg_type_encoding;

/* GPDB added foreign key definitions for gpcheckcat. */
FOREIGN_KEY(typid REFERENCES pg_type(oid));

/* ----------------
 *		Form_pg_type_encoding corresponds to a pointer to a tuple with
 *		the format of pg_type_encoding relation.
 * ----------------
 */
typedef FormData_pg_type_encoding *Form_pg_type_encoding;
DECLARE_TOAST(pg_type_encoding, 6416, 6417);
DECLARE_UNIQUE_INDEX(pg_type_encoding_typid_index, 6468, TypeEncodingTypidIndexId, on pg_type_encoding using btree(typid oid_ops));

#endif   /* PG_TYPE_ENCODING_H */

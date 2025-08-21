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
 * pg_tag.h
 *	  definition of the "tag" system catalog (pg_tag)
 *
 * src/include/catalog/pg_tag.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_TAG_H
#define PG_TAG_H

#include "catalog/genbki.h"
#include "catalog/pg_tag_d.h"
#include "parser/parse_node.h"

/* ----------------
 *		pg_tag definition.  cpp turns this into
 *		typedef struct FormData_pg_tag
 *
 *		If you change the following, make sure you change the structs for
 *		system attributes in catalog/heap.c also.
 *		You may need to change catalog/genbki.pl as well.
 */
CATALOG(pg_tag,6461,TagRelationId) BKI_SHARED_RELATION BKI_ROWTYPE_OID(6462,TagRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	Oid		oid		BKI_FORCE_NOT_NULL;		/* oid */
	NameData	tagname		BKI_FORCE_NOT_NULL;	/* name of tag */
	Oid		tagowner		BKI_DEFAULT(POSTGRES)	BKI_LOOKUP(pg_authid);	/* owner of tag */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text	allowed_values[1];		/* per tag allowed values */
#endif
} FormData_pg_tag;

/* ----------------
 *		Form_pg_tag corresponds to a pointer to a tuple with
 *		the format of pg_tag relation.
 * ----------------
 */
typedef FormData_pg_tag *Form_pg_tag;

DECLARE_UNIQUE_INDEX_PKEY(pg_tag_tagname_index, 6463, TagNameIndexId, on pg_tag using btree(tagname name_ops));
DECLARE_UNIQUE_INDEX(pg_tag_oid_index, 6465, TagOidIndexId, on pg_tag using btree(oid oid_ops));

#endif							/* PG_TAG_H */
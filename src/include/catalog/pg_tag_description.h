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
 * pg_tag_description.h
 *	  definition of the "tag description" system catalog (pg_tag_description)
 *
 * src/include/catalog/pg_tag_description.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */

#ifndef PG_TAG_DESCRIPTION_H
#define PG_TAG_DESCRIPTION_H

#include "catalog/genbki.h"
#include "catalog/pg_tag_description_d.h"
#include "parser/parse_node.h"

/* ----------------
 *		pg_tag_description definition.    cpp turns this into
 *		typedef struct FormData_pg_tag_description
 * ----------------
 */
CATALOG(pg_tag_description,6485,TagDescriptionRelationId) BKI_SHARED_RELATION BKI_ROWTYPE_OID(6486,TagDescriptionRelation_Rowtype_Id) BKI_SCHEMA_MACRO
{
	Oid			oid		BKI_FORCE_NOT_NULL;				/* OID of this tag description */
	Oid			tddatabaseid	BKI_LOOKUP_OPT(pg_database);	/* OID of database containing object */
	Oid			tdclassid	BKI_LOOKUP_OPT(pg_class);		/* OID of table containing object */
	Oid 		tdobjid;			/* OID of object itself */
	Oid 		tagid	BKI_LOOKUP_OPT(pg_tag);			/* Oid of tag */
#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	text		tagvalue;		/* tag value for this object */
#endif
} FormData_pg_tag_description;

/* ----------------
 *		Form_pg_tag_description corresponds to a pointer to a tuple with
 *		the format of pg_tag_description relation.
 * ----------------
 */
typedef FormData_pg_tag_description *Form_pg_tag_description;

DECLARE_UNIQUE_INDEX_PKEY(pg_tag_description_d_c_o_t_index, 6487, TagDescriptionIndexId, on pg_tag_description using btree(tddatabaseid oid_ops, tdclassid oid_ops, tdobjid oid_ops, tagid oid_ops));
DECLARE_UNIQUE_INDEX(pg_tag_description_oid_index, 6488, TagDescriptionOidIndexId, on pg_tag_description using btree(oid oid_ops));
DECLARE_INDEX(pg_tag_description_tagidvalue_index, 6489, TagDescriptionTagidvalueIndexId, on pg_tag_description using btree(tagid oid_ops, tagvalue text_ops));

#endif							/* PG_TAG_DESCRIPTION_H */
/*-------------------------------------------------------------------------
 *
 * gp_indexing.h
 *	  This file provides some definitions to support indexing
 *	  on GPDB catalogs
 *
 * Portions Copyright (c) 2007-2010, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef GP_INDEXING_H
#define GP_INDEXING_H

#include "catalog/genbki.h"



/* GPDB_14_MERGE_FIXME: not used in upstream anymore */
/*
DECLARE_UNIQUE_INDEX(pg_pltemplate_name_index, 1137, on pg_pltemplate using btree(tmplname name_ops));
#define PLTemplateNameIndexId  1137
*/


/* MPP-6929: metadata tracking */

/* Note: no dbid */






/* GPDB_14_MERGE_FIXME: seems not directly used */

/* GPDB_14_MERGE_FIXME: oid conflicts, assgin new */

#endif							/* GP_INDEXING_H */

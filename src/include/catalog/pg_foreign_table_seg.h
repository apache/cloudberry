#ifndef PG_FOREIGN_TABLE_SEG_H
#define PG_FOREIGN_TABLE_SEG_H

#include "catalog/genbki.h"
#include "catalog/pg_foreign_table_seg_d.h"

/* ----------------
 *		pg_foreign_table_seg definition.  cpp turns this into
 *		typedef struct FormData_pg_foreign_table
 * ----------------
 */
CATALOG(pg_foreign_table_seg,5110,ForeignTableRelationSegId)
{
    Oid			ftsrelid BKI_LOOKUP(pg_class);	/* OID of foreign table */
    Oid			ftsserver BKI_LOOKUP(pg_foreign_server); /* OID of foreign server */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
    text		ftsoptions[1];	/* FDW-specific options */
#endif
} FormData_pg_foreign_table_seg;

typedef FormData_pg_foreign_table_seg *Form_pg_foreign_table_seg;

DECLARE_INDEX(pg_foreign_table_seg_relid_index, 5111, ForeignTableRelationSegIndexId, on pg_foreign_table_seg using btree(ftsrelid oid_ops));

#define GP_FOREIGN_SERVER_ID_FUNC 6024

#endif							/* PG_FOREIGN_TABLE_SEG_H */

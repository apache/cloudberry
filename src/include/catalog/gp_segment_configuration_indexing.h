/*-------------------------------------------------------------------------
 *
 * gp_indexing.h
 *	  This file provides some definitions to support gp_segment_configuration indexing
 *	  on GPDB catalogs
 *
 * Portions Copyright (c) 2007-2010, Greenplum inc
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef GP_SEGMENT_CONFIGURATION_INDEXING_H
#define GP_SEGMENT_CONFIGURATION_INDEXING_H

#include "catalog/genbki.h"

DECLARE_UNIQUE_INDEX(gp_segment_config_content_preferred_role_warehouse_index, 7139, on gp_segment_configuration using btree(content int2_ops, preferred_role char_ops, warehouseid oid_ops));
#define GpSegmentConfigContentPreferred_roleWarehouseIndexId	7139
DECLARE_UNIQUE_INDEX(gp_segment_config_dbid_warehouse_index, 7140, on gp_segment_configuration using btree(dbid int2_ops, warehouseid oid_ops));
#define GpSegmentConfigDbidWarehouseIndexId	7140

#endif // GP_SEGMENT_CONFIGURATION_INDEXING_H

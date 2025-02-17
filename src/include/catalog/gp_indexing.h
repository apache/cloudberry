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

DECLARE_INDEX(pg_authid_rolresqueue_index, 6029, on pg_authid using btree(rolresqueue oid_ops));
#define AuthIdRolResQueueIndexId	6029
DECLARE_INDEX(pg_authid_rolresgroup_index, 6440, on pg_authid using btree(rolresgroup oid_ops));
#define AuthIdRolResGroupIndexId	6440
DECLARE_INDEX(pg_authid_rolprofile_index, 6441, on pg_authid using btree(rolprofile oid_ops));
#define AuthIdRolProfileIndexId		6441
DECLARE_INDEX(pg_auth_time_constraint_authid_index, 6449, on pg_auth_time_constraint using btree(authid oid_ops));
#define AuthTimeConstraintAuthIdIndexId	6449

/* GPDB_14_MERGE_FIXME: not used in upstream anymore */
/*
DECLARE_UNIQUE_INDEX(pg_pltemplate_name_index, 1137, on pg_pltemplate using btree(tmplname name_ops));
#define PLTemplateNameIndexId  1137
*/

DECLARE_UNIQUE_INDEX(gp_distribution_policy_localoid_index, 7167, on gp_distribution_policy using btree(localoid oid_ops));
#define GpPolicyLocalOidIndexId  7167
DECLARE_UNIQUE_INDEX(pg_appendonly_relid_index, 7141, on pg_appendonly using btree(relid oid_ops));
#define AppendOnlyRelidIndexId  7141
DECLARE_UNIQUE_INDEX(gp_fastsequence_objid_objmod_index, 6067, on gp_fastsequence using btree(objid oid_ops, objmod  int8_ops));
#define FastSequenceObjidObjmodIndexId 6067

/* MPP-6929: metadata tracking */
DECLARE_UNIQUE_INDEX(pg_statlastop_classid_objid_staactionname_index, 6054, on pg_stat_last_operation using btree(classid oid_ops, objid oid_ops, staactionname name_ops));
#define StatLastOpClassidObjidStaactionnameIndexId  6054

/* Note: no dbid */
DECLARE_INDEX(pg_statlastshop_classid_objid_index, 6057, on pg_stat_last_shoperation using btree(classid oid_ops, objid oid_ops));
#define StatLastShOpClassidObjidIndexId  6057
DECLARE_UNIQUE_INDEX(pg_statlastshop_classid_objid_staactionname_index, 6058, on pg_stat_last_shoperation using btree(classid oid_ops, objid oid_ops, staactionname name_ops));
#define StatLastShOpClassidObjidStaactionnameIndexId  6058
DECLARE_UNIQUE_INDEX(pg_resqueue_oid_index, 6027, on pg_resqueue using btree(oid oid_ops));
#define ResQueueOidIndexId	6027
DECLARE_UNIQUE_INDEX(pg_resqueue_rsqname_index, 6028, on pg_resqueue using btree(rsqname name_ops));
#define ResQueueRsqnameIndexId	6028
DECLARE_UNIQUE_INDEX(pg_resourcetype_oid_index, 6061, on pg_resourcetype using btree(oid oid_ops));
#define ResourceTypeOidIndexId	6061
DECLARE_UNIQUE_INDEX(pg_resourcetype_restypid_index, 6062, on pg_resourcetype using btree(restypid int2_ops));
#define ResourceTypeRestypidIndexId	6062
DECLARE_UNIQUE_INDEX(pg_resourcetype_resname_index, 6063, on pg_resourcetype using btree(resname name_ops));
#define ResourceTypeResnameIndexId	6063
DECLARE_INDEX(pg_resqueuecapability_resqueueid_index, 6442, on pg_resqueuecapability using btree(resqueueid oid_ops));
#define ResQueueCapabilityResqueueidIndexId	6442
DECLARE_INDEX(pg_resqueuecapability_restypid_index, 6443, on pg_resqueuecapability using btree(restypid int2_ops));
#define ResQueueCapabilityRestypidIndexId	6443
DECLARE_UNIQUE_INDEX(pg_resgroup_oid_index, 6447, on pg_resgroup using btree(oid oid_ops));
#define ResGroupOidIndexId	6447
DECLARE_UNIQUE_INDEX(pg_resgroup_rsgname_index, 6444, on pg_resgroup using btree(rsgname name_ops));
#define ResGroupRsgnameIndexId	6444
DECLARE_UNIQUE_INDEX(pg_resgroupcapability_resgroupid_reslimittype_index, 6445, on pg_resgroupcapability using btree(resgroupid oid_ops, reslimittype int2_ops));
#define ResGroupCapabilityResgroupidResLimittypeIndexId	6445
DECLARE_INDEX(pg_resgroupcapability_resgroupid_index, 6446, on pg_resgroupcapability using btree(resgroupid oid_ops));
#define ResGroupCapabilityResgroupidIndexId	6446
DECLARE_UNIQUE_INDEX(pg_extprotocol_oid_index, 7156, on pg_extprotocol using btree(oid oid_ops));
#define ExtprotocolOidIndexId	7156
DECLARE_UNIQUE_INDEX(pg_extprotocol_ptcname_index, 7177, on pg_extprotocol using btree(ptcname name_ops));
#define ExtprotocolPtcnameIndexId	7177
DECLARE_INDEX(pg_attribute_encoding_attrelid_index, 6236, on pg_attribute_encoding using btree(attrelid oid_ops));
#define AttributeEncodingAttrelidIndexId	6236
DECLARE_UNIQUE_INDEX(pg_attribute_encoding_attrelid_attnum_index, 6237, on pg_attribute_encoding using btree(attrelid oid_ops, attnum int2_ops));

/* GPDB_14_MERGE_FIXME: seems not directly used */
#define AttributeEncodingAttrelidAttnumIndexId	6237
DECLARE_UNIQUE_INDEX(pg_type_encoding_typid_index, 6207, on pg_type_encoding using btree(typid oid_ops));
#define TypeEncodingTypidIndexId	6207
DECLARE_UNIQUE_INDEX(pg_proc_callback_profnoid_promethod_index, 9926, on pg_proc_callback using btree(profnoid oid_ops, promethod char_ops));
#define ProcCallbackProfnoidPromethodIndexId	9926
DECLARE_UNIQUE_INDEX(pg_compression_compname_index, 7059, on pg_compression using btree(compname name_ops));
#define CompressionCompnameIndexId	7059

/* GPDB_14_MERGE_FIXME: oid conflicts, assgin new */
DECLARE_UNIQUE_INDEX(gp_partition_template_relid_level_index, 7168, on gp_partition_template using btree(relid oid_ops, level int2_ops));
#define GpPartitionTemplateRelidLevelIndexId  7168

#endif							/* GP_INDEXING_H */

/*-------------------------------------------------------------------------
 *
 * gp_matview_aux.h
 *	  definitions for the gp_matview_aux catalog table
 *
 * Portions Copyright (c) 2024-Present HashData, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/gp_matview_aux.h
 *
 * NOTES
 *
 *-------------------------------------------------------------------------
 */

#ifndef GP_MATVIEW_AUX_H
#define GP_MATVIEW_AUX_H

#include "catalog/genbki.h"
#include "catalog/gp_matview_aux_d.h"

/*
 * Defines for gp_matview_aux
 */
CATALOG(gp_matview_aux,7153,GpMatviewAuxId)
{
	Oid			mvoid; 	/* materialized view oid */
	NameData	mvname; /* materialized view name */
	bool		has_foreign;	/* view query has foreign tables? */
	/* view's data status */
	char		datastatus; 
} FormData_gp_matview_aux;


/* ----------------
 *		Form_gp_matview_aux corresponds to a pointer to a tuple with
 *		the format of gp_matview_aux relation.
 * ----------------
 */
typedef FormData_gp_matview_aux *Form_gp_matview_aux;

DECLARE_UNIQUE_INDEX(gp_matview_aux_mvoid_index, 7147, on gp_matview_aux using btree(mvoid oid_ops));
#define GpMatviewAuxMvoidIndexId 7147

DECLARE_INDEX(gp_matview_aux_mvname_index, 7148, on gp_matview_aux using btree(mvname name_ops));
#define GpMatviewAuxMvnameIndexId 7148

DECLARE_INDEX(gp_matview_aux_datastatus_index, 7149, on gp_matview_aux using btree(datastatus char_ops));
#define GpMatviewAuxDatastatusIndexId 7149

#define		MV_DATA_STATUS_UP_TO_DATE				'u'	/* data is up to date */
#define		MV_DATA_STATUS_UP_REORGANIZED			'r' /* data is up to date, but reorganized. VACUUM FULL/CLUSTER */
#define		MV_DATA_STATUS_EXPIRED					'e'	/* data is expired */
#define		MV_DATA_STATUS_EXPIRED_INSERT_ONLY		'i'	/* expired but has only INSERT operation since latest REFRESH */

#define		MV_DATA_STATUS_TRANSFER_DIRECTION_UP	'u'	/* set status recursivly up to the top. */
#define		MV_DATA_STATUS_TRANSFER_DIRECTION_DOWN	'd'	/* set status recursivly down to the leaf. */
#define		MV_DATA_STATUS_TRANSFER_DIRECTION_ALL	'a'	/* set status recursivly up and down */

extern void InsertMatviewAuxEntry(Oid mvoid, const Query *viewQuery, bool skipdata);

extern void RemoveMatviewAuxEntry(Oid mvoid);

extern List* GetViewBaseRelids(const Query *viewQuery, bool *has_foreign);

extern void SetRelativeMatviewAuxStatus(Oid relid, char status, char direction);

extern void SetMatviewAuxStatus(Oid mvoid, char status);

extern bool MatviewHasForeignTables(Oid mvoid);

extern bool MatviewUsableForAppendAgg(Oid mvoid);

extern bool MatviewIsGeneralyUpToDate(Oid mvoid);

extern bool MatviewIsUpToDate(Oid mvoid);

#endif			/* GP_MATVIEW_AUX_H */

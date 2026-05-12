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
 * gp_matview_tables.h
 *	  Definitions for the gp_matview_tables catalog table.
 *	  This is used to record the relations between
 *	  materialized view and base tables.
 *
 *
 * IDENTIFICATION
 *	    src/include/catalog/gp_matview_tables.h
 *
 * NOTES
 * 		This should cooperate with gp_matview_aux.
 *
 *-------------------------------------------------------------------------
 */

#ifndef gp_matview_tables_H
#define gp_matview_tables_H

#include "catalog/genbki.h"
#include "catalog/gp_matview_tables_d.h"

/*
 * Defines for gp_matview_tables
 */
CATALOG(gp_matview_tables,7150,GpMatviewTablesId)
{
	Oid			mvoid; 	/* materialized view oid */
	Oid			relid; 	/* base table oid */
} FormData_gp_matview_tables;


/* ----------------
 *		Form_gp_matview_tables corresponds to a pointer to a tuple with
 *		the format of gp_matview_tables relation.
 * ----------------
 */
typedef FormData_gp_matview_tables *Form_gp_matview_tables;

DECLARE_UNIQUE_INDEX(gp_matview_tables_mvoid_relid_index, 7151, GpMatviewTablesMvRelIndexId, on gp_matview_tables using btree(mvoid oid_ops, relid oid_ops));

DECLARE_INDEX(gp_matview_tables_relid_index, 7152, GpMatviewTablesRelIndexId, on gp_matview_tables using btree(relid oid_ops));

#endif			/* gp_matview_tables_H */

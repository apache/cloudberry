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
 * pax_rel.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/pax_rel.h
 *
 *-------------------------------------------------------------------------
 */


#ifndef SRC_CPP_COMM_PAX_REL_H_
#define SRC_CPP_COMM_PAX_REL_H_

// Oid of pg_ext_aux.pg_pax_tables
#define PAX_TABLES_RELATION_ID 7061
#define PAX_TABLES_RELID_INDEX_ID 7047

#define PAX_TABLE_AM_OID 7047
#define PAX_AMNAME "pax"
#define PAX_AM_HANDLER_OID 7600
#define PAX_AM_HANDLER_NAME "pax_tableam_handler"

#define PAX_AUX_STATS_IN_OID 7601
#define PAX_AUX_STATS_OUT_OID 7602
#define PAX_AUX_STATS_TYPE_OID 7603
#define PAX_AUX_STATS_TYPE_NAME "paxauxstats"

#define PAX_FASTSEQUENCE_OID 7604
#define PAX_FASTSEQUENCE_INDEX_OID 7605

#define PG_PAX_FASTSEQUENCE_NAMESPACE "pg_ext_aux"
#define PG_PAX_FASTSEQUENCE_TABLE "pg_pax_fastsequence"
#define PG_PAX_FASTSEQUENCE_INDEX_NAME "pg_pax_fastsequence_objid_idx"

#define AMHandlerIsPAX(amhandler) ((amhandler) == PAX_AM_HANDLER_OID)
#define RelationIsPAX(relation) AMHandlerIsPAX((relation)->rd_amhandler)

#endif  // SRC_CPP_COMM_PAX_REL_H_
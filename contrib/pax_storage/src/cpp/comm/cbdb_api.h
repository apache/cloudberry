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
 * cbdb_api.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/comm/cbdb_api.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SRC_CPP_COMM_CBDB_API_H_
#define SRC_CPP_COMM_CBDB_API_H_

#include "comm/pax_rel.h"

#ifdef __cplusplus
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wregister"
extern "C" {
#endif

#include "postgres.h"  //  NOLINT

#include "access/detoast.h"
#include "access/genam.h"
#include "access/hash.h"
#include "access/heapam.h"
#include "access/nbtree.h"
#include "access/relscan.h"
#include "access/sdir.h"
#include "access/tableam.h"
#include "access/toast_compression.h"
#include "access/toast_internals.h"  // for TOAST_COMPRESS_SET_SIZE_AND_COMPRESS_METHOD
#include "access/tsmapi.h"
#include "access/tupdesc.h"
#include "access/tupdesc_details.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/gp_indexing.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "catalog/oid_dispatch.h"
#include "catalog/partition.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_am.h"
#include "catalog/pg_amop.h"
#include "catalog/pg_amproc.h"
#include "catalog/pg_attribute_encoding.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "catalog/pg_directory_table.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_opclass.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/storage_directory_table.h"
#include "catalog/toasting.h"
#include "cdb/cdbdisp_query.h"
#include "commands/defrem.h"
#include "commands/progress.h"
#include "commands/tablecmds.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/bitmapset.h"
#include "nodes/execnodes.h"
#include "nodes/makefuncs.h"
#include "parser/parse_expr.h"
#include "parser/parse_oper.h"
#include "parser/parse_utilcmd.h"
#include "partitioning/partbounds.h"
#include "partitioning/partdesc.h"
#include "pgstat.h"
#include "postmaster/postmaster.h"
#include "utils/cash.h"
#include "utils/geo_decls.h"
#include "utils/inet.h"
#include "utils/partcache.h"
#include "utils/ruleutils.h"
#include "utils/uuid.h"
#include "utils/varbit.h"
#include "utils/varlena.h"
#ifndef BUILD_PAX_FORMAT
#include "access/reloptions.h"
#endif
#include "catalog/storage.h"
#include "cdb/cdbvars.h"
#include "commands/cluster.h"
#include "common/file_utils.h"
#include "common/int128.h"
#include "common/pg_lzcompress.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "nodes/nodeFuncs.h"
#include "postmaster/syslogger.h"  // for PIPE_CHUNK_SIZE
#include "storage/block.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/md.h"
#include "storage/procarray.h"
#include "storage/relfilenode.h"
#include "storage/smgr.h"
#include "storage/ufile.h"
#include "tcop/utility.h"
#include "utils/backend_progress.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "utils/faultinjector.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"
#include "utils/spccache.h"
#include "utils/syscache.h"
#include "utils/tuplesort.h"
#include "utils/wait_event.h"
#include "access/xlogutils.h"

// no header file in cbdb
extern BlockNumber system_nextsampleblock(SampleScanState *node,  // NOLINT
                                          BlockNumber nblocks);
extern bool extractcolumns_from_node(Node *expr, bool *cols,  // NOLINT
                                     AttrNumber natts);
extern int get_partition_for_tuple(PartitionKey key,
                                   PartitionDesc partdesc,  // NOLINT
                                   Datum *values, bool *isnull);
extern Oid GetDefaultOpClass(Oid type_id, Oid am_id);

#ifdef __cplusplus
}
#pragma GCC diagnostic pop
#endif

#endif  // SRC_CPP_COMM_CBDB_API_H_

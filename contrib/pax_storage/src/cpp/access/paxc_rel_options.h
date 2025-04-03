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
 * paxc_rel_options.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/access/paxc_rel_options.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include "comm/cbdb_api.h"

#include "exceptions/CException.h"
#include "storage/pax_defined.h"
#include "storage/proto/proto_wrappers.h"  // for ColumnEncoding_Kind

namespace paxc {

#define ColumnEncoding_Kind_NO_ENCODED_STR "none"
#define ColumnEncoding_Kind_RLE_V2_STR "rle"
#define ColumnEncoding_Kind_DIRECT_DELTA_STR "delta"
#define ColumnEncoding_Kind_DICTIONARY_STR "dict"
#define ColumnEncoding_Kind_COMPRESS_ZSTD_STR "zstd"
#define ColumnEncoding_Kind_COMPRESS_ZLIB_STR "zlib"

#define STORAGE_FORMAT_TYPE_PORC "porc"
#define STORAGE_FORMAT_TYPE_PORC_VEC "porc_vec"
#define STORAGE_FORMAT_TYPE_DEFAULT STORAGE_FORMAT_TYPE_PORC

#define PAX_DEFAULT_COMPRESSLEVEL AO_DEFAULT_COMPRESSLEVEL
#define PAX_MIN_COMPRESSLEVEL AO_MIN_COMPRESSLEVEL
#define PAX_MAX_COMPRESSLEVEL AO_MAX_COMPRESSLEVEL
#define PAX_DEFAULT_COMPRESSTYPE ColumnEncoding_Kind_NO_ENCODED_STR

#define PAX_MIN_PARALLEL_WORKERS 0
#define PAX_MAX_PARALLEL_WORKERS 64
#define PAX_DEFAULT_PARALLEL_WORKERS -1
#define PAX_ZORDER_CLUSTER_TYPE "zorder"
#define PAX_LEXICAL_CLUSTER_TYPE "lexical"
#define PAX_CLUSTER_TYPE_DEFAULT PAX_ZORDER_CLUSTER_TYPE

#define PAX_SOPT_STORAGE_FORMAT "storage_format"
#define PAX_SOPT_COMPTYPE SOPT_COMPTYPE
#define PAX_SOPT_COMPLEVEL SOPT_COMPLEVEL
#define PAX_SOPT_MINMAX_COLUMNS "minmax_columns"
#define PAX_SOPT_BLOOMFILTER_COLUMNS "bloomfilter_columns"
#define PAX_SOPT_CLUSTER_COLUMNS "cluster_columns"
#define PAX_SOPT_PARALLEL_WORKERS "parallel_workers"
#define PAX_SOPT_CLUSTER_TYPE "cluster_type"

// plain structure used by reloptions, can be accessed from C++ code.
struct PaxOptions {
  // Pax needs to define the StdRdOptions instead of just vl_len.
  // This is because many places in the CBDB assume that option in
  // relation can be cast into StdRdOptions.
  StdRdOptions rd_options;
  char storage_format[16];
  char compress_type[16];
  char cluster_type[16];
  int compress_level;
  int minmax_columns_offset = 0;
  int bloomfilter_columns_offset = 0;
  int cluster_columns_offset = 0;

  char *minmax_columns() {
    return minmax_columns_offset == 0
               ? NULL
               : reinterpret_cast<char *>(this) + minmax_columns_offset;
  }

  char *bloomfilter_columns() {
    return bloomfilter_columns_offset == 0
               ? NULL
               : reinterpret_cast<char *>(this) + bloomfilter_columns_offset;
  }

  char *cluster_columns() {
    return cluster_columns_offset == 0
               ? NULL
               : reinterpret_cast<char *>(this) + cluster_columns_offset;
  }
};

#define RelationGetOptions(relation, field_name, default_opt)     \
  ((relation)->rd_options                                         \
       ? ((paxc::PaxOptions *)(relation)->rd_options)->field_name \
       : (default_opt))

/*
 * used to register pax rel options
 */
void paxc_reg_rel_options();

/*
 * parse the rel options in `pg_attribute_encoding` and relation
 * if no ENCODING setting in `pg_attribute_encoding` will fill with
 * the default one
 */
bytea *paxc_default_rel_options(Datum reloptions, char /*relkind*/,
                                bool validate);

/*
 * parse the attr options from `pg_attribute_encoding`
 * if no ENCODING setting in `pg_attribute_encoding` will fill with
 * the default one
 */
PaxOptions **paxc_relation_get_attribute_options(Relation rel);

/*
 * validate the ENCODING CLAUSES
 * like `CREATE TABLE t1 (c1 int, COLUMN c1 ENCODING (key=value)) using
 * pax`
 */
void paxc_validate_column_encoding_clauses(List *encoding_opts);

/*
 * transform the ENCODING options if key no setting
 * validate will become true only when the encoding syntax is true
 * like `CREATE TABLE t1 (c1 int ENCODING (key=value)) using pax`
 *
 * pax no need transform the ENCODING options if key no setting
 * it will deal the default value inside pax colomn
 */
List *paxc_transform_column_encoding_clauses(List *encoding_opts, bool validate,
                                             bool fromType);

Bitmapset *paxc_get_columns_index_by_options(
    Relation rel, const char *columns_options,
    void (*check_attr)(Form_pg_attribute), bool validate);

}  // namespace paxc

namespace pax {

// use to transform compress type str to encoding kind
extern ColumnEncoding_Kind CompressKeyToColumnEncodingKind(
    const char *encoding_str);

extern PaxStorageFormat StorageFormatKeyToPaxStorageFormat(
    const char *storage_format_str);

}  // namespace pax

namespace cbdb {
std::vector<int> GetMinMaxColumnIndexes(Relation rel);
std::vector<int> GetBloomFilterColumnIndexes(Relation rel);
std::vector<int> GetClusterColumnIndexes(Relation rel);
std::vector<std::tuple<pax::ColumnEncoding_Kind, int>> GetRelEncodingOptions(
    Relation rel);
}  // namespace cbdb

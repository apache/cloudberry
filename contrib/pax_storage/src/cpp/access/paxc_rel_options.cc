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
 * paxc_rel_options.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/access/paxc_rel_options.cc
 *
 *-------------------------------------------------------------------------
 */

#include "access/paxc_rel_options.h"

#include "clustering/zorder_utils.h"
#include "comm/cbdb_wrappers.h"

namespace paxc {

typedef struct {
  const char *optname; /* option's name */
  const pax::ColumnEncoding_Kind kind;
} relopt_compress_type_mapping;

static const relopt_compress_type_mapping kSelfRelCompressMap[] = {
    {ColumnEncoding_Kind_NO_ENCODED_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_NO_ENCODED},
    {ColumnEncoding_Kind_RLE_V2_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_RLE_V2},
    {ColumnEncoding_Kind_DIRECT_DELTA_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_DIRECT_DELTA},
    {ColumnEncoding_Kind_DICTIONARY_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY},
    {ColumnEncoding_Kind_COMPRESS_ZSTD_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZSTD},
    {ColumnEncoding_Kind_COMPRESS_ZLIB_STR,
     pax::ColumnEncoding_Kind::ColumnEncoding_Kind_COMPRESS_ZLIB},
};

typedef struct {
  const char *optname; /* option's name */
  const pax::PaxStorageFormat format;
} relopt_format_type_mapping;

static const relopt_format_type_mapping kSelfRelFormatMap[] = {
    {STORAGE_FORMAT_TYPE_PORC, pax::PaxStorageFormat::kTypeStoragePorcNonVec},
    {STORAGE_FORMAT_TYPE_PORC_VEC, pax::PaxStorageFormat::kTypeStoragePorcVec},
};

// reloptions structure and variables.
static relopt_kind self_relopt_kind;

#define PAX_COPY_STR_OPT(pax_opts_, pax_opt_name_)                            \
  do {                                                                        \
    PaxOptions *pax_opts = reinterpret_cast<PaxOptions *>(pax_opts_);         \
    int pax_name_offset_ = *reinterpret_cast<int *>(pax_opts->pax_opt_name_); \
    if (pax_name_offset_)                                                     \
      strlcpy(pax_opts->pax_opt_name_,                                        \
              reinterpret_cast<char *>(pax_opts) + pax_name_offset_,          \
              sizeof(pax_opts->pax_opt_name_));                               \
  } while (0)

static const char *kSelfColumnEncodingClauseWhiteList[] = {
    PAX_SOPT_COMPTYPE,
    PAX_SOPT_COMPLEVEL,
};

static const char *kSelfClusterTypeWhiteList[] = {
    PAX_ZORDER_CLUSTER_TYPE,
    PAX_LEXICAL_CLUSTER_TYPE,
};

static const relopt_parse_elt kSelfReloptTab[] = {
    // relation->rd_options has stored PaxOptions, but when call
    // RelationGetParallelWorkers() outsize of extension, kernel not know about
    // PaxOptions
    {PAX_SOPT_PARALLEL_WORKERS, RELOPT_TYPE_INT,
     offsetof(StdRdOptions, parallel_workers)},
    // no allow set with encoding
    {PAX_SOPT_STORAGE_FORMAT, RELOPT_TYPE_STRING,
     offsetof(PaxOptions, storage_format)},
    // allow with encoding
    {PAX_SOPT_COMPTYPE, RELOPT_TYPE_STRING,
     offsetof(PaxOptions, compress_type)},
    {PAX_SOPT_CLUSTER_TYPE, RELOPT_TYPE_STRING,
     offsetof(PaxOptions, cluster_type)},
    {PAX_SOPT_COMPLEVEL, RELOPT_TYPE_INT, offsetof(PaxOptions, compress_level)},
    {PAX_SOPT_MINMAX_COLUMNS, RELOPT_TYPE_STRING,
     offsetof(PaxOptions, minmax_columns_offset)},
    {PAX_SOPT_BLOOMFILTER_COLUMNS, RELOPT_TYPE_STRING,
     offsetof(PaxOptions, bloomfilter_columns_offset)},
    {PAX_SOPT_CLUSTER_COLUMNS, RELOPT_TYPE_STRING,
     offsetof(PaxOptions, cluster_columns_offset)},

};

static void paxc_validate_rel_options_storage_format(const char *value) {
  size_t i;

  for (i = 0; i < lengthof(kSelfRelFormatMap); i++) {
    if (strcmp(value, kSelfRelFormatMap[i].optname) == 0) return;
  }
  ereport(ERROR, (errmsg("unsupported storage format: '%s'", value)));
}

static void paxc_validate_rel_options_compress_type(const char *value) {
  size_t i;

  for (i = 0; i < lengthof(kSelfRelCompressMap); i++) {
    if (strcmp(value, kSelfRelCompressMap[i].optname) == 0) return;
  }
  ereport(ERROR, (errmsg("unsupported compress type: '%s'", value)));
}

static void paxc_validate_rel_option(PaxOptions *options) {
  Assert(options);
  if (strcmp(ColumnEncoding_Kind_NO_ENCODED_STR, options->compress_type) == 0 ||
      strcmp(ColumnEncoding_Kind_RLE_V2_STR, options->compress_type) == 0 ||
      strcmp(ColumnEncoding_Kind_DIRECT_DELTA_STR, options->compress_type) ==
          0 ||
      strcmp(ColumnEncoding_Kind_DICTIONARY_STR, options->compress_type) == 0) {
    if (options->compress_level != 0) {
      ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("compresslevel=%d should setting is not work for "
                             "current encoding.",
                             options->compress_level)));
    }
  }
}

static void paxc_validate_rel_options_cluster_type(const char *value) {
  size_t i;

  for (i = 0; i < lengthof(kSelfClusterTypeWhiteList); i++) {
    if (strcmp(value, kSelfClusterTypeWhiteList[i]) == 0) return;
  }
  ereport(ERROR,
          (errmsg("unsupported cluster type: '%s', only support %s and %s",
                  value, PAX_ZORDER_CLUSTER_TYPE, PAX_LEXICAL_CLUSTER_TYPE)));
}

bytea *paxc_default_rel_options(Datum reloptions, char /*relkind*/,
                                bool validate) {
  Assert(self_relopt_kind != 0);
  bytea *rdopts = (bytea *)build_reloptions(
      reloptions, validate, self_relopt_kind, sizeof(PaxOptions),
      kSelfReloptTab, lengthof(kSelfReloptTab));

  PAX_COPY_STR_OPT(rdopts, storage_format);
  PAX_COPY_STR_OPT(rdopts, compress_type);
  PAX_COPY_STR_OPT(rdopts, cluster_type);
  return rdopts;
}

PaxOptions **paxc_relation_get_attribute_options(Relation rel) {
  Datum *dats;
  PaxOptions **opts;
  int i;

  Assert(rel && OidIsValid(RelationGetRelid(rel)));

  opts = (PaxOptions **)palloc0(RelationGetNumberOfAttributes(rel) *
                                sizeof(PaxOptions *));

  dats = get_rel_attoptions(RelationGetRelid(rel),
                            RelationGetNumberOfAttributes(rel));

  for (i = 0; i < RelationGetNumberOfAttributes(rel); i++) {
    if (DatumGetPointer(dats[i]) != NULL) {
      opts[i] = (PaxOptions *)paxc_default_rel_options(dats[i], 0, false);
      pfree(DatumGetPointer(dats[i]));
    }
  }
  pfree(dats);

  return opts;
}

static void paxc_validate_single_column_encoding_clauses(
    List *single_column_encoding) {
  ListCell *cell = NULL;
  Datum d;
  PaxOptions *option = NULL;
  /* not allow caller pass the `PAX_SOPT_STORAGE_FORMAT`
   */
  foreach (cell, single_column_encoding) {
    DefElem *def = (DefElem *)lfirst(cell);
    bool not_in_white_list = true;

    if (!def->defname) {
      continue;
    }

    for (size_t i = 0; i < lengthof(kSelfColumnEncodingClauseWhiteList); i++) {
      if (strcmp(kSelfColumnEncodingClauseWhiteList[i], def->defname) == 0) {
        not_in_white_list = false;
        break;
      }
    }

    if (not_in_white_list) {
      ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                      errmsg("%s not allow setting in ENCODING CLAUSES.",
                             def->defname)));
    }
  }

  d = transformRelOptions(PointerGetDatum(NULL), single_column_encoding, NULL,
                          NULL, true, false);

  option = (PaxOptions *)paxc_default_rel_options(d, 0, true);
  paxc_validate_rel_option(option);
}

void paxc_validate_column_encoding_clauses(List *encoding_opts) {
  ListCell *lc;
  foreach (lc, encoding_opts) {
    ColumnReferenceStorageDirective *crsd =
        (ColumnReferenceStorageDirective *)lfirst(lc);
    paxc_validate_single_column_encoding_clauses(crsd->encoding);
  }
}

List *paxc_transform_column_encoding_clauses(List *encoding_opts, bool validate,
                                             bool fromType) {
  List *ret_list = NIL;

  if (fromType) {
    return NIL;
  }

  ret_list = list_copy(encoding_opts);
  /* there are no need to do column encoding clauses transform in pax
   * because pax will setting default encoding inside
   */
  if (validate) {
    paxc_validate_single_column_encoding_clauses(encoding_opts);
  }

  /* if column no setting the encoding clauses
   * in transformColumnEncoding will pass the relation option
   * to column encoding clauses, should remove the
   * `PAX_SOPT_STORAGE_FORMAT` from it.
   */
  ListCell *cell = NULL;
  foreach (cell, ret_list) {
    DefElem *def = (DefElem *)lfirst(cell);
    bool not_in_white_list = true;
    if (!def->defname) {
      continue;
    }

    for (size_t i = 0; i < lengthof(kSelfColumnEncodingClauseWhiteList); i++) {
      if (strcmp(kSelfColumnEncodingClauseWhiteList[i], def->defname) == 0) {
        not_in_white_list = false;
        break;
      }
    }

    if (not_in_white_list) {
      ret_list = foreach_delete_current(ret_list, cell);
    }
  }

  return ret_list;
}

Bitmapset *paxc_get_columns_index_by_options(
    Relation rel, const char *columns_options,
    void (*check_attr)(Form_pg_attribute), bool validate) {
  Assert(rel->rd_rel->relam == PAX_AM_OID);

  List *list = NIL;
  ListCell *lc;
  auto tupdesc = RelationGetDescr(rel);
  auto natts = RelationGetNumberOfAttributes(rel);
  Bitmapset *bms = nullptr;

  if (!columns_options) return nullptr;

  auto value = pstrdup(columns_options);
  if (!SplitDirectoriesString(value, ',', &list))
    elog(ERROR, "invalid columns: '%s' '%s'", value, columns_options);

  pfree(value);
  foreach (lc, list) {
    auto s = (char *)lfirst(lc);
    int i;
    bool dropped_column = false;
    bool valid_column;

    for (i = 0; i < natts; i++) {
      auto attr = TupleDescAttr(tupdesc, i);
      if (strcmp(s, NameStr(attr->attname)) == 0) {
        if (!attr->attisdropped) {
          if (check_attr) check_attr(attr);
          break;
        }

        if (validate) elog(ERROR, "pax: can't use dropped column");
        dropped_column = true;
        break;
      }
    }
    valid_column = !dropped_column && i < natts;
    if (validate) {
      if (i == natts) elog(ERROR, "invalid column name '%s'", s);

      if (bms_is_member(i, bms)) elog(ERROR, "duplicated column name '%s'", s);
    }

    if (valid_column) bms = bms_add_member(bms, i);
  }
  list_free_deep(list);
  return bms;
}

void paxc_reg_rel_options() {
  self_relopt_kind = add_reloption_kind();

  add_int_reloption(self_relopt_kind, PAX_SOPT_PARALLEL_WORKERS,
                    "parallel workers", PAX_DEFAULT_PARALLEL_WORKERS,
                    PAX_MIN_PARALLEL_WORKERS, PAX_MAX_PARALLEL_WORKERS,
                    AccessExclusiveLock);

  add_string_reloption(self_relopt_kind, PAX_SOPT_STORAGE_FORMAT,
                       "pax storage format", STORAGE_FORMAT_TYPE_DEFAULT,
                       paxc_validate_rel_options_storage_format,
                       AccessExclusiveLock);
  add_string_reloption(self_relopt_kind, PAX_SOPT_COMPTYPE, "pax compress type",
                       PAX_DEFAULT_COMPRESSTYPE,
                       paxc_validate_rel_options_compress_type,
                       AccessExclusiveLock);
  add_int_reloption(self_relopt_kind, PAX_SOPT_COMPLEVEL, "pax compress level",
                    PAX_DEFAULT_COMPRESSLEVEL, PAX_MIN_COMPRESSLEVEL,
                    PAX_MAX_COMPRESSLEVEL, AccessExclusiveLock);
  add_string_reloption(self_relopt_kind, PAX_SOPT_MINMAX_COLUMNS,
                       "minmax columns", NULL, NULL, AccessExclusiveLock);
  add_string_reloption(self_relopt_kind, PAX_SOPT_BLOOMFILTER_COLUMNS,
                       "minmax columns", NULL, NULL, AccessExclusiveLock);
  add_string_reloption(self_relopt_kind, PAX_SOPT_CLUSTER_COLUMNS,
                       "cluster columns", NULL, NULL, AccessExclusiveLock);
  add_string_reloption(self_relopt_kind, PAX_SOPT_CLUSTER_TYPE, "cluster type",
                       PAX_CLUSTER_TYPE_DEFAULT,
                       paxc_validate_rel_options_cluster_type,
                       AccessExclusiveLock);
}

}  // namespace paxc

namespace pax {

ColumnEncoding_Kind CompressKeyToColumnEncodingKind(const char *encoding_str) {
  Assert(encoding_str);

  for (size_t i = 0; i < lengthof(paxc::kSelfRelCompressMap); i++) {
    if (strcmp(paxc::kSelfRelCompressMap[i].optname, encoding_str) == 0) {
      return paxc::kSelfRelCompressMap[i].kind;
    }
  }

  CBDB_RAISE(cbdb::CException::kExTypeLogicError);
}

PaxStorageFormat StorageFormatKeyToPaxStorageFormat(
    const char *storage_format_str) {
  Assert(storage_format_str);

  for (size_t i = 0; i < lengthof(paxc::kSelfRelFormatMap); i++) {
    if (strcmp(paxc::kSelfRelFormatMap[i].optname, storage_format_str) == 0) {
      return paxc::kSelfRelFormatMap[i].format;
    }
  }

  CBDB_RAISE(cbdb::CException::kExTypeLogicError);
}

}  // namespace pax

static std::vector<int> GetColumnsIndexByOptions(
    Relation rel, char *options, void (*check_attr)(Form_pg_attribute)) {
  std::vector<int> indexes;
  Bitmapset *bms = nullptr;
  int idx;

  if (!options) {
    return indexes;
  }

  CBDB_WRAP_START;
  {
    bms = paxc::paxc_get_columns_index_by_options(rel, options, check_attr,
                                                  false);
  }
  CBDB_WRAP_END;

  idx = -1;
  while ((idx = bms_next_member(bms, idx)) >= 0) {
    indexes.push_back(idx);
  }

  {
    CBDB_WRAP_START;
    { bms_free(bms); }
    CBDB_WRAP_END;
  }

  return indexes;
}

namespace cbdb {

std::vector<int> GetMinMaxColumnIndexes(Relation rel) {
  auto options = (paxc::PaxOptions *)rel->rd_options;
  return GetColumnsIndexByOptions(
      rel, options ? options->minmax_columns() : nullptr, nullptr);
}

std::vector<int> GetBloomFilterColumnIndexes(Relation rel) {
  auto options = (paxc::PaxOptions *)rel->rd_options;
  return GetColumnsIndexByOptions(
      rel, options ? options->bloomfilter_columns() : nullptr, nullptr);
}

std::vector<int> GetClusterColumnIndexes(Relation rel) {
  auto options = (paxc::PaxOptions *)rel->rd_options;
  return GetColumnsIndexByOptions(
      rel, options ? options->cluster_columns() : nullptr,
      [](Form_pg_attribute attr) {
        // this callback in the paxc env
        if (!paxc::support_zorder_type(attr->atttypid)) {
          elog(ERROR, "the type of column %s does not support zorder cluster",
               attr->attname.data);
        }
      });
}

std::vector<std::tuple<pax::ColumnEncoding_Kind, int>> GetRelEncodingOptions(
    Relation rel) {
  size_t natts = 0;
  paxc::PaxOptions **pax_options = nullptr;
  std::vector<std::tuple<pax::ColumnEncoding_Kind, int>> encoding_opts;

  CBDB_WRAP_START;
  { pax_options = paxc::paxc_relation_get_attribute_options(rel); }
  CBDB_WRAP_END;
  Assert(pax_options);

  natts = rel->rd_att->natts;

  for (size_t index = 0; index < natts; index++) {
    if (pax_options[index]) {
      encoding_opts.emplace_back(
          std::make_tuple(pax::CompressKeyToColumnEncodingKind(
                              pax_options[index]->compress_type),
                          pax_options[index]->compress_level));
    } else {
      // TODO(jiaqizho): In pax, we will fill a `DEF_ENCODED` if user not set
      // the encoding clause. Need a GUC to decide whether we should use
      // `NO_ENCODE` or keep use `DEF_ENCODED` also may allow user define
      // different default encoding type for the different pg_type?
      encoding_opts.emplace_back(
          std::make_tuple(pax::ColumnEncoding_Kind_DEF_ENCODED, 0));
    }
  }
  cbdb::Pfree(pax_options);
  return encoding_opts;
}

}  //  namespace cbdb

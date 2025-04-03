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
 * micro_partition_udf.cc
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/micro_partition_udf.cc
 *
 *-------------------------------------------------------------------------
 */


#include "comm/cbdb_wrappers.h"
#include "comm/paxc_wrappers.h"
#include "storage/orc/orc_dump_reader.h"

extern "C" {
extern Datum dump_pax_file_desc(PG_FUNCTION_ARGS);
extern Datum dump_pax_file_desc_post_script(PG_FUNCTION_ARGS);
extern Datum dump_pax_file_desc_footer(PG_FUNCTION_ARGS);
extern Datum dump_pax_file_desc_schema(PG_FUNCTION_ARGS);
extern Datum dump_pax_file_desc_group_info(PG_FUNCTION_ARGS);
extern Datum dump_pax_file_desc_group_footer(PG_FUNCTION_ARGS);
extern Datum dump_pax_file_data(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(dump_pax_file_desc);
PG_FUNCTION_INFO_V1(dump_pax_file_desc_post_script);
PG_FUNCTION_INFO_V1(dump_pax_file_desc_footer);
PG_FUNCTION_INFO_V1(dump_pax_file_desc_schema);
PG_FUNCTION_INFO_V1(dump_pax_file_desc_group_info);
PG_FUNCTION_INFO_V1(dump_pax_file_desc_group_footer);
PG_FUNCTION_INFO_V1(dump_pax_file_data);
}

static char *do_dump(pax::tools::DumpConfig *config) {
  bool ok;
  char *result_rc;
  std::string result;
  pax::tools::OrcDumpReader *reader = nullptr;

  CBDB_TRY();
  {
    reader = new pax::tools::OrcDumpReader(config);
    ok = reader->Open();
    if (ok) {
      result = reader->Dump();
    }
  }
  CBDB_CATCH_DEFAULT();
  CBDB_FINALLY({
    if (reader) {
      reader->Close();
      delete reader;
    }
  });
  CBDB_END_TRY();

  if (!ok) {
    ereport(ERROR, (errmsg("Failed to dump current file: %s, Toast file: %s",
                           config->file_name,
                           config->toast_file_name ? config->toast_file_name
                                                   : "NULL")));
  }

  result_rc = (char *)cbdb::Palloc0(result.length() + 1);
  memcpy(result_rc, result.c_str(), result.length());

  return result_rc;
}

// CREATE OR REPLACE FUNCTION dump_pax_file_desc(file_path text, spcid Oid)
// RETURNS text
//      AS '$libdir/pax', 'dump_pax_file_desc' LANGUAGE C IMMUTABLE;
Datum dump_pax_file_desc(PG_FUNCTION_ARGS) {
  pax::tools::DumpConfig config;

  if (PG_ARGISNULL(0)) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("No specify the path of PAX file")));
  }

  config.file_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
  config.print_all_desc = true;

  if (!PG_ARGISNULL(1)) {
    Oid spcid = PG_GETARG_OID(1);
    if (spcid != InvalidOid && paxc::IsDfsTablespaceById(spcid)) {
      config.dfs_tblspcid = spcid;
    }
  }

  PG_RETURN_TEXT_P(cstring_to_text(do_dump(&config)));
}

// CREATE OR REPLACE FUNCTION dump_pax_file_desc_post_script(file_path text,
// spcid Oid) RETURNS text
//      AS '$libdir/pax', 'dump_pax_file_desc_post_script' LANGUAGE C IMMUTABLE;
Datum dump_pax_file_desc_post_script(PG_FUNCTION_ARGS) {
  pax::tools::DumpConfig config;

  if (PG_ARGISNULL(0)) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("No specify the path of PAX file")));
  }

  config.file_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
  config.print_post_script = true;

  if (!PG_ARGISNULL(1)) {
    Oid spcid = PG_GETARG_OID(1);
    if (spcid != InvalidOid && paxc::IsDfsTablespaceById(spcid)) {
      config.dfs_tblspcid = spcid;
    }
  }

  PG_RETURN_TEXT_P(cstring_to_text(do_dump(&config)));
}

// CREATE OR REPLACE FUNCTION dump_pax_file_desc_footer(file_path text, spcid
// Oid) RETURNS text
//      AS '$libdir/pax', 'dump_pax_file_desc_footer' LANGUAGE C IMMUTABLE;
Datum dump_pax_file_desc_footer(PG_FUNCTION_ARGS) {
  pax::tools::DumpConfig config;

  if (PG_ARGISNULL(0)) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("No specify the path of PAX file")));
  }

  config.file_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
  config.print_footer = true;

  if (!PG_ARGISNULL(1)) {
    Oid spcid = PG_GETARG_OID(1);
    if (spcid != InvalidOid && paxc::IsDfsTablespaceById(spcid)) {
      config.dfs_tblspcid = spcid;
    }
  }

  PG_RETURN_TEXT_P(cstring_to_text(do_dump(&config)));
}

// CREATE OR REPLACE FUNCTION dump_pax_file_desc_schema(file_path text, spcid
// Oid) RETURNS text
//      AS '$libdir/pax', 'dump_pax_file_desc_schema' LANGUAGE C IMMUTABLE;
Datum dump_pax_file_desc_schema(PG_FUNCTION_ARGS) {
  pax::tools::DumpConfig config;

  if (PG_ARGISNULL(0)) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("No specify the path of PAX file")));
  }

  config.file_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
  config.print_schema = true;

  if (!PG_ARGISNULL(1)) {
    Oid spcid = PG_GETARG_OID(1);
    if (spcid != InvalidOid && paxc::IsDfsTablespaceById(spcid)) {
      config.dfs_tblspcid = spcid;
    }
  }

  PG_RETURN_TEXT_P(cstring_to_text(do_dump(&config)));
}

// CREATE OR REPLACE FUNCTION dump_pax_file_desc_group_info(file_path text,
// spcid Oid, group_start int4, group_len int4) RETURNS text
//      AS '$libdir/pax', 'dump_pax_file_desc_group_info' LANGUAGE C IMMUTABLE;
Datum dump_pax_file_desc_group_info(PG_FUNCTION_ARGS) {
  pax::tools::DumpConfig config;

  if (PG_ARGISNULL(0)) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("No specify the path of PAX file")));
  }

  config.file_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
  config.print_group_info = true;

  if (!PG_ARGISNULL(1)) {
    Oid spcid = PG_GETARG_OID(1);
    if (spcid != InvalidOid && paxc::IsDfsTablespaceById(spcid)) {
      config.dfs_tblspcid = spcid;
    }
  }

  if (!PG_ARGISNULL(2) && !PG_ARGISNULL(3)) {
    config.group_id_start = PG_GETARG_INT32(2);
    config.group_id_len = PG_GETARG_INT32(3);
  }

  PG_RETURN_TEXT_P(cstring_to_text(do_dump(&config)));
}

// CREATE OR REPLACE FUNCTION dump_pax_file_desc_group_footer(file_path text,
// spcid Oid, group_start int4, group_len int4) RETURNS text
//      AS '$libdir/pax', 'dump_pax_file_desc_group_footer' LANGUAGE C
//      IMMUTABLE;
Datum dump_pax_file_desc_group_footer(PG_FUNCTION_ARGS) {
  pax::tools::DumpConfig config;

  if (PG_ARGISNULL(0)) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("No specify the path of PAX file")));
  }

  config.file_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
  config.print_group_footer = true;

  if (!PG_ARGISNULL(1)) {
    Oid spcid = PG_GETARG_OID(1);
    if (spcid != InvalidOid && paxc::IsDfsTablespaceById(spcid)) {
      config.dfs_tblspcid = spcid;
    }
  }

  if (!PG_ARGISNULL(2) && !PG_ARGISNULL(3)) {
    config.group_id_start = PG_GETARG_INT32(2);
    config.group_id_len = PG_GETARG_INT32(3);
  }

  PG_RETURN_TEXT_P(cstring_to_text(do_dump(&config)));
}

// CREATE OR REPLACE FUNCTION dump_pax_file_data(file_path text, toast_file_path
// text, spcid Oid, groupid int4, colid_start int4, colid_len int4, rowid_start
// int4, rowid_len int4) RETURNS text
//      AS '$libdir/pax', 'dump_pax_file_data' LANGUAGE C IMMUTABLE;
Datum dump_pax_file_data(PG_FUNCTION_ARGS) {
  pax::tools::DumpConfig config;

  if (PG_ARGISNULL(0)) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("No specify the path of PAX file")));
  }

  config.file_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
  config.print_all_data = true;

  if (!PG_ARGISNULL(1)) {
    config.toast_file_name = text_to_cstring(PG_GETARG_TEXT_PP(1));
  }

  if (!PG_ARGISNULL(2)) {
    Oid spcid = PG_GETARG_OID(2);
    if (spcid != InvalidOid && paxc::IsDfsTablespaceById(spcid)) {
      config.dfs_tblspcid = spcid;
    }
  }

  if (PG_ARGISNULL(3)) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("group_id cannot be null")));
  }
  config.group_id_start = PG_GETARG_INT32(3);
  config.group_id_len = 1;

  if (PG_ARGISNULL(4)) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("colid_start cannot be null")));
  }
  config.column_id_start = PG_GETARG_INT32(4);

  if (PG_ARGISNULL(5)) {
    ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("colid_len cannot be null")));
  }
  config.column_id_len = PG_GETARG_INT32(5);

  if (!PG_ARGISNULL(6) && !PG_ARGISNULL(7)) {
    config.row_id_start = PG_GETARG_INT32(6);
    config.row_id_len = PG_GETARG_INT32(7);
  }

  PG_RETURN_TEXT_P(cstring_to_text(do_dump(&config)));
}

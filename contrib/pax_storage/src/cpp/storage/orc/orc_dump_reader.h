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
 * orc_dump_reader.h
 *
 * IDENTIFICATION
 *	  contrib/pax_storage/src/cpp/storage/orc/orc_dump_reader.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include <cstdlib>
#include <string>
namespace pax {
class OrcFormatReader;
namespace tools {

#define NO_SPEC_ID -1
#define NO_SPEC_LEN 0

struct DumpConfig {
  char *file_name = nullptr;
  char *toast_file_name = nullptr;
  bool print_all = false;
  bool print_all_desc = false;
  bool print_post_script = false;
  bool print_footer = false;
  bool print_schema = false;
  bool print_group_info = false;
  bool print_group_footer = false;
  bool print_all_data = false;
  unsigned int dfs_tblspcid = 0;

  int64_t group_id_start = NO_SPEC_ID;
  int64_t group_id_len = NO_SPEC_LEN;

  int64_t column_id_start = NO_SPEC_ID;
  int64_t column_id_len = NO_SPEC_LEN;

  int64_t row_id_start = NO_SPEC_ID;
  int64_t row_id_len = NO_SPEC_LEN;
};

class OrcDumpReader final {
 public:
  explicit OrcDumpReader(DumpConfig *config);

  bool Open();
  std::string Dump();
  void Close();

 private:
  std::string DumpAllInfo();
  std::string DumpAllDesc();
  std::string DumpPostScript();
  std::string DumpFooter();
  std::string DumpSchema();
  std::string DumpGroupInfo();
  std::string DumpGroupFooter();
  std::string DumpAllData();

 private:
  DumpConfig *config_;
  OrcFormatReader *format_reader_;
};

}  // namespace tools
}  // namespace pax

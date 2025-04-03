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
 * orc_dump_reader.cpp
 *
 * IDENTIFICATION (option)
 *	  contrib/pax_storage/src/cpp/storage/orc/orc_dump_reader.cpp
 *
 *-------------------------------------------------------------------------
 */

#include "storage/orc/orc_dump_reader.h"

#include <sstream>
#include <tabulate/table.hpp>

#include "comm/fmt.h"
#include "comm/singleton.h"
#include "exceptions/CException.h"
#include "storage/columns/pax_column_traits.h"
#include "storage/file_system.h"
#include "storage/local_file_system.h"
#include "storage/orc/orc_defined.h"
#include "storage/orc/orc_format_reader.h"
#include "storage/orc/orc_group.h"

namespace pax::tools {

static inline std::string GetWriterDesc(uint32 writer_id) {
  switch (writer_id) {
    case 1:
      return "CPP writer";
    default:
      return "Unknown";
  }
}

static inline std::string GetStorageFormat(uint32 storage_format) {
  switch (storage_format) {
    case PaxStorageFormat::kTypeStoragePorcNonVec:
      return "Origin format";
    case PaxStorageFormat::kTypeStoragePorcVec:
      return "VEC format";
    default:
      return "Unknown";
  }
}

static inline std::string BoolCast(const bool b) {
  std::ostringstream ss;
  ss << std::boolalpha << b;
  return ss.str();
}

static inline std::tuple<int64, int64, bool> ParseRange(
    const int64 start, const int64 len, const int64 total_size) {
  int64 range_start = 0;
  int64 range_end = total_size;

  if (start != NO_SPEC_ID) {
    if (len == 0 || start + len > total_size) {
      return {range_start, range_end, false};
    }

    range_start = start;
    range_end = start + len;
  }

  return {range_start, range_end, true};
}

OrcDumpReader::OrcDumpReader(DumpConfig *config)
    : config_(config), format_reader_(nullptr) {}

bool OrcDumpReader::Open() {
  FileSystem *fs = nullptr;
  std::shared_ptr<FileSystemOptions> fs_opt;
  std::shared_ptr<File> open_file;
  std::shared_ptr<File> open_toast_file;

  assert(config_);
  assert(config_->file_name);

  fs = pax::Singleton<LocalFileSystem>::GetInstance();

  open_file = fs->Open(config_->file_name, fs::kReadMode, fs_opt);
  if (open_file->FileLength() == 0) {
    goto err1_out;
  }

  if (config_->toast_file_name) {
    open_toast_file = fs->Open(config_->toast_file_name, fs::kReadMode, fs_opt);
    if (open_toast_file->FileLength() == 0) {
      goto err2_out;
    }
  }

  format_reader_ = new OrcFormatReader(open_file, open_toast_file);
  format_reader_->Open();

  return true;

err2_out:
  open_toast_file->Close();

err1_out:
  open_file->Close();

  return false;
}

void OrcDumpReader::Close() {
  if (format_reader_) {
    format_reader_->Close();
    delete format_reader_;
    format_reader_ = nullptr;
  }
}

std::string OrcDumpReader::Dump() {
  if (config_->print_all) {
    return DumpAllInfo();
  }

  if (config_->print_all_desc) {
    return DumpAllDesc();
  }

  if (config_->print_post_script) {
    return DumpPostScript();
  }

  if (config_->print_footer) {
    return DumpFooter();
  }

  if (config_->print_schema) {
    return DumpSchema();
  }

  if (config_->print_group_info) {
    return DumpGroupInfo();
  }

  if (config_->print_group_footer) {
    return DumpGroupFooter();
  }

  if (config_->print_all_data) {
    return DumpAllData();
  }

  // defualt dump all of desc
  return DumpAllDesc();
}

std::string OrcDumpReader::DumpAllInfo() {
  std::stringstream all_info;
  all_info << DumpAllDesc() << "\n" << DumpAllData();
  all_info.flush();
  return all_info.str();
}

std::string OrcDumpReader::DumpAllDesc() {
  std::stringstream all_desc;
  all_desc << DumpPostScript() << "\n"
           << DumpFooter() << "\n"
           << DumpSchema() << "\n"
           << DumpGroupInfo() << "\n"
           << DumpGroupFooter();
  all_desc.flush();

  return all_desc.str();
}

std::string OrcDumpReader::DumpPostScript() {
  tabulate::Table post_srcipt_table;
  tabulate::Table desc_table;
  auto postsrcipt = &(format_reader_->post_script_);

  desc_table.add_row(
      {"Major version", std::to_string(postsrcipt->majorversion())});
  desc_table.add_row(
      {"Minor version", std::to_string(postsrcipt->minorversion())});
  desc_table.add_row({"Writer desc", GetWriterDesc(postsrcipt->writer())});
  desc_table.add_row(
      {"Footer length", std::to_string(postsrcipt->footerlength())});
  desc_table.add_row({"Magic", postsrcipt->magic()});

  post_srcipt_table.add_row(tabulate::Table::Row_t{"Post Script description"});
  post_srcipt_table[0].format().font_align(tabulate::FontAlign::center);
  post_srcipt_table[0].format().font_color(tabulate::Color::red);

  post_srcipt_table.add_row(tabulate::Table::Row_t{desc_table});
  post_srcipt_table[1].format().hide_border_top();

  return post_srcipt_table.str();
}

std::string OrcDumpReader::DumpSchema() {
  tabulate::Table schema_table;
  tabulate::Table desc_table;

  auto footer = &(format_reader_->file_footer_);
  auto col_infos = &(footer->colinfo());

  // verify schema exist
  auto max_id = footer->types_size();
  CBDB_CHECK(max_id > 0, cbdb::CException::ExType::kExTypeInvalidPORCFormat,
             fmt("Invalid FOOTER pb structure, the schema is empty or invalid. "
                 "[type mid=%d]",
                 max_id));

  // verify schema defined
  auto struct_types = &(footer->types(0));
  CBDB_CHECK(struct_types->kind() == pax::porc::proto::Type_Kind_STRUCT,
             cbdb::CException::ExType::kExTypeInvalidPORCFormat,
             fmt("Invalid FOOTER pb structure, the schema is invalid."
                 "The first type in PORC must be %d but got %d",
                 pax::porc::proto::Type_Kind_STRUCT, struct_types->kind()));
  CBDB_CHECK(
      struct_types->subtypes_size() == col_infos->size(),
      cbdb::CException::ExType::kExTypeInvalidPORCFormat,
      fmt("Invalid FOOTER pb structure, the schema is invalid."
          "Subtypes not match the type sizes [type mid=%d, subtypes size=%d]",
          max_id, struct_types->subtypes_size()));

  // create desc header, types and basic info
  tabulate::Table::Row_t desc_table_header{""};
  tabulate::Table::Row_t desc_table_types{"Type kind"};
  tabulate::Table::Row_t desc_table_typeids{"PG typeid"};
  tabulate::Table::Row_t desc_table_collation{"PG collation"};

  int64 column_start, column_end;
  bool succ;
  std::tie(column_start, column_end, succ) =
      ParseRange(config_->column_id_start, config_->column_id_len,
                 struct_types->subtypes_size());
  CBDB_CHECK(succ, cbdb::CException::ExType::kExTypeInvalid,
             fmt("Fail to parse the column range [file=%s] column range should "
                 "in [0, %u)",
                 config_->file_name, struct_types->subtypes_size()));

  for (int j = column_start; j < column_end; ++j) {
    int sub_type_id = static_cast<int>(struct_types->subtypes(j)) + 1;
    auto sub_type = &(footer->types(sub_type_id));

    desc_table_header.emplace_back(std::string("column" + std::to_string(j)));
    desc_table_types.emplace_back(std::to_string(sub_type->kind()));
    desc_table_typeids.emplace_back(std::to_string((*col_infos)[j].typid()));
    desc_table_collation.emplace_back(
        std::to_string((*col_infos)[j].collation()));
  }

  desc_table.add_row(desc_table_header);
  desc_table.add_row(desc_table_types);
  desc_table.add_row(desc_table_typeids);
  desc_table.add_row(desc_table_collation);

  schema_table.add_row(tabulate::Table::Row_t{"Schema description"});
  schema_table[0].format().font_align(tabulate::FontAlign::center);
  schema_table[0].format().font_color(tabulate::Color::red);

  schema_table.add_row(tabulate::Table::Row_t{desc_table});
  schema_table[1].format().hide_border_top();

  return schema_table.str();
}

std::string OrcDumpReader::DumpFooter() {
  tabulate::Table footer_table;
  tabulate::Table desc_table;

  auto footer = &(format_reader_->file_footer_);

  desc_table.add_row(
      {"Length of Content", std::to_string(footer->contentlength())});
  desc_table.add_row(
      {"Number Of Groups", std::to_string(footer->stripes_size())});
  desc_table.add_row(
      {"Number Of Columns", std::to_string(footer->colinfo_size())});
  desc_table.add_row(
      {"Number Of Rows", std::to_string(footer->numberofrows())});
  desc_table.add_row(
      {"Storage Format", GetStorageFormat(footer->storageformat())});

  footer_table.add_row(tabulate::Table::Row_t{"Footer description"});
  footer_table[0].format().font_align(tabulate::FontAlign::center);
  footer_table[0].format().font_color(tabulate::Color::red);

  footer_table.add_row(tabulate::Table::Row_t{desc_table});
  footer_table[1].format().hide_border_top();

  return footer_table.str();
}

std::string OrcDumpReader::DumpGroupInfo() {
  std::string group_infos;
  auto footer = &(format_reader_->file_footer_);
  auto stripes = &(footer->stripes());
  auto number_of_column = footer->colinfo_size();

  bool succ;
  int64 group_start, group_end;
  int64 column_start, column_end;

  std::tie(group_start, group_end, succ) = ParseRange(
      config_->group_id_start, config_->group_id_len, stripes->size());
  CBDB_CHECK(succ, cbdb::CException::ExType::kExTypeInvalid,
             fmt("Fail to parse the group range [file=%s], group range should "
                 "in [0, %u)",
                 config_->file_name, stripes->size()));

  std::tie(column_start, column_end, succ) = ParseRange(
      config_->column_id_start, config_->column_id_len, number_of_column);
  CBDB_CHECK(succ, cbdb::CException::ExType::kExTypeInvalid,
             fmt("Fail to parse the column range [file=%s], column range "
                 "should in [0, %u)",
                 config_->file_name, number_of_column));

  for (int i = group_start; i < group_end; i++) {
    tabulate::Table group_table;
    tabulate::Table group_desc_table;

    tabulate::Table group_col_desc_table;
    tabulate::Table::Row_t col_desc_table_header{""};
    tabulate::Table::Row_t col_desc_table_allnulls{"All null"};
    tabulate::Table::Row_t col_desc_table_hasnulls{"Has null"};
    tabulate::Table::Row_t col_desc_table_hastoast{"Has Toast"};
    tabulate::Table::Row_t col_desc_table_mins{"Minimal"};
    tabulate::Table::Row_t col_desc_table_maxs{"Maximum"};

    auto stripe = (*stripes)[i];

    CBDB_CHECK(stripe.colstats_size() == number_of_column,
               cbdb::CException::ExType::kExTypeInvalidPORCFormat);

    // full group desc
    group_desc_table.add_row({"Group no", std::to_string(i)});
    group_desc_table.add_row({"Offset", std::to_string(stripe.offset())});
    group_desc_table.add_row(
        {"Data length", std::to_string(stripe.datalength())});
    group_desc_table.add_row(
        {"Stripe footer length", std::to_string(stripe.footerlength())});
    group_desc_table.add_row(
        {"Number of rows", std::to_string(stripe.numberofrows())});
    group_desc_table.add_row(
        {"Toast offset", std::to_string(stripe.toastoffset())});
    group_desc_table.add_row(
        {"Toast length", std::to_string(stripe.toastlength())});
    group_desc_table.add_row(
        {"Number of toast", std::to_string(stripe.numberoftoast())});
    group_desc_table.add_row({"Number of external toast",
                              std::to_string(stripe.exttoastlength_size())});

    // full group col statistics desc
    for (int j = column_start; j < column_end; j++) {
      const auto &col_stats = stripe.colstats(j);
      const auto &col_data_stats = col_stats.coldatastats();
      col_desc_table_header.emplace_back(
          std::string("column" + std::to_string(j)));
      col_desc_table_allnulls.emplace_back(BoolCast(col_stats.allnull()));
      col_desc_table_hasnulls.emplace_back(BoolCast(col_stats.hasnull()));
      col_desc_table_hastoast.emplace_back(BoolCast(col_stats.hastoast()));

      int64 minimal_val;
      int64 maximum_val;
      bool support = true;

      switch (col_data_stats.maximum().size()) {
        case 1: {
          minimal_val = *reinterpret_cast<const int8 *>(  // NOLINT
              col_data_stats.minimal().data());
          maximum_val = *reinterpret_cast<const int8 *>(  // NOLINT
              col_data_stats.maximum().data());
          break;
        }
        case 2: {
          minimal_val =
              *reinterpret_cast<const int16 *>(col_data_stats.minimal().data());
          maximum_val =
              *reinterpret_cast<const int16 *>(col_data_stats.maximum().data());
          break;
        }
        case 4: {
          minimal_val =
              *reinterpret_cast<const int32 *>(col_data_stats.minimal().data());
          maximum_val =
              *reinterpret_cast<const int32 *>(col_data_stats.maximum().data());
          break;
        }
        case 8: {
          minimal_val =
              *reinterpret_cast<const int64 *>(col_data_stats.minimal().data());
          maximum_val =
              *reinterpret_cast<const int64 *>(col_data_stats.maximum().data());
          break;
        }
        default: {
          support = false;
        }
      }

      if (support) {
        col_desc_table_mins.emplace_back(std::to_string(minimal_val));
        col_desc_table_maxs.emplace_back(std::to_string(maximum_val));
      } else {
        col_desc_table_mins.emplace_back(col_data_stats.minimal());
        col_desc_table_maxs.emplace_back(col_data_stats.maximum());
      }
    }

    // build group col desc table
    group_col_desc_table.add_row(col_desc_table_header);
    group_col_desc_table.add_row(col_desc_table_allnulls);
    group_col_desc_table.add_row(col_desc_table_hasnulls);
    group_col_desc_table.add_row(col_desc_table_hastoast);
    group_col_desc_table.add_row(col_desc_table_mins);
    group_col_desc_table.add_row(col_desc_table_maxs);

    // build group table
    group_table.add_row(tabulate::Table::Row_t{
        std::string("Group description " + std::to_string(i))});
    group_table[0].format().font_align(tabulate::FontAlign::center);
    group_table[0].format().font_color(tabulate::Color::red);
    group_table.add_row(tabulate::Table::Row_t{group_desc_table});
    group_table[1].format().hide_border_top();

    group_table.add_row(tabulate::Table::Row_t{group_col_desc_table});
    group_table[2].format().hide_border_top();

    group_infos += group_table.str() + "\n";
  }

  return group_infos;
}

std::string OrcDumpReader::DumpGroupFooter() {
  std::string group_footers;
  std::shared_ptr<DataBuffer<char>> data_buffer;
  auto footer = &(format_reader_->file_footer_);
  auto stripes = &(footer->stripes());
  auto number_of_columns = footer->colinfo_size();

  bool succ;
  int64 group_start, group_end;
  int64 column_start, column_end;

  std::tie(group_start, group_end, succ) = ParseRange(
      config_->group_id_start, config_->group_id_len, stripes->size());
  CBDB_CHECK(succ, cbdb::CException::ExType::kExTypeInvalid,
             fmt("Fail to parse the group range [file=%s], group range should "
                 "in [0, %u)",
                 config_->file_name, stripes->size()));

  std::tie(column_start, column_end, succ) = ParseRange(
      config_->column_id_start, config_->column_id_len, number_of_columns);
  CBDB_CHECK(succ, cbdb::CException::ExType::kExTypeInvalid,
             fmt("Fail to parse the column range [file=%s], column range "
                 "should in [0, %u)",
                 config_->file_name, number_of_columns));

  data_buffer = std::make_shared<DataBuffer<char>>(8192);

  size_t streams_index;
  for (int i = group_start; i < group_end; i++) {
    auto stripe_footer = format_reader_->ReadStripeFooter(data_buffer, i);
    const pax::porc::proto::Stream *n_stream = nullptr;
    const pax::ColumnEncoding *column_encoding = nullptr;

    tabulate::Table group_footer_table;
    tabulate::Table group_footer_desc_tables;
    tabulate::Table::Row_t group_footer_descs;
    tabulate::Table::Row_t group_footer_desc_cids;
    tabulate::Table stream_desc_table;

    streams_index = 0;

    for (int j = 0; j < column_start;) {
      n_stream = &stripe_footer.streams(streams_index++);
      if (n_stream->kind() ==
          ::pax::porc::proto::Stream_Kind::Stream_Kind_DATA) {
        j++;
      }
    }

    for (int j = column_start; j < column_end;) {
      n_stream = &stripe_footer.streams(streams_index++);
      stream_desc_table.add_row(
          {"Stream type", std::to_string(n_stream->kind())});
      stream_desc_table.add_row({"Column", std::to_string(n_stream->column())});
      stream_desc_table.add_row({"Length", std::to_string(n_stream->length())});
      stream_desc_table.add_row(
          {"Padding", std::to_string(n_stream->padding())});

      if (n_stream->kind() ==
          ::pax::porc::proto::Stream_Kind::Stream_Kind_DATA) {
        column_encoding = &stripe_footer.pax_col_encodings(j);

        tabulate::Table group_footer_desc_table;

        group_footer_desc_table.add_row(
            {"Compress type", std::to_string(column_encoding->kind())});
        group_footer_desc_table.add_row(
            {"Compress level",
             std::to_string(column_encoding->compress_lvl())});
        group_footer_desc_table.add_row(
            {"Origin length", std::to_string(column_encoding->length())});
        group_footer_desc_table.add_row({"Streams", stream_desc_table});
        stream_desc_table = tabulate::Table();

        group_footer_desc_cids.emplace_back(
            std::string("Column" + std::to_string(j)));
        group_footer_descs.emplace_back(group_footer_desc_table);
        j++;
      }
    }

    group_footer_desc_tables.add_row(group_footer_desc_cids);
    group_footer_desc_tables.add_row(group_footer_descs);

    group_footer_table.add_row(tabulate::Table::Row_t{
        std::string("Group footer description " + std::to_string(i))});
    group_footer_table[0].format().font_align(tabulate::FontAlign::center);
    group_footer_table[0].format().font_color(tabulate::Color::red);
    group_footer_table.add_row(
        tabulate::Table::Row_t{group_footer_desc_tables});
    group_footer_table[1].format().hide_border_top();

    group_footers += group_footer_table.str() + "\n";
  }

  return group_footers;
}

std::string OrcDumpReader::DumpAllData() {
  auto footer = &(format_reader_->file_footer_);
  auto stripes = &(footer->stripes());
  auto number_of_columns = footer->colinfo_size();

  bool succ;
  int64 group_start, group_end;
  int64 column_start, column_end;
  int64 row_start, row_end;
  bool is_vec = footer->storageformat() == kTypeStoragePorcVec;

  std::tie(group_start, group_end, succ) = ParseRange(
      config_->group_id_start, config_->group_id_len, stripes->size());
  CBDB_CHECK(succ && config_->group_id_len == 1,
             cbdb::CException::ExType::kExTypeInvalid,
             fmt("Invalid group range, with option -d/--print-data(or "
                 "-a/--print-all) the range "
                 "length should be 1"
                 "[file=%s] group range shoule in [0, %u)",
                 config_->file_name, stripes->size()));

  std::tie(column_start, column_end, succ) = ParseRange(
      config_->column_id_start, config_->column_id_len, number_of_columns);
  CBDB_CHECK(succ, cbdb::CException::ExType::kExTypeInvalid,
             fmt("Fail to parse the column range [file=%s] column range should "
                 "in [0, %u)",
                 config_->file_name, number_of_columns));

  std::shared_ptr<DataBuffer<char>> data_buffer;
  auto stripe_info = (*stripes)[group_start];
  size_t number_of_rows = 0;
  pax::porc::proto::StripeFooter stripe_footer;
  std::vector<bool> proj_map(number_of_columns, false);
  std::unique_ptr<PaxColumns> columns;
  std::unique_ptr<OrcGroup> group;

  data_buffer = std::make_shared<DataBuffer<char>>(8192);
  number_of_rows = stripe_info.numberofrows();
  stripe_footer = format_reader_->ReadStripeFooter(data_buffer, group_start);

  std::tie(row_start, row_end, succ) =
      ParseRange(config_->row_id_start, config_->row_id_len, number_of_rows);

  CBDB_CHECK(succ, cbdb::CException::ExType::kExTypeInvalid,
             fmt("Fail to parse the row range [file=%s], row range should in "
                 "[0, %lu)",
                 config_->file_name, number_of_rows));

  for (int column_index = column_start; column_index < column_end;
       column_index++) {
    proj_map[column_index] = true;
  }

  if (stripe_info.toastlength() != 0 && config_->toast_file_name == nullptr) {
    CBDB_RAISE(cbdb::CException::ExType::kExTypeInvalid,
               fmt("Current file %s exist toast, must specify the toast file",
                   config_->file_name));
  }

  columns =
      format_reader_->ReadStripe(group_start, proj_map);

  if (!is_vec)
    group = std::make_unique<OrcGroup>(std::move(columns), 0, nullptr);
  else
    group = std::make_unique<OrcVecGroup>(std::move(columns), 0, nullptr);

  auto all_columns = group->GetAllColumns().get();
  tabulate::Table data_table;
  tabulate::Table data_datum_table;
  tabulate::Table::Row_t data_table_header;
  for (int column_index = column_start; column_index < column_end;
       column_index++) {
    data_table_header.emplace_back(
        std::string("Column" + std::to_string(column_index)));
  }
  data_datum_table.add_row(data_table_header);

  for (int row_index = row_start; row_index < row_end; row_index++) {
    tabulate::Table::Row_t current_row;

    for (int column_index = column_start; column_index < column_end;
         column_index++) {
      Datum d;
      bool null;
      const auto &column = (*all_columns)[column_index];

      std::tie(d, null) = group->GetColumnValueNoMissing((size_t)column_index,
                                                         (size_t)row_index);
      if (null) {
        current_row.emplace_back("");
      } else {
        switch (column->GetPaxColumnTypeInMem()) {
          case kTypeBpChar:
          case kTypeDecimal:
          case kTypeVecBpChar:
          case kTypeVecNoHeader:
          case kTypeNonFixed:
            current_row.emplace_back(std::string(DatumGetPointer(d)));
            break;
          case kTypeBitPacked:
          case kTypeFixed: {
            switch (column->GetTypeLength()) {
              case 1:
                current_row.emplace_back(std::to_string(cbdb::Int8ToDatum(d)));
                break;
              case 2:
                current_row.emplace_back(std::to_string(cbdb::Int16ToDatum(d)));
                break;
              case 4:
                current_row.emplace_back(std::to_string(cbdb::Int32ToDatum(d)));
                break;
              case 8:
                current_row.emplace_back(std::to_string(cbdb::Int64ToDatum(d)));
                break;
              default:
                Assert(
                    !"should't be here, fixed type len should be 1, 2, 4, 8");
            }
            break;
          }
          case kTypeVecDecimal: {
            CBDB_WRAP_START;
            {
              auto numeric_str = numeric_normalize(DatumGetNumeric(d));
              current_row.emplace_back(std::string(numeric_str));
              pfree(numeric_str);
            }
            CBDB_WRAP_END;
            break;
          }
          default:
            Assert(!"should't be here, non-implemented column type in memory");
            break;
        }
      }
    }

    data_datum_table.add_row(current_row);
  }

  data_table.add_row(tabulate::Table::Row_t{"Table data"});
  data_table[0].format().font_align(tabulate::FontAlign::center);
  data_table[0].format().font_color(tabulate::Color::red);

  data_table.add_row(tabulate::Table::Row_t{data_datum_table});
  data_table[1].format().hide_border_top();

  return data_table.str();
}

}  // namespace pax::tools

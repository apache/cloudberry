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
 * pax_dump.cpp.cpp
 *
 * IDENTIFICATION (option)
 *	  contrib/pax_storage/src/cpp/storage/tools/pax_dump.cpp.cpp
 *
 *-------------------------------------------------------------------------
 */

#include <getopt.h>
#include <stdlib.h>
#include <unistd.h>

#include "exceptions/CException.h"
#include "storage/orc/orc_dump_reader.h"
#include "storage/proto/proto_wrappers.h"

const char *progname = NULL;

static void Usage() {
  std::cout
      << progname << " - Pax dump tools \n\n"
      << "Used to dump pax file && pax aux table \n"
      << "Usage:\n " << progname << " -f [FILE] [OPTION]...\n\n"
      << "Options:\n"

      << "  -h, -?, --help                    Print this help, then exit\n"
      << "  -v, --version                     Print the version of current pax "
         "tools\n"

      << "  -f, --file [file]                 Specify the file name to be "
         "dumped\n"
      << "  -t, --toast-file [file]           Specify the toast file name\n"
      << "  -g, --group-range [start, len]    Specify print with range of "
         "group ids\n"
      << "  -c, --column-range [start, len]   Specify print with range of "
         "column ids\n"
      << "  -r, --row-range [start, len]      Specify print with range of "
         "column ids\n"

      << "  -a, --print-all                   Print the all information, which "
         "contains meta and data\n"
      << "  -m, --print-all-desc              Print the all desc part "
         "information, which contains version, post script, footer\n"
      << "  -p, --print-post-script-info      Print the postscript "
         "information\n"
      << "  -e, --print-footer-info           Print the footer information\n"
      << "  -s, --print-schema-info           Print the schema information\n"
      << "  -b, --print-group-info            Print the group information\n"
      << "  -o, --print-group-footer          Print the group footer "
         "information\n"
      << "  -d, --print-data                  Print the data part "
         "information\n\n"
      << "Example:\n"
      << "  " << progname << " -f test.file -a -g 0,3 -c 0,5 -r 0,10\n"
      << std::endl;
}

static struct option long_options[] = {
    {"help", no_argument, NULL, '?'},
    {"version", no_argument, NULL, 'v'},
    {"file", required_argument, NULL, 'f'},
    {"toast-file", required_argument, NULL, 't'},
    {"group-range", optional_argument, NULL, 'g'},
    {"column-range", optional_argument, NULL, 'c'},
    {"row-range", optional_argument, NULL, 'r'},
    {"print-all", optional_argument, NULL, 'a'},
    {"print-all-desc", optional_argument, NULL, 'm'},
    {"print-post-script-info", optional_argument, NULL, 'p'},
    {"print-footer-info", optional_argument, NULL, 'e'},
    {"print-schema-info", optional_argument, NULL, 's'},
    {"print-group-info", optional_argument, NULL, 'b'},
    {"print-all-data", optional_argument, NULL, 'd'},
    {"print-group-footer", optional_argument, NULL, 'o'},
    {NULL, 0, NULL, 0}};

static inline std::pair<int64_t, int64_t> ParseRangeConfig(
    const char *range_opt) {
  char *p;
  int64_t start, len;
  char *range;
  bool succ = false;
  const char *range_split = ",";

  auto parse_ll = [](char *part) -> std::pair<int64_t, bool> {
    char *end_ptr;
    int64_t ll_val = -1;

    if (part == nullptr) {
      return {ll_val, false};
    }

    ll_val = strtoll(part, &end_ptr, 10);
    if (end_ptr == part || *end_ptr != '\0') {
      return {ll_val, false};
    }

    return {ll_val, true};
  };

  range = strdup(range_opt);

  p = strtok(range, range_split);
  std::tie(start, succ) = parse_ll(p);
  if (!succ) {
    goto invalid_args;
  }

  p = strtok(NULL, range_split);
  std::tie(len, succ) = parse_ll(p);
  if (!succ) {
    goto invalid_args;
  }

  if (start < 0 || len <= 0) {
    goto invalid_args;
  }

  p = strtok(NULL, range_split);
  if (p != nullptr) {
    goto invalid_args;
  }

  free(range);  // NOLINT
  return {start, len};

invalid_args:
  std::cout << "Invalid args: " << range_opt << std::endl;
  free(range);  // NOLINT
  exit(-1);
}

static void InitConfig(struct pax::tools::DumpConfig *config, int argc,
                       char **argv) {
  int c = 0;
  if (argc > 1) {
    if (strcmp(argv[1], "--help") == 0 || strcmp(argv[1], "-?") == 0 ||
        strcmp(argv[1], "-h") == 0) {
      Usage();
      exit(0);
    }
  }

  optind = 1;
  while (optind < argc) {
    while ((c = getopt_long(argc, argv, "vf:g:c:r:t:ampesbod", long_options,
                            NULL)) != -1) {
      switch (c) {
        case 'f': {
          if (!optarg) {
            goto invalid_args;
          }
          config->file_name = strdup(optarg);
          break;
        }
        case 'g': {
          if (!optarg) {
            goto invalid_args;
          }
          std::tie(config->group_id_start, config->group_id_len) =
              ParseRangeConfig(optarg);
          break;
        }
        case 'c': {
          if (!optarg) {
            goto invalid_args;
          }
          std::tie(config->column_id_start, config->column_id_len) =
              ParseRangeConfig(optarg);
          break;
        }
        case 'r': {
          if (!optarg) {
            goto invalid_args;
          }
          std::tie(config->row_id_start, config->row_id_len) =
              ParseRangeConfig(optarg);
          break;
        }
        case 't': {
          if (!optarg) {
            goto invalid_args;
          }
          config->toast_file_name = strdup(optarg);
          break;
        }
        case 'a': {
          config->print_all = true;
          break;
        }
        case 'm': {
          config->print_all_desc = true;
          break;
        }
        case 'p': {
          config->print_post_script = true;
          break;
        }
        case 'e': {
          config->print_footer = true;
          break;
        }
        case 's': {
          config->print_schema = true;
          break;
        }
        case 'b': {
          config->print_group_info = true;
          break;
        }
        case 'o': {
          config->print_group_footer = true;
          break;
        }
        case 'd': {
          config->print_all_data = true;
          break;
        }
        default:
          /* do nothing */
          break;
      }
    }
  }

  if (!config->file_name) {
    std::cout << "Invalid file. The input file must be specified via -f/--file"
              << std::endl;
    exit(-1);
  }

  return;
invalid_args:
  std::cout << "Invalid args: -" << (char)(c) << ", failed to parse this arg."
            << std::endl;
  exit(-1);
}

int main(int argc, char **argv) {
  pax::tools::DumpConfig config;
  int rc = 0;

  progname = argv[0];
  InitConfig(&config, argc, argv);

  pax::tools::OrcDumpReader reader(&config);
  try {
    bool ok = reader.Open();
    if (!ok) {
      std::cout << "Failed to dump current file: " << config.file_name
                << ", Toast file: " << config.toast_file_name  // safe
                << std::endl;
      rc = -1;
      goto finish;
    }

    std::cout << reader.Dump() << std::endl;
  } catch (cbdb::CException &e) {
    std::cout << "error happend while dumping.\n  " << e.What() << std::endl;
    std::cout << "Stack: \n" << e.Stack() << std::endl;
    rc = -1;
    goto finish;
  }

finish:
  free(config.file_name);
  free(config.toast_file_name);
  reader.Close();
  return rc;
}

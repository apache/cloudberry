# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed
# with this work for additional information regarding copyright
# ownership.  The ASF licenses this file to You under the Apache
# License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain a copy of the
# License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.  See the License for the specific language governing
# permissions and limitations under the License.

# include/makefile.inc hardcodes
#   COMMON_LINK_OPTIONS = -lstdc++ -lxml2 -pthread -lcrypto -lcurl -lz
#   COMMON_CPP_FLAGS    = -std=c++11 -fPIC -I/usr/include/libxml2
#                         -I/usr/local/opt/openssl/include
# The literal paths only work on some machines; use real dependencies instead.
gpcloud_common_sources = files(
  'src/compress_writer.cpp',
  'src/decompress_reader.cpp',
  'src/gpreader.cpp',
  'src/gpwriter.cpp',
  'src/s3bucket_reader.cpp',
  'src/s3common_reader.cpp',
  'src/s3common_writer.cpp',
  'src/s3conf.cpp',
  'src/s3http_headers.cpp',
  'src/s3interface.cpp',
  'src/s3key_reader.cpp',
  'src/s3key_writer.cpp',
  'src/s3log.cpp',
  'src/s3restful_service.cpp',
  'src/s3url.cpp',
  'src/s3utils.cpp',
)

gpcloud_lib_sources = files(
  'lib/http_parser.cpp',
  'lib/ini.cpp',
)

gpcloud_inc = include_directories('include', 'lib')
gpcloud_deps = [libcurl, libxml, ssl, zlib, thread_dep]

gpcloud = shared_module('gpcloud',
  files('src/gpcloud.cpp'),
  gpcloud_common_sources,
  gpcloud_lib_sources,
  include_directories: gpcloud_inc,
  override_options: ['cpp_std=c++11'],
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'] + gpcloud_deps,
  },
)
contrib_targets += gpcloud

# The Makefile overrides contrib-global.mk's install target purely to add this
# symlink: ln -sf gpcloud.so $(pkglibdir)/gps3ext.so
meson.add_install_script(gp_install_symlink,
  dir_lib_pkg / 'gps3ext' + dlsuffix,
  'gpcloud' + dlsuffix,
)

subdir('bin/gpcheckcloud')

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

# fstream.c and gfile.c are shared with the backend. The Makefile symlinks them
# into this directory; meson can just reference them where they live. Note the
# asymmetry the Makefile encodes: only gfile.c is built with -DFRONTEND.
gpfdist_sources = files(
  'gpfdist.c',
  'gpfdist_helper.c',
  '../../backend/utils/misc/fstream/fstream.c',
)

gpfdist_c_args = []
gpfdist_deps = [apr, libevent, libbz2, zlib]

# ifeq ($(have_yaml),yes): transform support, plus -DGPFXDIST for the whole
# translation unit set (except win32).
if libyaml.found()
  gpfdist_sources += files('transform.c')
  gpfdist_deps += libyaml
  if host_system != 'windows'
    gpfdist_c_args += '-DGPFXDIST'
  endif
endif

# gfile.o: CPPFLAGS := $(CPPFLAGS) -DFRONTEND
gpfdist_gfile = static_library('gpfdist_gfile',
  files('../../backend/utils/misc/fstream/gfile.c'),
  c_args: gpfdist_c_args + ['-DFRONTEND'],
  include_directories: [postgres_inc, include_directories('.')],
  # gfile.c pulls in gpfxdist.h, which needs apr when transform support is on
  dependencies: [frontend_stlib_code, zlib, libbz2] + gpfdist_deps,
  kwargs: internal_lib_args,
)

gpfdist = executable('gpfdist',
  gpfdist_sources,
  c_args: gpfdist_c_args,
  include_directories: [postgres_inc, include_directories('.')],
  link_with: gpfdist_gfile,
  dependencies: [frontend_code] + gpfdist_deps,
  kwargs: default_bin_args,
)
bin_targets += gpfdist

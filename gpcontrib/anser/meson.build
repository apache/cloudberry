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

# gpcontrib/anser/Makefile: the library alone. It installs no extension --
# GUCs configure it and its data travels over the dispatch connection -- and
# links no libpq: it writes on the dispatcher's own connections, through
# libpq's internal headers, with the backend's copy of libpq.
anser_sources = files(
  'src/anserbloomconsume.c',
  'src/anserbloomproduce.c',
  'src/anserdispatch.c',
  'src/anserfilter.c',
  'src/anserinit.c',
  'src/anserpayload.c',
  'src/anserplan.c',
  'src/anserplankeys.c',
  'src/anserplanexec.c',
  'src/ansersideband.c',
)

anser = shared_module('anser',
  anser_sources,
  include_directories: [include_directories('include'), libpq_inc],
  kwargs: contrib_mod_args,
)
contrib_targets += anser

subdir('anser_test')

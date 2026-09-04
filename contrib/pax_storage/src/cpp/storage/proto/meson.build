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

# The sources include these as "storage/proto/xxx.pb.h", so the generated
# headers must sit in a storage/proto directory inside the build tree.
# Generating from this subdirectory puts them exactly there; the parent adds
# src/cpp to the include path, which covers the build tree as well.
#
# The CMake build instead runs protoc during configure and writes the results
# back into the source tree (PROTO_OUTPUT_DIR is the source directory).
pax_proto_sources = []

foreach base : ['micro_partition_stats', 'pax', 'orc_proto']
  pax_proto_sources += custom_target('pax_' + base + '_pb',
    input: base + '.proto',
    output: [base + '.pb.cc', base + '.pb.h'],
    # protoc needs -I to be an exact textual prefix of the file argument.
    command: [protoc,
      '-I', meson.current_source_dir(),
      '--cpp_out=@OUTDIR@',
      meson.current_source_dir() / (base + '.proto')],
  )
endforeach

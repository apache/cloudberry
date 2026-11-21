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

# The .proto files import each other as "protos/xxx.proto", so protoc's
# proto_path has to be the extension root, not this directory -- matching the
# Makefile's `protoc -I . --cpp_out=src protos/*.proto`. With --cpp_out set to
# the parent build directory, the generated files land back in this directory,
# which is where the outputs are declared.
#
# The Makefile then renames .pb.cc to .pb.cpp so its own pattern rules match;
# meson infers the language from the extension, so no rename is needed.
gpsc_proto_sources = []

foreach base : ['gpsc_plan', 'gpsc_metrics', 'gpsc_set_service']
  gpsc_proto_sources += custom_target(base + '_pb',
    input: base + '.proto',
    output: [base + '.pb.cc', base + '.pb.h'],
    # protoc requires -I to be an exact textual prefix of the file argument,
    # so both are absolute.
    command: [protoc,
      '-I', gpsc_root_src,
      '--cpp_out=' + gpsc_root_build,
      gpsc_root_src / 'protos' / (base + '.proto')],
  )
endforeach

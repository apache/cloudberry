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

# The Makefile generates the scanner and parser into the *source* directory;
# meson's generators put them in the build directory, which is what we want.
# FLEXFLAGS = -i (case-insensitive) is passed through to flex as an extra flag.
# yaml_parse.y and yaml_scan.l hardcode their own output names:
#   yaml_parse.y:1  %output="yaml_parse.c"
#   yaml_scan.l:1   %option outfile="yaml_scan.c"
# Those directives override -o, so both tools write into the *current
# directory* -- which is why the Makefile does `cd $(SRCDIR)` and passes no -o.
# Run them with the build directory as cwd so the hardcoded names land there.
gpmapreduce_srcdir = meson.current_source_dir() / 'src'

gpmapreduce_yaml_parse = custom_target('yaml_parse',
  input: 'src/yaml_parse.y',
  output: ['yaml_parse.c', 'yaml_parse.h'],
  command: [bash, '-c', 'cd "$1" && exec "$0" -Wno-deprecated -d "$2"',
    bison, meson.current_build_dir(), gpmapreduce_srcdir / 'yaml_parse.y'],
)

gpmapreduce_yaml_scan = custom_target('yaml_scan',
  input: 'src/yaml_scan.l',
  output: 'yaml_scan.c',
  # FLEXFLAGS = -i in the Makefile: case-insensitive scanner.
  command: [bash, '-c', 'cd "$1" && exec "$0" -i "$2"',
    flex, meson.current_build_dir(), gpmapreduce_srcdir / 'yaml_scan.l'],
)

# $(MAPREDOBJS): override CPPFLAGS += -DFRONTEND
gpmapreduce = executable('gpmapreduce',
  files('src/main.c', 'src/mapred.c', 'src/parse.c'),
  gpmapreduce_yaml_parse,
  gpmapreduce_yaml_scan,
  # The Makefile disables -Wimplicit-fallthrough for this module: "it is hard
  # to change some macros using switch-case".
  c_args: ['-DFRONTEND', '-Wno-implicit-fallthrough'],
  # '.' picks up this directory in the *build* tree too, where bison puts
  # yaml_parse.h -- the generated scanner includes it.
  include_directories: include_directories('.', 'src', 'include'),
  dependencies: [frontend_code, libpq, libyaml],
  kwargs: default_bin_args,
)
bin_targets += gpmapreduce

# NAME = regress / OBJS = mapred_test.o builds a regress.so used only by this
# directory's installcheck target. It is never installed, so it is deferred
# along with the rest of the test infrastructure.

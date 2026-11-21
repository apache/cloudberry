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

# The unit tests `make unittest-check` runs: cmockery programs that link the
# backend with some of its modules replaced by mocks generated from their
# sources. Run them with
#
#   meson test -C <builddir> --suite unittest
#
# The tests stay defined in their Makefiles under src/*/test; unittests.py
# reads them from there. They are not built by default.
#
# src/backend/mock.mk links each test against every backend object except the
# mocked ones and a list it excludes. Here each test links against the
# backend's static library instead, after its mocks: the linker takes from an
# archive only the members something still needs, so a mocked module's real
# object is never pulled in -- the same set of objects, chosen by the linker
# rather than by a list.

cmockery_inc = include_directories('cmockery')
cmockery = static_library('cmockery', 'cmockery/cmockery.c',
  include_directories: cmockery_inc,
  dependencies: [thread_dep],
  kwargs: internal_lib_args,
)

unittest_bin_args = default_bin_args + {'install': false, 'build_by_default': false}

mocker = files('mock/mocker.py')

# mock.mk's CPPFLAGS, for the tests and the generated mocks alike (less its
# src/backend/utils/stat, which no longer exists).
unittest_inc = [
  cmockery_inc,
  include_directories('mock'),
  include_directories('../../backend/libpq'),
  include_directories('../../interfaces/libpq'),
  include_directories('../../backend/postmaster'),
]
# make builds the tests with CFLAGS=-DUNITTEST, which replaces every other
# flag. Hence -O0, which some tests depend on -- syslogger's reroutes a
# function with --defsym, which only reaches calls the compiler did not
# resolve or inline itself -- and no -Wmissing-prototypes, which the mocks,
# generated and hand-written, would trip on every function they define.
unittest_c_args = ['-DUNITTEST', '-O0', '-Wno-missing-prototypes',
                   '-Wno-unused-function', '-Wno-unused-variable']

# What every backend test links in place of fmgrtab.o and main.o, and of
# ORCA when it is built.
unittest_fixed_mocks = files('mock/fmgrtab_mock.c', 'mock/main_mock.c')
if xerces.found()
  unittest_fixed_mocks += files('mock/gpopt_mock.c')
endif

# One generated mock per mocked source, shared by the tests that use it.
unittest_mocks = {}

unittest_specs = run_command(python, files('unittests.py'), meson.source_root(),
  check: true).stdout().strip().split('\n')

foreach spec : unittest_specs
  f = spec.split('|')
  kind = f[0]
  testdir = f[1]
  name = f[2]

  if kind == 'mock'
    srcs = [meson.source_root() / testdir / name + '_test.c'] + unittest_fixed_mocks
    foreach src : f[3] == '' ? [] : f[3].split(',')
      key = src.underscorify()
      if not unittest_mocks.has_key(key)
        unittest_mocks += {key: custom_target(key + '_mock',
          input: meson.source_root() / src,
          output: key + '_mock.c',
          command: [python, mocker, '--output', '@OUTPUT@', '@INPUT@'],
          build_by_default: false,
        )}
      endif
      srcs += unittest_mocks[key]
    endforeach
    link_args = []
    foreach w : f[4] == '' ? [] : f[4].split(',')
      link_args += '-Wl,--defsym,@0@=__wrap_@0@'.format(w)
    endforeach
    exe = executable(name + '.t', srcs,
      c_args: unittest_c_args,
      include_directories: unittest_inc,
      link_with: [cmockery, postgres_lib],
      link_args: link_args,
      link_language: postgres_link_language,
      dependencies: backend_build_deps + [thread_dep],
      kwargs: unittest_bin_args,
    )

  # The three with a rule of their own in their Makefile.
  elif testdir == 'src/backend/access/gin/test'
    # ginpostinglist.o, its fakes and the test; no backend.
    exe = executable(name + '.t',
      files('../../backend/access/gin/ginpostinglist.c',
            '../../backend/access/gin/test/ginpostinglist_fakes.c',
            '../../backend/access/gin/test/ginpostinglist_test.c'),
      c_args: unittest_c_args,
      include_directories: unittest_inc,
      link_with: [cmockery],
      dependencies: [backend_code, thread_dep],
      kwargs: unittest_bin_args,
    )
  elif testdir == 'src/backend/catalog/test'
    # storage_tablespace.o and the test; no backend.
    exe = executable(name + '.t',
      files('../../backend/catalog/storage_tablespace.c',
            '../../backend/catalog/test/storage_tablespace_test.c'),
      c_args: unittest_c_args,
      include_directories: unittest_inc,
      link_with: [cmockery],
      dependencies: [backend_code, thread_dep],
      kwargs: unittest_bin_args,
    )
  elif testdir == 'src/bin/pg_dump/test'
    # A frontend test, linked with libpq.
    exe = executable(name + '.t',
      files('../../bin/pg_dump/test/dumputils_test.c'),
      c_args: unittest_c_args,
      include_directories: unittest_inc,
      link_with: [cmockery],
      dependencies: [frontend_code, libpq, thread_dep],
      kwargs: unittest_bin_args,
    )
  else
    error('@0@ builds @1@ with a rule of its own; add it here'.format(testdir, name))
  endif

  # Serially, as make runs them; some create files in their working directory.
  # With meson's MALLOC_PERTURB_, which make does not set, except for memprot:
  # it allocates blocks of up to 2 GB it never touches, to test the vmem
  # limits, and glibc filling them commits the memory until the kernel kills
  # the test.
  unittest_env = name == 'memprot' ? {'MALLOC_PERTURB_': '0'} : {}
  if f[5] != ''
    # The Makefile creates this before running the test.
    test(name, find_program('sh'),
      args: ['-c', 'mkdir -p "$1" && exec "$2"', 'sh', f[5], exe.full_path()],
      depends: exe,
      workdir: meson.current_build_dir(),
      suite: 'unittest',
      env: unittest_env,
      is_parallel: false,
    )
  else
    test(name, exe,
      workdir: meson.current_build_dir(),
      suite: 'unittest',
      env: unittest_env,
      is_parallel: false,
    )
  endif
endforeach

# Copyright (c) 2022-2023, PostgreSQL Global Development Group

# also used by isolationtester and ecpg tests
pg_regress_c = files('pg_regress.c')
pg_regress_inc = include_directories('.')

regress_sources = pg_regress_c + files(
  'pg_regress_main.c'
)

# Need make up something roughly like x86_64-pc-mingw64. resultmap matches on
# patterns like ".*-.*-mingw.*". We probably can do better, but for now just
# replace 'gcc' with 'mingw' on windows.
host_tuple_cc = cc.get_id()
if host_system == 'windows' and host_tuple_cc == 'gcc'
  host_tuple_cc = 'mingw'
elif host_system == 'cygwin' and host_tuple_cc == 'gcc'
  host_tuple_cc = 'cygwin'
endif
host_tuple = '@0@-@1@-@2@'.format(host_cpu, host_system, host_tuple_cc)

pg_regress_cflags = ['-DHOST_TUPLE="@0@"'.format(host_tuple), '-DSHELLPROG="/bin/sh"']

if host_system == 'windows'
  regress_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pg_regress',
    '--FILEDESC', 'pg_regress - test driver',])
endif

pg_regress = executable('pg_regress',
  regress_sources,
  c_args: pg_regress_cflags,
  dependencies: [frontend_code],
  kwargs: default_bin_args + {
    'install_dir': dir_pgxs / 'src/test/regress',
  },
)
bin_targets += pg_regress

# regress_gp.c carries Cloudberry's test functions and talks to the segments
# through libpq. Unlike upstream, the Makefile installs regress.so into
# pkglibdir, so it is installed here too.
regress_module = shared_module('regress',
  ['regress.c', 'regress_gp.c'],
  kwargs: pg_mod_args + {
    'dependencies': pg_mod_args['dependencies'] + [libpq],
  },
)
test_install_libs += regress_module

# Get some extra C modules from contrib/spi but mark them as not to be
# installed.
# FIXME: avoid the duplication.

autoinc_regress = shared_module('autoinc',
  ['../../../contrib/spi/autoinc.c'],
  kwargs: pg_test_mod_args,
)
test_install_libs += autoinc_regress

refint_regress = shared_module('refint',
  ['../../../contrib/spi/refint.c'],
  c_args: refint_cflags,
  kwargs: pg_test_mod_args,
)
test_install_libs += refint_regress



###############################################################
# Apache Cloudberry: the rest of the regression harness
###############################################################

# pg_regress compares results with gpdiff.pl, and finds it -- together with
# gpstringsubs.pl -- next to its own executable, insisting that
# `gpdiff.pl --version` print exactly "gpdiff.pl <GP_VERSION>". The version
# comes from GPTest.pm, which configure writes from GPTest.pm.in into the
# source tree; generate it here instead, with the same substitution.
#
# Everything the Makefile's install target puts in pgxs/src/test/regress goes
# there here as well, and is also copied into this build directory so that the
# pg_regress beside them works from the build tree. Without these, pg_regress
# cannot compare a single result, so neither an in-tree run nor a PGXS
# `make USE_PGXS=1 installcheck` of any extension could work against a meson
# install.
gptest_pm = configure_file(
  input: 'GPTest.pm.in',
  output: 'GPTest.pm',
  command: [sed, 's,##.*##,' + gp_version_long + ',', '@INPUT@'],
  capture: true,
  install: true,
  install_dir: dir_pgxs / 'src/test/regress',
  install_mode: 'rwxr-xr-x',
)

regress_scripts = [
  'atmsort.pl',
  'atmsort.pm',
  'explain.pl',
  'explain.pm',
  'gpdiff.pl',
  'gpstringsubs.pl',
  'scan_flaky_fault_injectors.sh',
]
foreach f : regress_scripts
  configure_file(input: f, output: f, copy: true)
endforeach
install_data(regress_scripts,
  install_dir: dir_pgxs / 'src/test/regress',
  install_mode: 'rwxr-xr-x',
)

# Client programs some tests run from the output directory, as
# \! ./twophase_pqexecparams. The Makefile installs only the first.
twophase_pqexecparams = executable('twophase_pqexecparams',
  files('twophase_pqexecparams.c'),
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args + {
    'install_dir': dir_pgxs / 'src/test/regress',
  },
)
extended_protocol_resqueue = executable('extended_protocol_resqueue',
  files('extended_protocol_resqueue.c'),
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args + {'install': false},
)

# Hook modules the hooktest and query_info_hook_test cases LOAD by absolute
# path, from the hooktest/ and query_info_hook_test/ directories beside the
# output directory -- so they are built in subdirectories of the same name.
subdir('hooktest')
subdir('query_info_hook_test')

tests += {
  'name': 'regress',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'schedule': files('parallel_schedule'),
    'test_kwargs': {
      'priority': 50,
      'timeout': 1000,
    },
    'dbname': 'regression',
  },
}

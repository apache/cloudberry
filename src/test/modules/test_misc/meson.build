# Copyright (c) 2022-2023, PostgreSQL Global Development Group

test_custom_stats = shared_module('test_custom_stats',
  files('test_custom_stats.c'),
  kwargs: pg_test_mod_args,
)
test_install_libs += test_custom_stats

tests += {
  'name': 'test_misc',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_constraint_validation.pl',
      't/002_tablespace.pl',
      't/003_check_guc.pl',
      't/004_io_direct.pl',
      't/019_resqueue_stats_restart.pl',
      't/020_custom_stats_restart.pl',
    ],
  },
}

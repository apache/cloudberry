# Copyright (c) 2022-2026, PostgreSQL Global Development Group
#
# ext_vacuum_statistics - extended vacuum statistics via hook
# Requires shared_preload_libraries = 'ext_vacuum_statistics'

ext_vacuum_statistics_sources = files(
  'vacuum_statistics.c',
)

ext_vacuum_statistics = shared_module('ext_vacuum_statistics',
  ext_vacuum_statistics_sources,
  kwargs: contrib_mod_args + {
    'dependencies': contrib_mod_args['dependencies'],
  },
)
contrib_targets += ext_vacuum_statistics

install_data(
  'ext_vacuum_statistics.control',
  'ext_vacuum_statistics--1.0.sql',
  kwargs: contrib_data_args,
)

tests += {
  'name': 'ext_vacuum_statistics',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'isolation': {
    'specs': [
      'vacuum-extending-in-repetable-read',
    ],
    'regress_args': ['--temp-config', files('ext_vacuum_statistics.conf')],
    'runningcheck': false,
  },
  'tap': {
    'tests': [
      't/052_vacuum_extending_basic_test.pl',
      't/054_vacuum_extending_gucs_test.pl',
      't/056_vacuum_stats_gc.pl',
      't/058_vacuum_stats_counters.pl',
      't/060_vacuum_stats_reset.pl',
      't/060_heap_page_counters.pl',
      't/061_vacuum_measurements.pl',
      't/062_ao_hidden_tuples.pl',
      't/063_ao_phase_stats.pl',
    ],
  },
}

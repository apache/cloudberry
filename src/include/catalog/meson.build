# Copyright (c) 2022-2023, PostgreSQL Global Development Group

catalog_headers = [
  'pg_proc.h',
  'pg_type.h',
  'pg_attribute.h',
  'pg_class.h',
  'pg_attrdef.h',
  'pg_constraint.h',
  'pg_inherits.h',
  'pg_index.h',
  'pg_operator.h',
  'pg_opfamily.h',
  'pg_opclass.h',
  'pg_am.h',
  'pg_amop.h',
  'pg_amproc.h',
  'pg_language.h',
  'pg_largeobject_metadata.h',
  'pg_largeobject.h',
  'pg_aggregate.h',
  'pg_statistic.h',
  'pg_statistic_ext.h',
  'pg_statistic_ext_data.h',
  'pg_rewrite.h',
  'pg_trigger.h',
  'pg_event_trigger.h',
  'pg_description.h',
  'pg_cast.h',
  'pg_enum.h',
  'pg_namespace.h',
  'pg_conversion.h',
  'pg_depend.h',
  'pg_database.h',
  'pg_db_role_setting.h',
  'pg_tablespace.h',
  'pg_authid.h',
  'pg_auth_members.h',
  'pg_shdepend.h',
  'pg_shdescription.h',
  'pg_ts_config.h',
  'pg_ts_config_map.h',
  'pg_ts_dict.h',
  'pg_ts_parser.h',
  'pg_ts_template.h',
  'pg_extension.h',
  'pg_foreign_data_wrapper.h',
  'pg_foreign_server.h',
  'pg_user_mapping.h',
  'pg_foreign_table.h',
  'pg_policy.h',
  'pg_replication_origin.h',
  'pg_default_acl.h',
  'pg_init_privs.h',
  'pg_seclabel.h',
  'pg_shseclabel.h',
  'pg_collation.h',
  'pg_parameter_acl.h',
  'pg_partitioned_table.h',
  'pg_range.h',
  'pg_transform.h',
  'pg_sequence.h',
  'pg_publication.h',
  'pg_publication_namespace.h',
  'pg_publication_rel.h',
  'pg_subscription.h',
  'pg_subscription_rel.h',

  # Apache Cloudberry catalogs
  'pg_resqueue.h',
  'pg_resqueuecapability.h',
  'pg_resourcetype.h',
  'pg_resgroup.h',
  'pg_resgroupcapability.h',
  'gp_configuration_history.h',
  'gp_id.h',
  'gp_distribution_policy.h',
  'gp_version_at_initdb.h',
  'gp_warehouse.h',
  'pg_appendonly.h',
  'gp_fastsequence.h',
  'pg_extprotocol.h',
  'pg_attribute_encoding.h',
  'pg_auth_time_constraint.h',
  'pg_compression.h',
  'pg_proc_callback.h',
  'pg_type_encoding.h',
  'pg_stat_last_operation.h',
  'pg_stat_last_shoperation.h',
  'pg_foreign_table_seg.h',
  'gp_partition_template.h',
  'pg_task.h',
  'pg_task_run_history.h',
  'pg_profile.h',
  'pg_password_history.h',
  'pg_directory_table.h',
  'gp_storage_server.h',
  'gp_storage_user_mapping.h',
  'pg_tag.h',
  'pg_tag_description.h',
  'gp_matview_aux.h',
  'gp_matview_tables.h',
]

# gp_segment_configuration is only a catalog when the internal FTS is used.
# Mirrors: ifeq ($(USE_INTERNAL_FTS_FOUND), true) in ../../backend/catalog/Makefile
if not get_option('external_fts')
  catalog_headers += ['gp_segment_configuration.h']
endif

bki_data = [
  'pg_aggregate.dat',
  'pg_am.dat',
  'pg_amop.dat',
  'pg_amproc.dat',
  'pg_authid.dat',
  'pg_cast.dat',
  'pg_class.dat',
  'pg_collation.dat',
  'pg_conversion.dat',
  'pg_database.dat',
  'pg_language.dat',
  'pg_namespace.dat',
  'pg_opclass.dat',
  'pg_operator.dat',
  'pg_opfamily.dat',
  'pg_proc.dat',
  'pg_range.dat',
  'pg_tablespace.dat',
  'pg_ts_config.dat',
  'pg_ts_config_map.dat',
  'pg_ts_dict.dat',
  'pg_ts_parser.dat',
  'pg_ts_template.dat',
  'pg_type.dat',

  # Apache Cloudberry catalog data
  'pg_profile.dat',
  'pg_compression.dat',
  'pg_resgroup.dat',
  'pg_resgroupcapability.dat',
]
bki_data_f = files(bki_data)

# gp_version_at_initdb.dat is generated from a template (autoconf does this via
# AC_CONFIG_FILES). genbki.pl derives each .dat path from its .h path, so the
# generated .dat and its header must sit in the same directory -- copy the
# header into the build dir next to the generated .dat.
gp_version_at_initdb_dat = configure_file(
  input: 'gp_version_at_initdb.dat.in',
  output: 'gp_version_at_initdb.dat',
  configuration: {'GP_VERSION_LONG': gp_version_long},
)
gp_version_at_initdb_h = configure_file(
  input: 'gp_version_at_initdb.h',
  output: 'gp_version_at_initdb.h',
  copy: true,
)


input = []
output_files = ['postgres.bki', 'system_constraints.sql', 'schemapg.h', 'system_fk_info.h']
output_install = [dir_data, dir_data, dir_include_server / 'catalog', dir_include_server / 'catalog']

foreach h : catalog_headers
  fname = h.split('.h')[0] + '_d.h'
  if h == 'gp_version_at_initdb.h'
    input += gp_version_at_initdb_h
  else
    input += files(h)
  endif
  output_files += fname
  output_install += dir_include_server / 'catalog'
endforeach

# Extra BKI sources that are not catalogs themselves (no _d.h output).
# Mirrors POSTGRES_BKI_SRCS += ... in ../../backend/catalog/Makefile
input += files('gp_indexing.h')
if not get_option('external_fts')
  input += files('gp_segment_configuration_indexing.h')
endif

# gppylib's foreign-key JSON is built from this same set of headers, which is
# why the Makefiles keep it in one variable. Export it rather than let
# gpMgmt/bin/gppylib/data grow a second copy of the list that can drift.
postgres_bki_srcs = input

generated_catalog_headers = custom_target('generated_catalog_headers',
  output: output_files,
  install_dir: output_install,
  input: input,
  depend_files: bki_data_f + catalog_pm + [gp_version_at_initdb_dat],
  build_by_default: true,
  install: true,
  command: [
    perl,
    files('../../backend/catalog/genbki.pl'),
    '--include-path=@SOURCE_ROOT@/src/include',
    '--set-version=' + pg_version_major.to_string(),
    '--output=@OUTDIR@', '@INPUT@'
  ],
)

generated_headers += generated_catalog_headers.to_list()

# autoconf generates the file there, ensure we get a conflict
generated_sources_ac += {'src/backend/catalog': output_files + ['bki-stamp']}
generated_sources_ac += {'src/include/catalog': ['header-stamp']}

# 'reformat-dat-files' is a convenience target for rewriting the
# catalog data files in our standard format.  This includes collapsing
# out any entries that are redundant with a BKI_DEFAULT annotation.
run_target('reformat-dat-files',
  command: [perl, files('reformat_dat_file.pl'), '--output', '@CURRENT_SOURCE_DIR@', bki_data_f],
)

# 'expand-dat-files' is a convenience target for expanding out all
# default values in the catalog data files.  This should be run before
# altering or removing any BKI_DEFAULT annotation.
run_target('expand-dat-files',
  command: [perl, files('reformat_dat_file.pl'), '--output', '@CURRENT_SOURCE_DIR@', bki_data_f, '--full-tuples'],
)

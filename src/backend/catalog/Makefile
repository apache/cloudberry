#-------------------------------------------------------------------------
#
# Makefile for backend/catalog
#
# Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
# Portions Copyright (c) 1994, Regents of the University of California
#
# src/backend/catalog/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/catalog
top_builddir = ../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	aclchk.o \
	catalog.o \
	dependency.o \
	heap.o \
	index.o \
	indexing.o \
	namespace.o \
	objectaccess.o \
	objectaddress.o \
	partition.o \
	pg_aggregate.o \
	pg_cast.o \
	pg_collation.o \
	pg_constraint.o \
	pg_conversion.o \
	pg_db_role_setting.o \
	pg_depend.o \
	pg_enum.o \
	pg_inherits.o \
	pg_largeobject.o \
	pg_namespace.o \
	pg_operator.o \
	pg_proc.o \
	pg_publication.o \
	pg_range.o \
	pg_shdepend.o \
	pg_subscription.o \
	pg_type.o \
	storage.o \
	toasting.o

OBJS += pg_extprotocol.o \
       pg_proc_callback.o \
       aoseg.o aoblkdir.o gp_fastsequence.o gp_segment_config.o \
       pg_attribute_encoding.o pg_compression.o aovisimap.o \
       pg_appendonly.o \
       oid_dispatch.o aocatalog.o storage_tablespace.o storage_database.o \
       storage_tablespace_twophase.o storage_tablespace_xact.o \
       gp_partition_template.o pg_task.o pg_task_run_history.o \
	   gp_matview_aux.o \
       pg_directory_table.o storage_directory_table.o

GP_SYSVIEW_IN = system_views_gp.in
GP_SYSVIEW_SQL = system_views_gp.sql

CATALOG_JSON:= $(addprefix $(top_srcdir)/gpMgmt/bin/gppylib/data/, $(addsuffix .json,$(GP_MAJORVERSION)))

include $(top_srcdir)/src/backend/common.mk

# Note: the order of this list determines the order in which the catalog
# header files are assembled into postgres.bki.  BKI_BOOTSTRAP catalogs
# must appear first, and pg_statistic before pg_statistic_ext_data, and
# there are reputedly other, undocumented ordering dependencies.
CATALOG_HEADERS := \
	pg_proc.h pg_type.h pg_attribute.h pg_class.h \
	pg_attrdef.h pg_constraint.h pg_inherits.h pg_index.h pg_operator.h \
	pg_opfamily.h pg_opclass.h pg_am.h pg_amop.h pg_amproc.h \
	pg_language.h pg_largeobject_metadata.h pg_largeobject.h pg_aggregate.h \
	pg_statistic.h pg_statistic_ext.h pg_statistic_ext_data.h \
	pg_rewrite.h pg_trigger.h pg_event_trigger.h pg_description.h \
	pg_cast.h pg_enum.h pg_namespace.h pg_conversion.h pg_depend.h \
	pg_database.h pg_db_role_setting.h pg_tablespace.h \
	pg_authid.h pg_auth_members.h pg_shdepend.h pg_shdescription.h \
	pg_ts_config.h pg_ts_config_map.h pg_ts_dict.h \
	pg_ts_parser.h pg_ts_template.h pg_extension.h \
	pg_foreign_data_wrapper.h pg_foreign_server.h pg_user_mapping.h \
	pg_resqueue.h pg_resqueuecapability.h pg_resourcetype.h \
	pg_resgroup.h pg_resgroupcapability.h \
	gp_configuration_history.h gp_id.h gp_distribution_policy.h gp_version_at_initdb.h \
	gp_warehouse.h \
	pg_appendonly.h \
	gp_fastsequence.h pg_extprotocol.h \
	pg_attribute_encoding.h \
	pg_auth_time_constraint.h \
	pg_compression.h \
	pg_proc_callback.h \
	pg_type_encoding.h \
	pg_stat_last_operation.h pg_stat_last_shoperation.h \
	pg_foreign_table.h pg_foreign_table_seg.h pg_policy.h pg_replication_origin.h \
	pg_default_acl.h pg_init_privs.h pg_seclabel.h pg_shseclabel.h \
	pg_collation.h pg_partitioned_table.h pg_range.h pg_transform.h \
	pg_sequence.h pg_publication.h pg_publication_rel.h pg_subscription.h \
	pg_subscription_rel.h gp_partition_template.h pg_task.h pg_task_run_history.h \
	pg_profile.h pg_password_history.h pg_directory_table.h gp_storage_server.h \
	gp_storage_user_mapping.h pg_tag.h pg_tag_description.h \
	gp_matview_aux.h \
	gp_matview_tables.h

USE_INTERNAL_FTS_FOUND := $(if $(findstring USE_INTERNAL_FTS,$(CFLAGS)),true,false)

ifeq ($(USE_INTERNAL_FTS_FOUND), true)
CATALOG_HEADERS += gp_segment_configuration.h
endif

GENERATED_HEADERS := $(CATALOG_HEADERS:%.h=%_d.h) schemapg.h system_fk_info.h

POSTGRES_BKI_SRCS := $(addprefix $(top_srcdir)/src/include/catalog/,\
	$(CATALOG_HEADERS)  gp_indexing.h \
	)

ifeq ($(USE_INTERNAL_FTS_FOUND), true)
POSTGRES_BKI_SRCS += $(addprefix $(top_srcdir)/src/include/catalog/, gp_segment_configuration_indexing.h)
endif

# The .dat files we need can just be listed alphabetically.
POSTGRES_BKI_DATA = $(addprefix $(top_srcdir)/src/include/catalog/,\
	pg_aggregate.dat pg_am.dat pg_amop.dat pg_amproc.dat pg_authid.dat \
	pg_cast.dat pg_class.dat pg_collation.dat pg_conversion.dat \
	pg_database.dat pg_language.dat \
	pg_namespace.dat pg_opclass.dat pg_operator.dat pg_opfamily.dat \
	pg_proc.dat pg_range.dat pg_tablespace.dat \
	pg_ts_config.dat pg_ts_config_map.dat pg_ts_dict.dat pg_ts_parser.dat \
	pg_ts_template.dat pg_type.dat pg_profile.dat \
	)

POSTGRES_BKI_DATA += $(addprefix $(top_srcdir)/src/include/catalog/,\
	pg_compression.dat \
	pg_resgroup.dat pg_resgroupcapability.dat \
	) \
	$(top_builddir)/src/include/catalog/gp_version_at_initdb.dat


all: distprep generated-header-symlinks $(GP_SYSVIEW_SQL)

distprep: bki-stamp

.PHONY: generated-header-symlinks

catalog_json: $(CATALOG_JSON)

$(CATALOG_JSON): process_foreign_keys.pl $(POSTGRES_BKI_SRCS) $(top_srcdir)/src/include/catalog/catversion.h
	cat $(POSTGRES_BKI_SRCS) $(top_srcdir)/src/include/catalog/catversion.h | $(PERL) process_foreign_keys.pl > $@

generated-header-symlinks: $(top_builddir)/src/include/catalog/header-stamp

# bki-stamp records the last time we ran genbki.pl.  We don't rely on
# the timestamps of the individual output files, because the Perl script
# won't update them if they didn't change (to avoid unnecessary recompiles).
# Technically, this should depend on Makefile.global which supplies
# $(MAJORVERSION); but then genbki.pl would need to be re-run after every
# configure run, even in distribution tarballs.  So depending on configure.ac
# instead is cheating a bit, but it will achieve the goal of updating the
# version number when it changes.
catalog_data_extra_path =
ifeq ($(enable_catalog_ext),yes)
catalog_data_extra_path = $(top_srcdir)/src/backend/catalog-extension/
endif

bki-stamp: genbki.pl Catalog.pm $(POSTGRES_BKI_SRCS) $(POSTGRES_BKI_DATA) $(top_srcdir)/configure.ac $(top_builddir)/src/Makefile.global
	$(PERL) $< --include-path=$(top_srcdir)/src/include/ \
		--extra-path=$(catalog_data_extra_path) \
		--set-version=$(PG_MAJORVERSION) $(POSTGRES_BKI_SRCS)
	touch $@

# The generated headers must all be symlinked into builddir/src/include/,
# using absolute links for the reasons explained in src/backend/Makefile.
# We use header-stamp to record that we've done this because the symlinks
# themselves may appear older than bki-stamp.
$(top_builddir)/src/include/catalog/header-stamp: bki-stamp
	prereqdir=`cd '$(dir $<)' >/dev/null && pwd` && \
	cd '$(dir $@)' && for file in $(GENERATED_HEADERS); do \
	  rm -f $$file && $(LN_S) "$$prereqdir/$$file" . ; \
	done
	touch $@

$(GP_SYSVIEW_SQL): $(GP_SYSVIEW_IN) Makefile
	echo "-- MPP-aware system views of the PG system views. " \
	"Auto-generated from a list of system views specified in $(GP_SYSVIEW_IN). " \
	"Do not edit this file directly." > $@
	grep -v "#" $< | sed 's/pg_//g' | sed "s/.*/\
	CREATE OR REPLACE VIEW gp_& AS \
	SELECT gp_execution_segment() as gp_segment_id, * \
	FROM gp_dist_random('pg_&') \
	UNION ALL \
	SELECT -1 as gp_segment_id, * from pg_&;/g" >> $@

# Note: installation of generated headers is handled elsewhere
.PHONY: install-data
install-data: bki-stamp installdirs
	$(INSTALL_DATA) $(call vpathsearch,postgres.bki) '$(DESTDIR)$(datadir)/postgres.bki'
	$(INSTALL_DATA) $(call vpathsearch,system_constraints.sql) '$(DESTDIR)$(datadir)/system_constraints.sql'
ifeq ($(USE_INTERNAL_FTS_FOUND), false)
	$(INSTALL_DATA) $(srcdir)/external_fts.sql '$(DESTDIR)$(datadir)/external_fts.sql'
endif
	$(INSTALL_DATA) $(srcdir)/system_functions.sql '$(DESTDIR)$(datadir)/system_functions.sql'
	$(INSTALL_DATA) $(srcdir)/system_views.sql '$(DESTDIR)$(datadir)/system_views.sql'
	$(INSTALL_DATA) $(srcdir)/$(GP_SYSVIEW_SQL) '$(DESTDIR)$(datadir)/$(GP_SYSVIEW_SQL)'
	$(INSTALL_DATA) $(srcdir)/information_schema.sql '$(DESTDIR)$(datadir)/information_schema.sql'
	$(INSTALL_DATA) $(call vpathsearch,cdb_schema.sql) '$(DESTDIR)$(datadir)/cdb_init.d/cdb_schema.sql'
	$(INSTALL_DATA) $(srcdir)/sql_features.txt '$(DESTDIR)$(datadir)/sql_features.txt'

installdirs:
	$(MKDIR_P) '$(DESTDIR)$(datadir)'
	$(MKDIR_P) '$(DESTDIR)$(datadir)/cdb_init.d'

.PHONY: uninstall-data
uninstall-data:
	rm -f $(addprefix '$(DESTDIR)$(datadir)'/, postgres.bki system_constraints.sql system_functions.sql system_views.sql information_schema.sql cdb_init.d/cdb_schema.sql cdb_init.d/gp_toolkit.sql sql_features.txt)
ifeq ($(USE_INTERNAL_FTS_FOUND), false)
	rm -f $(addprefix '$(DESTDIR)$(datadir)'/, external_fts.sql)
endif
# postgres.bki, system_constraints.sql, and the generated headers are
# in the distribution tarball, so they are not cleaned here.
clean:

maintainer-clean: clean
	rm -f bki-stamp postgres.bki system_constraints.sql $(GENERATED_HEADERS) $(GP_SYSVIEW_SQL)

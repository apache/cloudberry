#ifndef PG_UPGRADE_GREENGAGE_H
#define PG_UPGRADE_GREENGAGE_H
/*
 *	greengage/pg_upgrade_greengage.h
 *
 *	Portions Copyright (c) 2019-Present, Pivotal Software Inc
 *	contrib/pg_upgrade/greengage/pg_upgrade_greengage.h
 */


#include "pg_upgrade.h"
#include <portability/instr_time.h>

#ifdef EXTRA_DYNAMIC_MEMORY_DEBUG
#include "utils/palloc_memory_debug_undef.h"
#endif

#define PG_OPTIONS_UTILITY_MODE " PGOPTIONS='-c gp_session_role=utility' "

typedef struct {
	instr_time start_time;
	instr_time end_time;
} step_timer;

/*
 * Enumeration for operations in the progress report
 */
typedef enum
{
	CHECK,
	SCHEMA_DUMP,
	SCHEMA_RESTORE,
	FILE_MAP,
	FILE_COPY,
	FIXUP,
	ABORT,
	DONE
} progress_type;

typedef enum {
	GREENGAGE_MODE_OPTION = 1,
	GREENGAGE_PROGRESS_OPTION = 2,
	GREENGAGE_OLD_GP_DBID = 3,
	GREENGAGE_NEW_GP_DBID = 4,
	GREENGAGE_OLD_TABLESPACES_FILE = 5,
	GREENGAGE_CONTINUE_CHECK_ON_FATAL = 6,
	GREENGAGE_SKIP_TARGET_CHECK = 7,
	GREENGAGE_SKIP_CHECKS = 8
} greengageOption;


#define GREENGAGE_OPTIONS \
	{"mode", required_argument, NULL, GREENGAGE_MODE_OPTION}, \
	{"progress", no_argument, NULL, GREENGAGE_PROGRESS_OPTION}, \
	{"old-gp-dbid", required_argument, NULL, GREENGAGE_OLD_GP_DBID}, \
	{"new-gp-dbid", required_argument, NULL, GREENGAGE_NEW_GP_DBID}, \
	{"old-tablespaces-file", required_argument, NULL, GREENGAGE_OLD_TABLESPACES_FILE}, \
	{"continue-check-on-fatal", no_argument, NULL, GREENGAGE_CONTINUE_CHECK_ON_FATAL}, \
	{"skip-target-check", no_argument, NULL, GREENGAGE_SKIP_TARGET_CHECK}, \
	{"skip-checks", no_argument, NULL, GREENGAGE_SKIP_CHECKS},

#define GREENGAGE_USAGE "\
	--mode=TYPE               designate node type to upgrade, \"segment\" or \"dispatcher\" (default \"segment\")\n\
	--progress                enable progress reporting\n\
	--old-gp-dbid             greengage database id of the old segment\n\
	--new-gp-dbid             greengage database id of the new segment\n\
	--old-tablespaces-file    file containing the tablespaces from an old gpdb five cluster\n\
	--continue-check-on-fatal continue to run through all pg_upgrade checks without upgrade. Stops on major issues\n\
	--skip-target-check       skip all checks and comparisons of new cluster\n\
	--skip-checks             skip all checks\n\
"

/* option_gp.c */
void initialize_greengage_user_options(void);
bool process_greengage_option(greengageOption option);
bool is_greengage_dispatcher_mode(void);
bool is_show_progress_mode(void);
void validate_greengage_options(void);
bool is_continue_check_on_fatal(void);
void set_check_fatal_occured(void);
bool get_check_fatal_occurred(void);
bool is_skip_target_check(void);
bool skip_checks(void);

/* pg_upgrade_greengage.c */
extern void freeze_master_data(void);
void reset_system_identifier(void);

/* frozenxids_gp.c */
void update_db_xids(void);

/* version_gp.c */

void check_hash_partition_usage(void);
void old_GPDB5_check_for_unsupported_distribution_key_data_types(void);
void invalidate_indexes(void);
void reset_invalid_indexes(void);

/* check_gp.c */

void check_greengage(void);

/* reporting.c */

void report_progress(ClusterInfo *cluster, progress_type op, char *fmt,...)
pg_attribute_printf(3, 4);
void close_progress(void);
void log_with_timing(step_timer *timer, const char *msg);
void duration(instr_time duration, char *buf, size_t len);

/* tablespace_gp.c */

void generate_old_tablespaces_file(ClusterInfo *oldCluster);
void populate_gpdb6_cluster_tablespace_suffix(ClusterInfo *cluster);
bool is_gpdb_version_with_filespaces(ClusterInfo *cluster);
void populate_os_info_with_file_contents(void);

/* server_gp.c */
char *greengage_extra_pg_ctl_flags(GreengageClusterInfo *info);

static inline bool
is_gpdb6(ClusterInfo *cluster)
{
	return GET_MAJOR_VERSION(cluster->major_version) == 904;
}

extern void set_old_cluster_chkpnt_oldstxid(void);

#endif /* PG_UPGRADE_GREENGAGE_H */

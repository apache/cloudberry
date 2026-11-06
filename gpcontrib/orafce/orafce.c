#include "postgres.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/guc.h"

#if PG_VERSION_NUM >=  160000

#include "utils/guc_hooks.h"

#else

#include "commands/variable.h"

#endif

#include "orafce.h"
#include "builtins.h"
#include "pipe.h"
#include "miscadmin.h"
#include "utils/builtins.h"

/*
 * Cloudberry: GUCs marked GUC_GPDB_NEED_SYNC are passed to every gang the
 * coordinator starts, so new segment processes get the session's value.
 */
#ifdef GUC_GPDB_NEED_SYNC
#define ORAFCE_GUC_SYNC		GUC_GPDB_NEED_SYNC
#else
#define ORAFCE_GUC_SYNC		0
#endif


/*  default value */
char	   *nls_date_format = NULL;
char	   *orafce_timezone = NULL;

bool		orafce_initialized = false;

size_t			orafce_shmemmsgsz;

static const struct config_enum_entry orafce_compatibility_options[] = {
	{"warning_oracle", ORAFCE_COMPATIBILITY_WARNING_ORACLE, false},
	{"warning_orafce", ORAFCE_COMPATIBILITY_WARNING_ORAFCE, false},
	{"oracle", ORAFCE_COMPATIBILITY_ORACLE, false},
	{"orafce", ORAFCE_COMPATIBILITY_ORAFCE, false},
	{NULL, 0, false}
};

#if PG_VERSION_NUM >= 150000

static shmem_request_hook_type prev_shmem_request_hook = NULL;

#endif

#if PG_VERSION_NUM >= 150000

static void
orafce_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(orafce_shmemmsgsz);
}

#endif

static bool
check_sys_guid_source(char **newval, void **extra, GucSource source)
{
	char	   *value = *newval;
	const char *canonicalstr;
	char	   *result;

	if (pg_strcasecmp(value, "uuid_generate_v1") == 0)
		canonicalstr = "uuid_generate_v1";
	else if (pg_strcasecmp(value, "uuid_generate_v1mc") == 0)
		canonicalstr = "uuid_generate_v1mc";
	else if (pg_strcasecmp(value, "uuid_generate_v4") == 0)
		canonicalstr = "uuid_generate_v4";
	else if (pg_strcasecmp(value, "gen_random_uuid") == 0)
		canonicalstr = "gen_random_uuid";
	else
		return false;

#if PG_VERSION_NUM >= 160000

	result = (char *) guc_malloc(LOG, 32);
	if (!result)
		return false;

	strcpy(result, canonicalstr);
	guc_free(*newval);
	*newval = result;

#else

	result = (char *) malloc(32);
	if (!result)
		return false;

	strcpy(result, canonicalstr);
	free(*newval);
	*newval = result;

#endif

	return true;
}


void
_PG_init(void)
{
	if (process_shared_preload_libraries_in_progress)
		orafce_shmemmsgsz = SHMEMMSGSZ_DEFAULT;
	else
		orafce_shmemmsgsz = SHMEMMSGSZ_MIN;


#if PG_VERSION_NUM >= 150000

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = orafce_shmem_request;

#else

	RequestAddinShmemSpace(orafce_shmemmsgsz);

#endif

	/* Define custom GUC variables. */
	DefineCustomStringVariable("orafce.nls_date_format",
							   "Emulate oracle's date output behaviour.",
							   NULL,
							   &nls_date_format,
							   NULL,
							   PGC_USERSET,
							   0,
							   NULL,
							   NULL, NULL);

	DefineCustomStringVariable("orafce.timezone",
							   "Specify timezone used for sysdate function.",
							   NULL,
							   &orafce_timezone,
							   "GMT",
							   PGC_USERSET,
							   0,
							   check_timezone, NULL, NULL);

	DefineCustomBoolVariable("orafce.varchar2_null_safe_concat",
							 "Specify wether empty string should be used instead of NULL in concat clauses with varchar2 and nvarchar2 data type.",
							 NULL,
							 &orafce_varchar2_null_safe_concat,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("orafce.sys_guid_source",
							   "Specify function from uuid-ossp extension used for making result.",
							   NULL,
							   &orafce_sys_guid_source,
							   "uuid_generate_v1",
							   PGC_USERSET,
							   0,
							   check_sys_guid_source, NULL, NULL);

	DefineCustomEnumVariable("orafce.using_substring_zero_width_in_substr",
							 gettext_noop("behaviour of substr function when substring_length argument is zero"),
							 NULL,
							 &orafce_substring_length_is_zero,
							 ORAFCE_COMPATIBILITY_WARNING_ORACLE,
							 orafce_compatibility_options,
							 PGC_USERSET, 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("orafce.umask",
							   "Specify umask used by utl_file.fopen.",
							   NULL,
							   &orafce_umask_str,
							   "0077",
							   PGC_USERSET,
							   0,
							   orafce_umask_check_hook,
							   orafce_umask_assign_hook, NULL);

	DefineCustomBoolVariable("orafce.oracle_compatibility_date_limit",
							 "Specify if an error is raised when the Oracle to_date() bug is reached.",
							 NULL,
							 &orafce_emit_error_on_date_bug,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	/*
	 * Cloudberry: session settings made by plvdate.set_*,
	 * plvsubst.setsubst() and oracle.set_nls_sort().  The functions keep
	 * them here so that they reach the segments; see orafce_set_config().
	 */
	DefineCustomStringVariable("orafce.plvdate_state",
							   "Calendar set by the plvdate functions.",
							   NULL,
							   &orafce_plvdate_state,
							   "",
							   PGC_USERSET,
							   GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE |
							   GUC_DISALLOW_IN_FILE | ORAFCE_GUC_SYNC,
							   orafce_plvdate_state_check_hook,
							   orafce_plvdate_state_assign_hook, NULL);

	DefineCustomStringVariable("orafce.plvsubst_keyword",
							   "Substitution keyword used by plvsubst.string(), set by plvsubst.setsubst().",
							   NULL,
							   &orafce_plvsubst_keyword,
							   "%s",
							   PGC_USERSET,
							   ORAFCE_GUC_SYNC,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("orafce.nls_sort",
							   "Locale used by nlssort() without a locale argument, set by oracle.set_nls_sort().",
							   NULL,
							   &orafce_nls_sort,
							   "",
							   PGC_USERSET,
							   ORAFCE_GUC_SYNC,
							   NULL, NULL, NULL);

	EmitWarningsOnPlaceholders("orafce");

	RegisterXactCallback(orafce_xact_cb, NULL);
	RegisterSubXactCallback(orafce_subxact_cb, NULL);

	orafce_initialized = true;
}

/*
 * Cloudberry: sets a GUC like set_config(name, value, false) does.  On the
 * coordinator set_config() also sends the new value to the segments, so a
 * session setting kept in a GUC is the same wherever a function runs.
 */
void
orafce_set_config(const char *name, const char *value)
{
	DirectFunctionCall3(set_config_by_name,
						CStringGetTextDatum(name),
						CStringGetTextDatum(value),
						BoolGetDatum(false));
}

#ifndef __ORAFCE__
#define __ORAFCE__

#include "postgres.h"
#include "access/xact.h"
#include "catalog/catversion.h"
#include "nodes/pg_list.h"
#include <sys/time.h>
#include "utils/datetime.h"
#include "utils/datum.h"
#include "utils/guc.h"

#define TextPCopy(t) \
	DatumGetTextP(datumCopy(PointerGetDatum(t), false, -1))

#define PG_GETARG_IF_EXISTS(n, type, defval) \
	((PG_NARGS() > (n) && !PG_ARGISNULL(n)) ? PG_GETARG_##type(n) : (defval))

/* alignment of this struct must fit for all types */
typedef union vardata
{
	char		c;
	short		s;
	int			i;
	long		l;
	float		f;
	double		d;
	void	   *p;
} vardata;

typedef enum orafce_compatibility
{
	ORAFCE_COMPATIBILITY_WARNING_ORACLE,
	ORAFCE_COMPATIBILITY_WARNING_ORAFCE,
	ORAFCE_COMPATIBILITY_ORACLE,
	ORAFCE_COMPATIBILITY_ORAFCE,
} orafce_compatibility;

extern int	ora_instr(text *txt, text *pattern, int start, int nth);
extern int	ora_mb_strlen(text *str, char **sizes, int **positions);
extern int	ora_text_mbstrlen(text *str);

extern char *nls_date_format;
extern char *orafce_timezone;
extern char *orafce_umask_str;
extern bool orafce_initialized;
extern bool orafce_varchar2_null_safe_concat;
extern int	orafce_substring_length_is_zero;
extern bool orafce_emit_error_on_date_bug;

extern char *orafce_sys_guid_source;

extern void orafce_xact_cb(XactEvent event, void *arg);
extern void orafce_subxact_cb(SubXactEvent event, SubTransactionId mySubid,
							SubTransactionId parentSubid, void *arg);

extern void orafce_umask_assign_hook(const char *newvalue, void *extra);
extern bool orafce_umask_check_hook(char **newval, void **extra, GucSource source);

/*
 * pg_mblen_range() and pg_mblen_cstr() replace the now deprecated pg_mblen().
 * They were not added by a major release - they appeared in the minor releases
 * 14.21, 15.16, 16.12, 17.8 and 18.2 - so a plain "PG_VERSION_NUM < 140000"
 * test would make orafce fail to build against every older minor of those
 * branches.
 */
#if PG_VERSION_NUM >= 190000 || \
	(PG_VERSION_NUM >= 180002 && PG_VERSION_NUM < 190000) || \
	(PG_VERSION_NUM >= 170008 && PG_VERSION_NUM < 180000) || \
	(PG_VERSION_NUM >= 160012 && PG_VERSION_NUM < 170000) || \
	(PG_VERSION_NUM >= 150016 && PG_VERSION_NUM < 160000) || \
	(PG_VERSION_NUM >= 140021 && PG_VERSION_NUM < 150000)
#define ORAFCE_HAVE_PG_MBLEN_RANGE
#endif

#ifndef ORAFCE_HAVE_PG_MBLEN_RANGE

#define pg_mblen_range(str,end)		orafce_mblen_range(str,end)
#define pg_mblen_cstr(str)			pg_mblen(str)

extern int orafce_mblen_range(const char *mbstr, const char *end);

#endif



/*
 * Version compatibility
 */

extern Oid	equality_oper_funcid(Oid argtype);

/*
 * Date utils
 */
#define STRING_PTR_FIELD_TYPE const char *const

extern STRING_PTR_FIELD_TYPE ora_days[];

extern int	ora_seq_search(const char *name, STRING_PTR_FIELD_TYPE array[], size_t max);

#ifdef _MSC_VER

#define int2size(v)			(size_t)(v)
#define size2int(v)			(int)(v)

#else

#define int2size(v)			v
#define size2int(v)			v

#endif

#endif

#include "postgres.h"
#include "c.h"
#include <signal.h>
#include <fcntl.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#ifdef WIN32
#include <io.h>
#endif
#include "libpq/pqsignal.h"
#include "gpmon.h"

#include "executor/execdesc.h"
#include "utils/guc.h"
#include "utils/memutils.h"

#include "access/xact.h"
#include "cdb/cdbtm.h"
#include "cdb/cdbvars.h"
#include "commands/explain.h"
#include "executor/executor.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "utils/metrics_utils.h"
#include "utils/metrics_utils.h"
#include "utils/snapmgr.h"

#include "pg_query_state.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

/* Extern stuff */
extern char *get_database_name(Oid dbid);

static void gpmon_record_kv_with_file(const char* key,
				  const char* value,
				  bool extraNewLine,
				  FILE* fp);
static void gpmon_record_update(gpmon_qlogkey_t key,
						 int32 status);
static const char* gpmon_null_subst(const char* input);

/* gpmon hooks */
static query_info_collect_hook_type prev_query_info_collect_hook = NULL;

static void gpmon_query_info_collect_hook(QueryMetricsStatus status, void *queryDesc);

static gpmon_packet_t* gpmon_qlog_packet_init();
static void init_gpmon_hooks(void);
static char* get_query_text(QueryDesc *queryDesc);
static bool check_query(QueryDesc *queryDesc, QueryMetricsStatus status);

static void gpmon_qlog_query_submit(gpmon_packet_t *gpmonPacket, QueryDesc *qd);
static void gpmon_qlog_query_text(const gpmon_packet_t *gpmonPacket,
		const char *queryText,
		const char *plan,
		const char *appName,
		const char *resqName,
		const char *resqPriority,
		int status);
static void gpmon_qlog_query_start(gpmon_packet_t *gpmonPacket, QueryDesc *qd);
static void gpmon_qlog_query_end(gpmon_packet_t *gpmonPacket, QueryDesc *qd, bool updateRecord);
static void gpmon_qlog_query_error(gpmon_packet_t *gpmonPacket, QueryDesc *qd);
static void gpmon_qlog_query_canceling(gpmon_packet_t *gpmonPacket, QueryDesc *qd);
static void gpmon_send(gpmon_packet_t*);
static inline void set_query_key(gpmon_qlogkey_t *key, int32 ccnt);

struct  {
    int    gxsock;
	pid_t  pid;
	struct sockaddr_in gxaddr;
} gpmon = {0};

int64 gpmon_tick = 0;

typedef struct
{
	/* data */
	int 		query_command_count;
	QueryDesc 	*queryDesc;
	int32 		tstart;
	int32 		tsubmit;
} PerfmonQuery;
PerfmonQuery *toppest_query;

static void reset_toppest_query(QueryDesc *qd);
static void init_toppest_query(QueryDesc *qd);
static inline PerfmonQuery* get_toppest_perfmon_query(void);
static inline void set_query_tsubmit(int32 tsubmit,QueryDesc *qd);
static inline void set_query_tstart(int32 tstart,QueryDesc *qd);
static inline int get_query_command_count(QueryDesc *qd);
static inline int32 get_query_tsubmit(QueryDesc *qd);
static inline int32 get_query_tstart(QueryDesc *qd);

void gpmon_sig_handler(int sig);

void gpmon_sig_handler(int sig)
{
	gpmon_tick++;
}

void gpmon_init(void)
{
//	struct itimerval tv;
#ifndef WIN32
	pqsigfunc sfunc;
#endif
	pid_t pid = getpid();
	int sock;

	if (pid == gpmon.pid)
		return;
#ifndef WIN32
	sfunc = pqsignal(SIGVTALRM, gpmon_sig_handler);
	if (sfunc == SIG_ERR) {
		elog(WARNING, "[perfmon]: unable to set signal handler for SIGVTALRM (%m)");
	}
	else if (sfunc == gpmon_sig_handler) {
		close(gpmon.gxsock); 
		gpmon.gxsock = -1;
	}
	else {
		Assert(sfunc == 0);
	}
#endif

	sock = socket(AF_INET, SOCK_DGRAM, 0);
	if (sock == -1) {
		elog(WARNING, "[perfmon]: cannot create socket (%m)");
	}
#ifndef WIN32
    if (fcntl(sock, F_SETFL, O_NONBLOCK) == -1) {
		elog(WARNING, "[perfmon] fcntl(F_SETFL, O_NONBLOCK) failed");
    }
    if (fcntl(sock, F_SETFD, 1) == -1) {
		elog(WARNING, "[perfmon] fcntl(F_SETFD) failed");
    }
#endif 
	gpmon.gxsock = sock;
	memset(&gpmon.gxaddr, 0, sizeof(gpmon.gxaddr));
	gpmon.gxaddr.sin_family = AF_INET;
	gpmon.gxaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
	gpmon.gxaddr.sin_port = htons(perfmon_port);
	gpmon.pid = pid;
}

static void gpmon_record_kv_with_file(const char* key,
				  const char* value,
				  bool extraNewLine,
				  FILE* fp)
{
	int len = strlen(value);

	fprintf(fp, "%d %s\n", len, key);
	fwrite(value, 1, len, fp);
	fprintf(fp, "\n");

	if (extraNewLine)
	{
		fprintf(fp, "\n");
	}
}

static void
gpmon_record_update(gpmon_qlogkey_t key,
						 int32 status)
{
	char fname[GPMON_DIR_MAX_PATH];
	FILE *fp;

	snprintf(fname, GPMON_DIR_MAX_PATH, "%sq%d-%d-%d.txt", GPMON_DIR, key.tmid, key.ssid, key.ccnt);

	fp = fopen(fname, "r+");

	if (!fp)
		return;

	if (0 == fseek(fp, -1, SEEK_END))
	{
		fprintf(fp, "%d", status);
	}
	fclose(fp);
}

static inline void
set_query_key(gpmon_qlogkey_t *key, int32 ccnt)
{
	key->tmid = 0;
	key->ssid = gp_session_id;
	key->ccnt =  ccnt;
}

static void
gpmon_send(gpmon_packet_t* p)
{
	if (p->magic != GPMON_MAGIC)  {
		elog(WARNING, "[perfmon] - bad magic %x", p->magic);
		return;
	}

	if (p->pkttype == GPMON_PKTTYPE_QEXEC) {
		elog(DEBUG1,
				"[perfmon] Perfmon Executor Packet: (tmid, ssid, ccnt, segid, pid, nid, status) = "
				"(%d, %d, %d, %d, %d, %d, %d)",
				p->u.qexec.key.qkey.tmid, p->u.qexec.key.qkey.ssid, p->u.qexec.key.qkey.ccnt,
				p->u.qexec.key.hash_key.segid, p->u.qexec.key.hash_key.pid, p->u.qexec.key.hash_key.nid,
				p->u.qexec.status);
	}

	if (gpmon.gxsock > 0) {
		int n = sizeof(*p);
		if (n != sendto(gpmon.gxsock, (const char *)p, n, 0,
						(struct sockaddr*) &gpmon.gxaddr,
						sizeof(gpmon.gxaddr))) {
			elog(LOG, "[perfmon]: cannot send (%m socket %d)", gpmon.gxsock);
		}
	}
}

#define GPMON_QLOG_PACKET_ASSERTS(gpmonPacket) \
		Assert(perfmon_enabled && Gp_role == GP_ROLE_DISPATCH); \
		Assert(gpmonPacket); \
		Assert(gpmonPacket->magic == GPMON_MAGIC); \
		Assert(gpmonPacket->version == GPMON_PACKET_VERSION); \
		Assert(gpmonPacket->pkttype == GPMON_PKTTYPE_QLOG)

/**
 * Create and init a qlog packet
 *
 * It is called by gpmon_query_info_collect_hook each time
 * gpsmon and gpmmon will merge the packets with the same
 * key together in 'update_qlog'
 */
static gpmon_packet_t*
gpmon_qlog_packet_init(QueryDesc *qd)
{
	const char *username = NULL;
	gpmon_packet_t *gpmonPacket = NULL;
	gpmonPacket = (gpmon_packet_t *) palloc(sizeof(gpmon_packet_t));
	memset(gpmonPacket, 0, sizeof(gpmon_packet_t));

	Assert(perfmon_enabled && Gp_role == GP_ROLE_DISPATCH);
	Assert(gpmonPacket);

	gpmonPacket->magic = GPMON_MAGIC;
	gpmonPacket->version = GPMON_PACKET_VERSION;
	gpmonPacket->pkttype = GPMON_PKTTYPE_QLOG;
	gpmonPacket->u.qlog.status = GPMON_QLOG_STATUS_SILENT;

	set_query_key(&gpmonPacket->u.qlog.key, get_query_command_count(qd));
	gpmonPacket->u.qlog.key.ssid = gp_session_id;
	gpmonPacket->u.qlog.pid = MyProcPid;

	username = GetConfigOption("session_authorization", false, false); /* does not have to be freed */
	/* User Id.  We use session authorization_string (so to make sense with session id) */
	snprintf(gpmonPacket->u.qlog.user, sizeof(gpmonPacket->u.qlog.user), "%s",
			username ? username : "");
	gpmonPacket->u.qlog.dbid = MyDatabaseId;

	return gpmonPacket;
}


/**
 * Create and init a qexec packet
 *
 * It is called by gpmon_query_info_collect_hook each time
 */
static gpmon_packet_t*
gpmon_qexec_packet_init()
{
	gpmon_packet_t *gpmonPacket = NULL;
	gpmonPacket = (gpmon_packet_t *) palloc(sizeof(gpmon_packet_t));
	memset(gpmonPacket, 0, sizeof(gpmon_packet_t));

	Assert(perfmon_enabled && Gp_role == GP_ROLE_EXECUTE);
	Assert(gpmonPacket);
	gpmonPacket->magic = GPMON_MAGIC;
	gpmonPacket->version = GPMON_PACKET_VERSION;
	gpmonPacket->pkttype = GPMON_PKTTYPE_QEXEC;

	/* Better to use get_query_command_count here */
	set_query_key(&gpmonPacket->u.qexec.key.qkey, gp_command_count);
	gpmonPacket->u.qexec.key.hash_key.segid = GpIdentity.segindex;
	gpmonPacket->u.qexec.key.hash_key.pid = MyProcPid;
	return gpmonPacket;
}

/**
 * Call this method when query is submitted.
 */
static void
gpmon_qlog_query_submit(gpmon_packet_t *gpmonPacket, QueryDesc *qd)
{
	struct timeval tv;

	GPMON_QLOG_PACKET_ASSERTS(gpmonPacket);

	gettimeofday(&tv, 0);
	gpmonPacket->u.qlog.status = GPMON_QLOG_STATUS_SUBMIT;
	gpmonPacket->u.qlog.tsubmit = tv.tv_sec;
	set_query_tsubmit(tv.tv_sec, qd);
	gpmon_send(gpmonPacket);
}

/**
 * Wrapper function that returns string if not null. Returns GPMON_UNKNOWN if it is null.
 */
static const char* gpmon_null_subst(const char* input)
{
	return input ? input : GPMON_UNKNOWN;
}


/**
 * Call this method to let gpmon know the query text, application name, resource queue name and priority
 * at submit time. It writes 4 key value pairs using keys: qtext, appname, resqname and priority using
 * the format as described as below:
 * This method adds a key-value entry to the gpmon text file. The format it uses is:
 * <VALUE_LENGTH> <KEY>\n
 * <VALUE>\n
 * Boolean value extraByte indicates whether an additional newline is desired. This is
 * necessary because gpmon overwrites the last byte to indicate status.
 *
 * Have tested the speed of this function on local machine
 * - each file is 0B, 1000 files, take about 50ms
 * - each file is 102B, 1000 files, take about 70ms
 * - each file is 57K, 1000 files, take about 240ms
 */
static void
gpmon_qlog_query_text(const gpmon_packet_t *gpmonPacket,
		const char *queryText,
		const char *plan,
		const char *appName,
		const char *resqName,
		const char *resqPriority,
		int status)
{
	GPMON_QLOG_PACKET_ASSERTS(gpmonPacket);
	char fname[GPMON_DIR_MAX_PATH];
	FILE* fp;

	queryText = gpmon_null_subst(queryText);
	plan = gpmon_null_subst(plan);
	appName = gpmon_null_subst(appName);
	resqName = gpmon_null_subst(resqName);
	resqPriority = gpmon_null_subst(resqPriority);

	Assert(queryText);
	Assert(appName);
	Assert(resqName);
	Assert(resqPriority);


	snprintf(fname, GPMON_DIR_MAX_PATH, "%sq%d-%d-%d.txt", GPMON_DIR,
										gpmonPacket->u.qlog.key.tmid,
										gpmonPacket->u.qlog.key.ssid,
										gpmonPacket->u.qlog.key.ccnt);
	fp = fopen(fname, "w+");

	if (!fp)
		return;

	gpmon_record_kv_with_file("qtext", queryText, false, fp);
	gpmon_record_kv_with_file("plan", plan, false, fp);
	gpmon_record_kv_with_file("appname", appName, false, fp);
	gpmon_record_kv_with_file("resqname", resqName, false, fp);
	gpmon_record_kv_with_file("priority", resqPriority, true, fp);
	fprintf(fp, "%d", status);
	fclose(fp);
}

/**
 * Call this method when query starts executing.
 */
static void
gpmon_qlog_query_start(gpmon_packet_t *gpmonPacket, QueryDesc *qd)
{
	struct timeval tv;

	GPMON_QLOG_PACKET_ASSERTS(gpmonPacket);

	gettimeofday(&tv, 0);
	
	gpmonPacket->u.qlog.status = GPMON_QLOG_STATUS_START;
	gpmonPacket->u.qlog.tsubmit = get_query_tsubmit(qd);
	gpmonPacket->u.qlog.tstart = tv.tv_sec;
	set_query_tstart(tv.tv_sec, qd);
	gpmon_record_update(gpmonPacket->u.qlog.key,
			gpmonPacket->u.qlog.status);
	gpmon_send(gpmonPacket);
}

/**
 * Call this method when query finishes executing.
 */
static void
gpmon_qlog_query_end(gpmon_packet_t *gpmonPacket, QueryDesc *qd, bool updateRecord)
{
	struct timeval tv;

	GPMON_QLOG_PACKET_ASSERTS(gpmonPacket);
	gettimeofday(&tv, 0);
	
	gpmonPacket->u.qlog.status = GPMON_QLOG_STATUS_DONE;
	gpmonPacket->u.qlog.tsubmit = get_query_tsubmit(qd);
	gpmonPacket->u.qlog.tstart = get_query_tstart(qd);
	gpmonPacket->u.qlog.tfin = tv.tv_sec;
	if (updateRecord)
		gpmon_record_update(gpmonPacket->u.qlog.key,
							gpmonPacket->u.qlog.status);

	gpmon_send(gpmonPacket);
}

/**
 * Call this method when query errored out.
 */
static void
gpmon_qlog_query_error(gpmon_packet_t *gpmonPacket, QueryDesc *qd)
{
	struct timeval tv;

	GPMON_QLOG_PACKET_ASSERTS(gpmonPacket);

	gettimeofday(&tv, 0);
	
	gpmonPacket->u.qlog.status = GPMON_QLOG_STATUS_ERROR;
	gpmonPacket->u.qlog.tsubmit = get_query_tsubmit(qd);
	gpmonPacket->u.qlog.tstart = get_query_tstart(qd);
	gpmonPacket->u.qlog.tfin = tv.tv_sec;
	
	gpmon_record_update(gpmonPacket->u.qlog.key,
			gpmonPacket->u.qlog.status);
	
	gpmon_send(gpmonPacket);
}

/*
 * gpmon_qlog_query_canceling
 *    Record that the query is being canceled.
 */
static void
gpmon_qlog_query_canceling(gpmon_packet_t *gpmonPacket, QueryDesc *qd)
{
	GPMON_QLOG_PACKET_ASSERTS(gpmonPacket);
	gpmonPacket->u.qlog.status = GPMON_QLOG_STATUS_CANCELING;
	gpmonPacket->u.qlog.tsubmit = get_query_tsubmit(qd);
	gpmonPacket->u.qlog.tstart = get_query_tstart(qd);
	gpmon_record_update(gpmonPacket->u.qlog.key,
			gpmonPacket->u.qlog.status);
	
	gpmon_send(gpmonPacket);
}

static void
gpmon_query_info_collect_hook(QueryMetricsStatus status, void *queryDesc)
{
	char *query_text;
	char *plan;
	bool updateRecord = true;
	gpmon_packet_t *gpmonPacket = NULL;
	if (prev_query_info_collect_hook)
		(*prev_query_info_collect_hook)(status, queryDesc);

	if (queryDesc == NULL || !perfmon_enabled)
		return;

	if (Gp_role == GP_ROLE_DISPATCH && !check_query((QueryDesc*)queryDesc, status))
		return;

	PG_TRY();
	{
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			QueryDesc *qd = (QueryDesc *)queryDesc;
			switch (status)
			{
			case METRICS_QUERY_SUBMIT:
				init_toppest_query(qd);
				gpmonPacket = gpmon_qlog_packet_init(qd);
				query_text = get_query_text((QueryDesc *)queryDesc);
				gpmon_qlog_query_text(gpmonPacket,
									  query_text,
									  NULL,
									  application_name,
									  NULL,
									  NULL,
									  GPMON_QLOG_STATUS_SUBMIT);
				gpmon_qlog_query_submit(gpmonPacket, qd);
				break;
			case METRICS_QUERY_START:
				gpmonPacket = gpmon_qlog_packet_init(qd);
				gpmon_qlog_query_start(gpmonPacket, qd);
				break;
			case METRICS_QUERY_DONE:
			case METRICS_INNER_QUERY_DONE:
				gpmonPacket = gpmon_qlog_packet_init(qd);
				/*
				 * plannedstmt in queryDesc may have been cleaned ,
				 * so we cannot check queryId here.
				 * Only check gp_command_count
				 */
				if (enable_qs_runtime() && CachedQueryStateInfo != NULL &&
					get_command_count(CachedQueryStateInfo) == get_query_command_count(qd))
				{
					query_text = get_query_text(qd);
					plan = (char *)CachedQueryStateInfo->data;
					gpmon_qlog_query_text(gpmonPacket,
										  query_text,
										  plan,
										  application_name,
										  NULL,
										  NULL,
										  GPMON_QLOG_STATUS_DONE);
					updateRecord = false;
				}
				gpmon_qlog_query_end(gpmonPacket, qd, updateRecord);
				reset_toppest_query(qd);
				break;
				/* TODO: no GPMON_QLOG_STATUS for METRICS_QUERY_CANCELED */
			case METRICS_QUERY_CANCELING:
				gpmonPacket = gpmon_qlog_packet_init(qd);
				gpmon_qlog_query_canceling(gpmonPacket, qd);
				break;
			case METRICS_QUERY_ERROR:
			case METRICS_QUERY_CANCELED:
				gpmonPacket = gpmon_qlog_packet_init(qd);
				gpmon_qlog_query_error(gpmonPacket, qd);
				reset_toppest_query(qd);
				break;
			default:
				break;
			}
			if (gpmonPacket != NULL)
				pfree(gpmonPacket);
		}
		else if (Gp_role == GP_ROLE_EXECUTE)
		{
			gpmonPacket = gpmon_qexec_packet_init();
			switch (status)
			{
			case METRICS_QUERY_START:
				gpmon_send(gpmonPacket);
				break;
			default:
				break;
			}
			pfree(gpmonPacket);
		}
		gpmonPacket = NULL;
	}
	PG_CATCH();
	{
		EmitErrorReport();
		/* swallow any error in this hook */
		FlushErrorState();
		if (gpmonPacket != NULL)
			pfree(gpmonPacket);
	}
	PG_END_TRY();
}

static void
init_gpmon_hooks(void)
{
	prev_query_info_collect_hook =  query_info_collect_hook;
	query_info_collect_hook = gpmon_query_info_collect_hook;
}

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR, (errmsg("gpmon not in shared_preload_libraries")));
	}
	else
	{
		if (!perfmon_enabled)
			return;
		/* add version info */
		ereport(LOG, (errmsg("booting gpmon")));
	}
	init_gpmon_hooks();
	gpmon_init();
	init_pg_query_state();
}

void
_PG_fini(void)
{}

static
char* get_query_text(QueryDesc *qd)
{
		/* convert to UTF8 which is encoding for gpperfmon database */
		char *query_text = (char *)qd->sourceText;
		/**
		 * When client encoding and server encoding are different, do apply the conversion.
		 */
		if (GetDatabaseEncoding() != pg_get_client_encoding())
		{
				query_text = (char *)pg_do_encoding_conversion((unsigned char*)qd->sourceText,
								strlen(qd->sourceText), GetDatabaseEncoding(), PG_UTF8);
		}
		return query_text;
}

static
bool check_query(QueryDesc *queryDesc, QueryMetricsStatus status)
{
	PerfmonQuery *query;
	switch (status)
	{
	case METRICS_QUERY_SUBMIT:
		return is_querystack_empty();
	case METRICS_QUERY_START:
	case METRICS_QUERY_DONE:
	case METRICS_INNER_QUERY_DONE:
	case METRICS_QUERY_ERROR:
	case METRICS_QUERY_CANCELING:
	case METRICS_QUERY_CANCELED:
		query = get_toppest_perfmon_query();
		return query != NULL && query->queryDesc == queryDesc;
	default:
		return true;
	}
	/*
	 * get_query returns the toppest query in the stack or NULL
	 */
	return false;
}

static void
init_toppest_query(QueryDesc *qd)
{
	MemoryContext oldCtx = CurrentMemoryContext;
	MemoryContextSwitchTo(TopMemoryContext);
	if (is_querystack_empty())
	{
		if (toppest_query != NULL)
		{
			elog(WARNING, "toppest_query not reset properly %d", toppest_query->query_command_count);
			pfree(toppest_query);
			toppest_query = NULL;
		}
		PerfmonQuery *query = (PerfmonQuery *)palloc(sizeof(PerfmonQuery));
		query->query_command_count = gp_command_count;
		query->tstart = 0;
		query->tsubmit = 0;
		query->queryDesc = qd;
		toppest_query = query;
	}
	MemoryContextSwitchTo(oldCtx);
}

static void
reset_toppest_query(QueryDesc *qd)
{
	if (toppest_query != NULL && toppest_query->queryDesc == qd)
	{
		pfree(toppest_query);
		toppest_query = NULL;
	}
}

static inline PerfmonQuery*
get_toppest_perfmon_query(void)
{
	return toppest_query;
}

static inline void
set_query_tsubmit(int32 tsubmit, QueryDesc *qd)
{
	PerfmonQuery *query = get_toppest_perfmon_query();
	if (query != NULL)
	{
		Assert(qd == query->queryDesc);
		query->tsubmit = tsubmit;
	}
}

static inline void
set_query_tstart(int32 tstart,QueryDesc *qd)
{
	PerfmonQuery *query = get_toppest_perfmon_query();
	if (query != NULL)
	{
		Assert(qd == query->queryDesc);
		query->tstart = tstart;
	}
}

static inline int
get_query_command_count(QueryDesc *qd)
{
	PerfmonQuery *query = get_toppest_perfmon_query();
	if (query != NULL)
	{
		Assert(qd == query->queryDesc);
		return query->query_command_count;
	}
	return gp_command_count;
}

static inline int32
get_query_tsubmit(QueryDesc *qd)
{
	PerfmonQuery *query = get_toppest_perfmon_query();
	if (query != NULL)
	{
		Assert(qd == query->queryDesc);
		return query->tsubmit;
	}
	return 0;
}

static inline int32
get_query_tstart(QueryDesc *qd)
{

	PerfmonQuery *query = get_toppest_perfmon_query();
	if (query != NULL)
	{
		Assert(qd == query->queryDesc);
		return query->tstart;
	}
	return 0;
}

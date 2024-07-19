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
static int32 init_tmid = -1;;

void _PG_init(void);
void _PG_fini(void);

/* Extern stuff */
extern char *get_database_name(Oid dbid);

static void gpmon_record_kv_with_file(const char* key,
				  const char* value,
				  bool extraNewLine,
				  FILE* fp);
static void gpmon_record_update(int32 tmid, int32 ssid,
								int32 ccnt, int32 status);
static const char* gpmon_null_subst(const char* input);

/* gpmon hooks */
static query_info_collect_hook_type prev_query_info_collect_hook = NULL;

static void gpmon_query_info_collect_hook(QueryMetricsStatus status, void *queryDesc);

static gpmon_packet_t* gpmon_qlog_packet_init();
static void init_gpmon_hooks(void);
static char* get_plan(QueryDesc *queryDesc);
static char* get_query_text(QueryDesc *queryDesc);
static int32 tstart = 0;
static int32 tsubmit = 0;

struct  {
    int    gxsock;
	pid_t  pid;
	struct sockaddr_in gxaddr;
} gpmon = {0};

int64 gpmon_tick = 0;

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

//	/*TODO: what exactly perfmon_send_interval does? */
//	tv.it_interval.tv_sec = perfmon_send_interval;
//	//tv.it_interval.tv_sec = 5;
//	tv.it_interval.tv_usec = 0;
//	tv.it_value = tv.it_interval;
//#ifndef WIN32
//	if (-1 == setitimer(ITIMER_VIRTUAL, &tv, 0)) {
//		elog(WARNING, "[perfmon]: unable to start timer (%m)");
//	}
//#endif 
//
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

void gpmon_record_update(int32 tmid, int32 ssid, int32 ccnt,
						 int32 status)
{
	char fname[GPMON_DIR_MAX_PATH];
	FILE *fp;

	snprintf(fname, GPMON_DIR_MAX_PATH, "%sq%d-%d-%d.txt", GPMON_DIR, tmid, ssid, ccnt);

	fp = fopen(fname, "r+");

	if (!fp)
		return;

	if (0 == fseek(fp, -1, SEEK_END))
	{
		fprintf(fp, "%d", status);
	}
	fclose(fp);
}

void gpmon_gettmid(int32* tmid)
{
	Assert(init_tmid > -1);
	*tmid = init_tmid;
} 


void gpmon_send(gpmon_packet_t* p)
{
	if (p->magic != GPMON_MAGIC)  {
		elog(WARNING, "[perfmon] - bad magic %x", p->magic);
		return;
	}


	if (p->pkttype == GPMON_PKTTYPE_QEXEC) {
		elog(DEBUG1,
				"[perfmon] Perfmon Executor Packet: (tmid, ssid, ccnt, segid, pid, nid, status) = "
				"(%d, %d, %d, %d, %d, %d, %d)",
				p->u.qexec.key.tmid, p->u.qexec.key.ssid, p->u.qexec.key.ccnt,
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
gpmon_qlog_packet_init()
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

	gpmon_gettmid(&gpmonPacket->u.qlog.key.tmid);
	gpmonPacket->u.qlog.key.ssid = gp_session_id;
	gpmonPacket->u.qlog.pid = MyProcPid;


	username = GetConfigOption("session_authorization", false, false); /* does not have to be freed */
	/* User Id.  We use session authorization_string (so to make sense with session id) */
	snprintf(gpmonPacket->u.qlog.user, sizeof(gpmonPacket->u.qlog.user), "%s",
			username ? username : "");
	gpmonPacket->u.qlog.dbid = MyDatabaseId;

	/* Fix up command count */
	gpmonPacket->u.qlog.key.ccnt = gp_command_count;
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

	gpmon_gettmid(&gpmonPacket->u.qexec.key.tmid);
	gpmonPacket->u.qexec.key.ssid = gp_session_id;
	gpmonPacket->u.qexec.key.ccnt = gp_command_count;
	gpmonPacket->u.qexec.key.hash_key.segid = GpIdentity.segindex;
	gpmonPacket->u.qexec.key.hash_key.pid = MyProcPid;
	return gpmonPacket;
}

/**
 * Call this method when query is submitted.
 */
void gpmon_qlog_query_submit(gpmon_packet_t *gpmonPacket)
{
	struct timeval tv;


	GPMON_QLOG_PACKET_ASSERTS(gpmonPacket);

	gettimeofday(&tv, 0);
	tsubmit = tv.tv_sec;
	gpmonPacket->u.qlog.status = GPMON_QLOG_STATUS_SUBMIT;
	gpmonPacket->u.qlog.tsubmit = tsubmit;
	
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
 */

void gpmon_qlog_query_text(const gpmon_packet_t *gpmonPacket,
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
void gpmon_qlog_query_start(gpmon_packet_t *gpmonPacket)
{
	struct timeval tv;

	GPMON_QLOG_PACKET_ASSERTS(gpmonPacket);

	gettimeofday(&tv, 0);
	tstart = tv.tv_sec;
	
	gpmonPacket->u.qlog.status = GPMON_QLOG_STATUS_START;
	gpmonPacket->u.qlog.tsubmit = tsubmit;
	gpmonPacket->u.qlog.tstart = tstart;
	gpmon_record_update(gpmonPacket->u.qlog.key.tmid,
			gpmonPacket->u.qlog.key.ssid,
			gpmonPacket->u.qlog.key.ccnt,
			gpmonPacket->u.qlog.status);
	gpmon_send(gpmonPacket);
}

/**
 * Call this method when query finishes executing.
 */
void gpmon_qlog_query_end(gpmon_packet_t *gpmonPacket, bool updateRecord)
{
	struct timeval tv;

	GPMON_QLOG_PACKET_ASSERTS(gpmonPacket);
	gettimeofday(&tv, 0);
	
	gpmonPacket->u.qlog.status = GPMON_QLOG_STATUS_DONE;
	gpmonPacket->u.qlog.tsubmit = tsubmit;
	gpmonPacket->u.qlog.tstart = tstart;
	gpmonPacket->u.qlog.tfin = tv.tv_sec;
	if (updateRecord)
		gpmon_record_update(gpmonPacket->u.qlog.key.tmid,
							gpmonPacket->u.qlog.key.ssid,
							gpmonPacket->u.qlog.key.ccnt,
							gpmonPacket->u.qlog.status);

	gpmon_send(gpmonPacket);
}

/**
 * Call this method when query errored out.
 */
void gpmon_qlog_query_error(gpmon_packet_t *gpmonPacket)
{
	struct timeval tv;

	GPMON_QLOG_PACKET_ASSERTS(gpmonPacket);

	gettimeofday(&tv, 0);
	
	gpmonPacket->u.qlog.status = GPMON_QLOG_STATUS_ERROR;
	gpmonPacket->u.qlog.tsubmit = tsubmit;
	gpmonPacket->u.qlog.tstart = tstart;
	gpmonPacket->u.qlog.tfin = tv.tv_sec;
	
	gpmon_record_update(gpmonPacket->u.qlog.key.tmid,
			gpmonPacket->u.qlog.key.ssid,
			gpmonPacket->u.qlog.key.ccnt,
			gpmonPacket->u.qlog.status);
	
	gpmon_send(gpmonPacket);
}

/*
 * gpmon_qlog_query_canceling
 *    Record that the query is being canceled.
 */
void
gpmon_qlog_query_canceling(gpmon_packet_t *gpmonPacket)
{
	GPMON_QLOG_PACKET_ASSERTS(gpmonPacket);
	gpmonPacket->u.qlog.status = GPMON_QLOG_STATUS_CANCELING;
	gpmonPacket->u.qlog.tsubmit = tsubmit;
	gpmonPacket->u.qlog.tstart = tstart;
	
	gpmon_record_update(gpmonPacket->u.qlog.key.tmid,
			gpmonPacket->u.qlog.key.ssid,
			gpmonPacket->u.qlog.key.ccnt,
			gpmonPacket->u.qlog.status);
	
	gpmon_send(gpmonPacket);
}

static void 
gpmon_query_info_collect_hook(QueryMetricsStatus status, void *queryDesc)
{
	char *query_text;
	char *plan;
	QueryDesc *qd = (QueryDesc *)queryDesc;
	bool updateRecord = true;
	if (perfmon_enabled && qd != NULL)
	{
		gpmon_packet_t *gpmonPacket = NULL;
		PG_TRY();
		{
		if (Gp_role == GP_ROLE_DISPATCH)
		{
			gpmonPacket = gpmon_qlog_packet_init();
			switch (status)
			{
				case METRICS_QUERY_START:
					gpmon_qlog_query_start(gpmonPacket);
					break;
				case METRICS_QUERY_SUBMIT:
					query_text = get_query_text(qd);
					gpmon_qlog_query_text(gpmonPacket,
							query_text,
							NULL,
							application_name,
							NULL,
							NULL,
							GPMON_QLOG_STATUS_SUBMIT);
					gpmon_qlog_query_submit(gpmonPacket);
					break;
				case METRICS_QUERY_DONE:
					if (enable_qs_runtime() && CachedQueryStateInfo != NULL &&
						CachedQueryStateInfo->gp_command_count == gp_command_count)
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
					gpmon_qlog_query_end(gpmonPacket, updateRecord);
					break;
					/* TODO: no GPMON_QLOG_STATUS for METRICS_QUERY_CANCELED */
				case METRICS_QUERY_CANCELING:
					gpmon_qlog_query_canceling(gpmonPacket);
					break;
				case METRICS_QUERY_ERROR:
				case METRICS_QUERY_CANCELED:
					gpmon_qlog_query_error(gpmonPacket);
					break;
				case METRICS_PLAN_NODE_INITIALIZE:
					if (!enable_qs_runtime())
					{
						query_text = get_query_text(qd);
						plan = get_plan(qd);
						gpmon_qlog_query_text(gpmonPacket,
											  query_text,
											  plan,
											  application_name,
											  NULL,
											  NULL,
											  GPMON_QLOG_STATUS_START);
						pfree(plan);
					}
					break;
				default:
					break;
			}
			pfree(gpmonPacket);
		}
		else if (Gp_role == GP_ROLE_EXECUTE)
		{
                        gpmonPacket = gpmon_qexec_packet_init();
                        switch (status)
                        {
                                case METRICS_QUERY_START:
                                case METRICS_PLAN_NODE_EXECUTING:
                                        gpmon_send(gpmonPacket);
                                        break;
                                default:
                                        break;
                        }
                        pfree(gpmonPacket);
		}
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
	if (prev_query_info_collect_hook)
		(*prev_query_info_collect_hook) (status, qd);
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
	time_t t;
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

	t = time(NULL);

	if (t == (time_t) -1)
	{
		elog(PANIC, "[perfmon] cannot generate global transaction id");
	}
	init_tmid = t;
	gpmon_init();
	init_pg_query_state();
}

void
_PG_fini(void)
{}

static
char* get_plan(QueryDesc *queryDesc)
{
	char *plan;
	ExplainState *es = NewExplainState();

	es->analyze = false;
	es->verbose = true;
	es->buffers = true;
	es->wal = true;
	es->timing = true;
	es->summary = es->analyze;
	es->format = EXPLAIN_FORMAT_JSON;
	es->settings = true;
	ExplainBeginOutput(es);
	ExplainQueryText(es, queryDesc);
	ExplainPrintPlan(es, queryDesc);
	ExplainEndOutput(es);

	/* Remove last line break */
	if (es->str->len > 0 && es->str->data[es->str->len - 1] == '\n')
		es->str->data[--es->str->len] = '\0';

	/* Fix JSON to output an object */
	es->str->data[0] = '{';
	es->str->data[es->str->len - 1] = '}';
	plan = es->str->data;
	pfree(es);
	return plan;
}

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

/*-------------------------------------------------------------------------
 *
 * gp_inject_fault.c
 *
 * IDENTIFICATION
 *          gpcontrib/gp_inject_fault/gp_inject_fault.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/genam.h"
#include "access/xlog.h"
#include "catalog/indexing.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbutil.h"
#include "libpq/ifaddr.h"
#include "libpq-fe.h"
#include "libpq-int.h"
#include "postmaster/fts.h"
#include "utils/builtins.h"
#include "utils/faultinjector.h"
#include "utils/fmgroids.h"

#include "catalog/gp_indexing.h"

PG_MODULE_MAGIC;

static const char *const faultInjectModuleName = "$libdir/gp_inject_fault";

extern Datum gp_inject_fault(PG_FUNCTION_ARGS);
extern Datum insert_noop_xlog_record(PG_FUNCTION_ARGS);
void _PG_init(void);

static void
fts_with_panic_warning(FaultInjectorEntry_s faultEntry)
{
	/*
	 * Here, if the FTS probe is skipped on the QE or QD, then return directly.
	 * On the contrary, the prompt prints a WARNING.
	 */
#ifdef USE_INTERNAL_FTS
	if (!FtsIsActive())
		return;
#endif
	if (faultEntry.faultInjectorType == FaultInjectorTypePanic)
		ereport(WARNING, (
			errmsg("consider disabling FTS probes while injecting a panic."),
				errhint("Inject an infinite 'skip' into the 'fts_probe' fault to disable FTS probing.")));
}
/*
 * Intercept log messages.
 * Define a method here to override default notice handling routines.
 * Because the default notice handling routines does not support standard output for log levels below error.
 */
static void 
print_log_handler(void *arg, const PGresult *pgresult)
{
	ErrorData	*edata = NULL;
	char		*fdlevel = "";

	if(pgresult == NULL || arg == NULL)
		return;
	const char *seddbdesc = (char *) arg;
	PGresult *result = (PGresult *) pgresult;
	pqSaveMessageField(result, PG_DIAG_GP_PROCESS_TAG, seddbdesc);
	edata = cdbdisp_get_PQerror(result);
	if (edata == NULL)
	{
		elog(ERROR, "cdbdisp_get_PQerror execution failed");
		return;
	}
	fdlevel = PQresultErrorField(pgresult, PG_DIAG_SEVERITY);
	if (strcmp(fdlevel, "DEBUG") == 0)
		edata->elevel = DEBUG1;
	else if (strcmp(fdlevel, "DEBUG1") == 0)
		edata->elevel = DEBUG1;
	else if (strcmp(fdlevel, "DEBUG2") == 0)
		edata->elevel = DEBUG2;
	else if (strcmp(fdlevel, "DEBUG3") == 0)
		edata->elevel = DEBUG3;
	else if (strcmp(fdlevel, "DEBUG4") == 0)
		edata->elevel = DEBUG4;
	else if (strcmp(fdlevel, "DEBUG5") == 0)
		edata->elevel = DEBUG5;
	else if (strcmp(fdlevel, "LOG") == 0)
		edata->elevel = LOG;
	else if (strcmp(fdlevel, "INFO") == 0)
		edata->elevel = INFO;
	else if (strcmp(fdlevel, "NOTICE") == 0)
		edata->elevel = NOTICE;
	else if (strcmp(fdlevel, "WARNING") == 0)
		edata->elevel = WARNING;
	else
		elog(ERROR, "unrecognized log severity: \"%s\"", fdlevel);

	ThrowErrorData(edata);
}
/*
 * Register warning when extension is loaded.
 *
 */
void
_PG_init(void)
{
	InjectFaultInit();

	MemoryContext oldContext = MemoryContextSwitchTo(TopMemoryContext);
	register_fault_injection_warning(fts_with_panic_warning);
	MemoryContextSwitchTo(oldContext);
}

static void
get_segment_configuration(int dbid, char **hostname, int *port, int *content)
{
#ifndef USE_INTERNAL_FTS
	GpSegConfigEntry * config = dbid_get_dbinfo(dbid);
	*hostname = config->hostname;
	*port = config->port;
	*content = config->segindex;
#else
	HeapTuple	tuple;
	Relation    configrel;
	ScanKeyData scankey[1];
	SysScanDesc scan;
	Datum       attr;
	bool        isNull;
	Oid			warehouseid = InvalidOid;
	bool		find_config = false;

	configrel = table_open(GpSegmentConfigRelationId, AccessShareLock);
	ScanKeyInit(&scankey[0],
				Anum_gp_segment_configuration_dbid,
				BTEqualStrategyNumber, F_INT2EQ,
				Int16GetDatum(dbid));
	scan = systable_beginscan(configrel, GpSegmentConfigDbidWarehouseIndexId, true,
							  NULL, 1, scankey);

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		attr = heap_getattr(tuple, Anum_gp_segment_configuration_warehouseid,
							RelationGetDescr(configrel), &isNull);
		Assert(!isNull);
		warehouseid = DatumGetObjectId(attr);

		if (!OidIsValid(warehouseid) || warehouseid == GetCurrentWarehouseId())
		{
			attr = heap_getattr(tuple, Anum_gp_segment_configuration_hostname,
								RelationGetDescr(configrel), &isNull);
			Assert(!isNull);
			*hostname = TextDatumGetCString(attr);

			attr = heap_getattr(tuple, Anum_gp_segment_configuration_port,
								RelationGetDescr(configrel), &isNull);
			Assert(!isNull);
			*port = DatumGetInt32(attr);

			attr = heap_getattr(tuple, Anum_gp_segment_configuration_content,
								RelationGetDescr(configrel), &isNull);
			Assert(!isNull);
			*content = DatumGetInt32(attr);

			find_config = true;
			break;
		}
	}
	if (!find_config)
		elog(ERROR, "dbid %d not found", dbid);

	systable_endscan(scan);
	table_close(configrel, NoLock);
#endif
}

PG_FUNCTION_INFO_V1(gp_inject_fault);
Datum
gp_inject_fault(PG_FUNCTION_ARGS)
{
	char	*faultName = TextDatumGetCString(PG_GETARG_DATUM(0));
	char	*type = TextDatumGetCString(PG_GETARG_DATUM(1));
	char	*ddlStatement = TextDatumGetCString(PG_GETARG_DATUM(2));
	char	*databaseName = TextDatumGetCString(PG_GETARG_DATUM(3));
	char	*tableName = TextDatumGetCString(PG_GETARG_DATUM(4));
	int		startOccurrence = PG_GETARG_INT32(5);
	int		endOccurrence = PG_GETARG_INT32(6);
	int		extraArg = PG_GETARG_INT32(7);
	int		dbid = PG_GETARG_INT32(8);
	/*
	 * gpSessionid: -1 means the fault could be triggered by any process,
	 * others mean the fault could only be triggered by the specific session.
	 */
	int		gpSessionid = PG_GETARG_INT32(9);
	char	*hostname;
	int		port;
	char	*response;
	int		content;

	/* Fast path if injecting fault in our postmaster. */
	if (GpIdentity.dbid == dbid)
	{
		response = InjectFault(
			faultName, type, ddlStatement, databaseName,
			tableName, startOccurrence, endOccurrence, extraArg, gpSessionid);
		if (!response)
			elog(ERROR, "failed to inject fault locally (dbid %d)", dbid);
		if (strncmp(response, "Success:",  strlen("Success:")) != 0)
			elog(ERROR, "%s", response);
	}
	else
	{
		char conninfo[1024];
		char msg[1024];
		char seddbdesc[1024];
		PGconn *conn;
		PGresult *res;

		get_segment_configuration(dbid, &hostname, &port, &content);
		snprintf(conninfo, 1024, "host=%s port=%d %s=%s",
				 hostname, port, GPCONN_TYPE, GPCONN_TYPE_FAULT);
		conn = PQconnectdb(conninfo);
		if (PQstatus(conn) != CONNECTION_OK)
			elog(ERROR, "connection to dbid %d %s:%d failed", dbid, hostname, port);

		/*
		 * Override default notice handling routines.
		 * By PQsetNoticeReceiver to register interceptors.
		 */
		snprintf(seddbdesc, sizeof(seddbdesc), "seg%d %s:%d pid=%d",
				 content, conn->connip, port, conn->be_pid);
		PQsetNoticeReceiver(conn, print_log_handler, seddbdesc);

		/*
		 * If ddl, dbname or tablename is not specified, send '#' instead.
		 * This allows sscanf to be used on the receiving end to parse the
		 * message.
		 */
		if (!ddlStatement || ddlStatement[0] == '\0')
			ddlStatement = "#";
		if (!databaseName || databaseName[0] == '\0')
			databaseName = "#";
		if (!tableName || tableName[0] == '\0')
			tableName = "#";

#ifdef USE_INTERNAL_FTS
		snprintf(msg, 1024, "faultname=%s type=%s ddl=%s db=%s table=%s modulename=%s "
				 "start=%d end=%d extra=%d sid=%d skipftsprobe=%d ",
				 faultName, type,
				 ddlStatement,
				 databaseName,
				 tableName,
				 faultInjectModuleName,
				 startOccurrence,
				 endOccurrence,
				 extraArg,
				 gpSessionid,
				 !FtsIsActive());
#else
		snprintf(msg, 1024, "faultname=%s type=%s ddl=%s db=%s table=%s modulename=%s "
				 "start=%d end=%d extra=%d sid=%d ",
				 faultName, type,
				 ddlStatement,
				 databaseName,
				 tableName,
				 faultInjectModuleName,
				 startOccurrence,
				 endOccurrence,
				 extraArg,
				 gpSessionid);
#endif
		res = PQexec(conn, msg);
		if (PQresultStatus(res) != PGRES_TUPLES_OK)
			elog(ERROR, "failed to inject fault: %s", PQerrorMessage(conn));

		if (PQntuples(res) != 1)
		{
			PQclear(res);
			PQfinish(conn);
			elog(ERROR, "invalid response from %s:%d", hostname, port);
		}

		response = pstrdup(PQgetvalue(res, 0, Anum_fault_message_response_status));
		PQclear(res);
		PQfinish(conn);
		if (strncmp(response, "Success:",  strlen("Success:")) != 0)
			elog(ERROR, "%s", response);
	}
	PG_RETURN_TEXT_P(cstring_to_text(response));
}

PG_FUNCTION_INFO_V1(insert_noop_xlog_record);
Datum
insert_noop_xlog_record(PG_FUNCTION_ARGS)
{
	char *no_op_string = "no-op";

	/* Xlog records of length = 0 are disallowed and cause a panic. Thus,
	 * supplying a dummy non-zero length
	 */
	XLogBeginInsert();

	XLogRegisterData(no_op_string, strlen(no_op_string));

	XLogFlush(XLogInsert(RM_XLOG_ID, XLOG_NOOP));

	PG_RETURN_VOID();
}

/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * ic_udp2.c
 *
 * IDENTIFICATION
 *    contrib/udp2/ic_udp2.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/cdbdisp.h"
#include "cdb/cdbgang.h"
#include "cdb/cdbmotion.h"
#include "cdb/cdbvars.h"
#include "cdb/tupchunklist.h"
#include "postmaster/bgworker.h"
#include "postmaster/postmaster.h"
#include "storage/latch.h"
#include "storage/pmsignal.h"
#include "tcop/tcopprot.h"
#include "utils/wait_event.h"
#include "utils/memutils.h"

/* local interconnect */
#include "ic_udp2.h"

/* from ic_common packeage */
#include "ic_types.h"
#include "udp2/ic_udp2.h"


#define HandleLastError() \
do { \
	ICError *error = GetLastError(); \
	Assert(error); \
	if (error->level == LEVEL_ERROR) \
	{ \
		Assert(error->msg); \
		elog(ERROR, "%s", error->msg); \
	} \
	if (error->level == LEVEL_FATAL) \
	{ \
		Assert(error->msg); \
		elog(FATAL, "%s", error->msg); \
	} \
} while (0)

#define ML_CHECK_FOR_INTERRUPTS(teardownActive) \
		do {if (!teardownActive && InterruptPending) CHECK_FOR_INTERRUPTS(); } while (0)

/*
 * Resource manager
 */
typedef void (*TeardownInterconnectCallBack)(ChunkTransportState *transportStates, bool hasErrors);
typedef struct interconnect_handle_t
{
	ChunkTransportState		*interconnect_context; /* Interconnect state */

	// callback for interconnect been abort
	TeardownInterconnectCallBack teardown_cb;

	ResourceOwner owner;	/* owner of this handle */
	struct interconnect_handle_t *next;
	struct interconnect_handle_t *prev;
} interconnect_handle_t;

static interconnect_handle_t * open_interconnect_handles;
static bool interconnect_resowner_callback_registered;

static void destroy_interconnect_handle(interconnect_handle_t *h);
static interconnect_handle_t * allocate_interconnect_handle(TeardownInterconnectCallBack callback);
static interconnect_handle_t * find_interconnect_handle(ChunkTransportState *icContext);


static void SetupGlobalMotionLayerIPCParam(GlobalMotionLayerIPCParam *param);
static void SetupSessionMotionLayerIPCParam(SessionMotionLayerIPCParam *param);
static bool CheckPostmasterIsAlive(void);
static void CheckCancelOnQD(ICChunkTransportState *pTransportStates);
static void CheckInterrupts(int teardownActive);
static void SimpleFaultInjector(const char *faultname);
static void *CreateOpaqueData(void);
static void DestroyOpaqueData(void **opaque);
static ICSliceTable* ConvertToICSliceTable(SliceTable *tbl);
static TupleChunkListItem ConvertToTupleChunk(ChunkTransportState *transportStates, DataBlock *data);
static ChunkTransportState *CreateChunkTransportState(EState *estate, ICChunkTransportState *udp2_state);


int
GetMaxTupleChunkSizeUDP2(void)
{
	int header_size = UDP2_GetICHeaderSizeUDP();
	return Gp_max_packet_size - header_size - TUPLE_CHUNK_HEADER_SIZE;
}

int32
GetListenPortUDP2(void)
{
	return UDP2_GetListenPortUDP();
}

void
InitMotionIPCLayerUDP2(void)
{
	GlobalMotionLayerIPCParam param;
	SetupGlobalMotionLayerIPCParam(&param);

	param.checkPostmasterIsAliveCallback = CheckPostmasterIsAlive;
	param.checkInterruptsCallback        = CheckInterrupts;
	param.simpleFaultInjectorCallback    = SimpleFaultInjector;

	param.createOpaqueDataCallback       = CreateOpaqueData;
	param.destroyOpaqueDataCallback      = DestroyOpaqueData;

	param.checkCancelOnQDCallback        = CheckCancelOnQD;

	ResetLastError();
	UDP2_InitUDPIFC(&param);
	HandleLastError();
}

void
CleanUpMotionLayerIPCUDP2(void)
{
	if (gp_log_interconnect >= GPVARS_VERBOSITY_DEBUG)
		elog(DEBUG3, "Cleaning Up Motion Layer IPC...");

	ResetLastError();
	UDP2_CleanUpUDPIFC();
	HandleLastError();
}

void
WaitInterconnectQuitUDPIFC2(void)
{
	ResetLastError();
	UDP2_WaitQuitUDPIFC();
	HandleLastError();
}

void
SetupInterconnectUDP2(EState *estate)
{
	if (estate->interconnect_context)
		elog(ERROR, "SetupInterconnectUDP: already initialized.");

	if (!estate->es_sliceTable)
		elog(ERROR, "SetupInterconnectUDP: no slice table ?");

	SessionMotionLayerIPCParam param;
	SetupSessionMotionLayerIPCParam(&param);

	interconnect_handle_t *h;
	h = allocate_interconnect_handle(TeardownInterconnectUDP2);

	ICSliceTable *tbl = ConvertToICSliceTable(estate->es_sliceTable);

	ResetLastError();
	ICChunkTransportState *udp2_state = UDP2_SetupUDP(tbl, &param);
	HandleLastError();

	Assert(udp2_state);
	ChunkTransportState *state = CreateChunkTransportState(estate, udp2_state);
	h->interconnect_context = state;

	h->interconnect_context->estate = estate;
	estate->interconnect_context = h->interconnect_context;
	estate->es_interconnect_is_setup = true;

	/* Check if any of the QEs has already finished with error */
	if (Gp_role == GP_ROLE_DISPATCH)
	{
		ChunkTransportState *pTransportStates = h->interconnect_context;

		Assert(pTransportStates);
		Assert(pTransportStates->estate);

		if (cdbdisp_checkForCancel(pTransportStates->estate->dispatcherState))
		{
			ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					errmsg(CDB_MOTION_LOST_CONTACT_STRING)));
			/* not reached */
		}
	}
}

void
TeardownInterconnectUDP2(ChunkTransportState *transportStates, bool hasErrors)
{
	if (transportStates == NULL || transportStates->sliceTable == NULL)
	{
		elog(LOG, "TeardownUDPIFCInterconnect: missing slice table.");
		return;
	}

	/* TODO: should pass interconnect_handle_t as arg? */
	interconnect_handle_t *h = find_interconnect_handle(transportStates);

	ResetLastError();
	HOLD_INTERRUPTS();

	ICChunkTransportState *udp2_state =
		(ICChunkTransportState *)transportStates->implement_state;
	UDP2_TeardownUDP(udp2_state, hasErrors);

	transportStates->activated = false;
	transportStates->sliceTable = NULL;

	RESUME_INTERRUPTS();
	HandleLastError();

	if (h != NULL)
		destroy_interconnect_handle(h);
}

bool
SendTupleChunkToAMSUDP2(ChunkTransportState *transportStates,
						int16 motNodeID,
						int16 targetRoute,
						TupleChunkListItem tcItem)
{
	if (!transportStates)
	{
		elog(FATAL, "SendTupleChunkToAMS: no transport-states.");
	}

	if (!transportStates->activated)
	{
		elog(FATAL, "SendTupleChunkToAMS: transport states inactive");
	}

	/* check em' */
	ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG3, "sendtuplechunktoams: calling get_transport_state"
		 "w/transportStates %p transportState->size %d motnodeid %d route %d",
		 transportStates, transportStates->size, motNodeID, targetRoute);
#endif

	/* get the number of TupleChunkListItem */
	int num = 0;
	TupleChunkListItem item = tcItem;
	while (item)
	{
		num++;
		item = item->p_next;
	}

	/* convert to DataBlock */
	DataBlock *pblocks = (DataBlock *)palloc0(sizeof(DataBlock) * num);
	item = tcItem;
	for (int i = 0; i < num; ++i)
	{
		pblocks[i].pos = item->chunk_data;
		pblocks[i].len = item->chunk_length;

		item = item->p_next;
	}

	bool broadcast = (targetRoute == BROADCAST_SEGIDX);

	ResetLastError();
	ICChunkTransportState *udp2_state =
		(ICChunkTransportState *)transportStates->implement_state;
	bool rs = UDP2_SendData(udp2_state,
						motNodeID,
						targetRoute,
						pblocks,
						num,
						broadcast);
	HandleLastError();

	return rs;
}

void
SendEOSUDPIFC2(ChunkTransportState *transportStates,
			   int motNodeID,
			   TupleChunkListItem tcItem)
{
	if (!transportStates)
	{
		elog(FATAL, "SendEOSUDPIFC: missing interconnect context.");
	}
	else if (!transportStates->activated && !transportStates->teardownActive)
	{
		elog(FATAL, "SendEOSUDPIFC: context and teardown inactive.");
	}

	/* check em' */
	ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

	DataBlock db;
	db.pos = tcItem->chunk_data;
	db.len = tcItem->chunk_length;

	ResetLastError();
	ICChunkTransportState *udp2_state =
		(ICChunkTransportState *)transportStates->implement_state;
	UDP2_SendEOS(udp2_state, motNodeID, &db);
	HandleLastError();
}

void
SendStopMessageUDPIFC2(ChunkTransportState *transportStates, int16 motNodeID)
{
	if (!transportStates->activated)
		return;

	ResetLastError();
	ICChunkTransportState *udp2_state =
		(ICChunkTransportState *)transportStates->implement_state;
	UDP2_SendStop(udp2_state, motNodeID);
	HandleLastError();
}

TupleChunkListItem
RecvTupleChunkFromAnyUDPIFC2(ChunkTransportState *transportStates,
							 int16 motNodeID,
							 int16 *srcRoute)
{
	if (!transportStates)
	{
		elog(FATAL, "RecvTupleChunkFromAnyUDPIFC: missing context");
	}
	else if (!transportStates->activated)
	{
		elog(FATAL, "RecvTupleChunkFromAnyUDPIFC: interconnect context not active!");
	}

	DataBlock db = {NULL, 0};

	ResetLastError();
	ICChunkTransportState *udp2_state =
		(ICChunkTransportState *)transportStates->implement_state;
	UDP2_RecvAny(udp2_state, motNodeID, srcRoute, NULL, &db);
	HandleLastError();

	if (db.pos == NULL)
		return NULL;

	return ConvertToTupleChunk(transportStates, &db);
}

TupleChunkListItem
RecvTupleChunkFromUDPIFC2(ChunkTransportState *transportStates,
						  int16 motNodeID,
						  int16 srcRoute)
{
	if (!transportStates)
	{
		elog(FATAL, "RecvTupleChunkFromUDPIFC: missing context");
	}
	else if (!transportStates->activated)
	{
		elog(FATAL, "RecvTupleChunkFromUDPIFC: interconnect context not active!");
	}

#ifdef AMS_VERBOSE_LOGGING
	elog(LOG, "RecvTupleChunkFromUDPIFC().");
#endif

	/* check em' */
	ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);

#ifdef AMS_VERBOSE_LOGGING
	elog(DEBUG5, "RecvTupleChunkFromUDPIFC(motNodID=%d, srcRoute=%d)", motNodeID, srcRoute);
#endif

	DataBlock db = {NULL, 0};

	ResetLastError();
	ICChunkTransportState *udp2_state =
		(ICChunkTransportState *)transportStates->implement_state;
	UDP2_RecvRoute(udp2_state, motNodeID, srcRoute, NULL, &db);
	HandleLastError();

	if (db.pos == NULL)
		return NULL;

	return ConvertToTupleChunk(transportStates, &db);
}

void
MlPutRxBufferUDPIFC2(ChunkTransportState *transportStates, int motNodeID, int route)
{
	ResetLastError();
	ICChunkTransportState *udp2_state =
		(ICChunkTransportState *)transportStates->implement_state;
	UDP2_ReleaseAndAck(udp2_state, motNodeID, route);
	HandleLastError();
}

void
DeregisterReadInterestUDP2(ChunkTransportState *transportStates,
						   int motNodeID,
						   int srcRoute,
						   const char *reason)
{
	if (!transportStates)
	{
		elog(FATAL, "DeregisterReadInterestUDP: no transport states");
	}

	if (!transportStates->activated)
		return;

	ResetLastError();
	ICChunkTransportState *udp2_state =
		(ICChunkTransportState *)transportStates->implement_state;
	UDP2_DeactiveRoute(udp2_state, motNodeID, srcRoute, reason);
	HandleLastError();
}

uint32
GetActiveMotionConnsUDPIFC2(void)
{
	return UDP2_GetActiveConns();
}

void
GetTransportDirectBufferUDPIFC2(ChunkTransportState * transportStates,
								int16 motNodeID,
								int16 targetRoute,
								struct directTransportBuffer *b)
{
	if (!transportStates)
	{
		elog(FATAL, "GetTransportDirectBuffer: no transport states");
	}
	else if (!transportStates->activated)
	{
		elog(FATAL, "GetTransportDirectBuffer: inactive transport states");
	}
	else if (targetRoute == BROADCAST_SEGIDX)
	{
		elog(FATAL, "GetTransportDirectBuffer: can't direct-transport to broadcast");
	}

	BufferBlock buf = {NULL, 0};

	ResetLastError();
	ICChunkTransportState *udp2_state =
		(ICChunkTransportState *)transportStates->implement_state;
	UDP2_GetFreeSpace(udp2_state, motNodeID, targetRoute, &buf);
	HandleLastError();

	b->pri    = buf.pos;
	b->prilen = buf.len;
}

void
PutTransportDirectBufferUDPIFC2(ChunkTransportState * transportStates,
								int16 motNodeID,
								int16 targetRoute,
								int length)
{
	if (!transportStates)
	{
		elog(FATAL, "PutTransportDirectBuffer: no transport states");
	}
	else if (!transportStates->activated)
	{
		elog(FATAL, "PutTransportDirectBuffer: inactive transport states");
	}
	else if (targetRoute == BROADCAST_SEGIDX)
	{
		elog(FATAL, "PutTransportDirectBuffer: can't direct-transport to broadcast");
	}

	ResetLastError();
	ICChunkTransportState *udp2_state =
		(ICChunkTransportState *)transportStates->implement_state;
	UDP2_ReduceFreeSpace(udp2_state, motNodeID, targetRoute, length);
	HandleLastError();
}

TupleRemapper*
GetMotionConnTupleRemapperUDPIFC2(ChunkTransportState * transportStates,
								  int16 motNodeID,
								  int16 targetRoute)
{
	TupleRemapper *remapper = NULL;

	if (!transportStates)
	{
		elog(FATAL, "GetMotionConnTupleRemapper: no transport states");
	}

	if (!transportStates->activated)
	{
		elog(FATAL, "GetMotionConnTupleRemapper: inactive transport states");
	}

	ResetLastError();
	ICChunkTransportState *udp2_state =
		(ICChunkTransportState *)transportStates->implement_state;
	remapper = (TupleRemapper *)UDP2_GetOpaqueDataInConn(udp2_state, motNodeID, targetRoute);
	HandleLastError();

	return remapper;
}

int32*
GetMotionSentRecordTypmodUDPIFC2(ChunkTransportState * transportStates,
								 int16 motNodeID,
								 int16 targetRoute)
{
	int32 *rs = NULL;

	if (!transportStates)
	{
		elog(FATAL, "GetMotionConnTupleRemapper: no transport states");
	}

	if (!transportStates->activated)
	{
		elog(FATAL, "GetMotionConnTupleRemapper: inactive transport states");
	}

	targetRoute = targetRoute == BROADCAST_SEGIDX ? 0 : targetRoute;

	ResetLastError();
	ICChunkTransportState *udp2_state =
		(ICChunkTransportState *)transportStates->implement_state;
	rs = UDP2_GetSentRecordTypmodInConn(udp2_state, motNodeID, targetRoute);
	HandleLastError();

	return rs;
}

static void
SetupGlobalMotionLayerIPCParam(GlobalMotionLayerIPCParam *param)
{
	if (param == NULL)
		return;

	param->interconnect_address = interconnect_address;
	param->Gp_role              = Gp_role;
	param->ic_htab_size         = ic_htab_size;
	param->segment_number       = getgpsegmentCount();
	param->MyProcPid            = MyProcPid;
	param->dbid                 = GpIdentity.dbid;
	param->segindex             = GpIdentity.segindex;
	param->MyProcPort           = MyProcPort != NULL;
	param->myprocport_sock      = param->MyProcPort ? MyProcPort->sock : -1;
	param->Gp_max_packet_size   = Gp_max_packet_size;
	param->Gp_udp_bufsize_k     = Gp_udp_bufsize_k;
	param->Gp_interconnect_address_type = Gp_interconnect_address_type;
}

static bool
CheckPostmasterIsAlive(void)
{
	return PostmasterIsAlive();
}

static void
CheckCancelOnQD(ICChunkTransportState *state)
{
	int				nevent = 0, nrevent = 0;
	int				*waitFds = NULL;
	WaitEvent		*rEvents = NULL;
	WaitEventSet	*waitset = NULL;
	ChunkTransportState *pTransportStates = NULL;

	if (Gp_role != GP_ROLE_DISPATCH)
		return;

	pTransportStates = (ChunkTransportState *)state->clientState;

	/* get all wait sock fds */
	waitFds = cdbdisp_getWaitSocketFds(pTransportStates->estate->dispatcherState, &nevent);
	if (waitFds == NULL)
		return;

	/* init WaitEventSet */
	waitset = CreateWaitEventSet(CurrentMemoryContext, nevent);
	rEvents = palloc(nevent * sizeof(WaitEvent)); /* returned events */
	for (int i = 0; i < nevent; ++i)
		AddWaitEventToSet(waitset, WL_SOCKET_READABLE, waitFds[i], NULL, NULL);

	/* wait for event from QE */
	nrevent = WaitEventSetWait(waitset, 0, rEvents, nevent, WAIT_EVENT_INTERCONNECT);

	/* check to see if the dispatcher should cancel */
	for (int i = 0; i < nrevent; i++)
	{
		if (rEvents[i].events & WL_SOCKET_READABLE)
		{
			/* event happened on wait fds, need to check cancel */
			Assert(pTransportStates);
			Assert(pTransportStates->estate);

			if (cdbdisp_checkForCancel(pTransportStates->estate->dispatcherState))
			{
				ereport(ERROR, (errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
							errmsg(CDB_MOTION_LOST_CONTACT_STRING)));
				/* not reached */
			}
			break;
		}
	}

	if (waitset)
		FreeWaitEventSet((WaitEventSet *)waitset);
	if (rEvents)
		pfree(rEvents);
}

static void
CheckInterrupts(int teardownActive)
{
	ML_CHECK_FOR_INTERRUPTS(teardownActive);
}

static void
SimpleFaultInjector(const char *faultname)
{
	SIMPLE_FAULT_INJECTOR((faultname));
}

static void *
CreateOpaqueData(void)
{
	return CreateTupleRemapper();
}

static void
DestroyOpaqueData(void **opaque)
{
	if (*opaque == NULL)
		return;
	*opaque = NULL;
}

static void
SetupSessionMotionLayerIPCParam(SessionMotionLayerIPCParam *param)
{
	if (param == NULL)
		return;

	TransactionId localTransId = 0;
	TransactionId subtransId = 0;

	param->Gp_interconnect_queue_depth                = Gp_interconnect_queue_depth;
	param->Gp_interconnect_snd_queue_depth            = Gp_interconnect_snd_queue_depth;
	param->Gp_interconnect_timer_period               = Gp_interconnect_timer_period;
	param->Gp_interconnect_timer_checking_period      = Gp_interconnect_timer_checking_period;
	param->Gp_interconnect_default_rtt                = Gp_interconnect_default_rtt;
	param->Gp_interconnect_min_rto                    = Gp_interconnect_min_rto;
	param->Gp_interconnect_transmit_timeout           = Gp_interconnect_transmit_timeout;
	param->Gp_interconnect_min_retries_before_timeout = Gp_interconnect_min_retries_before_timeout;
	param->Gp_interconnect_debug_retry_interval       = Gp_interconnect_debug_retry_interval;
	param->gp_interconnect_full_crc                   = gp_interconnect_full_crc;
	param->gp_interconnect_aggressive_retry           = gp_interconnect_aggressive_retry;
	param->gp_interconnect_cache_future_packets       = gp_interconnect_cache_future_packets;
	param->gp_interconnect_log_stats                  = gp_interconnect_log_stats;
	param->interconnect_setup_timeout                 = interconnect_setup_timeout;
	param->gp_log_interconnect                        = gp_log_interconnect;
	param->gp_session_id                              = gp_session_id;
	param->Gp_interconnect_fc_method                  = Gp_interconnect_fc_method;
	param->gp_command_count                           = gp_command_count;
	param->gp_interconnect_id                         = gp_interconnect_id;
	param->log_min_messages                           = log_min_messages;
	GetAllTransactionXids(&param->distTransId, &localTransId, &subtransId);

#ifdef USE_ASSERT_CHECKING
	param->gp_udpic_dropseg              = gp_udpic_dropseg;
	param->gp_udpic_dropacks_percent     = gp_udpic_dropacks_percent;
	param->gp_udpic_dropxmit_percent     = gp_udpic_dropxmit_percent;
	param->gp_udpic_fault_inject_percent = gp_udpic_fault_inject_percent;
	param->gp_udpic_fault_inject_bitmap  = gp_udpic_fault_inject_bitmap;
	param->gp_udpic_network_disable_ipv6 = gp_udpic_network_disable_ipv6;
#endif
}

ICSliceTable*
ConvertToICSliceTable(SliceTable *tbl)
{
	ICSliceTable *ic_tbl = (ICSliceTable *)malloc(sizeof(ICSliceTable));
	memset(ic_tbl, 0, sizeof(ICSliceTable));

	ic_tbl->localSlice = tbl->localSlice;
	ic_tbl->ic_instance_id = tbl->ic_instance_id;

	ic_tbl->numSlices = tbl->numSlices;
	ic_tbl->slices = (ICExecSlice *)malloc(sizeof(ICExecSlice) * ic_tbl->numSlices);
	memset(ic_tbl->slices, 0, sizeof(ICExecSlice) * ic_tbl->numSlices);

	for (int i = 0; i < ic_tbl->numSlices; ++i)
	{
		ExecSlice *slice      = tbl->slices + i;
		ICExecSlice *ic_slice = ic_tbl->slices + i;

		ic_slice->sliceIndex = slice->sliceIndex;
		ic_slice->parentIndex= slice->parentIndex;
		ic_slice->numSegments = list_length(slice->segments);

		ic_slice->numChildren = list_length(slice->children);
		ic_slice->children = malloc(sizeof(int) * ic_slice->numChildren);
		memset(ic_slice->children, 0, sizeof(int) * ic_slice->numChildren);

		for (int i = 0; i < ic_slice->numChildren; ++i)
			ic_slice->children[i] = list_nth_int(slice->children, i);

		ic_slice->numPrimaryProcesses = list_length(slice->primaryProcesses);
		ic_slice->primaryProcesses = malloc(sizeof(ICCdbProcess) * ic_slice->numPrimaryProcesses);
		memset(ic_slice->primaryProcesses, 0, sizeof(ICCdbProcess) * ic_slice->numPrimaryProcesses);

		for (int i = 0; i < ic_slice->numPrimaryProcesses; ++i)
		{
			CdbProcess *process = (CdbProcess *)list_nth(slice->primaryProcesses, i);
			if (!process)
				continue;

			ICCdbProcess *ic_process = ic_slice->primaryProcesses + i;

			ic_process->valid        = true;
			ic_process->listenerAddr = process->listenerAddr;
			ic_process->listenerPort = process->listenerPort;
			ic_process->pid          = process->pid;
			ic_process->contentid    = process->contentid;
			ic_process->dbid         = process->dbid;
		}
	}

	return ic_tbl;
}

/*
 * msg MUST BE conn->msgPos, msg_size should be conn->msgSize - sizeof(icpkthdr)
 * +----------------+-----------+--------------+------------+---+--------------+------------+
 * | tcp/udp header | ic header | chunk header | chunk data |...| chunk header | chunk data |
 * +----------------+-----------+--------------+------------+---+--------------+------------+
 *                              |<-----#1 tuple chunk ----->|...|<-----#n tuple chunk ----->|
 *                  |<------------------------- Gp_max_packet_size ------------------------>|
 *
 *                              |<------------------------ msg_size ----------------------->|
 *                             msg
 */
TupleChunkListItem
ConvertToTupleChunk(ChunkTransportState *transportStates, DataBlock *data)
{
	TupleChunkListItem tcItem;
	TupleChunkListItem firstTcItem = NULL;
	TupleChunkListItem lastTcItem = NULL;

	uint32      tcSize;
	int         bytesProcessed = 0;

	while (bytesProcessed != data->len)
	{
		if (data->len - bytesProcessed < TUPLE_CHUNK_HEADER_SIZE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					 errmsg("interconnect error parsing message: insufficient data received"),
					 errdetail("conn->msgSize %d bytesProcessed %d < chunk-header %d",
						 data->len, bytesProcessed, TUPLE_CHUNK_HEADER_SIZE)));
		}
		tcSize = TUPLE_CHUNK_HEADER_SIZE + (*(uint16 *) (data->pos + bytesProcessed));

		/* sanity check */
		if (tcSize > Gp_max_packet_size)
		{
			/*
			 * see MPP-720: it is possible that our message got messed up by a
			 * cancellation ?
			 */
			ML_CHECK_FOR_INTERRUPTS(transportStates->teardownActive);
			/*
			 * MPP-4010: add some extra debugging.
			 */
			if (lastTcItem != NULL)
				elog(LOG, "Interconnect error parsing message: last item length %d inplace %p", lastTcItem->chunk_length, lastTcItem->inplace);
			else
				elog(LOG, "Interconnect error parsing message: no last item");

			ereport(ERROR,
					(errcode(ERRCODE_GP_INTERCONNECTION_ERROR),
					errmsg("interconnect error parsing message"),
					errdetail("tcSize %d > max %d header %d processed %d/%d from %p",
							 tcSize, Gp_max_packet_size,
							 TUPLE_CHUNK_HEADER_SIZE, bytesProcessed,
							 data->len, data->pos)));
		}

		Assert(tcSize <= data->len);

		/*
		 * We store the data inplace, and handle any necessary copying later
		 * on
		 */
		tcItem = (TupleChunkListItem) palloc(sizeof(TupleChunkListItemData));
		tcItem->p_next = NULL;
		tcItem->chunk_length = tcSize;
		tcItem->inplace = (char *) (data->pos + bytesProcessed);

		bytesProcessed += tcSize;
		if (firstTcItem == NULL)
		{
			firstTcItem = tcItem;
			lastTcItem = tcItem;
		}
		else
		{
			lastTcItem->p_next = tcItem;
			lastTcItem = tcItem;
		}
	}

	return firstTcItem;
}

static ChunkTransportState *
CreateChunkTransportState(EState *estate, ICChunkTransportState *udp2_state)
{
	MemoryContext oldContext;
	ChunkTransportState *state;

	/* init ChunkTransportState */
	Assert(InterconnectContext != NULL);
	oldContext = MemoryContextSwitchTo(InterconnectContext);
	state = (ChunkTransportState *)palloc0(sizeof(ChunkTransportState));
	MemoryContextSwitchTo(oldContext);

	state->size            = 0;
	state->states          = NULL;
	state->activated       = udp2_state->activated;
	state->teardownActive  = udp2_state->teardownActive;
	state->aggressiveRetry = false;
	state->incompleteConns = NIL;
	state->sliceTable      = estate->es_sliceTable;
	state->sliceId         = estate->es_sliceTable->localSlice;
	state->estate          = estate;
	state->proxyContext    = NULL;

	state->networkTimeoutIsLogged  = false;

	/* save the reference each other */
	state->implement_state  = udp2_state;
	udp2_state->clientState = state;

	return state;
}

/*
 * must offer an empty proxy fucntion if ic-proxy is enabled(--enable-ic-proxy).
 */
int
ic_proxy_server_main(void)
{
	/* Establish signal handlers. */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* dry run */
	while (true)
	{
		pg_usleep(1000000);
		CHECK_FOR_INTERRUPTS();
	}

	return 0;
}

/*
 * Fucntions for Resource manager
 */
static void
destroy_interconnect_handle(interconnect_handle_t * h)
{
	h->interconnect_context = NULL;
	/* unlink from linked list first */
	if (h->prev)
		h->prev->next = h->next;
	else
		open_interconnect_handles = h->next;
	if (h->next)
		h->next->prev = h->prev;

	pfree(h);

	if (open_interconnect_handles == NULL)
		MemoryContextReset(InterconnectContext);
}

static void
cleanup_interconnect_handle(interconnect_handle_t * h)
{
	if (h->interconnect_context == NULL)
	{
		destroy_interconnect_handle(h);
		return;
	}
	h->teardown_cb(h->interconnect_context, true);
}

static void
interconnect_abort_callback(ResourceReleasePhase phase,
							bool isCommit,
							bool isTopLevel,
							void *arg)
{
	interconnect_handle_t *curr;
	interconnect_handle_t *next;

	if (phase != RESOURCE_RELEASE_AFTER_LOCKS)
		return;

	next = open_interconnect_handles;
	while (next)
	{
		curr = next;
		next = curr->next;

		if (curr->owner == CurrentResourceOwner)
		{
			if (isCommit)
				elog(WARNING, "interconnect reference leak: %p still referenced", curr);

			cleanup_interconnect_handle(curr);
		}
	}
}

static interconnect_handle_t *
allocate_interconnect_handle(TeardownInterconnectCallBack callback)
{
	interconnect_handle_t *h;

	if (InterconnectContext == NULL)
		InterconnectContext = AllocSetContextCreate(TopMemoryContext,
													"Interconnect Context",
													ALLOCSET_DEFAULT_MINSIZE,
													ALLOCSET_DEFAULT_INITSIZE,
													ALLOCSET_DEFAULT_MAXSIZE);

	h = MemoryContextAllocZero(InterconnectContext, sizeof(interconnect_handle_t));

	h->teardown_cb = callback;
	h->owner = CurrentResourceOwner;
	h->next = open_interconnect_handles;
	h->prev = NULL;
	if (open_interconnect_handles)
		open_interconnect_handles->prev = h;
	open_interconnect_handles = h;

	if (!interconnect_resowner_callback_registered)
	{
		RegisterResourceReleaseCallback(interconnect_abort_callback, NULL);
		interconnect_resowner_callback_registered = true;
	}
	return h;
}

static interconnect_handle_t *
find_interconnect_handle(ChunkTransportState * icContext)
{
	interconnect_handle_t *head = open_interconnect_handles;

	while (head != NULL)
	{
		if (head->interconnect_context == icContext)
			return head;
		head = head->next;
	}
	return NULL;
}
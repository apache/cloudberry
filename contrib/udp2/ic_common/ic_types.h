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
 * ic_types.h
 *
 * IDENTIFICATION
 *    contrib/udp2/ic_common/ic_types.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef IC_TYPES_H
#define IC_TYPES_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * data type
 */
typedef signed char       int8;   /* == 8 bits */
typedef signed short      int16;  /* == 16 bits */
typedef signed int        int32;  /* == 32 bits */
typedef unsigned char     uint8;  /* == 8 bits */
typedef unsigned short    uint16; /* == 16 bits */
typedef unsigned int      uint32; /* == 32 bits */
typedef long int          int64;
typedef unsigned long int uint64;

typedef uint64 DistributedTransactionId;

/* the lite version of CdbProcess */
typedef struct ICCdbProcess
{
	bool valid;
	char *listenerAddr; /* Interconnect listener IPv4 address, a C-string */
	int listenerPort;   /* Interconnect listener port */
	int pid;            /* Backend PID of the process. */
	int contentid;
	int dbid;

} ICCdbProcess;

/* the lite version of ExecSlice */
typedef struct ICExecSlice
{
	int			sliceIndex;
	int			parentIndex;

	int			numChildren;
	int			*children;

	int			numSegments;

	int			numPrimaryProcesses;
	ICCdbProcess *primaryProcesses;

} ICExecSlice;

/* the lite version of SliceTable */
typedef struct ICSliceTable
{
	int			localSlice;		/* Index of the slice to execute. */
	int			numSlices;
	ICExecSlice *slices;		/* Array of ICExecSlice, indexed by SliceIndex */
	uint32		ic_instance_id;

} ICSliceTable;

typedef struct ICChunkTransportState
{
	/* keeps track of if we've "activated" connections via SetupInterconnect(). */
	bool		activated;
	bool		teardownActive;

	/* slice table stuff. */
	struct ICSliceTable  *sliceTable;
	int			sliceId;
	int			icInstanceId; /* the same as sliceTable->ic_instance_id */

	/* whether we've logged when network timeout happens */
	bool		networkTimeoutIsLogged;

	/* save client's state */
	void		*clientState;

} ICChunkTransportState;

struct MemoryBlock
{
	unsigned char *pos;
	int           len;
};

typedef struct MemoryBlock BufferBlock;
typedef struct MemoryBlock DataBlock;

typedef int (*GetDataLenInPacket)(unsigned char *msg, int msg_size);

/*
 * GlobalMotionLayerIPCParam and SessionMotionLayerIPCParam
 */
typedef bool (*CheckPostmasterIsAliveCallback)(void);
typedef void  (*CheckInterruptsCallback)(int teardownActive);
typedef void  (*SimpleFaultInjectorCallback)(const char *faultname);
typedef void *(*CreateOpaqueDataWithConn)(void);
typedef void (*DestroyOpaqueDataInConn)(void **);
typedef void (*CheckCancelOnQDCallback)(ICChunkTransportState *pTransportStates);

typedef struct GlobalMotionLayerIPCParam
{
	char *interconnect_address; /* postmaster.h */
	int  Gp_role;               /* Gp_role */
	int  ic_htab_size;          /* cdbgang.h */
	int  segment_number;        /* getgpsegmentCount() */
	int  MyProcPid;             /* miscadmin.h */
	int  dbid;                  /* GpIdentity */
	int  segindex;              /* GpIdentity */
	bool MyProcPort;            /* miscadmin.h */
	int  myprocport_sock;       /* MyProcPort->sock */
	int  Gp_max_packet_size;    /* default: 8192 */
	int  Gp_udp_bufsize_k;      /* default: 0 */
	int  Gp_interconnect_address_type; /* default: INTERCONNECT_ADDRESS_TYPE_UNICAST_IC */

	CheckPostmasterIsAliveCallback checkPostmasterIsAliveCallback;
	CheckInterruptsCallback        checkInterruptsCallback;
	SimpleFaultInjectorCallback    simpleFaultInjectorCallback;

	CreateOpaqueDataWithConn       createOpaqueDataCallback;
	DestroyOpaqueDataInConn        destroyOpaqueDataCallback;

	CheckCancelOnQDCallback        checkCancelOnQDCallback;

} GlobalMotionLayerIPCParam;

typedef struct SessionMotionLayerIPCParam
{
	int  Gp_interconnect_queue_depth;                 /* default: 4 */
	int  Gp_interconnect_snd_queue_depth;             /* default: 2 */
	int  Gp_interconnect_cursor_ic_table_size;        /* default: 128 */
	int  Gp_interconnect_timer_period;                /* default: 5 */
	int  Gp_interconnect_timer_checking_period;       /* default: 20 */
	int  Gp_interconnect_default_rtt;                 /* default: 20 */
	int  Gp_interconnect_min_rto;                     /* default: 20 */
	int  Gp_interconnect_transmit_timeout;            /* default: 3600 */
	int  Gp_interconnect_min_retries_before_timeout;  /* default: 100 */
	int  Gp_interconnect_debug_retry_interval;        /* default: 10 */
	bool gp_interconnect_full_crc;                    /* default: false */
	bool gp_interconnect_aggressive_retry;            /* default: true */
	bool gp_interconnect_cache_future_packets;        /* default: true */
	bool gp_interconnect_log_stats;                   /* default: false */
	int  interconnect_setup_timeout;                  /* default: 7200 */
	int  gp_log_interconnect;                         /* default: terse */
	int  gp_session_id;                               /* global unique id for session. */
	int  Gp_interconnect_fc_method;                   /* default: INTERCONNECT_FC_METHOD_LOSS */
	int  gp_command_count;
	uint32  gp_interconnect_id;
	int  log_min_messages;                            /* default: IC_WARNING */
	DistributedTransactionId distTransId;             /* default: 0 */

	int gp_udpic_dropseg;              /* default: -2 */
	int gp_udpic_dropacks_percent;     /* default: 0 */
	int gp_udpic_dropxmit_percent;     /* default: 0 */
	int gp_udpic_fault_inject_percent; /* default: 0 */
	int gp_udpic_fault_inject_bitmap;  /* default: 0 */
	int gp_udpic_network_disable_ipv6; /* default: 0 */

} SessionMotionLayerIPCParam;

/*
 * handle error
 */
#define MSGLEN 1024

typedef enum ErrorLevel
{
	LEVEL_OK,
	LEVEL_ERROR,
	LEVEL_FATAL,
} ErrorLevel;

typedef struct ICError
{
	ErrorLevel level;
	char       msg[MSGLEN];
} ICError;

extern void ResetLastError();
extern ICError* GetLastError();
extern void SetLastError(ErrorLevel level, const char *msg);

#ifdef __cplusplus
}
#endif

#endif // IC_TYPES_H
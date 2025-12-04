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
 * ic_modules.c
 *
 * IDENTIFICATION
 *    contrib/udp2/ic_modules.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "cdb/ml_ipc.h"
#include "ic_modules.h"
#include "ic_udp2.h"

PG_MODULE_MAGIC;

static
MotionIPCLayer udp2_ipc_layer = {
    .ic_type = INTERCONNECT_TYPE_UDP2,
    .type_name = "udp2",

    .GetMaxTupleChunkSize = GetMaxTupleChunkSizeUDP2,
    .GetListenPort        = GetListenPortUDP2,

    .InitMotionLayerIPC    = InitMotionIPCLayerUDP2,
    .CleanUpMotionLayerIPC = CleanUpMotionLayerIPCUDP2,
    .WaitInterconnectQuit  = WaitInterconnectQuitUDPIFC2,
    .SetupInterconnect     = SetupInterconnectUDP2,
    .TeardownInterconnect  = TeardownInterconnectUDP2,

    .SendTupleChunkToAMS = SendTupleChunkToAMSUDP2,
    .SendChunk           = NULL,
    .SendEOS             = SendEOSUDPIFC2,
    .SendStopMessage     = SendStopMessageUDPIFC2,

    .RecvTupleChunkFromAny = RecvTupleChunkFromAnyUDPIFC2,
    .RecvTupleChunkFrom    = RecvTupleChunkFromUDPIFC2,
    .RecvTupleChunk        = NULL,

    .DirectPutRxBuffer = MlPutRxBufferUDPIFC2,

    .DeregisterReadInterest = DeregisterReadInterestUDP2,
    .GetActiveMotionConns   = GetActiveMotionConnsUDPIFC2,

    .GetTransportDirectBuffer = GetTransportDirectBufferUDPIFC2,
    .PutTransportDirectBuffer = PutTransportDirectBufferUDPIFC2,

#ifdef ENABLE_IC_PROXY
    .IcProxyServiceMain = ic_proxy_server_main,
#else 
    .IcProxyServiceMain = NULL,
#endif

    .GetMotionConnTupleRemapper = GetMotionConnTupleRemapperUDPIFC2,
    .GetMotionSentRecordTypmod  = GetMotionSentRecordTypmodUDPIFC2,
};

void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not load udp2 outside process shared preload")));
	}

	RegisterIPCLayerImpl(&udp2_ipc_layer);
}
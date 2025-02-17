/*-------------------------------------------------------------------------
 * ml_ipc.h
 *	  Motion IPC Layer.
 *
 * Portions Copyright (c) 2005-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/ml_ipc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ML_IPC_H
#define ML_IPC_H

#include "cdb/cdbselect.h"
#include "cdb/cdbinterconnect.h"
#include "cdb/cdbmotion.h"
#include "cdb/cdbvars.h"
#include "cdb/cdbgang.h"

struct SliceTable;                          /* #include "nodes/execnodes.h" */
struct EState;                              /* #include "nodes/execnodes.h" */

/* listener filedescriptors */
extern int		TCP_listenerFd;
extern int		UDP_listenerFd;

/* 2 bytes to store the size of the entire packet.	a packet is composed of
 * of one or more serialized TupleChunks (each of which has a TupleChunk
 * header.
 */
#define PACKET_HEADER_SIZE 4

typedef struct MotionIPCLayer
{
	GpVars_Interconnect_Type ic_type;
	const char *type_name;

	/* Get max tuple chuck size.
	 */
	int (*GetMaxTupleChunkSize) (void);

	/* Get IPC service listen port.
	 * Interface to replace `Gp_listener_port`
	 */
	int32 (*GetListenPort) (void);

	/* Performs initialization of the MotionLayerIPC.  This should be called before
	 * any work is performed through functions here.  Generally, this should only
	 * need to be called only once during process startup.
	 *
	 * Errors are indicated by calls to ereport(), and are therefore not indicated
	 * by a return code.
	 *
	 */
	void (*InitMotionLayerIPC) (void);

	/* Performs any cleanup necessary by the Motion Layer IPC.	This is the cleanup
	 * function that matches InitMotionLayerIPC, it should only be called during
	 * shutdown of the process. This includes shutting down the Motion Listener.
	 *
	 * Errors are indicated by calls to ereport(), and are therefore not indicated
	 * in the return code.
	 */
	void (*CleanUpMotionLayerIPC) (void);

	/*
	 * Wait interconnect thread to quit, called when proc exit.
	 */
	void (*WaitInterconnectQuit) (void);

	/* The SetupInterconnect() function should be called at the beginning of
	 * executing any DML statement that will need to use the interconnect.
	 *
	 * This function goes through the slicetable and makes any appropriate
	 * outgoing connections as well as accepts any incoming connections.  Incoming
	 * connections will have a "Register" message from them to see which remote
	 * CdbProcess sent it.
	 *
	 * So this function essentially performs all of the setup the interconnect has
	 * to perform for all of the motion nodes in the upcoming DML statement.
	 *
	 * PARAMETERS
	 *
	 *	 mySliceTable - slicetable structure that correlates to the upcoming DML
	 *					statement.
	 *
	 *	 mySliceId - the index of the slice in the slicetable that we are a member of.
	 *
	 */
	void (*SetupInterconnect)(struct EState *estate);


	/* The TeardownInterconnect() function should be called at the end of executing
	 * a DML statement to close down all socket resources that were setup during
	 * SetupInterconnect().
	 *
	 * NOTE: it is important that TeardownInterconnect() happens
	 *		 regardless of the outcome of the statement. i.e. gets called
	 *		 even if an ERROR occurs during the statement. For abnormal
	 *		 statement termination we can force an end-of-stream notification.
	 *
	 */
	void (*TeardownInterconnect)(ChunkTransportState *transportStates,
								 bool hasErrors);

	/* Sends a tuple chunk from the Postgres process to the local AMS process via
	 * IPC.  This function does not block; if the IPC channel cannot accept the
	 * tuple chunk for some reason, then this is indicated by a return-code.
	 *
	 * Errors are indicated by calls to ereport(), and are therefore not indicated
	 * in the return code.
	 *
	 *
	 * PARAMETERS:
	 *	 - motNodeID:	motion node Id that the tcItem belongs to.
	 *	 - targetRoute: route to send this tcItem out over.
	 *	 - tcItem:		The tuple-chunk data to send.
	 *
	 */
	bool (*SendTupleChunkToAMS)(ChunkTransportState *transportStates, 
								int16 motNodeID, 
								int16 targetRoute, 
								TupleChunkListItem tcItem);

	/* Internal API - should not call outside interconnection layer
	 * 
	 * Sends a tuple chunk from the Postgres process to remote or local via
	 * IPC.  This function does not block; if the IPC channel cannot accept the
	 * tuple chunk for some reason, then this is indicated by a return-code.
	 *
	 * Errors are indicated by calls to ereport(), and are therefore not indicated
	 * in the return code.
	 *
	 *
	 * PARAMETERS:
	 *	 - conn:	actived connection.
	 *	 - tcItem:	The tuple-chunk data to send.
	 *	 - motionId: motion node Id that the tcItem belongs to.
	 *
	 */
	bool (*SendChunk)(struct ChunkTransportState *transportStates, 
		struct ChunkTransportStateEntry *pEntry, 
		struct MotionConn *conn, 
		TupleChunkListItem tcItem, 
		int16 motionId);

	/* The SendEOS() function is used to send an "End Of Stream" message to
	 * one of connected receivers
	 * PARAMETERS:
	 *	 - motNodeID:	motion node Id that the tcItem belongs to.
	 *	 - tcItem:		The tuple-chunk data to send.
	 *
	 */
	void (*SendEOS)(struct ChunkTransportState *transportStates, int motNodeID, TupleChunkListItem tcItem);

	/* The SendStopMessage() function is used to send stop messages to all senders.
	 *
	 * PARAMETERS:
	 *	 - motNodeID:	motion node Id that the tcItem belongs to.
	 */
	void (*SendStopMessage)(struct ChunkTransportState *transportStates, int16 motNodeID);

	/* The RecvTupleChunkFromAny() function attempts to receive one or more tuple
	 * chunks from any of the incoming connections.  This function blocks until
	 * at least one TupleChunk is received. (Although PG Interrupts are still
	 * checked for within this call).
	 *
	 * This function makes some effort to "fairly" pull data from peers with data
	 * available (a peer with data available is always better than waiting for
	 * one without data available; but a peer with data available which hasn't been
	 * read from recently is better than a peer with data available which has
	 * been read from recently).
	 *
	 * NOTE: The TupleChunkListItem can have other's chained to it.  The caller
	 *		 should check and process all in list.
	 *
	 * PARAMETERS:
	 *	- motNodeID:  motion node id to receive for.
	 *	- srcRoute: output parameter that allows the function to return back which
	 *				route the TupleChunkListItem is from.
	 *
	 * RETURN:
	 *	 - A populated TupleChunkListItemData structure (allocated with palloc()).
	 */
	TupleChunkListItem (*RecvTupleChunkFromAny)(ChunkTransportState *transportStates,
						 int16 motNodeID,
						 int16 *srcRoute);

	/* The RecvTupleChunkFrom() function is similar to the RecvTupleChunkFromAny()
	 * function except that the connection we are interested in is specified with
	 * srcRoute.
	 *
	 * PARAMETERS:
	 *	 - motNodeID: motion node id to receive for.
	 *	 - srcRoute:  which connection to receive on.
	 * RETURN:
	 *	 - A populated TupleChunkListItemData structure (allocated with palloc()).
	 */
	TupleChunkListItem (*RecvTupleChunkFrom)(ChunkTransportState *transportStates, 
											 int16 motNodeID, 
											 int16 srcRoute);

	/* Internal API - should not call outside interconnection layer
	 *
	 * Recv a tuple chunk from the remote or local via IPC.  
	 * This function does not block; if the IPC channel cannot accept the
	 * tuple chunk for some reason, then this is indicated by a return-code.
	 *
	 * Errors are indicated by calls to ereport(), and are therefore not indicated
	 * in the return code.
	 *
	 * PARAMETERS:
	 *	 - conn:	actived connection.
	 * RETURN:
	 *	 - A populated TupleChunkListItemData structure (allocated with palloc()).
	 *
	 */
	TupleChunkListItem (*RecvTupleChunk)(struct MotionConn *conn, ChunkTransportState *transportStates);

	/* 
	 * Direct access receive buffer to our freelist.
	 *
	 * allows us to "keep" a buffer held for a connection, to avoid a copy
	 * (see inplace in chunklist).
	 * 
	 * The cdbmotion code has discarded our pointer to the motion-conn
	 * structure, but has enough info to fully specify it.
	 * 
	 * DirectPutRxBuffer() is specify for UDPIFC, can't used in other IPC
	 */
	void (*DirectPutRxBuffer)(ChunkTransportState *transportStates, int motNodeID, int route);

	/* The DeregisterReadInterest() function is used to specify that we are no
	 * longer interested in reading from the specified srcRoute. After calling this
	 * function, we should no longer ever return TupleChunks from this srcRoute
	 * when calling RecvTupleChunkFromAny().
	 *
	 * PARAMTERS:
	 *	 - motNodeID: motion node id that this applies to.
	 *	 - srcRoute:  which connection to turn off reads for.
	 *
	 */
	void (*DeregisterReadInterest)(ChunkTransportState *transportStates, 
								   int                  motNodeID, 
								   int                  srcRoute,
								   const char          *reason);

	/* Get the number of active motion connections.
	 *
	 */
	uint32 (*GetActiveMotionConns)(void);

	/*
	 * Return a direct pointer to a transmit buffer. This is actually two pointers
	 * with accompanying lengths since we have separate xmit buffers for primary and mirror
	 * segments.
	 */
	void (*GetTransportDirectBuffer)(ChunkTransportState *transportStates,
									 int16 motNodeID, 
									 int16 targetRoute, 
									 struct directTransportBuffer *b);

	/*
 	* Advance direct buffer beyond the message we just added.
 	*/
	void (*PutTransportDirectBuffer)(ChunkTransportState *transportStates,
									 int16 motNodeID,
									 int16 targetRoute, 
									 int serializedLength);

	/**
	 * bgworker call extension method
	 * Only for ic_proxy.
	 */
	int (*IcProxyServiceMain) (void);

	/*
 	 * Get the TupleRemapper from MotionConn
	 * TupleRemapper will be set in interconnect and used in cdbmotion layer
 	 */
	TupleRemapper *(*GetMotionConnTupleRemapper) (ChunkTransportState *transportStates,
		int16 motNodeID,
		int16 targetRoute);

} MotionIPCLayer;

/* MotionIPCLayer selected */
extern MotionIPCLayer *CurrentMotionIPCLayer;

/*
 * Called by interconnect.so to register a new IPC layer implement.
 */
extern void RegisterIPCLayerImpl(MotionIPCLayer *impl);

/*
 * Check and set the CurrentMotionIPCLayer when gp_interconnect_type is changed.
 */
extern void SetCurrentMotionIPCLayer(const char *new_type);
extern bool CheckGpInterconnectTypeStr(char **type_name);

/*
 * Called by PostmasterMain() to reset the CurrentMotionIPCLayer after
 * interconnect.so is loaded.
 */
extern void InitializeCurrentMotionIPCLayer(void);

#endif   /* ML_IPC_H */

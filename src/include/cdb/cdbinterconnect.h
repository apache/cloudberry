/*-------------------------------------------------------------------------
 * cdbinterconnect.h
 *	  Defines state that is used by both the Motion Layer and IPC Layer.
 *
 * Portions Copyright (c) 2006-2008, Greenplum inc
 * Portions Copyright (c) 2012-Present VMware, Inc. or its affiliates.
 *
 *
 * IDENTIFICATION
 *	    src/include/cdb/cdbinterconnect.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef CDBINTERCONNECT_H
#define CDBINTERCONNECT_H

#include "libpq/libpq-be.h"
#include "nodes/primnodes.h"
#include "cdb/tupchunklist.h"
#include "access/htup.h"
#include "cdb/htupfifo.h"

#include "cdb/cdbselect.h"
#include "cdb/tupser.h"
#include "cdb/tupchunk.h"
#include "cdb/tupchunklist.h"
#include "cdb/tupleremap.h"

/*
 * This structure is used to keep track of partially completed tuples,
 * and tuples that have been completed but have not been consumed by
 * the executor yet.
 */
typedef struct ChunkSorterEntry
{
	bool		init;

	/*
	 * A tuple-chunk list containing the chunks for the currently incomplete
	 * HeapTuple being received.
	 */
	TupleChunkListData chunk_list;

	/*
	 * A FIFO to hold the tuples that have been completed but not yet
	 * retrieved.  This will not be initialized until it is actually needed.
	 */
	htup_fifo	ready_tuples;

	/*
	 * Flag recording whether end-of-stream has been reported from the source.
	 */
	bool		end_of_stream;
}	ChunkSorterEntry;

/* This is the entry data-structure for a motion node. */
typedef struct MotionNodeEntry
{
	/*
	 * First value in entry has to be the key value.  The key is the motion
	 * node ID.
	 */
	int16           motion_node_id;

	/*
	 * Flag specifying whether the order of tuples from each source should be
	 * maintained or preserved.
	 */
	bool            preserve_order;

	/*
	 * Our route-based array of htup_fifos, for the case where we are a merge receive.
	 */
	ChunkSorterEntry *ready_tuple_lists;

	/* The description of tuples that this motion node will be exchanging. */
	TupleDesc       tuple_desc;

	/*
	 * The cached information to perform tuple serialization and
	 * deserialization as quickly as possible.
	 */
	SerTupInfo      ser_tup_info;

	/*
	 * If preserve_order is false, this is used to hold completed tuples that
	 * have not yet been consumed.  If preserve_order is true, this is NULL.
	 */
	htup_fifo       ready_tuples;

	/*
	 * Variable that records the total number of senders to this motion node.
	 * This is expected to always be (number of qExecs).
	 */
	uint32          num_senders;

	/*
	 * Variable that tracks number of senders that have reported end-of-stream
	 * for this motion node.  When the local node sends end-of-stream, that is
	 * also recorded.
	 */
	uint32          num_stream_ends_recvd;

	bool            cleanedUp;
	bool            valid;
	bool            moreNetWork;
	bool            stopped;

	/*
	 * PER-MOTION-NODE STATISTICS
	 */

	uint64          stat_total_chunks_sent; /* Tuple-chunks sent. */
	uint64          stat_total_bytes_sent;  /* Bytes sent, including headers. */
	uint64          stat_tuple_bytes_sent;  /* Bytes of pure tuple-data sent. */

	uint64          stat_total_chunks_recvd;                /* Tuple-chunks received. */
	uint64          stat_total_bytes_recvd; /* Bytes received, including headers. */
	uint64          stat_tuple_bytes_recvd; /* Bytes of pure tuple-data received. */

	uint64          stat_total_sends;               /* Total calls to SendTuple. */

	uint64          stat_total_recvs;               /* Total calls to RecvTuple/etc. */

	uint64          stat_tuples_available;  /* Total tuples awaiting receive. */
	uint64          stat_tuples_available_hwm;              /* High-water-mark of this
		* value. */
}       MotionNodeEntry;


/*=========================================================================
* MOTION LAYER DATA STRUCTURE
*/

typedef struct MotionLayerState
{
	/* The host ID that this segment-database is on. */
	int			host_id;

	/*
	 * Memory context for the whole motion layer.  This is a child context of
	 * the Executor State Context, so the if it fails to get cleaned up the
	 * estate context should free our resources at the end of the query.
	 */
	MemoryContext motion_layer_mctx;

	/*
	 * MOTION NODE STATE - Initialized and used on per-statement basis.
	 */
	int			mneCount;
	MotionNodeEntry *mnEntries;

	/*
	 * GLOBAL MOTION-LAYER STATISTICS
	 */
	uint32		stat_total_chunks_sent; /* Tuple-chunks sent. */
	uint32		stat_total_bytes_sent;	/* Bytes sent, including headers. */
	uint32		stat_tuple_bytes_sent;	/* Bytes of pure tuple-data sent. */

	uint32		stat_total_chunks_recvd;/* Tuple-chunks received. */
	uint32		stat_total_bytes_recvd; /* Bytes received, including headers. */
	uint32		stat_tuple_bytes_recvd; /* Bytes of pure tuple-data received. */

	uint32		stat_total_chunkproc_calls;		/* Calls to processIncomingChunks() */

}	MotionLayerState;


/* ChunkTransportState array initial size */
#define CTS_INITIAL_SIZE (10)

struct SliceTable;                          /* #include "nodes/execnodes.h" */
struct EState;                              /* #include "nodes/execnodes.h" */
struct ICProxyBackendContext;
struct MotionConn;
struct ChunkTransportStateEntry;
typedef struct MotionConn MotionConn;
typedef struct ChunkTransportStateEntry ChunkTransportStateEntry;

typedef struct MotionConnKey 
{
	int mot_node_id;
	int conn_index;
} MotionConnKey;

typedef struct MotionConnSentRecordTypmodEnt
{
	MotionConnKey key;
	int32 sent_record_typmod;
} MotionConnSentRecordTypmodEnt;

typedef struct ChunkTransportState
{
	/* array of per-motion-node chunk transport state
	 *
	 * MUST pay attention: use getChunkTransportStateNoValid/getChunkTransportState
	 * to get ChunkTransportStateEntry.
	 * must not use `->states[index]` to get ChunkTransportStateEntry. Because the struct
	 * ChunkTransportStateEntry is a base structure for ChunkTransportStateEntryTCP and
	 * ChunkTransportStateEntryUDP. After interconnect setup, the `states` will be fill
	 * with EntryUDP/EntryTCP, but the pointer still is ChunkTransportStateEntry which
	 * should use `CONTAINER_OF` to get the real object.
	 */
	int size;
	struct ChunkTransportStateEntry *states;

	/* keeps track of if we've "activated" connections via SetupInterconnect(). */
	bool		activated;

	bool		aggressiveRetry;
	
	/* whether we've logged when network timeout happens */
	bool		networkTimeoutIsLogged;

	bool		teardownActive;
	List		*incompleteConns;

	/* slice table stuff. */
	struct SliceTable  *sliceTable;
	int			sliceId;

	/* Estate pointer for this statement */
	struct EState *estate;

	/*
	 * used by the sender.
	 *
	 * the typmod of last sent record type in current connection,
	 * if the connection is for broadcasting then we only check
	 * and update this attribute on connection 0.
	 * 
	 * mapping the MotionConn -> int32
	 */
	HTAB* conn_sent_record_typmod;

	/* ic_proxy backend context */
	struct ICProxyBackendContext *proxyContext;
} ChunkTransportState;

#endif   /* CDBINTERCONNECT_H */

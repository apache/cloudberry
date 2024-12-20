/*
 * pg_query_state.h
 *		Headers for pg_query_state extension.
 *
 * Copyright (c) 2016-2024, Postgres Professional
 *
 * IDENTIFICATION
 *	  contrib/pg_query_state/pg_query_state.h
 */
#ifndef __PG_QUERY_STATE_H__
#define __PG_QUERY_STATE_H__

#include <postgres.h>

#include "commands/explain.h"
#include "nodes/pg_list.h"
#include "storage/procarray.h"
#include "storage/shm_mq.h"


#define	QUEUE_SIZE			(16 * 1024)
#define MSG_MAX_SIZE		1024
#define WRITING_DELAY		(100 * 1000) /* 100ms */
#define NUM_OF_ATTEMPTS		6

#define TIMINIG_OFF_WARNING 1
#define BUFFERS_OFF_WARNING 2

#define	PG_QS_MODULE_KEY	0xCA94B108
#define	PG_QS_RCV_KEY       0
#define	PG_QS_SND_KEY       1

/* Receive timeout should be larger than send timeout to let workers stop waiting before polling process */
#define MAX_RCV_TIMEOUT   6000 /* 6 seconds */
#define MAX_SND_TIMEOUT   3000 /* 3 seconds */

/*
 * Delay for receiving parts of full message (in case SHM_MQ_WOULD_BLOCK code),
 * should be tess than MAX_RCV_TIMEOUT
 */
#define PART_RCV_DELAY    1000 /* 1 second */

/*
 * Result status on query state request from asked backend
 */
typedef enum
{
	QUERY_NOT_RUNNING,		/* Backend doesn't execute any query */
	STAT_DISABLED,			/* Collection of execution statistics is disabled */
	QS_RETURNED			/* Backend succx[esfully returned its query state */
} PG_QS_RequestResult;

/*
 *	Format of transmited data through message queue
 */
typedef struct
{
	int     reqid;
	int		length;							/* size of message record, for sanity check */
	int 	pid;
	PG_QS_RequestResult	result_code;
	int warnings;
	int		stack_depth;
	char	stack[FLEXIBLE_ARRAY_MEMBER];	/* sequencially laid out stack frames in form of
												text records */
} shm_mq_msg;

#define BASE_SIZEOF_SHM_MQ_MSG (offsetof(shm_mq_msg, stack_depth))

typedef struct
{
	int32 	segid;
	int32 	pid;
} gp_segment_pid;

/*
 *	Format of transmited data gp_backend_info through message queue
 */
typedef struct
{
	int     reqid;
	int		length;							/* size of message record, for sanity check */
	PGPROC	*proc;
	PG_QS_RequestResult	result_code;
	int		number;
	gp_segment_pid pids[FLEXIBLE_ARRAY_MEMBER];
} backend_info;

typedef struct 
{
	int     reqid;
	int		length;							/* size of message record, for sanity check */
	PGPROC	*proc;

	PG_QS_RequestResult	result_code;
	int 	sliceIndex;
	uint64 	queryId;
	/* data saves the CdbExplain_StatHdr */
	char 	data[FLEXIBLE_ARRAY_MEMBER];
} query_state_info;


#define BASE_SIZEOF_GP_BACKEND_INFO (offsetof(backend_info, pids))
/* pg_query_state arguments */
typedef struct
{
	int     reqid;
	bool 	verbose;
	bool	costs;
	bool	timing;
	bool	buffers;
	bool	triggers;
	ExplainFormat format;
} pg_qs_params;

/* moved from signal_handler.c*/
/*
 * An self-explanarory enum describing the send_msg_by_parts results
 */
typedef enum
{
	MSG_BY_PARTS_SUCCEEDED,
	MSG_BY_PARTS_FAILED
} msg_by_parts_result;


/* pg_query_state */
extern bool 	pg_qs_enable;
extern bool 	pg_qs_timing;
extern bool 	pg_qs_buffers;
extern List 	*QueryDescStack;
extern pg_qs_params *params;
extern shm_mq 	*mq;

extern query_state_info *CachedQueryStateInfo; 

/* pg_query_stat.c */
extern shm_mq_result
shm_mq_receive_with_timeout(shm_mq_handle *mqh,
							Size *nbytesp,
							void **datap,
							int64 timeout);
extern bool enable_qs_runtime(void);
extern bool enable_qs_done(void);


/* signal_handler.c */
extern void SendQueryState(void);
extern void SendCdbComponents(void);
extern void DetachPeer(void);
extern void AttachPeer(void);
extern void UnlockShmem(LOCKTAG *tag);
extern void LockShmem(LOCKTAG *tag, uint32 key);
extern void init_pg_query_state(void);
extern msg_by_parts_result send_msg_by_parts(shm_mq_handle *mqh, Size nbytes, const void *data);

extern bool check_msg(shm_mq_result mq_receive_result, shm_mq_msg *msg, Size len, int reqid);
extern void create_shm_mq(PGPROC *sender, PGPROC *receiver);
extern bool filter_running_query(QueryDesc *queryDesc);
extern query_state_info *new_queryStateInfo(int sliceIndex, StringInfo strInfo, int reqid,
											uint64 queryId,
											PG_QS_RequestResult result_code);
extern bool wait_for_mq_detached(shm_mq_handle *mqh);

extern bool is_querystack_empty(void);
extern QueryDesc *get_toppest_query(void);
extern int get_command_count(query_state_info *info);
#endif

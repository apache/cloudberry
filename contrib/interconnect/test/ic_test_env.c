
#include <unistd.h>
#include <sys/wait.h>
#include <assert.h>

#include "postgres.h"
#include "ic_test_env.h"
#include "ic_modules.h"
#include "postmaster/postmaster.h"
#include "utils/memutils.h"
#include "storage/latch.h"


void
generate_seq_buffer(char *buffer, const size_t buffer_len)
{
	for (size_t i = 0; i < buffer_len; i++)
	{
		buffer[i] = i;
	}
}

TupleDesc
prepare_tuple_desc()
{
	TupleDesc	tuple_desc;

	tuple_desc = palloc0(sizeof(TupleDescData) + sizeof(FormData_pg_attribute) * 2);
	tuple_desc->natts = COLUMN_NUMS;
	tuple_desc->attrs[0].attlen = -1;
	tuple_desc->attrs[0].attbyval = false;
	tuple_desc->attrs[0].attalign = TYPALIGN_CHAR;
	tuple_desc->attrs[1].attlen = 4;
	tuple_desc->attrs[1].attbyval = true;
	tuple_desc->attrs[1].attalign = TYPALIGN_INT;

	return tuple_desc;
}

TupleTableSlot *
prepare_tuple_slot()
{
	TupleDesc	tuple_desc;
	TupleTableSlot *slot;
	text	   *column1_text;
	Size		basesz = 0,
				allocsz = 0;
	TupleTableSlotOps *current_slot_ops = (TupleTableSlotOps *) &TTSOpsVirtual;

	tuple_desc = prepare_tuple_desc();

	basesz = current_slot_ops->base_slot_size;

	allocsz = MAXALIGN(basesz) + MAXALIGN(tuple_desc->natts * sizeof(Datum)) +
		MAXALIGN(tuple_desc->natts * sizeof(bool));

	slot = palloc0(allocsz);

	*((const TupleTableSlotOps **) &slot->tts_ops) = current_slot_ops;
	slot->type = T_TupleTableSlot;
	slot->tts_flags = TTS_FLAG_EMPTY | TTS_FLAG_FIXED;
	slot->tts_tupleDescriptor = tuple_desc;
	slot->tts_mcxt = CurrentMemoryContext;
	slot->tts_nvalid = 0;

	slot->tts_values = (Datum *) (((char *) (slot)) + MAXALIGN(basesz));
	slot->tts_isnull = (bool *) (
								 (((char *) (slot)) + MAXALIGN(basesz) +
								  MAXALIGN(tuple_desc->natts * sizeof(Datum))));
	slot->tts_isnull[0] = false;
	slot->tts_isnull[1] = false;
	slot->tts_ops->init(slot);

	column1_text = (text *) (palloc0(COLUMN_TEXT_LEN + VARHDRSZ));
	SET_VARSIZE(column1_text, COLUMN_TEXT_LEN + VARHDRSZ);
	generate_seq_buffer(((char *) VARDATA(column1_text)), COLUMN_TEXT_LEN);

	slot->tts_values[0] = PointerGetDatum(column1_text);
	slot->tts_values[1] = Int32GetDatum(COLUMN_INT4_VALUE);

	return slot;
}

HeapTupleTableSlot *
get_tuple_slot_from_chunk_list_serialized_tuple(TupleChunkListItem tc_item)
{
	char	   *tc_item_data = NULL;
	int			tc_item_len = 0;
	char	   *tc_item_body_pos = NULL;
	int			tc_item_body_len = 0;
	char	   *tc_item_minimal_tuple_body = NULL;
	unsigned int tc_item_minimal_tuple_len = 0;
	MinimalTuple tc_item_minimal_tuple = NULL;
	HeapTuple	tc_item_heap_tuple = NULL;
	Size		tc_item_heap_tuple_desc_basesz = 0,
				tc_item_heap_tuple_desc_allocsz = 0;
	HeapTupleTableSlot *tc_item_heap_tuple_slot = NULL;

	tc_item_data = (char *) GetChunkDataPtr(tc_item) + TUPLE_CHUNK_HEADER_SIZE;
	tc_item_len = tc_item->chunk_length - TUPLE_CHUNK_HEADER_SIZE;

	tc_item_body_pos = tc_item_data;

	memcpy(&tc_item_body_len, tc_item_body_pos, sizeof(tc_item_body_len));
	tc_item_body_pos += sizeof(tc_item_body_len);

	tc_item_minimal_tuple_len = tc_item_body_len + MINIMAL_TUPLE_DATA_OFFSET;

	tc_item_minimal_tuple = palloc0(tc_item_minimal_tuple_len);
	tc_item_minimal_tuple->t_len = tc_item_minimal_tuple_len;

	tc_item_minimal_tuple_body = (char *) tc_item_minimal_tuple + MINIMAL_TUPLE_DATA_OFFSET;
	memcpy(tc_item_minimal_tuple_body, tc_item_body_pos, tc_item_body_len);

	tc_item_heap_tuple = heap_tuple_from_minimal_tuple(tc_item_minimal_tuple);
	TupleDesc	heap_tuple_desc = prepare_tuple_desc();

	tc_item_heap_tuple_desc_basesz = TTSOpsHeapTuple.base_slot_size;

	tc_item_heap_tuple_desc_allocsz = MAXALIGN(tc_item_heap_tuple_desc_basesz) + MAXALIGN(heap_tuple_desc->natts * sizeof(Datum)) +
		MAXALIGN(heap_tuple_desc->natts * sizeof(bool));

	tc_item_heap_tuple_slot = palloc0(sizeof(tc_item_heap_tuple_desc_allocsz));
	tc_item_heap_tuple_slot->tuple = tc_item_heap_tuple;
	tc_item_heap_tuple_slot->off = 0;
	tc_item_heap_tuple_slot->base.tts_tupleDescriptor = heap_tuple_desc;
	tc_item_heap_tuple_slot->base.tts_isnull = (bool *) (
														 (((char *) (&tc_item_heap_tuple_slot->base)) + MAXALIGN(tc_item_heap_tuple_desc_basesz) +
														  MAXALIGN(heap_tuple_desc->natts * sizeof(Datum))));
	tc_item_heap_tuple_slot->base.tts_values = (Datum *) (((char *) (&tc_item_heap_tuple_slot->base)) + MAXALIGN(tc_item_heap_tuple_desc_basesz));
	tc_item_heap_tuple_slot->base.tts_flags = TTS_FLAG_FIXED;
	TTSOpsHeapTuple.getsomeattrs(&tc_item_heap_tuple_slot->base, heap_tuple_desc->natts);

	pfree(tc_item_minimal_tuple);
	return tc_item_heap_tuple_slot;
}

void
cleanup_tuple_desc(TupleDesc desc)
{
	pfree(desc);
}

void
cleanup_tuple_slot(TupleTableSlot *slot)
{
	cleanup_tuple_desc(slot->tts_tupleDescriptor);
	pfree(DatumGetPointer(slot->tts_values[0]));
	pfree(slot);
}

void
cleanup_heap_tuple_slot(HeapTupleTableSlot *slot)
{
	cleanup_tuple_desc(slot->base.tts_tupleDescriptor);
	pfree(slot);
}

static void
init_exec_slices(ExecSlice * parent_exec_slice,
				 ExecSlice * child_exec_slice,
				 int32 parent_listen_port,
				 int32 child_listen_port,
				 pid_t parent_pid,
				 pid_t child_pid)
{

	CdbProcess *parent_process;
	CdbProcess *child_process;

	memset(parent_exec_slice, 0, sizeof(ExecSlice));
	memset(child_exec_slice, 0, sizeof(ExecSlice));

	parent_process = (CdbProcess *) palloc0(sizeof(CdbProcess));
	child_process = (CdbProcess *) palloc0(sizeof(CdbProcess));

	parent_process->type = T_CdbProcess;
	parent_process->listenerAddr = "127.0.1.1";
	parent_process->listenerPort = parent_listen_port;

	parent_process->dbid = 1;
	parent_process->pid = parent_pid;
	parent_process->contentid = -1;

	child_process->type = T_CdbProcess;
	child_process->listenerAddr = "127.0.1.1";
	child_process->listenerPort = child_listen_port;

	child_process->dbid = 2;
	child_process->pid = child_pid;
	child_process->contentid = 0;

	parent_exec_slice->sliceIndex = 0;
	parent_exec_slice->rootIndex = 0;
	parent_exec_slice->planNumSegments = 1;
	parent_exec_slice->gangType = GANGTYPE_UNALLOCATED;
	parent_exec_slice->segments = 0;
	parent_exec_slice->primaryGang = 0;

	parent_exec_slice->primaryProcesses = lappend(parent_exec_slice->primaryProcesses, parent_process);
	parent_exec_slice->processesMap = 0;

	parent_exec_slice->children = lappend_int(parent_exec_slice->children, 1);
	parent_exec_slice->parentIndex = -1;

	child_exec_slice->sliceIndex = 1;
	child_exec_slice->rootIndex = 0;
	child_exec_slice->planNumSegments = 1;
	child_exec_slice->gangType = GANGTYPE_PRIMARY_READER;
	child_exec_slice->segments = 0;
	child_exec_slice->primaryGang = 0;

	child_exec_slice->primaryProcesses = lappend(child_exec_slice->primaryProcesses, child_process);
	child_exec_slice->processesMap = 0;

	child_exec_slice->children = NIL;
	child_exec_slice->parentIndex = 0;
}


extern EState *
prepare_estate(const int local_slice,
			   const int32 parent_listen_port,
			   const int32 child_listen_port,
			   const pid_t parent_pid,
			   const pid_t child_pid)
{
	EState	   *estate;
	SliceTable *slice_table;

	estate = (EState *) palloc0(sizeof(EState));
	slice_table = (SliceTable *) palloc0(sizeof(SliceTable));
	slice_table->type = T_SliceTable;
	slice_table->localSlice = local_slice;
	slice_table->numSlices = 2;
	slice_table->slices = (ExecSlice *) palloc(sizeof(ExecSlice) * slice_table->numSlices);
	slice_table->ic_instance_id = 1;

	estate->es_sliceTable = slice_table;

	init_exec_slices(&estate->es_sliceTable->slices[0],
					 &estate->es_sliceTable->slices[1],
					 parent_listen_port,
					 child_listen_port,
					 parent_pid,
					 child_pid);

	return estate;
}

extern void
cleanup_estate(EState *estate)
{
	list_free(estate->es_sliceTable->slices[0].primaryProcesses);
	list_free(estate->es_sliceTable->slices[1].primaryProcesses);
	pfree(estate->es_sliceTable->slices);
	pfree(estate->es_sliceTable);
	pfree(estate);
}


TupleChunkListData *
prepare_chunk_list_raw_data(const char *raw_buffer, const size_t raw_buffer_len)
{
	TupleChunkListData *tc_list;
	TupleChunkListItem item;

	tc_list = (TupleChunkListData *) palloc0(sizeof(TupleChunkListData));

	tc_list->p_first = NULL;
	tc_list->p_last = NULL;
	tc_list->num_chunks = 0;
	tc_list->serialized_data_length = 0;
	tc_list->max_chunk_length = Gp_max_tuple_chunk_size;

	item = (TupleChunkListItem)
		palloc(sizeof(TupleChunkListItemData) + Gp_max_tuple_chunk_size);
	MemSetAligned(item, 0, sizeof(TupleChunkListItemData) + 4);

	SetChunkType(item->chunk_data, TC_WHOLE);
	item->chunk_length = TUPLE_CHUNK_HEADER_SIZE;
	appendChunkToTCList(tc_list, item);

	memcpy(item->chunk_data, &raw_buffer_len, 4);
	memcpy(item->chunk_data + 4, raw_buffer, raw_buffer_len);
	item->chunk_length = raw_buffer_len + 4;

	return tc_list;
}

void
cleanup_chunk_list_raw_data(TupleChunkListData * tc_list)
{
	/* only set zero/one item in test */
	if (tc_list->p_first)
		pfree(tc_list->p_first);
	pfree(tc_list);
}

bool
verify_chunk_list_raw_data(const TupleChunkListItem tc_item_raw,
						   const char *verify_buffer, const size_t verify_buffer_len)
{
	char	   *tc_item_raw_data = NULL;
	int			tc_item_raw_len = 0;

	tc_item_raw_data = (char *) GetChunkDataPtr(tc_item_raw) + TUPLE_CHUNK_HEADER_SIZE;
	tc_item_raw_len = tc_item_raw->chunk_length - TUPLE_CHUNK_HEADER_SIZE;

	return tc_item_raw_len == verify_buffer_len && strncmp(tc_item_raw_data, verify_buffer, verify_buffer_len) == 0;
}

void
client_side_global_var_init(MotionIPCLayer * motion_ipc_layer, pid_t *ic_proxy_pid)
{
	Gp_max_packet_size = 1500;

	GpIdentity.segindex = 0;
	GpIdentity.dbid = 2;

	MyProcPid = getpid();

	CurrentMotionIPCLayer = motion_ipc_layer;
	interconnect_address = "127.0.1.1";

	Gp_max_tuple_chunk_size = CurrentMotionIPCLayer->GetMaxTupleChunkSize();

	/* udpifc  */
	ic_htab_size = 2;

	Gp_interconnect_queue_depth = 800;
	Gp_interconnect_snd_queue_depth = 600;
	Gp_interconnect_mem_size = 20;
	Gp_interconnect_timer_period = 1;
	Gp_interconnect_timer_checking_period = 2;
	InitializeLatchSupport();
	if (pipe(postmaster_alive_fds) != 0 || fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFL, O_NONBLOCK) != 0)
	{
		ereport(ERROR, "fail to init postmaster_alive_fds");
	}

	/* proxy */
	if (motion_ipc_layer->ic_type == INTERCONNECT_TYPE_PROXY)
	{
		assert(motion_ipc_layer->IcProxyServiceMain);
		PostmasterPid = getpid();

		gp_interconnect_proxy_addresses = "1:-1:127.0.1.1:7111,2:0:127.0.1.1:7112";
		*ic_proxy_pid = fork();
		assert(*ic_proxy_pid != -1);
		if (*ic_proxy_pid == 0)
		{
			motion_ipc_layer->IcProxyServiceMain();
		}
	}
}

void
server_side_global_var_init(MotionIPCLayer * motion_ipc_layer, pid_t *ic_proxy_pid)
{
	Gp_max_packet_size = 1500;

	GpIdentity.segindex = -1;
	GpIdentity.dbid = 1;

	MyProcPid = getpid();

	CurrentMotionIPCLayer = motion_ipc_layer;
	interconnect_address = "127.0.1.1";

	Gp_max_tuple_chunk_size = CurrentMotionIPCLayer->GetMaxTupleChunkSize();

	/* udpifc  */
	ic_htab_size = 2;

	Gp_interconnect_queue_depth = 800;
	Gp_interconnect_snd_queue_depth = 600;
	Gp_interconnect_mem_size = 20;
	Gp_interconnect_timer_period = 1;
	Gp_interconnect_timer_checking_period = 2;
	InitializeLatchSupport();
	if (pipe(postmaster_alive_fds) != 0 || fcntl(postmaster_alive_fds[POSTMASTER_FD_WATCH], F_SETFL, O_NONBLOCK) != 0)
	{
		ereport(ERROR, "fail to init postmaster_alive_fds");
	}

	/* proxy */
	if (motion_ipc_layer->ic_type == INTERCONNECT_TYPE_PROXY)
	{
		assert(motion_ipc_layer->IcProxyServiceMain);
		PostmasterPid = getpid();

		gp_interconnect_proxy_addresses = "1:-1:127.0.1.1:7111,2:0:127.0.1.1:7112";
		*ic_proxy_pid = fork();
		assert(*ic_proxy_pid != -1);
		if (*ic_proxy_pid == 0)
		{
			motion_ipc_layer->IcProxyServiceMain();
		}
	}
}

void
shutdown_ic_proxy_if_need(pid_t ic_proxy_pid)
{
	if (ic_proxy_pid != -1)
	{
		/* fixme: uv_signal_start setting is not right */
		kill(ic_proxy_pid, SIGKILL);
	}
}

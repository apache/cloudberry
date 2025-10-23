#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"

#include "c.h"
#include "postgres.h"
#include "utils/memutils.h"
#include "../syslogger.c"

#define MOCK_PIPE_CHUNK_PAYLOAD_SIZE 16
static char alphabets[26] = "abcdefghijklmnopqrstuvwxyz";
static char mock_content[MOCK_PIPE_CHUNK_PAYLOAD_SIZE];
extern void __wrap_syslogger_log_chunk_data(PipeProtoHeader* p, char *data, int len);

void __wrap_syslogger_log_chunk_data(PipeProtoHeader* p, char *data, int len)
{
	check_expected(data);
	mock();
}

static void mock_chunk(PipeProtoChunk* p, int dest)
{	
	p->hdr.zero = 0;
	p->hdr.thid = mythread();
	p->hdr.main_thid = mainthread();
	p->hdr.chunk_no = 0;
	p->hdr.is_segv_msg = 'f';
	p->hdr.next = -1;
	p->hdr.len = MOCK_PIPE_CHUNK_PAYLOAD_SIZE;
	p->hdr.pid = 10000;
	p->hdr.flags = 0;

	if (dest == LOG_DESTINATION_STDERR)
		p->hdr.flags |= PIPE_PROTO_DEST_STDERR;
	else if (dest == LOG_DESTINATION_CSVLOG)
		p->hdr.flags |= PIPE_PROTO_DEST_CSVLOG;
	else if (dest == LOG_DESTINATION_JSONLOG)
		p->hdr.flags |= PIPE_PROTO_DEST_JSONLOG;
}

static int
interleave_chunks_to_buffer(char* logbuffer, int pids, int chunks, int dest)
{
	Assert(chunks > 0);
	
	int total = 0;
	PipeProtoChunk p;
	mock_chunk(&p, LOG_DESTINATION_CSVLOG);

	/* interleaved write non-last chunks */
	for (int c = 0; c < chunks - 1; c++) 
	{
		for (int i = 0; i < pids; i++)
		{
			p.hdr.pid = i + 10000;
			memset(&mock_content, alphabets[i], MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
			memcpy(p.data, mock_content, MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
			memcpy(logbuffer, &p, PIPE_HEADER_UNALIGNED_SIZE + MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
			logbuffer += PIPE_HEADER_UNALIGNED_SIZE + MOCK_PIPE_CHUNK_PAYLOAD_SIZE;
			total += PIPE_HEADER_UNALIGNED_SIZE + MOCK_PIPE_CHUNK_PAYLOAD_SIZE;
			++p.hdr.chunk_no;
		}
	}

	/* interleaved write last chunks */
	p.hdr.flags |= PIPE_PROTO_IS_LAST;
	for (int i = 0; i < pids; i++)
	{
		p.hdr.pid = i + 10000;
		memset(&mock_content, alphabets[i], MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
		memcpy(p.data, mock_content, MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
		memcpy(logbuffer, &p, PIPE_HEADER_UNALIGNED_SIZE + MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
		logbuffer += PIPE_HEADER_UNALIGNED_SIZE + MOCK_PIPE_CHUNK_PAYLOAD_SIZE;
		total += PIPE_HEADER_UNALIGNED_SIZE + MOCK_PIPE_CHUNK_PAYLOAD_SIZE;
	}

	return total;
}

static int
thirdparty_chunks_to_buffer(char* logbuffer, int dest)
{
	int total = 0;

	/* mock 3rd party message before protocol chunk */
	char tmp1[100] = "testing_3rd_log_before_protocol_chunk";
	strcpy(logbuffer, tmp1);
	logbuffer += strlen(tmp1);
	total += strlen(tmp1);

	/* mock protocol chunk */
	PipeProtoChunk p;
	mock_chunk(&p, LOG_DESTINATION_CSVLOG);
	p.hdr.flags |= PIPE_PROTO_IS_LAST;
	memset(&mock_content, 'a', MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
	memcpy(p.data, mock_content, MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
	memcpy(logbuffer, &p, PIPE_HEADER_UNALIGNED_SIZE + MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
	logbuffer += PIPE_HEADER_UNALIGNED_SIZE + MOCK_PIPE_CHUNK_PAYLOAD_SIZE;
	total += PIPE_HEADER_UNALIGNED_SIZE + MOCK_PIPE_CHUNK_PAYLOAD_SIZE;

	/* actually '\0' is not needed. just helps us to mark string end and check the expect value. */
	*(logbuffer++) = '\0';
	total++;
	/* mock 3rd party message after protocol chunk */
	char tmp2[100] = "testing_3rd_log_after_protocol_chunk";
	strcpy(logbuffer, tmp2);
	logbuffer += strlen(tmp2);
	total += strlen(tmp2);

	/* another protocol chunk to force the third party message be flushed */
	memset(&mock_content, 'b', MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
	memcpy(p.data, mock_content, MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
	memcpy(logbuffer, &p, PIPE_HEADER_UNALIGNED_SIZE + MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
	logbuffer += PIPE_HEADER_UNALIGNED_SIZE + MOCK_PIPE_CHUNK_PAYLOAD_SIZE;
	total += PIPE_HEADER_UNALIGNED_SIZE + MOCK_PIPE_CHUNK_PAYLOAD_SIZE;

	return total;
}

static void 
test__process_pipe_input_third_party__ProcessPipeInputThirdParty(void **state)
{
	char logbuffer[READ_BUF_SIZE];
	memset(logbuffer, '\0', READ_BUF_SIZE);
	
	int bytes_in_logbuffer = thirdparty_chunks_to_buffer(logbuffer, LOG_DESTINATION_CSVLOG);

	expect_string(__wrap_syslogger_log_chunk_data, data, "aaaaaaaaaaaaaaaa");
	will_be_called(__wrap_syslogger_log_chunk_data);

	expect_string(__wrap_syslogger_log_chunk_data, data, "bbbbbbbbbbbbbbbb");
	will_be_called(__wrap_syslogger_log_chunk_data);

	process_pipe_input(logbuffer, &bytes_in_logbuffer);  
}

static void 
test__process_pipe_input_interleave__ProcessPipeInputInterleave(void **state)
{
	char logbuffer[READ_BUF_SIZE];
	memset(logbuffer, '\0', READ_BUF_SIZE);

	#define PIDS 3
	#define CHUNKS 5
	int bytes_in_logbuffer = interleave_chunks_to_buffer(logbuffer, PIDS, CHUNKS, LOG_DESTINATION_CSVLOG);
	char tmp[PIDS][CHUNKS * MOCK_PIPE_CHUNK_PAYLOAD_SIZE + 3];
	memset(tmp, '\0', PIDS * (CHUNKS * MOCK_PIPE_CHUNK_PAYLOAD_SIZE + 3));

	for (int i = 0; i < PIDS; i++) 
	{
		memset(&tmp[i], alphabets[i], CHUNKS * MOCK_PIPE_CHUNK_PAYLOAD_SIZE);
		expect_string(__wrap_syslogger_log_chunk_data, data, tmp[i]);
		will_be_called(__wrap_syslogger_log_chunk_data);
	}
	process_pipe_input(logbuffer, &bytes_in_logbuffer);   
}

int
main(int argc, char* argv[]) {
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test__process_pipe_input_third_party__ProcessPipeInputThirdParty),
		unit_test(test__process_pipe_input_interleave__ProcessPipeInputInterleave)
	};
	MemoryContextInit();

	return run_tests(tests);
}

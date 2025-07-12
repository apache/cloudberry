/*-------------------------------------------------------------------------
 *
 * fstream.h
 *
 * src/include/fstream/fstream.h
 *-------------------------------------------------------------------------
 */
#ifndef FSTREAM_H
#define FSTREAM_H

#include <fstream/gfile.h>
#include <sys/types.h>
/*#include "c.h"*/
#ifdef WIN32
typedef __int64 int64_t;
#endif

#define FILE_ERROR_SZ 200

struct gpfxdist_t;

typedef struct
{
	int 	gl_pathc;
	char**	gl_pathv;
	char**  gl_username;
	char**  gl_passwd;
	char**  gl_keyfile1;
	char**  gl_keyfile2;
	char**  gl_hostaddr;
	char**  gl_port;
} glob_and_copy_t;

struct fstream_options{
    int header;
    int is_csv;
    int verbose;
    char quote;		/* quote char */
    char escape;	/* escape char */
    int eol_type;
    int bufsize;
    int forwrite;   /* true for write, false for read */
	int usesync;    /* true if writes use O_SYNC */
    struct gpfxdist_t* transform;	/* for gpfxdist transformations */
};

/* A file stream - data may come from several files */
typedef struct fstream_t fstream_t;
struct fstream_t
{
	glob_and_copy_t glob;
	gfile_t 		fd;
	int 			fidx; /* current index in ffd[] */
	int64_t 		foff; /* current offset in ffd[fidx] */
	int64_t 		line_number;
	int64_t 		compressed_size;
	int64_t 		compressed_position;
	int 			skip_header_line;
	char* 			buffer;			 /* buffer to store data read from file */
	int 			buffer_cur_size; /* number of bytes in buffer currently */
	const char*		ferror; 		 /* error string */
	struct fstream_options options;
};


struct fstream_filename_and_offset{
    char 	fname[256];
    int64_t line_number; /* Line number of first line in buffer.  Zero means fstream doesn't know the line number. */
    int64_t foff;
};

// If read_whole_lines, then size must be at least the value of -m (blocksize). */
int fstream_read(fstream_t* fs, void* buffer, int size,
				 struct fstream_filename_and_offset* fo,
				 const int read_whole_lines,
				 const char *line_delim_str,
				 const int line_delim_length);
int fstream_write(fstream_t *fs,
				  void *buf,
				  int size,
				  const int write_whole_lines,
				  const char *line_delim_str,
				  const int line_delim_length);
int fstream_eof(fstream_t* fs);
int64_t fstream_get_compressed_size(fstream_t* fs);
int64_t fstream_get_compressed_position(fstream_t* fs);
const char* fstream_get_error(fstream_t* fs);
fstream_t* fstream_open(const char* path, const struct fstream_options* options,
						int* response_code, const char** response_string);
int fstream_close_with_error(fstream_t* fs, char* msg);
void fstream_close(fstream_t* fs);
bool_t fstream_is_win_pipe(fstream_t *fs);

int ParseFilePathUri(char *uri_str, sftp_info_t *info);
int get_sftp_counts(const char *sftp_request);

#endif

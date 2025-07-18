/*-------------------------------------------------------------------------
*
* fstream.c
*
*--------------------------------------------------------------------------
*/
#ifdef WIN32
/* exclude transformation features on windows for now */
#undef GPFXDIST
#endif

/*#include "c.h"*/
#ifdef WIN32
#define _WINSOCKAPI_
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#endif
#include <postgres.h>
#include <commands/copy.h>
#include <fstream/fstream.h>
#include <assert.h>
#include <glob.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#ifdef GPFXDIST
#include <gpfxdist.h>
#endif

#include <arpa/inet.h>

char* format_error(char* c1, char* c2);


/*
 * Returns a pointer to the end of the last delimiter occurrence,
 * or the start pointer if delimiter doesn't appear.
 *
 * delimiter_length MUST be > 0
 */
static char *find_last_eol_delim (const char *start, const int size,
								  const char *delimiter, const int delimiter_length)
{
	char* p;

	if (size <= delimiter_length)
		return (char*)start - 1;

	for (p = (char*)start + size - delimiter_length; start <= p; p--)
	{
		if (memcmp(p, delimiter, delimiter_length) == 0)
		{
			return (char*)p + delimiter_length - 1;
		}
	}
	return (char*)start - 1;
}

/*
 * Returns a pointer to the end of the first delimiter occurrence,
 * or the end pointer if delimiter doesn't appear.
 *
 * delimiter_length MUST be > 0
 */
static char *find_first_eol_delim (char *start, char *end,
								   const char *delimiter, const int delimiter_length)
{
	char *search_limit = (char*)end - delimiter_length;
	if (end - start <= delimiter_length)
		return end;

	for (; start <= search_limit; start++)
	{
		if (memcmp(start, delimiter, delimiter_length) == 0)
			return (char*)start + delimiter_length - 1;
	}

	return end;
}

/*
 * glob_and_copy
 *
 * Given a pattern, glob it and populate pglob with the results that match it.
 * For example, if 'pattern' equals '/somedir/<asterisk>.txt' and the filesystem has
 * 2 .txt files inside of somedir, namely 1.txt and 2.txt, then the result is:
 * g.gl_pathc = 2
 * g.gl_pathv[0] =  '/somedir/1.txt'
 * g.gl_pathv[1] =  '/somedir/2.txt'
 */
static int glob_and_copy(const char *pattern, int flags, int(*errfunc)(
						 const char *epath, int eerno), glob_and_copy_t *pglob)
{
	glob_t 	g;
	int 	i = glob(pattern, flags, errfunc, &g), j;

	if (!i)
	{
		char **a = gfile_malloc(sizeof *a * (pglob->gl_pathc + g.gl_pathc));

		if (!a)
			i = GLOB_NOSPACE;
		else
		{
			for (j = 0; j < pglob->gl_pathc; j++)
				a[j] = pglob->gl_pathv[j];
			if (pglob->gl_pathv)
				gfile_free(pglob->gl_pathv);
			pglob->gl_pathv = a;
			a += pglob->gl_pathc;
			for (j = 0; j < g.gl_pathc; j++)
			{
				char*b = g.gl_pathv[j];
				if (!(*a = gfile_malloc(strlen(b) + 1)))
				{
					i = GLOB_NOSPACE;
					break;
				}
				strcpy(*a++, b);
				pglob->gl_pathc++;
			}
		}

		globfree(&g);
	}
	return i;
}

/*
 * glob_and_copyfree
 *
 * free memory allocated in pglob.
 */
static void glob_and_copyfree(glob_and_copy_t *pglob)
{
	if (pglob->gl_pathv)
	{
		int i;

		for (i = 0; i < pglob->gl_pathc; i++)
			gfile_free(pglob->gl_pathv[i]);

		gfile_free(pglob->gl_pathv);
		pglob->gl_pathc = 0;
		pglob->gl_pathv = 0;
	}
}

static void glob_and_copyfree_sftp(glob_and_copy_t *pglob)
{
	if (pglob->gl_pathc)
	{
		int i;

		for (i = 0; i < pglob->gl_pathc; i++)
		{
			gfile_free(pglob->gl_pathv[i]);
			gfile_free(pglob->gl_username[i]);
			gfile_free(pglob->gl_passwd[i]);
			gfile_free(pglob->gl_hostaddr[i]);
			gfile_free(pglob->gl_port[i]);
		}

		gfile_free(pglob->gl_pathv);
		gfile_free(pglob->gl_username);
		gfile_free(pglob->gl_passwd);
		gfile_free(pglob->gl_hostaddr);
		gfile_free(pglob->gl_port);

		pglob->gl_pathc = 0;
		pglob->gl_pathv = 0;
		pglob->gl_username = 0;
		pglob->gl_passwd = 0;
		pglob->gl_hostaddr = 0;
		pglob->gl_port = 0;
	}
}

const char*
fstream_get_error(fstream_t*fs)
{
	return fs->ferror;
}

/*
 * scan_csv_records
 *
 * Scan the data in [p, q) according to the csv parsing rules. Return as many
 * complete csv records as possible. However, if 'one' was passed in, return the
 * first complete record only.
 *
 * We need this function for gpfdist because until 'text' format we can't just
 * peek at the line newline in the buffer and send the whole data chunk to the
 * server. That is because it may be inside a quote. We have to carefully parse
 * the data from the start in order to find the last unquoted newline.
 *
 */

static char*
scan_csv_records_crlf(char *p, char* q, int one, fstream_t* fs)
{
	int 	in_quote = 0;
	int 	last_was_esc = 0;
	int 	qc = fs->options.quote;
	int 	xc = fs->options.escape;
	char*	last_record_loc = 0;
	int 	ch = 0;
	int 	lastch = 0;

	while (p < q)
	{
		lastch = ch;
		ch = *p++;

		if (in_quote)
		{
			if (!last_was_esc)
			{
				if (ch == qc)
					in_quote = 0;
				else if (ch == xc)
					last_was_esc = 1;
			}
			else
				last_was_esc = 0;
		}
		else if (ch == '\n' && lastch == '\r')
		{
			last_record_loc = p;
			fs->line_number++;
			if (one)
				break;
		}
		else if (ch == qc)
			in_quote = 1;
	}

	return last_record_loc;
}

static char*
scan_csv_records_cr_or_lf(char *p, char *q, int one, fstream_t *fs, int nc)
{
	int 	in_quote = 0;
	int 	last_was_esc = 0;
	int 	qc = fs->options.quote;
	int 	xc = fs->options.escape;
	char*	last_record_loc = 0;

	while (p < q)
	{
		int ch = *p++;

		if (in_quote)
		{
			if (!last_was_esc)
			{
				if (ch == qc)
					in_quote = 0;
				else if (ch == xc)
					last_was_esc = 1;
			}
			else
				last_was_esc = 0;
		}
		else if (ch == nc)
		{
			last_record_loc = p;
			fs->line_number++;
			if (one)
				break;
		}
		else if (ch == qc)
			in_quote = 1;
	}

	return last_record_loc;
}

static char*
scan_csv_records(char *p, char *q, int one, fstream_t *fs)
{
	switch(fs->options.eol_type)
	{
		case EOL_CRNL:
		   return scan_csv_records_crlf(p, q, one, fs);
		case EOL_CR:
		   return scan_csv_records_cr_or_lf(p, q, one, fs, '\r');
		case EOL_NL:
		default:
		   return scan_csv_records_cr_or_lf(p, q, one, fs, '\n');
	}
}

#ifdef GPFXDIST
static void get_errmsg_from_writable_transform_errfile(gfile_t* fd, char *buf, apr_size_t nbytes)
{
	apr_status_t rv;
	apr_file_t* errfile = fd->transform->errfile;
	if (!errfile) {
		return;
	}
	/*
 	 * Close and then re-open (for reading) the temporary file we've used to capture stderr
 	 */
	apr_file_flush(errfile);
	apr_file_close(errfile);

	if ((rv = apr_file_open(&errfile, fd->transform->errfilename,
							APR_READ|APR_BUFFERED, APR_UREAD, fd->transform->mp)) == APR_SUCCESS)
	{
		rv = apr_file_read(errfile, buf, &nbytes);
		if (rv == APR_SUCCESS)
		{
			if (nbytes > 0)
			{
				buf[nbytes] = '\0';
			}
		}
		apr_file_close(errfile);
	}
	else
	{
		snprintf(buf, nbytes, "Loading error message from file %s failed.", fd->transform->errfilename);
	}
}
#endif

/* close the file stream and return error number */
int fstream_close_with_error(fstream_t* fs, char* error)
{
	int is_error = 0;
#ifdef GPFXDIST
	/*
	 * remove temporary file we created to hold the file paths
	 */
	if (fs->options.transform && fs->options.transform->tempfilename)
	{
		apr_file_remove(fs->options.transform->tempfilename, fs->options.transform->mp);
		fs->options.transform->tempfilename = NULL;
	}
#endif

	if(fs->buffer)
		gfile_free(fs->buffer);

	if (fs->fd.is_sftp)
	{
		glob_and_copyfree_sftp(&fs->glob);
	}
	else 
		glob_and_copyfree(&fs->glob);
	gfile_close(&fs->fd);
#ifdef GPFXDIST
		/* 
		 * For writable transform we should check error file content during fstream close.
		 * If there is an error, we should pass the error to the caller.
		 */
		if (fs->options.forwrite && fs->fd.transform)
		{
			char errmsg_buffer[FILE_ERROR_SZ];
			memset(errmsg_buffer, 0, FILE_ERROR_SZ);
			get_errmsg_from_writable_transform_errfile(&fs->fd, errmsg_buffer, FILE_ERROR_SZ-1);
			strcpy(error, errmsg_buffer);
			if (strlen(error))
			{
				is_error = 1;
			}
			/* clear the errfile after fstream close. */
			if (fs->fd.transform->errfile)
			{
				apr_file_close(fs->fd.transform->errfile);
				apr_file_remove(fs->fd.transform->errfilename, fs->fd.transform->mp);
				fs->fd.transform->errfile = NULL;
				fs->fd.transform->errfilename = NULL;
			}
		}
#endif
	gfile_free(fs);
	return is_error;
}

/* close the file stream */
void fstream_close(fstream_t* fs)
{
	char errmsg_buffer[FILE_ERROR_SZ];
	fstream_close_with_error(fs, errmsg_buffer);
}

static int fpath_all_directories(const glob_and_copy_t *glob)
{
	int i;

	for (i = 0; i < glob->gl_pathc; i++)
	{
		const char	*a = glob->gl_pathv[i];

		if (!*a || a[strlen(a) - 1] != '/')
			return 0;
	}
	return 1;
}

static int expand_directories(fstream_t *fs)
{
	glob_and_copy_t g;
	int 			i, j;

	memset(&g, 0, sizeof g);

	for (i = 0; i < fs->glob.gl_pathc; i++)
	{
		const char* a = fs->glob.gl_pathv[i];
		char* b = gfile_malloc(2 * strlen(a) + 2), *c;

		if (!b)
		{
			gfile_printf_then_putc_newline("fstream out of memory");
			glob_and_copyfree(&g);
			return 1;
		}

		for (c = b ; *a ; a++)
		{
			if (*a == '?' || *a == '*' || *a == '[')
				*c++ = '\\';
			*c++ = *a;
		}

		*c++ = '*';
		*c++ = 0;
		j = glob_and_copy(b, 0, 0, &g);
		gfile_free(b);

		if (j == GLOB_NOMATCH)
			gfile_printf_then_putc_newline("fstream %s is empty directory",
					fs->glob.gl_pathv[i]);
		else if (j)
		{
			gfile_printf_then_putc_newline("fstream glob failed");
			glob_and_copyfree(&g);
			return 1;
		}
	}

	glob_and_copyfree(&fs->glob);
	fs->glob = g;
	return 0;
}

static int glob_path(fstream_t *fs, const char *path)
{
	char	*path2 = gfile_malloc(strlen(path) + 1);

	if (!path2)
	{
		gfile_printf_then_putc_newline("fstream out of memory");
		return 1;
	}

	path = strcpy(path2, path);

	do
	{
		char*	p;

		while (*path == ' ')
			path++;

		p = strchr(path, ' ');

		if (p)
			*p++ = 0;

		if (*path &&
			glob_and_copy(path, GLOB_MARK | GLOB_NOCHECK, 0, &fs->glob))
		{
			gfile_printf_then_putc_newline("fstream glob failed");
			gfile_free(path2);
			return 1;
		}
		path = p;
	} while (path);

	gfile_free(path2);

	return 0;
}

int get_sftp_counts(const char *sftp_request)
{
	const char *start;
	const char *end;
	int count1 = 0;
	int count2 = 0;

	start = (char *)sftp_request;
	end = (char *)sftp_request;

	while (*start || *end)
	{
		if (*start == '<')
			count1++;
		start++;
		if (*end == '>')
			count2++;
		end++;
	}

	if (count1 != count2)
		return -1;

	return count1;
}

int ParseFilePathUri(char *uri_str, sftp_info_t *info)
{

	int protocol_len = 0;
	int len = 0;
	char *start, *end;

	if (strncmp(uri_str, "sftp://", 7) == 0)
	{
		protocol_len = 7;
	}
	else
	{
		return -2;
	}

	start = (char *)uri_str + protocol_len;
	end = strchr(start, ':');
	if (end == NULL)
	{

		return -2;
	}
	else
	{
		len = end - start;

		char *username = NULL;
		username = (char *)gfile_malloc(len + 1);
		if (username == NULL)
		{
			gfile_printf_then_putc_newline("out of memory");
			return -1;
		}
		strncpy(username, start, len);
		username[len] = '\0';
		strcpy(info->username, username);
		gfile_free(username);
	}
	start = end + 1;
	end = strchr(start, '@');

	if (end == NULL)
	{
		return -2;
	}
	else
	{
		len = end - start;

		char *passwd = NULL;
		passwd = (char *)gfile_malloc(len + 1);
		if (passwd == NULL)
		{
			gfile_printf_then_putc_newline("out of memory");
			return -1;
		}
		strncpy(passwd, start, len);
		passwd[len] = '\0';
		strcpy(info->password, passwd);
		gfile_free(passwd);
	}

	start = end + 1;
	if (strncmp(start, "[", 1) == 0)
	{
		end = strchr(start, ']');
		if (end == NULL)
			return -2;

		len = end - start;
		char *hostaddr6 = NULL;
		hostaddr6 = (char *)gfile_malloc(len);
		if (hostaddr6 == NULL)
		{
			gfile_printf_then_putc_newline("out of memory");
			return -1;
		}
		strncpy(hostaddr6, start + 1, len - 1);
		hostaddr6[len - 1] = '\0';
		strcpy(info->hostaddr, hostaddr6);
		gfile_free(hostaddr6);

		end++;
	}

	else
	{
		end = strchr(start, ':');
		if (end == NULL)
		{
			return -2;
		}
		else
		{
			len = end - start;

			char *hostaddr4 = NULL;
			hostaddr4 = (char *)gfile_malloc(len + 1);
			if (hostaddr4 == NULL)
			{
				gfile_printf_then_putc_newline("out of memory");
				return -1;
			}
			strncpy(hostaddr4, start, len);
			hostaddr4[len] = '\0';
			strcpy(info->hostaddr, hostaddr4);
			gfile_free(hostaddr4);
		}
	}

	start = end + 1;
	end = strchr(start, '/');
	if (end == NULL)
	{
		return -2;
	}
	else
	{
		len = end - start;

		char *port = NULL;
		port = (char *)gfile_malloc(len + 1);
		if (port == NULL)
		{
			gfile_printf_then_putc_newline("out of memory");
			return -1;
		}
		strncpy(port, start, len);
		port[len] = '\0';
		strcpy(info->port, port);
		gfile_free(port);
	}

	start = end;
	strcpy(info->fpath, start);

	return 0;
}

static int order_by_hostaddr(const void *e1, const void *e2)
{
	struct in_addr ip1;
	struct in_addr ip2;
	int result = 0;

	inet_pton(AF_INET, ((struct sftp_info_t *)e1)->hostaddr, &ip1);
	inet_pton(AF_INET, ((struct sftp_info_t *)e2)->hostaddr, &ip2);

	result = memcmp(&ip1, &ip2, sizeof(struct in_addr));

	if (result > 0)
		return 1;
	else if (result < 0)
		return -1;
	else
		return 0;
}

static int fetch_sftp_paths(fstream_t *fs, const char *path)
{
	int counts = get_sftp_counts(path);
	if(counts == -1)
	{
		gfile_printf_then_putc_newline("sftp path is Non-conforming, please check ");
		return 1;
	}

	if(counts == 0)
	{
		gfile_printf_then_putc_newline("sftp path is empty");
		return 1;
	}
	char **res = (char **)malloc(sizeof(char *) * counts);

	char **unames = (char **)gfile_malloc(sizeof(char *) * counts);
	char **passwds = (char **)gfile_malloc(sizeof(char *) * counts);
	char **hostaddrs = (char **)gfile_malloc(sizeof(char *) * counts);
	char **ports = (char **)gfile_malloc(sizeof(char *) * counts);
	char **fpaths = (char **)gfile_malloc(sizeof(char *) * counts);

	if (!unames || !passwds || !hostaddrs || !ports || !fpaths)
	{
		gfile_printf_then_putc_newline("out of memory");
		return 1;
	}

	sftp_info_t sftp_info_lists[64];

	fs->glob.gl_pathc = 0;
	fs->glob.gl_username = unames;
	fs->glob.gl_passwd = passwds;
	fs->glob.gl_hostaddr = hostaddrs;
	fs->glob.gl_port = ports;
	fs->glob.gl_pathv = fpaths;

	{
		const char *start;
		const char *end;
		start = (char *)path;
		end = (char *)path;

		for (int i = 0; i < counts; i++)
		{
			while (*start != '<')
				start++;
			while (*end != '>')
				end++;
			int len = end - start - 1;
			char *req = (char *)gfile_malloc(len + 1);
			if (!req)
			{
				gfile_printf_then_putc_newline("out of memory");
				return 1;
			}
			strncpy(req, start + 1, len);
			req[len] = '\0';
			res[i] = strdup(req);
			gfile_free(req);
			start++;
			end++;
		}
	}

	for (int i = 0; i < counts; i++)
	{
		int parse_result = ParseFilePathUri(res[i], &sftp_info_lists[i]);
		if(parse_result == 0)
			continue;
		else
		{
			for (int j = 0; j < counts; j++)
			{
				if(res[i])
					free(res[i]);
			}
			free(res);
		}
	}

	qsort(sftp_info_lists, counts, sizeof(sftp_info_t), order_by_hostaddr);

	for (int j = 0; j < counts; j++)
	{
		char *tmp_uname = sftp_info_lists[j].username;
		char *tmp_passwd = sftp_info_lists[j].password;
		char *tmp_hostaddr = sftp_info_lists[j].hostaddr;
		char *tmp_port = sftp_info_lists[j].port;
		char *tmp_fpath = sftp_info_lists[j].fpath;

		*unames = (char *)gfile_malloc(strlen(tmp_uname) + 1);
		*passwds = (char *)gfile_malloc(strlen(tmp_passwd) + 1);
		*hostaddrs = (char *)gfile_malloc(strlen(tmp_hostaddr) + 1);
		*ports = (char *)gfile_malloc(strlen(tmp_port) + 1);
		*fpaths = (char *)gfile_malloc(strlen(tmp_fpath) + 1);

		if (!(*unames) || !(*passwds) || !(*hostaddrs) || !(*ports) || !(*fpaths))
		{
			gfile_printf_then_putc_newline("out of memory!!!");
			return 1;
		}

		strcpy(*unames++, tmp_uname);
		strcpy(*passwds++, tmp_passwd);
		strcpy(*hostaddrs++, tmp_hostaddr);
		strcpy(*ports++, tmp_port);
		strcpy(*fpaths++, tmp_fpath);

		fs->glob.gl_pathc++;
	}

	for (int i = 0; i < counts; i++)
	{
		free(res[i]);
	}
	free(res);
	return 0;
}

#ifdef GPFXDIST
/*
 * adjust fs->glob so that it contains a single item which is a
 * properly allocated copy of the specified filename.  assumes fs->glob
 * contains at least one name.  
 */

static int glob_adjust(fstream_t* fs, char* filename, int* response_code, const char** response_string)
{
	int i;
	int tlen    = strlen(filename) + 1;
	char* tname = gfile_malloc(tlen);
	if (!tname) 
	{
		*response_code = 500;
		*response_string = "fstream out of memory allocating copy of temporary file name";
		gfile_printf_then_putc_newline("%s", *response_string);
		return 1;
	}

	for (i = 0; i<fs->glob.gl_pathc; i++)
	{
		gfile_free(fs->glob.gl_pathv[i]);
		fs->glob.gl_pathv[i] = NULL;
	}
	strcpy(tname, filename);
	fs->glob.gl_pathv[0] = tname;
	fs->glob.gl_pathc = 1;
	return 0;
}
#endif

/*
 * fstream_open
 *
 * Allocate a new file stream given a path (a url). in case of wildcards,
 * expand them here. we end up with a final list of files and include them
 * in our filestream that we return.
 *
 * In case of errors we set the proper http response code to send to the client.
 */
fstream_t*
fstream_open(const char *path, const struct fstream_options *options,
			 int *response_code, const char **response_string)
{
	int i;
	fstream_t* fs;
	bool_t is_sftp = FALSE;

	*response_code = 500;
	*response_string = "Internal Server Error";

	if (0 == (fs = gfile_malloc(sizeof *fs)))
	{
		gfile_printf_then_putc_newline("fstream out of memory");
		return 0;
	}

	memset(fs, 0, sizeof *fs);
	fs->options = *options;
	fs->buffer = gfile_malloc(options->bufsize);
	
	if (strncmp(path, "/<sftp://", 9) == 0)
	{
		is_sftp = TRUE;
	}

	/*
	 * get a list of all files that were requested to be read and include them
	 * in our fstream. This includes any wildcard pattern matching.
	 */
	if (is_sftp)
	{
		if (fetch_sftp_paths(fs, path))
		{
			fstream_close(fs);
			return 0;
		}
	}

	else
	{
		if (glob_path(fs, path))
		{
			fstream_close(fs);
			return 0;
		}

		/*
	 * If the list of files in our filestrem includes a directory name, expand
	 * the directory and add all the files inside of it.
	 */
		if (fpath_all_directories(&fs->glob))
		{
			if (expand_directories(fs))
			{
				fstream_close(fs);
				return 0;
			}
		}
	}

	/*
	 * check if we don't have any matching files
	 */
	if (fs->glob.gl_pathc == 0)
	{
		if(!is_sftp)
		{
			gfile_printf_then_putc_newline("fstream bad path: %s", path);
			fstream_close(fs);
			*response_code = 404;
			*response_string = "No matching file(s) found";
			return 0;
		}
	}

	if (fs->glob.gl_pathc != 1 && options->forwrite)
	{
		gfile_printf_then_putc_newline("fstream open for write found more than one file (%d)",
										fs->glob.gl_pathc);
		*response_code = 404;
		*response_string = "More than 1 file found for writing. Unsupported operation.";

		fstream_close(fs);
		return 0;
	}


#ifdef GPFXDIST
	/*
	 * when the subprocess transformation wants to handle iteration over the files
	 * we write the paths to a temporary file and replace the fs->glob items with
	 * just a single entry referencing the path to the temporary file.
	 */
	if (options->transform && options->transform->pass_paths)
	{
		apr_pool_t*  mp = options->transform->mp;
		apr_file_t*  f = NULL;
		const char*  tempdir = NULL;
		char*        tempfilename = NULL;
		apr_status_t rv;

		if ((rv = apr_temp_dir_get(&tempdir, mp)) != APR_SUCCESS)
		{
			*response_code = 500;
			*response_string = "failed to get temporary directory for paths file";
			gfile_printf_then_putc_newline("%s", *response_string);
			fstream_close(fs);
			return 0;
		}

	    tempfilename = apr_pstrcat(mp, tempdir, "/pathsXXXXXX", (char *) NULL);
		if ((rv = apr_file_mktemp(&f, tempfilename, APR_CREATE|APR_WRITE|APR_EXCL, mp)) != APR_SUCCESS)
		{
			*response_code = 500;
			*response_string = "failed to open temporary paths file";
			gfile_printf_then_putc_newline("%s", *response_string);
			fstream_close(fs);
			return 0;
		}

		options->transform->tempfilename = tempfilename;

		for (i = 0; i<fs->glob.gl_pathc; i++)
		{
			char* filename      = fs->glob.gl_pathv[i];
			apr_size_t expected = strlen(filename) + 1;
			
			if (apr_file_printf(f, "%s\n", filename) < expected)
			{
				apr_file_close(f);

				*response_code = 500;
				*response_string = "failed to fully write path to temporary paths file";
				gfile_printf_then_putc_newline("%s", *response_string);
				fstream_close(fs);
				return 0;
			}
		}

		apr_file_close(f);

		if (glob_adjust(fs, tempfilename, response_code, response_string)) 
		{
			fstream_close(fs);
			return 0;
		}
	}
#endif

	/*
	 * if writing - check write access rights for the one file.
	 * if reading - check read access right for all files, and 
	 * then close them, leaving the first file open.
	 * 
	 */
	for (i = fs->glob.gl_pathc; --i >= 0;)
	{
		/*
		 * CR-2173 - the fstream code allows the upper level logic to treat a
		 * collection of input sources as a single stream.  One problem it has
		 * to handle is the possibility that some of the underlying sources may
		 * not be readable.  Here we're trying to detect potential problems in
		 * advance by checking that we can open and close each source in our
		 * list in reverse order.
		 *
		 * However in the case of subprocess transformations, we don't want to 
		 * start and stop each transformation in this manner.  The check that 
		 * each transformation's underlying input source can be read is still 
		 * useful so we do those until we get to the first source, at which 
		 * point we proceed to just setup the tranformation for it.
		 */
		struct gpfxdist_t* transform = (i == 0) ? options->transform : NULL;

		gfile_close(&fs->fd);
#ifdef LIBSSH2
		if (is_sftp)
		{
			if (gfile_open_sftp(&fs->fd, fs->glob.gl_pathv[i], fs->glob.gl_username[i], fs->glob.gl_passwd[i],
								fs->glob.gl_hostaddr[i], fs->glob.gl_port[i], gfile_open_flags(options->forwrite, options->usesync), response_code, response_string, transform))
			{
				gfile_printf_then_putc_newline("fstream unable to open file %s", fs->glob.gl_pathv[i]);
				fstream_close(fs);
				return 0;
			}
		}
#endif
		else
		{
			if (gfile_open(&fs->fd, fs->glob.gl_pathv[i], gfile_open_flags(options->forwrite, options->usesync),
						   response_code, response_string, transform))
			{
				gfile_printf_then_putc_newline("fstream unable to open file %s",
											   fs->glob.gl_pathv[i]);
				fstream_close(fs);
				return 0;
			}
		}

		fs->compressed_size += gfile_get_compressed_size(&fs->fd);
	}

	fs->line_number = 1;
	fs->skip_header_line = options->header;

	return fs;
}

/*
 * Updates the currently used filename and line number and offset. Since we
 * may be reading from more than 1 file, we need to be up to date all the time.
 */
static void updateCurFileState(fstream_t* fs,
							   struct fstream_filename_and_offset* fo)
{
	if (fo)
	{
		fo->foff = fs->foff;
		fo->line_number = fs->line_number;
		strncpy(fo->fname, fs->glob.gl_pathv[fs->fidx], sizeof fo->fname);
		fo->fname[sizeof fo->fname - 1] = 0;
	}
}

/*
 * nextFile
 *
 * open the next source file, if any.
 *
 * return 1 if could not open the next file.
 * return 0 otherwise.
 */
static int nextFile(fstream_t*fs)
{
	int response_code;
	const char	*response_string;
	struct gpfxdist_t* transform = fs->options.transform;

	fs->compressed_position += gfile_get_compressed_size(&fs->fd);
	gfile_close(&fs->fd);
	fs->foff = 0;
	fs->line_number = 1;
	fs->fidx++;

	if (fs->fidx < fs->glob.gl_pathc)
	{
		fs->skip_header_line = fs->options.header;
#ifdef LIBSSH2
		if (fs->fd.is_sftp)
		{
			if (gfile_open_sftp(&fs->fd, fs->glob.gl_pathv[fs->fidx], fs->glob.gl_username[fs->fidx], fs->glob.gl_passwd[fs->fidx],
								fs->glob.gl_hostaddr[fs->fidx], fs->glob.gl_port[fs->fidx], GFILE_OPEN_FOR_READ, &response_code, &response_string, transform))
			{
				gfile_printf_then_putc_newline("fstream unable to open file %s",
											   fs->glob.gl_pathv[fs->fidx]);
				fs->ferror = "unable to open file";
				return 1;
			}
		}
#else
		else
		{
			if (gfile_open(&fs->fd, fs->glob.gl_pathv[fs->fidx], GFILE_OPEN_FOR_READ,
						   &response_code, &response_string, transform))
			{
				gfile_printf_then_putc_newline("fstream unable to open file %s",
											   fs->glob.gl_pathv[fs->fidx]);
				fs->ferror = "unable to open file";
				return 1;
			}
		}
#endif
	}

	return 0;
}

/*
 * format_error
 * enables addition of string parameters to the const char* error message in fstream_t
 * while enabling the calling functions not to worry about freeing memory - which is 
 * the present behaviour
 */
char* format_error(char* c1, char* c2)
{
	int len1, len2;
	
	static char err_msg[FILE_ERROR_SZ];
	memset(err_msg, 0, FILE_ERROR_SZ);
	
	len1 = strlen(c1);
	len2 = strlen(c2);
	if ( (len1 + len2) > FILE_ERROR_SZ )
	{
		gfile_printf_then_putc_newline("cannot read file");
		return "cannot read file";
	}
	
	char* targ = err_msg;
	memcpy(targ, c1, len1);
	targ += len1;
	memcpy(targ, c2, len2);
	
	gfile_printf_then_putc_newline("%s", err_msg);
	
	return err_msg;
}

/*
 * fstream_read
 *
 * Read 'size' bytes of data from the filestream into 'buffer'.
 * If 'read_whole_lines' is specified then read up to the last logical row
 * in the source buffer. 'fo' keeps the state (name, offset, etc) of the current
 * filestream file we are reading from.
 */
int fstream_read(fstream_t *fs,
				 void *dest,
				 int size,
				 struct fstream_filename_and_offset *fo,
				 const int read_whole_lines,
				 const char *line_delim_str,
				 const int line_delim_length)
{
	int buffer_capacity = fs->options.bufsize;
	static char err_buf[FILE_ERROR_SZ] = {0};
	
	if (fs->ferror)
		return -1;

	for (;;)
	{
		ssize_t bytesread; 		/* num bytes read from filestream */
		ssize_t bytesread2;		/* same, but when reading a second round */

		if (!size || fs->fidx == fs->glob.gl_pathc)
			return 0;

		/*
		 * If data source has a header, we consume it now and in order to
		 * move on to real data that follows it.
		 */
		if (fs->skip_header_line)
		{
			char* 	p = fs->buffer;
			char* 	q = p + fs->buffer_cur_size;
			size_t 	len = 0;

		    assert(fs->buffer_cur_size < buffer_capacity);

			/*
			 * read data from the source file and fill up the file stream buffer
			 */
			len = buffer_capacity - fs->buffer_cur_size;
			bytesread = gfile_read(&fs->fd, q, len);

			if (bytesread < 0)
			{
				fs->ferror = format_error("cannot read file - ", fs->glob.gl_pathv[fs->fidx]);
				return -1;
			}

			/* update the buffer size according to new byte count we just read */
			fs->buffer_cur_size += bytesread;
			q += bytesread;

			if (fs->options.is_csv)
			{
				/* csv header */
				p = scan_csv_records(p, q, 1, fs);
			}
			else
			{
				if (line_delim_length > 0)
				{
					/* text header with defined EOL */
					p = find_first_eol_delim (p, q, line_delim_str, line_delim_length);

				}
				else
				{
					/* text header with \n as delimiter (by default) */
					for (; p < q && *p != '\n'; p++)
						;
				}

				p = (p < q) ? p + 1 : 0;
				fs->line_number++;
			}

			if (!p)
			{
				if (fs->buffer_cur_size == buffer_capacity)
				{
					gfile_printf_then_putc_newline(
							"fstream ERROR: header too long in file %s",
							fs->glob.gl_pathv[fs->fidx]);
					
					fs->ferror = "line too long in file";
					return -1;
				}
				p = q;
			}

			/*
			 * update the filestream buffer offset to past last line read and
			 * copy the end of the buffer (past header data) to the beginning.
			 * we now bypassed the header data and can continue to real data.
			 */
			fs->foff += p - fs->buffer;
			fs->buffer_cur_size = q - p;
			memmove(fs->buffer, p, fs->buffer_cur_size);
			fs->skip_header_line = 0;
		}

		/*
		 * If we need to read all the data up to the last *complete* logical
		 * line in the data buffer (like gpfdist for example) - we choose this
		 * path. We grab the bigger chunk we can get that includes whole lines.
		 * Otherwise, if we just want the whole buffer we skip.
		 */
		if (read_whole_lines)
		{
			char	*p;
			ssize_t total_bytes = fs->buffer_cur_size;
			
			assert(size >= buffer_capacity);

			if (total_bytes > 0)
			{
				/*
				 * source buffer is not empty. copy the data from the beginning
				 * up to the current length before moving on to reading more
				 */
				fs->buffer_cur_size = 0;
				updateCurFileState(fs, fo);
				memcpy(dest, fs->buffer, total_bytes);
			}

			/* read more data from source file into destination buffer */
			bytesread2 = gfile_read(&fs->fd, (char*) dest + total_bytes, size - total_bytes);

			if (bytesread2 < 0)
			{
				fs->ferror = format_error("cannot read file - ", fs->glob.gl_pathv[fs->fidx]);
				return -1;
			}

			if (bytesread2 < size - total_bytes)
			{
				/*
				 * We didn't read as much as we asked for. Check why.
				 * We could be done reading data, we may need to move
				 * on the reading the next data file (if any).
				 */
				if (total_bytes == 0)
				{
					if (bytesread2 == 0)
					{
						if (nextFile(fs))
							return -1; /* found next file but failed to open */
						continue;
					}
					updateCurFileState(fs, fo);
				}

				/*
				 * try to open the next file if any, and return the number of
				 * bytes read to buffer earlier, if next file was found but
				 * could not open return -1
				 */
				return nextFile(fs) ? -1 : total_bytes + bytesread2;
			}

			updateCurFileState(fs, fo);

			/*
			 * Now that we have enough data in our filestream buffer, get a 
			 * chunk of whole rows and copy it into our dest buffer to be sent
			 * out later.
			 */
			if (fs->options.is_csv)
			{
				/* CSV: go slow, scan byte-by-byte for record boundary */
				p = scan_csv_records(dest, (char*)dest + size, 0, fs);
			}
			else
			{
				/*
				 * TEXT: go fast, scan for end of line delimiter (\n by default) for
				 * record boundary.
				 * find the last end of line delimiter from the back
				 */
				if (line_delim_length > 0)
				{
					p = find_last_eol_delim((char*)dest, size, line_delim_str, line_delim_length);
				}
				else
				{
					for (p = (char*)dest + size; (char*)dest <= --p && *p != '\n';)
						;
				}

				p = (char*)dest <= p ? p + 1 : 0;
				fs->line_number = 0;
			}

			/*
			 * could we not find even one complete row in this buffer? error.
			 */
			if (!p || (char*)dest + size >= p + buffer_capacity)
			{
#ifdef WIN32
				snprintf(err_buf, sizeof(err_buf)-1, "line too long in file %s near (%ld bytes)",
						 fs->glob.gl_pathv[fs->fidx], (long) fs->foff);
#else
				snprintf(err_buf, sizeof(err_buf)-1, "line too long in file %s near (%lld bytes)",
						 fs->glob.gl_pathv[fs->fidx], (long long) fs->foff);
#endif
				fs->ferror = err_buf;
				gfile_printf_then_putc_newline("%s", err_buf);
				return -1;
			}

			/* copy the result chunk of data into our buffer and we're done */
			fs->buffer_cur_size = (char*)dest + size - p;
			memcpy(fs->buffer, p, fs->buffer_cur_size);
			fs->foff += p - (char*)dest;

			return p - (char*)dest;
		}

		/*
		 * if we're here it means that we just want chunks of data and don't
		 * care if it includes whole rows or not (for example, backend url_read
		 * code - a segdb that reads all the data that gpfdist sent to it,
		 * buffer by buffer and then parses the lines internally).
		 */

		if (fs->buffer_cur_size)
		{
			ssize_t total_bytes = fs->buffer_cur_size;
			
			updateCurFileState(fs, fo);

			if (total_bytes > size)
				total_bytes = size;

			memcpy(dest, fs->buffer, total_bytes);
			fs->buffer_cur_size -= total_bytes;
			memmove(fs->buffer, fs->buffer + total_bytes, fs->buffer_cur_size);

			fs->foff += total_bytes;
			fs->line_number = 0;

			return total_bytes;
		}

		bytesread = gfile_read(&fs->fd, dest, size);

		if (bytesread < 0)
		{
			fs->ferror = format_error("cannot read file - ", fs->glob.gl_pathv[fs->fidx]);
			
			return -1;
		}

		if (bytesread)
		{
			updateCurFileState(fs, fo);
			fs->foff += bytesread;
			fs->line_number = 0;
			return bytesread;
		}

		if (nextFile(fs))
			return -1;
	}
}

int fstream_write(fstream_t *fs,
				  void *buf,
				  int size,
				  const int write_whole_lines,
				  const char* line_delim_str,
				  const int line_delim_length)
{
	int byteswritten = 0;
	
	if (fs->ferror)
		return -1;

	/*
	 * If we need to write all the data up to the last *complete* logical
	 * line in the data buffer (like gpfdist for example) - we choose this
	 * path (we don't want to write partial lines to the output file). 
	 * We grab the bigger chunk we can get that includes whole lines.
	 * Otherwise, if we just want the whole buffer we skip.
	 */	
	if (write_whole_lines)
	{
		char* last_delim;

	   /*
		* scan for EOL Delimiter (\n by default) for record boundary
		* find the last EOL Delimiter from the back
		*/
		if (line_delim_length > 0)
		{
			last_delim = find_last_eol_delim(buf, size, line_delim_str, line_delim_length);
		}
		else
		{
			for (last_delim = (char*)buf + size; (char*)buf <= --last_delim && *last_delim != '\n';)
				;
		}
		last_delim = (char*)buf <= last_delim ? last_delim + 1 : 0;

		if (last_delim == 0)
		{
			fs->ferror = "no complete data row found for writing";
			return -1;
		}
				
		/* TODO: need to do this more carefully for CSV, like in the read case */
		
                size = last_delim - (char *) buf;
		/* caller should move leftover data to start of buffer */
        }

	/* write data to destination file */
	byteswritten = (int)gfile_write(&fs->fd, (char*) buf, size);

	if (byteswritten < 0)
	{
		#ifdef GPFXDIST
			/* 
			 * If we've been requested to send stderr output to the server,
			 * we should always check errorfile.
			 */
			if (fs->fd.transform && fs->fd.transform->errfile)
			{
				static char errmsg_buffer[FILE_ERROR_SZ];
				memset(errmsg_buffer, 0, FILE_ERROR_SZ);
				get_errmsg_from_writable_transform_errfile(&fs->fd, errmsg_buffer, FILE_ERROR_SZ-1);
				fs->ferror = errmsg_buffer;
			}
			else
		#endif
			{
				gfile_printf_then_putc_newline("cannot write into file, byteswritten=%d, size=%d, errno=%d, errmsg=%s",
											byteswritten, size, errno, strerror(errno));
				fs->ferror = "cannot write into file";
			}
			return -1;
	}

	return byteswritten;
	
}
int fstream_eof(fstream_t *fs)
{
	return fs->fidx == fs->glob.gl_pathc;
}

int64_t fstream_get_compressed_size(fstream_t *fs)
{
	return fs->compressed_size;
}

int64_t fstream_get_compressed_position(fstream_t *fs)
{
	int64_t p = fs->compressed_position;
	if (fs->fidx != fs->glob.gl_pathc)
		p += gfile_get_compressed_position(&fs->fd);
	return p;
}

bool_t fstream_is_win_pipe(fstream_t *fs)
{
	return fs->fd.is_win_pipe;
}


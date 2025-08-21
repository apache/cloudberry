/*-------------------------------------------------------------------------
 *
 * kmgr_utils.c
 *	  Shared frontend/backend for cluster file encryption
 *
 * These functions handle reading the wrapped DEK files from the
 * file system, and wrapping and unwrapping them.  It also handles
 * running the cluster_key_command.
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/common/kmgr_utils.c
 *
 *-------------------------------------------------------------------------
 */

#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#ifdef FRONTEND
#include "common/logging.h"
#endif
#include "common/cryptohash.h"
#include "common/file_perm.h"
#include "common/string.h"
#include "crypto/kmgr.h"
#include "lib/stringinfo.h"
#include "storage/fd.h"
#include "postgres.h"
#include "utils/builtins.h"

#ifndef FRONTEND
#include "pgstat.h"
#include "storage/fd.h"
#endif

#define KMGR_PROMPT_MSG "Enter authentication needed to generate the cluster key: "

#ifdef FRONTEND
static FILE *open_pipe_stream(const char *command);
static int	close_pipe_stream(FILE *file);
#endif

static void read_wrapped_data_key(const char *cryptoKeyDir, uint32 id, unsigned char **key_p, int *key_len);

/*
 * We need this array in frontend and backend code, so define it here.
 * You can only add to the end of this array since the array index is
 * stored in pg_control.
 */
encryption_method encryption_methods[NUM_ENCRYPTION_METHODS] = {
	{"", 0},
	{"AES128", 128},
	{"AES192", 192},
	{"AES256", 256},
	{"SM4", 128}
};

/* This maps wrapped key filesnames to their array slots */
char	   *wkey_filenames[KMGR_NUM_DATA_KEYS] = {
	"relation",
	"wal"
};


/*
 * Wrap the given CryptoKey.
 *
 * Returns true and writes encrypted/wrapped/padded data to 'out', and the length
 * of the result to outlen, if successful.
 *
 * Otherwise returns false. The caller must allocate sufficient space
 * for cipher data calculated by using KmgrSizeOfCipherText(). Please note that
 * this function modifies 'out' data even on failure.
 */
bool
kmgr_wrap_data_key(PgCipherCtx *ctx, CryptoKey *in, unsigned char *out, int *outlen)
{
	Assert(ctx && in && out);

	if (!pg_cipher_keywrap(ctx, (unsigned char *) in, sizeof(CryptoKey), out, outlen))
		return false;

	return true;
}

/*
 * Decrypt the given data. Return true and set plain text data to `out` if
 * successful.  Otherwise return false. The caller must allocate sufficient
 * space for cipher data calculated by using KmgrSizeOfPlainText(). Please
 * note that this function modifies 'out' data even on failure.
 */
bool
kmgr_unwrap_data_key(PgCipherCtx *ctx, unsigned char *in, int inlen, CryptoKey *out)
{
	int			outlen;
	int 		out_buffer_len;
	unsigned char *out_buffer;

	/*
	 * When call EVP_DecryptUpdate,
	 * We need to alloc enough buffer
	 * More detail info see
	 * https://www.openssl.org/docs/man3.1/man3/EVP_DecryptUpdate.html
	 */
	out_buffer_len = pg_cipher_blocksize(ctx) + inlen;
	out_buffer = (unsigned char *)palloc0(out_buffer_len);

	Assert(ctx && in && out);

	if (!pg_cipher_keyunwrap(ctx, in, inlen, (unsigned char *) out_buffer, &outlen))
		return false;

	Assert(outlen == sizeof(CryptoKey));

	memcpy(out, out_buffer, sizeof(CryptoKey));

	pfree(out_buffer);

	return true;
}

/*
 * Verify the correctness of the given cluster key by unwrapping the given keys.
 * If the given cluster key is correct we set unwrapped keys to out_keys and return
 * true.  Otherwise return false.  Please note that this function changes the
 * contents of out_keys even on failure.  Both in_keys and out_keys must be the
 * same length.
 */
bool
kmgr_verify_cluster_key(unsigned char *cluster_key,
						unsigned char **in_keys, int *key_lens, CryptoKey *out_keys)
{
	PgCipherCtx *ctx;

	/* Create decryption context with the KEK. */
	ctx = pg_cipher_ctx_create(PG_CIPHER_AES_KWP, cluster_key,
							   KMGR_CLUSTER_KEY_LEN, false);

	/* unwrap each DEK */
	for (int i = 0; i < KMGR_NUM_DATA_KEYS; i++)
	{
		if (!kmgr_unwrap_data_key(ctx, in_keys[i], key_lens[i], &(out_keys[i])))
		{
			/* The cluster key is not correct */
			pg_cipher_ctx_free(ctx);
			return false;
		}
		explicit_bzero(in_keys[i], key_lens[i]);
	}

	/* The cluster key is correct, free the cipher context */
	pg_cipher_ctx_free(ctx);

	return true;
}

/*
 * Run cluster key command.
 *
 * Substitute %d for directory, %p for prompt, %R for file descriptor.
 *
 * The result will be put in buffer buf, which is of size "size".
 * The return value is the length of the actual result.
 */
int
kmgr_run_cluster_key_command(char *cluster_key_command, char *buf,
							 int size, char *dir, int terminal_fd)
{
	StringInfoData command;
	const char *sp;
	FILE	   *fh;
	int			pclose_rc;
	size_t		len = 0;

	buf[0] = '\0';

	Assert(size > 0);

	/*
	 * Build the command to be executed.
	 */
	initStringInfo(&command);

	for (sp = cluster_key_command; *sp; sp++)
	{
		if (*sp == '%')
		{
			switch (sp[1])
			{
					/* directory */
				case 'd':
					{
						char	   *nativePath;

						sp++;

						/*
						 * This needs to use a placeholder to not modify the
						 * input with the conversion done via
						 * make_native_path().
						 */
						nativePath = pstrdup(dir);
						make_native_path(nativePath);
						appendStringInfoString(&command, nativePath);
						pfree(nativePath);
						break;
					}
					/* prompt string */
				case 'p':
					appendStringInfoString(&command, " ");
					sp++;
					appendStringInfoString(&command, KMGR_PROMPT_MSG);
					break;
					/* file descriptor number */
				case 'R':
					{
						char		fd_str[20];

						if (terminal_fd == -1)
						{
#ifdef FRONTEND
							pg_fatal("cluster key command referenced %%R, but --authprompt not specified");
#else
							ereport(ERROR,
									(errcode(ERRCODE_INTERNAL_ERROR),
									 errmsg("cluster key command referenced %%R, but --authprompt not specified")));
#endif
						}

						appendStringInfoString(&command, " ");
						sp++;
						snprintf(fd_str, sizeof(fd_str), "%d", terminal_fd);
						appendStringInfoString(&command, fd_str);
						break;
					}
					/* literal "%" */
				case '%':
					/* convert %% to a single % */
					sp++;
					appendStringInfoChar(&command, *sp);
					break;
				default:
					/* otherwise treat the % as not special */
					appendStringInfoChar(&command, *sp);
					break;
			}
		}
		else
		{
			appendStringInfoChar(&command, *sp);
		}
	}

#ifdef FRONTEND
	fh = open_pipe_stream(command.data);
	if (fh == NULL)
	{
		pg_fatal("could not execute command \"%s\": %m",
				  command.data);
	}
#else
	fh = OpenPipeStream(command.data, "r");
	if (fh == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not execute command \"%s\": %m",
						command.data)));
#endif

	if (!fgets(buf, size, fh))
	{
		if (ferror(fh))
		{
#ifdef FRONTEND
			pg_fatal("could not read from command \"%s\": %m",
						 command.data);
#else
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read from command \"%s\": %m",
							command.data)));
#endif
		}
	}

#ifdef FRONTEND
	pclose_rc = close_pipe_stream(fh);
#else
	pclose_rc = ClosePipeStream(fh);
#endif

	if (pclose_rc == -1)
	{
#ifdef FRONTEND
		pg_fatal("could not close pipe to external command: %m");
#else
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close pipe to external command: %m")));
#endif
	}
	else if (pclose_rc != 0)
	{
#ifdef FRONTEND
		pg_fatal("command \"%s\" failed", command.data);
#else
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("command \"%s\" failed",
						command.data),
				 errdetail_internal("%s", wait_result_to_str(pclose_rc))));
#endif
	}

	/* strip trailing newline and carriage returns */
	len = pg_strip_crlf(buf);

	pfree(command.data);

	return len;
}

#ifdef FRONTEND
static FILE *
open_pipe_stream(const char *command)
{
	FILE	   *res;

#ifdef WIN32
	size_t		cmdlen = strlen(command);
	char	   *buf;
	int			save_errno;

	buf = malloc(cmdlen + 2 + 1);
	if (buf == NULL)
	{
		errno = ENOMEM;
		return NULL;
	}
	buf[0] = '"';
	memcpy(&buf[1], command, cmdlen);
	buf[cmdlen + 1] = '"';
	buf[cmdlen + 2] = '\0';

	res = _popen(buf, "r");

	save_errno = errno;
	free(buf);
	errno = save_errno;
#else
	res = popen(command, "r");
#endif							/* WIN32 */
	return res;
}

static int
close_pipe_stream(FILE *file)
{
#ifdef WIN32
	return _pclose(file);
#else
	return pclose(file);
#endif							/* WIN32 */
}
#endif							/* FRONTEND */

/*
 * Reads the keys at path.
 *
 * This routine simply reads in the raw encrypted/wrapped keys;
 * it does not handle any decryption, see kmgr_key_unwrap().
 *
 * For each key returned, the key and key length are returned
 * in the keys and key_lens arrays respectfully.
 *
 * Note that keys and key_lens must be allocated before calling
 * this function as arrays of at least KMGR_NUM_DATA_KEYS length.
 */
void
kmgr_read_wrapped_data_keys(const char *path, unsigned char **keys, int *key_lens)
{
/*	StaticAssertStmt(lengthof(wkey_filenames) == KMGR_NUM_DATA_KEYS,
					 "wkey_filenames[] must match KMGR_NUM_DATA_KEYS");
*/
	StaticAssertStmt(1 == 1,
					 "wkey_filenames[] must match KMGR_NUM_DATA_KEYS");

	for (int id = 0; id < KMGR_NUM_DATA_KEYS; id++)
		read_wrapped_data_key(path, id, &(keys[id]), &(key_lens[id]));

	return;
}

/* Read a wrapped DEK file */
static void
read_wrapped_data_key(const char *cryptoKeyDir, uint32 id, unsigned char **key_p, int *key_len)
{
	char		path[MAXPGPATH];
	int			fd;
	int			r;
	struct stat st;

	CryptoKeyFilePath(path, cryptoKeyDir, id);

#ifndef FRONTEND
	if ((fd = OpenTransientFile(path, O_RDONLY | PG_BINARY)) == -1)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" for reading: %m",
						path)));
	else if (fstat(fd, &st))
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m",
						path)));
#else
	if ((fd = open(path, O_RDONLY | PG_BINARY, 0)) == -1)
		pg_fatal("could not open file \"%s\" for reading: %m",
					 path);
	else if (fstat(fd, &st))
		pg_fatal("could not stat file \"%s\": %m",
					 path);
#endif

	*key_len = st.st_size;

#ifndef FRONTEND
	pgstat_report_wait_start(WAIT_EVENT_KEY_FILE_READ);
#endif

	*key_p = (unsigned char *) palloc0(*key_len);

	/* Get key bytes */
	r = read(fd, *key_p, *key_len);
	if (r != *key_len)
	{
		if (r < 0)
		{
#ifndef FRONTEND
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", path)));
#else
			pg_fatal("could not read file \"%s\": %m", path);
#endif
		}
		else
		{
#ifndef FRONTEND
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not read file \"%s\": read %d of %u",
							path, r, *key_len)));
#else
			pg_fatal("could not read file \"%s\": read %d of %u",
						 path, r, *key_len);
#endif
		}
	}

#ifndef FRONTEND
	pgstat_report_wait_end();
#endif

#ifndef FRONTEND
	if (CloseTransientFile(fd) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m",
						path)));
#else
	if (close(fd) != 0)
		pg_fatal("could not close file \"%s\": %m", path);
#endif
}

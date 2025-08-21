/*-------------------------------------------------------------------------
 *
 * kmgr.c
 *	 Cluster file encryption routines
 *
 * Cluster file encryption is enabled if user requests it during initdb.
 * During bootstrap, we generate data encryption keys, wrap them with the
 * cluster-level key, and store them into each file located at KMGR_DIR.
 * During startup, we decrypt all internal keys and load them to the shared
 * memory.  Internal keys in the shared memory are read-only.  The wrapping
 * and unwrapping key routines require the OpenSSL library.
 *
 * Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/crypto/kmgr.c
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/xlog.h"
#include "common/file_perm.h"
#include "common/kmgr_utils.h"
#include "common/sha2.h"
#include "access/xlog.h"
#include "common/controldata_utils.h"
#include "crypto/kmgr.h"
#include "postmaster/postmaster.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
/* Struct stores file encryption keys in plaintext format */
typedef struct KmgrShmemData
{
	CryptoKey	intlKeys[KMGR_NUM_DATA_KEYS];
} KmgrShmemData;
static KmgrShmemData *KmgrShmem = NULL;

/* GUC variables */
char	   *cluster_key_command = NULL;
bool	   tde_force_switch = true;

CryptoKey	bootstrap_keys[KMGR_NUM_DATA_KEYS];

extern char *bootstrap_old_key_datadir;
extern int	bootstrap_file_encryption_method;

static void bzeroKmgrKeys(int status, Datum arg);
static void KmgrWriteCryptoKeys(const char *dir, unsigned char **keys, int *key_lens);
static CryptoKey *generate_crypto_key(int len);

static void InitializeFileEncryptionStatus() 
{
    FileEncryptionEnabled = tde_force_switch && (GetFileEncryptionMethod() != DISABLED_ENCRYPTION_METHOD);
}

/*
 * This function must be called ONCE during initdb.  It creates the DEK
 * files wrapped with the KEK supplied by kmgr_run_cluster_key_command().
 * There is also an option for the keys to be copied from another cluster.
 */
void
BootStrapKmgr(void)
{
	char		live_path[MAXPGPATH];
	unsigned char *keys_wrap[KMGR_NUM_DATA_KEYS];
	int			key_lens[KMGR_NUM_DATA_KEYS];
	char		cluster_key_hex[ALLOC_KMGR_CLUSTER_KEY_LEN];
	int			cluster_key_hex_len;
	unsigned char cluster_key[KMGR_CLUSTER_KEY_LEN];

    InitializeFileEncryptionStatus();

	if (!FileEncryptionEnabled)
		return;

#ifndef USE_OPENSSL
	ereport(ERROR,
			(errcode(ERRCODE_CONFIG_FILE_ERROR),
			 (errmsg("cluster file encryption is not supported because OpenSSL is not supported by this build"),
			  errhint("Compile with --with-openssl to use this feature."))));
#endif

	snprintf(live_path, sizeof(live_path), "%s/%s", DataDir, LIVE_KMGR_DIR);

	/*
	 * Copy cluster file encryption keys from an old cluster? This is useful
	 * for pg_upgrade upgrades where the copied database files are already
	 * encrypted using the old cluster's DEK keys.
	 */
	if (bootstrap_old_key_datadir != NULL)
	{
		char		old_key_dir[MAXPGPATH];

		snprintf(old_key_dir, sizeof(old_key_dir), "%s/%s",
				 bootstrap_old_key_datadir, LIVE_KMGR_DIR);
		copydir(old_key_dir, LIVE_KMGR_DIR, true);
	}
	/* create an empty directory */
	else
	{
		if (mkdir(LIVE_KMGR_DIR, pg_dir_create_mode) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create cluster file encryption directory \"%s\": %m",
							LIVE_KMGR_DIR)));
	}

	/*
	 * Get key encryption key (KEK) from the cluster_key command.  The cluster
	 * key command might need to check for the existence of files in the live
	 * directory, e.g., PIV, so run this _after_ copying the directory in
	 * place.
	 */
	cluster_key_hex_len = kmgr_run_cluster_key_command(cluster_key_command,
													   cluster_key_hex,
													   ALLOC_KMGR_CLUSTER_KEY_LEN,
													   live_path, terminal_fd);

	/* decode supplied hex */
	if (hex_decode(cluster_key_hex, cluster_key_hex_len,
					  (char *) cluster_key) !=
		KMGR_CLUSTER_KEY_LEN)
		ereport(ERROR,
				(errmsg("cluster key must be %d hexadecimal characters",
						KMGR_CLUSTER_KEY_LEN * 2)));

	/* We are not in copy mode?  Generate new cluster file encryption keys. */
	if (bootstrap_old_key_datadir == NULL)
	{
		unsigned char *bootstrap_keys_wrap[KMGR_NUM_DATA_KEYS];
		PgCipherCtx *cluster_key_ctx;

		/* Create KEK encryption context */
		cluster_key_ctx = pg_cipher_ctx_create(PG_CIPHER_AES_KWP, cluster_key,
											   KMGR_CLUSTER_KEY_LEN, true);
		if (!cluster_key_ctx)
			elog(ERROR, "could not initialize encryption context");

		/* Wrap data encryption keys (DEK) using the key encryption key (KEK) */
		for (int id = 0; id < KMGR_NUM_DATA_KEYS; id++)
		{
			CryptoKey  *key;
			int block_size = 0;

			/* generate a DEK */
			key = generate_crypto_key(
									  encryption_methods[bootstrap_file_encryption_method].bit_length / 8);

			/* output generated random string as hex, for testing */
			{
				char		str[MAXPGPATH];
				int			out_len;

				out_len = hex_encode((char *) (key->key), key->klen,
										str);
				str[out_len] = '\0';
			}

			block_size = pg_cipher_blocksize(cluster_key_ctx);
			elog(LOG, "block_size:%d", block_size);
			bootstrap_keys_wrap[id] = palloc0(KMGR_MAX_KEY_LEN_BYTES +
											  block_size);

			/* wrap DEK with KEK */
			if (!kmgr_wrap_data_key(cluster_key_ctx, key, bootstrap_keys_wrap[id], &(key_lens[id])))
			{
				pg_cipher_ctx_free(cluster_key_ctx);
				elog(ERROR, "failed to wrap data encryption key");
			}

			/* remove DEK from memory */
			explicit_bzero(key, sizeof(CryptoKey));
		}

		/* Write data encryption keys to the disk */
		KmgrWriteCryptoKeys(LIVE_KMGR_DIR, bootstrap_keys_wrap, key_lens);

		pg_cipher_ctx_free(cluster_key_ctx);
	}

	/*
	 * We are either decrypting keys we copied from an old cluster, or
	 * decrypting keys we just wrote above --- either way, we decrypt them
	 * here and store them in a file-scoped variable for use in later
	 * encrypting during bootstrap mode.
	 */

	/* Get the crypto keys from the live directory */
	kmgr_read_wrapped_data_keys(LIVE_KMGR_DIR, keys_wrap, key_lens);

	if (!kmgr_verify_cluster_key(cluster_key, keys_wrap, key_lens, bootstrap_keys))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("supplied cluster key does not match expected cluster_key")));

	/* bzero DEK on exit */
	on_proc_exit(bzeroKmgrKeys, 0);

	/* bzero KEK */
	explicit_bzero(cluster_key_hex, cluster_key_hex_len);
	explicit_bzero(cluster_key, KMGR_CLUSTER_KEY_LEN);
}

/* Report shared-memory space needed by KmgrShmem */
Size
KmgrShmemSize(void)
{
	if (!tde_force_switch)
		return 0;

	return MAXALIGN(sizeof(KmgrShmemData));
}

/* Allocate and initialize key manager memory */
void
KmgrShmemInit(void)
{
	bool		found;

	if (!tde_force_switch)
		return;

	KmgrShmem = (KmgrShmemData *) ShmemInitStruct("File encryption key manager",
												  KmgrShmemSize(), &found);

	/* bzero DEK on exit */
	on_shmem_exit(bzeroKmgrKeys, 0);
}

/*
 * Get cluster key and verify it, then get the data encryption keys.
 * This function is called by postmaster at startup time.
 */
void
InitializeKmgr(void)
{
	unsigned char *keys_wrap[KMGR_NUM_DATA_KEYS];
	int			key_lens[KMGR_NUM_DATA_KEYS];
	char		cluster_key_hex[ALLOC_KMGR_CLUSTER_KEY_LEN];
	int			cluster_key_hex_len;
	struct stat buffer;
	char		live_path[MAXPGPATH];
	unsigned char cluster_key[KMGR_CLUSTER_KEY_LEN];

    InitializeFileEncryptionStatus();

	if (!FileEncryptionEnabled)
		return;

#ifndef USE_OPENSSL
	ereport(ERROR,
			(errcode(ERRCODE_CONFIG_FILE_ERROR),
			 (errmsg("cluster file encryption is not supported because OpenSSL is not supported by this build"),
			  errhint("Compile with --with-openssl to use this feature."))));
#endif

	elog(DEBUG1, "starting up cluster file encryption manager");

	if (stat(KMGR_DIR, &buffer) != 0 || !S_ISDIR(buffer.st_mode))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 (errmsg("cluster file encryption directory %s is missing", KMGR_DIR))));

	if (stat(KMGR_DIR_PID, &buffer) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 (errmsg("cluster had a pg_alterckey failure that needs repair or pg_alterckey is running"),
				  errhint("Run pg_alterckey --repair or wait for it to complete."))));

	/*
	 * We want OLD deleted since it allows access to the data encryption keys
	 * using the old cluster key.  If NEW exists, it means either the new
	 * directory is partly written, or NEW wasn't renamed to LIVE --- in
	 * either case, it needs to be repaired.  See src/bin/pg_alterckey/README
	 * for more details.
	 */
	if (stat(OLD_KMGR_DIR, &buffer) == 0 || stat(NEW_KMGR_DIR, &buffer) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 (errmsg("cluster had a pg_alterckey failure that needs repair"),
				  errhint("Run pg_alterckey --repair."))));

	/* If OLD, NEW, and LIVE do not exist, there is a serious problem. */
	if (stat(LIVE_KMGR_DIR, &buffer) != 0 || !S_ISDIR(buffer.st_mode))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 (errmsg("cluster has no data encryption keys"))));

	/* Get the cluster key (KEK) */
	snprintf(live_path, sizeof(live_path), "%s/%s", DataDir, LIVE_KMGR_DIR);
	cluster_key_hex_len = kmgr_run_cluster_key_command(cluster_key_command,
													   cluster_key_hex,
													   ALLOC_KMGR_CLUSTER_KEY_LEN,
													   live_path, terminal_fd);

	/* decode supplied hex */
	if (hex_decode(cluster_key_hex, cluster_key_hex_len,
					  (char *) cluster_key) !=
		KMGR_CLUSTER_KEY_LEN)
		ereport(ERROR,
				(errmsg("cluster key must be %d hexadecimal characters",
						KMGR_CLUSTER_KEY_LEN * 2)));

	/* Load wrapped DEKs from their files into an array */
	kmgr_read_wrapped_data_keys(LIVE_KMGR_DIR, keys_wrap, key_lens);

	/*
	 * Verify cluster key and store the unwrapped data encryption keys in
	 * shared memory.
	 */
	if (!kmgr_verify_cluster_key(cluster_key, keys_wrap, key_lens, KmgrShmem->intlKeys))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("supplied cluster key does not match expected cluster key")));

	/* Check that retrieved key lengths match controldata length. */
	for (int id = 0; id < KMGR_NUM_DATA_KEYS; id++)
		if (KmgrShmem->intlKeys[id].klen * 8 !=
			encryption_methods[GetFileEncryptionMethod()].bit_length)
		{
			char		path[MAXPGPATH];

			CryptoKeyFilePath(path, DataDir, id);

			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("data encryption key %s of length %d does not match controldata key length %d",
							path, KmgrShmem->intlKeys[id].klen * 8,
							encryption_methods[GetFileEncryptionMethod()].bit_length)));
		}

	/* bzero KEK */
	explicit_bzero(cluster_key_hex, cluster_key_hex_len);
	explicit_bzero(cluster_key, KMGR_CLUSTER_KEY_LEN);
}

static void
bzeroKmgrKeys(int status, Datum arg)
{
	if (IsBootstrapProcessingMode())
		explicit_bzero(bootstrap_keys, sizeof(bootstrap_keys));
	else
		explicit_bzero(KmgrShmem->intlKeys, sizeof(KmgrShmem->intlKeys));
}

/* return requested DEK */
const CryptoKey *
KmgrGetKey(int id)
{
	Assert(id < KMGR_NUM_DATA_KEYS);

	return (const CryptoKey *) (IsBootstrapProcessingMode() ?
								&(bootstrap_keys[id]) : &(KmgrShmem->intlKeys[id]));
}

bool CheckIsSM4Method(void)
{
	if (strcmp("SM4", encryption_methods[GetFileEncryptionMethod()].name) == 0)
		return true;
	
	return false;

}

/* Generate a DEK inside a CryptoKey */
static CryptoKey *
generate_crypto_key(int len)
{
	CryptoKey  *newkey;

	Assert(len <= KMGR_MAX_KEY_LEN);
	newkey = (CryptoKey *) palloc0(sizeof(CryptoKey));

	newkey->klen = len;

	if (!pg_strong_random(newkey->key, len))
		elog(ERROR, "failed to generate new file encryption key");

	return newkey;
}

/*
 * Write the DEKs to the disk.
 */
static void
KmgrWriteCryptoKeys(const char *dir, unsigned char **keys, int *key_lens)
{
	elog(DEBUG2, "writing data encryption keys wrapped using the cluster key");

	for (int i = 0; i < KMGR_NUM_DATA_KEYS; i++)
	{
		int			fd;
		char		path[MAXPGPATH];

		CryptoKeyFilePath(path, dir, i);

		if ((fd = BasicOpenFile(path, O_RDWR | O_CREAT | O_EXCL | PG_BINARY)) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m",
							path)));

		errno = 0;
		pgstat_report_wait_start(WAIT_EVENT_KEY_FILE_WRITE);
		if (write(fd, keys[i], key_lens[i]) != key_lens[i])
		{
			/* if write didn't set errno, assume problem is no disk space */
			if (errno == 0)
				errno = ENOSPC;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							path)));
		}
		pgstat_report_wait_end();

		pgstat_report_wait_start(WAIT_EVENT_KEY_FILE_SYNC);
		if (pg_fsync(fd) != 0)
			ereport(PANIC,
					(errcode_for_file_access(),
					 errmsg("could not fsync file \"%s\": %m",
							path)));
		pgstat_report_wait_end();

		if (close(fd) != 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not close file \"%s\": %m",
							path)));
	}
}

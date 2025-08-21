/*-------------------------------------------------------------------------
 *
 * bufenc.c
 *
 * Copyright (c) 2020, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/crypto/bufenc.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "lib/stringinfo.h"

#include "access/gist.h"
#include "access/xlog.h"
#include "crypto/bufenc.h"
#include "crypto/sm4_ofb.h"
#include "storage/bufpage.h"
#include "storage/fd.h"
#include "storage/shmem.h"

extern XLogRecPtr LSNForEncryption(bool use_wal_lsn);

/*
 * We use the page LSN, page number, and permanent-bit to indicate if a fake
 * LSN was used to create a nonce for each page.
 */
#define BUFENC_IV_SIZE		16

static unsigned char buf_encryption_iv[BUFENC_IV_SIZE];

void *BufEncCtx = NULL;
void *BufDecCtx = NULL;

static void set_buffer_encryption_iv(Page page, BlockNumber blkno);

void
InitializeBufferEncryption(void)
{
	const CryptoKey *key;

	if (!FileEncryptionEnabled)
		return;

	key = KmgrGetKey(KMGR_KEY_ID_REL);

	if (CheckIsSM4Method())
	{
		bool found;
		BufEncCtx = ShmemInitStruct("sm4 encryption method encrypt ctx",
												sizeof(sm4_ctx), &found);

		BufDecCtx = ShmemInitStruct("sm4 encryption method decrypt ctx",
												sizeof(sm4_ctx), &found);
		sm4_ofb_setkey_enc((sm4_ctx *)BufEncCtx, (unsigned char *)key->key);
		sm4_ofb_setkey_dec((sm4_ctx *)BufDecCtx, (unsigned char *)key->key);
	}
	else 
	{
		BufEncCtx = (void *)pg_cipher_ctx_create(PG_CIPHER_AES_CTR,
										(unsigned char *) key->key,
										(key->klen), true);

		BufDecCtx = (void *)pg_cipher_ctx_create(PG_CIPHER_AES_CTR,
										(unsigned char *) key->key,
										(key->klen), false);
	}
	if (!BufEncCtx)
			elog(ERROR, "cannot intialize encryption context");

	if (!BufDecCtx)
		elog(ERROR, "cannot intialize decryption context");
}

/* Encrypt the given page with the relation key */
void
EncryptPage(Page page, BlockNumber blkno)
{
	unsigned char *ptr = (unsigned char *) page + PageEncryptOffset;

	int			enclen;

	Assert(BufEncCtx != NULL);

	set_buffer_encryption_iv(page, blkno);
	if (CheckIsSM4Method())
	{
		/* sm4 ofb mode use enc ctx, not dec ctx */
		sm4_ofb_cipher(
					(sm4_ctx *)BufEncCtx,
					ptr,
					(const unsigned char *) ptr,
					SizeOfPageEncryption,
					buf_encryption_iv);
	}
	else
	{
		if (unlikely(!pg_cipher_encrypt((PgCipherCtx *)BufEncCtx, PG_CIPHER_AES_CTR,
										(const unsigned char *) ptr,	/* input  */
										SizeOfPageEncryption,
										ptr,	/* length */
										&enclen,	/* resulting length */
										buf_encryption_iv,	/* iv */
										BUFENC_IV_SIZE,
										NULL, 0)))
			elog(ERROR, "cannot encrypt page %u", blkno);

		Assert(enclen == SizeOfPageEncryption);
	}
}

/* Decrypt the given page with the relation key */
void
DecryptPage(Page page, BlockNumber blkno)
{
	unsigned char *ptr = (unsigned char *) page + PageEncryptOffset;
	int			enclen;

	Assert(BufDecCtx != NULL);

	set_buffer_encryption_iv(page, blkno);
	if (CheckIsSM4Method())
	{
		/* sm4 ofb mode use enc ctx, not dec ctx */
		sm4_ofb_cipher(
		(sm4_ctx *)BufEncCtx,
		ptr,
		(const unsigned char *) ptr,
		SizeOfPageEncryption,
		buf_encryption_iv);
	}
	else
	{
		if (unlikely(!pg_cipher_decrypt((PgCipherCtx *)BufDecCtx, PG_CIPHER_AES_CTR,
										(const unsigned char *) ptr,	/* input  */
										SizeOfPageEncryption,
										ptr,	/* output */
										&enclen,	/* resulting length */
										buf_encryption_iv,	/* iv */
										BUFENC_IV_SIZE,
										NULL, 0)))
			elog(ERROR, "cannot decrypt page %u", blkno);

		Assert(enclen == SizeOfPageEncryption);
	}
}

/* Construct iv for the given page */
static void
set_buffer_encryption_iv(Page page, BlockNumber blkno)
{
	unsigned char *p = buf_encryption_iv;

	MemSet(buf_encryption_iv, 0, BUFENC_IV_SIZE);

	/* block number (4 byte) */
	memcpy(p, &blkno, sizeof(BlockNumber));
	p += sizeof(BlockNumber);

	/*
	 * set the last remain 12 bytes
	 */
	for (int i = 0 ; i < 12; i++)
		*p++ = 0x80;
}

/* Construct iv for the given page */
static void
set_buffer_encryption_iv_for_ao(RelFileLocator *file_node)
{
	unsigned char *p = buf_encryption_iv;

	MemSet(buf_encryption_iv, 0, BUFENC_IV_SIZE);

	/* copy the whole file node (4 bytes) */
	memcpy(p, &file_node->dbOid, sizeof(file_node->dbOid));
	p += sizeof(file_node->dbOid);

	/*
	 * set the last remain 12 bytes
	 */
	for (int i = 0 ; i < 12; i++)
		*p++ = 0x80;
}

void
EncryptAOBLock(unsigned char *data_buf, const int buf_len,
				  RelFileLocator *file_node)
{
	int			enclen;
	Assert(BufEncCtx != NULL);

	set_buffer_encryption_iv_for_ao(file_node);
	if (CheckIsSM4Method())
	{
		sm4_ofb_cipher(
				(sm4_ctx *)BufEncCtx,
				data_buf,
				(const unsigned char *)data_buf,
				buf_len,
				buf_encryption_iv);
	}
	else
	{
		if (unlikely(!pg_cipher_encrypt((PgCipherCtx *)BufEncCtx, PG_CIPHER_AES_CTR,
										(const unsigned char *) data_buf,	/* input  */
										buf_len,
										data_buf,	/* length */
										&enclen,	/* resulting length */
										buf_encryption_iv,	/* iv */
										BUFENC_IV_SIZE,
										NULL, 0)))
			elog(ERROR, "cannot encrypt AO block");

		Assert(buf_len == enclen);
	}
}

/* Decrypt the given page with the relation key */
void
DecryptAOBlock(unsigned char *data_buf, const int buf_len,
			   RelFileLocator *file_node)
{
	int			enclen;
	Assert(BufDecCtx != NULL);

	set_buffer_encryption_iv_for_ao(file_node);
	if (CheckIsSM4Method())
	{
		/* sm4 ofb mode use enc ctx, not dec ctx */
		sm4_ofb_cipher(
					(sm4_ctx *)BufEncCtx,
					data_buf,
					(const unsigned char *)data_buf,
					buf_len,
					buf_encryption_iv);
	}
	else
	{
		if (unlikely(!pg_cipher_decrypt((PgCipherCtx *)BufDecCtx, PG_CIPHER_AES_CTR,
										(const unsigned char *) data_buf,	/* input  */
										buf_len,
										data_buf,	/* output */
										&enclen,	/* resulting length */
										buf_encryption_iv,	/* iv */
										BUFENC_IV_SIZE,
										NULL, 0)))
			elog(ERROR, "cannot decrypt ao block");

		Assert(enclen == buf_len);
	}
}

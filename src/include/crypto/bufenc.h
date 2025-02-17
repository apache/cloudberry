/*-------------------------------------------------------------------------
 *
 * bufenc.h
 *
 * Portions Copyright (c) 2021, PostgreSQL Global Development Group
 *
 * src/include/crypto/bufenc.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef BUFENC_H
#define BUFENC_H

#include "storage/bufmgr.h"
#include "crypto/kmgr.h"

/* Cluster encryption encrypts only main forks */
#define PageNeedsToBeEncrypted(forknum) \
	(FileEncryptionEnabled && (forknum) == MAIN_FORKNUM)


extern void InitializeBufferEncryption(void);
extern void EncryptPage(Page page,
						BlockNumber blkno);
extern void DecryptPage(Page page,
						BlockNumber blkno);
extern void
EncryptAOBLock(unsigned char *data_buf, const int buf_len,
				  RelFileNode *file_node);
extern void
DecryptAOBlock(unsigned char *data_buf, const int buf_len,
				  RelFileNode *file_node);

#endif							/* BUFENC_H */

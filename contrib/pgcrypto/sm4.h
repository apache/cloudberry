/*-------------------------------------------------------------------------
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * sm4.h
 *
 * IDENTIFICATION
 *          contrib/pgcrypto/sm4.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef _SM4_H_
#define _SM4_H_

#include "postgres.h"
#include "px.h"

typedef uint8 u1byte;
typedef uint32 u4byte;
typedef uint64 u8byte;

typedef struct _sm4_ctx
{
	u4byte		k_len;
	int			decrypt;
	u8byte		e_key[32];
	u8byte		d_key[32];
} sm4_ctx;

#define MODE_ECB 0
#define MODE_CBC 1

#define INT_MAX_KEY		(512/8)
#define INT_MAX_IV		(128/8)

struct sm4_init_ctx
{
	uint8		keybuf[INT_MAX_KEY];
	uint8		iv[INT_MAX_IV];
	union {
		sm4_ctx sm4;
	}			ctx;
	unsigned	keylen;
	int			is_init;
	int			mode;
};

void sm4_setkey_enc(sm4_ctx *ctx, u1byte* key);
void sm4_setkey_dec(sm4_ctx *ctx, u1byte* key);

void sm4_cbc_encrypt(sm4_ctx *ctx, u1byte *iva, u1byte *data, long len);
void sm4_cbc_decrypt(sm4_ctx *ctx, u1byte *iva, u1byte *data, long len);
void sm4_ecb_encrypt(sm4_ctx *ctx, u1byte *data, long len);
void sm4_ecb_decrypt(sm4_ctx *ctx, u1byte *data, long len);

PX_Cipher * sm4_load(int mode);

#endif /* _SM4_H_ */
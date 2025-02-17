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
 * sm4.c
 *
 * IDENTIFICATION
 *          contrib/pgcrypto/sm4.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "sm4.h"
#include "px.h"

#include <sys/param.h>

/* fixed SM4 param */
static const u1byte sbx_tab[256] = 
{
    0xd6,0x90,0xe9,0xfe,0xcc,0xe1,0x3d,0xb7,0x16,0xb6,0x14,0xc2,0x28,0xfb,0x2c,0x05,
    0x2b,0x67,0x9a,0x76,0x2a,0xbe,0x04,0xc3,0xaa,0x44,0x13,0x26,0x49,0x86,0x06,0x99,
    0x9c,0x42,0x50,0xf4,0x91,0xef,0x98,0x7a,0x33,0x54,0x0b,0x43,0xed,0xcf,0xac,0x62,
    0xe4,0xb3,0x1c,0xa9,0xc9,0x08,0xe8,0x95,0x80,0xdf,0x94,0xfa,0x75,0x8f,0x3f,0xa6,
    0x47,0x07,0xa7,0xfc,0xf3,0x73,0x17,0xba,0x83,0x59,0x3c,0x19,0xe6,0x85,0x4f,0xa8,
    0x68,0x6b,0x81,0xb2,0x71,0x64,0xda,0x8b,0xf8,0xeb,0x0f,0x4b,0x70,0x56,0x9d,0x35,
    0x1e,0x24,0x0e,0x5e,0x63,0x58,0xd1,0xa2,0x25,0x22,0x7c,0x3b,0x01,0x21,0x78,0x87,
    0xd4,0x00,0x46,0x57,0x9f,0xd3,0x27,0x52,0x4c,0x36,0x02,0xe7,0xa0,0xc4,0xc8,0x9e,
    0xea,0xbf,0x8a,0xd2,0x40,0xc7,0x38,0xb5,0xa3,0xf7,0xf2,0xce,0xf9,0x61,0x15,0xa1,
    0xe0,0xae,0x5d,0xa4,0x9b,0x34,0x1a,0x55,0xad,0x93,0x32,0x30,0xf5,0x8c,0xb1,0xe3,
    0x1d,0xf6,0xe2,0x2e,0x82,0x66,0xca,0x60,0xc0,0x29,0x23,0xab,0x0d,0x53,0x4e,0x6f,
    0xd5,0xdb,0x37,0x45,0xde,0xfd,0x8e,0x2f,0x03,0xff,0x6a,0x72,0x6d,0x6c,0x5b,0x51,
    0x8d,0x1b,0xaf,0x92,0xbb,0xdd,0xbc,0x7f,0x11,0xd9,0x5c,0x41,0x1f,0x10,0x5a,0xd8,
    0x0a,0xc1,0x31,0x88,0xa5,0xcd,0x7b,0xbd,0x2d,0x74,0xd0,0x12,0xb8,0xe5,0xb4,0xb0,
    0x89,0x69,0x97,0x4a,0x0c,0x96,0x77,0x7e,0x65,0xb9,0xf1,0x09,0xc5,0x6e,0xc6,0x84,
    0x18,0xf0,0x7d,0xec,0x3a,0xdc,0x4d,0x20,0x79,0xee,0x5f,0x3e,0xd7,0xcb,0x39,0x48
};
static const u4byte sm4_fk[4] = {0xa3b1bac6,0x56aa3350,0x677d9197,0xb27022dc};
static const u4byte sm4_ck[32] =
{
    0x00070e15,0x1c232a31,0x383f464d,0x545b6269,
    0x70777e85,0x8c939aa1,0xa8afb6bd,0xc4cbd2d9,
    0xe0e7eef5,0xfc030a11,0x181f262d,0x343b4249,
    0x50575e65,0x6c737a81,0x888f969d,0xa4abb2b9,
    0xc0c7ced5,0xdce3eaf1,0xf8ff060d,0x141b2229,
    0x30373e45,0x4c535a61,0x686f767d,0x848b9299,
    0xa0a7aeb5,0xbcc3cad1,0xd8dfe6ed,0xf4fb0209,
    0x10171e25,0x2c333a41,0x484f565d,0x646b7279
};

/* combine 4*b into n */
#define u8byte_comb(n,b,i) (n) = ((b)[(i)] << 24) | ((b)[(i) + 1] << 16) | ((b)[(i) + 2] << 8) | ((b)[(i) + 3]);

/* split n into 4*b */
#define u8byte_split(n,b,i)          \
{                                   \
    (b)[(i)    ] = (n) >> 24;       \
    (b)[(i) + 1] = (n) >> 16;       \
    (b)[(i) + 2] = (n) >>  8;       \
    (b)[(i) + 3] = (n)      ;       \
}

/* left rotate */
#define rotl(x,n) ((((x) & 0xFFFFFFFF) << (int)(n)) | ((x) >> (32 - (int)(n))))

static u8byte roundF(u8byte x0, u8byte x1, u8byte x2, u8byte x3, u8byte rk)
{
    u8byte bb = 0;
    u8byte c = 0;
    u1byte a[4];
	u1byte b[4];
    u8byte ka = x1^x2^x3^rk;

    u8byte_split(ka,a,0)
    b[0] = sbx_tab[a[0]];
    b[1] = sbx_tab[a[1]];
    b[2] = sbx_tab[a[2]];
    b[3] = sbx_tab[a[3]];
	u8byte_comb(bb,b,0)
    c =bb^(rotl(bb, 2))^(rotl(bb, 10))^(rotl(bb, 18))^(rotl(bb, 24));

    return (x0^c);
}

static void sm4_encrypt(u8byte * sk,
                    u1byte *input,
                    u1byte *output)
{
    size_t i = 0;
    u8byte ulbuf[36] = {0};

    u8byte_comb(ulbuf[0], input, 0)
    u8byte_comb(ulbuf[1], input, 4)
    u8byte_comb(ulbuf[2], input, 8)
    u8byte_comb(ulbuf[3], input, 12)

    while(i < 32)
    {
        ulbuf[i+4] = roundF(ulbuf[i], ulbuf[i+1], ulbuf[i+2], ulbuf[i+3], sk[i]);
	    i++;
    }

	u8byte_split(ulbuf[35],output,0);
	u8byte_split(ulbuf[34],output,4);
	u8byte_split(ulbuf[33],output,8);
	u8byte_split(ulbuf[32],output,12);
}

static u8byte sm4_rk(u8byte ka)
{
    u8byte bb = 0;
    u8byte rk = 0;
    u1byte a[4];
    u1byte b[4];
    u8byte_split(ka,a,0)
    b[0] = sbx_tab[a[0]];
    b[1] = sbx_tab[a[1]];
    b[2] = sbx_tab[a[2]];
    b[3] = sbx_tab[a[3]];
	u8byte_comb(bb,b,0)
    rk = bb^(rotl(bb, 13))^(rotl(bb, 23));
    return rk;
}

static void 
sm4_setkey(u8byte* SK, u1byte* key)
{
    u8byte MK[4];
    u8byte k[36];
    u8byte i = 0;

    u8byte_comb(MK[0], key, 0);
    u8byte_comb(MK[1], key, 4);
    u8byte_comb(MK[2], key, 8);
    u8byte_comb(MK[3], key, 12);
    k[0] = MK[0]^sm4_fk[0];
    k[1] = MK[1]^sm4_fk[1];
    k[2] = MK[2]^sm4_fk[2];
    k[3] = MK[3]^sm4_fk[3];
    for(; i<32; i++)
    {
        k[i+4] = k[i] ^ (sm4_rk(k[i+1]^k[i+2]^k[i+3]^sm4_ck[i]));
        SK[i] = k[i+4];
	}
}

void sm4_setkey_enc(sm4_ctx *ctx, u1byte* key)
{
	sm4_setkey(ctx->e_key, key);
}

void sm4_setkey_dec(sm4_ctx *ctx, u1byte* key)
{
    int i;
    size_t t = 0;
    sm4_setkey(ctx->d_key, key);
    for( i = 0; i < 16; i ++ )
    {
        t = ctx->d_key[i];
        ctx->d_key[i] = ctx->d_key[31-i];
        ctx->d_key[31-i] = t;
    }
}

void 
sm4_cbc_encrypt(sm4_ctx *ctx, u1byte *iv, u1byte *data, long len)
{
	int i;
    unsigned bs = 16;
    u1byte *input = data;

    while (len > 0)
    {
        for(i = 0; i < bs; i++ )
            input[i] ^= iv[i];

        sm4_encrypt(ctx->e_key, input, input);
        memcpy(iv, input, bs);

        input += bs;
        len -= bs;
    }
}

void 
sm4_cbc_decrypt(sm4_ctx *ctx, u1byte *iv, u1byte *data, long len)
{
    unsigned bs = 16;
    u1byte temp[16];
    u1byte *input = data;
    int i = 0;

    while (len > 0)
    {
        memcpy(temp, input, bs);
        sm4_encrypt(ctx->d_key, input, input);

        for(i = 0; i < bs; i++)
            input[i] ^= iv[i];

        memcpy(iv, temp, bs);

        input += bs;
        len -= bs;
    }
}

static void
sm4_ecb_crypto(u8byte *key, u1byte *data, long len) {
	u1byte *d = data;
	unsigned bs = 16;

	while(len > 0)
    {
        sm4_encrypt(key, d, d);
        d += bs;
        len -= bs;
    }
}

void 
sm4_ecb_encrypt(sm4_ctx *ctx, u1byte *data, long len)
{
	sm4_ecb_crypto(ctx->e_key, data, len);
}

void 
sm4_ecb_decrypt(sm4_ctx *ctx, u1byte *data, long len)
{
    sm4_ecb_crypto(ctx->d_key, data, len);
}

static void
sm4_init_ctx_free(PX_Cipher *c)
{
	struct sm4_init_ctx *cx = (struct sm4_init_ctx *) c->ptr;

	if (cx)
	{
		px_memset(cx, 0, sizeof *cx);
		pfree(cx);
	}
	pfree(c);
}

static unsigned
sm4_ctx_block_size(PX_Cipher *c)
{
	return 128 / 8;
}

static unsigned
sm4_ctx_key_size(PX_Cipher *c)
{
	return 256 / 8;
}

static unsigned
sm4_ctx_iv_size(PX_Cipher *c)
{
	return 128 / 8;
}

static int
sm4_ctx_init(PX_Cipher *c, const uint8 *key, unsigned klen, const uint8 *iv)
{
	struct sm4_init_ctx *cx = (struct sm4_init_ctx *) c->ptr;

	if (klen <= 128 / 8)
		cx->keylen = 128 / 8;
	else 
		return PXE_KEY_TOO_BIG;

	memcpy(&cx->keybuf, key, klen);

	if (iv)
		memcpy(cx->iv, iv, 128 / 8);

	return 0;
}

static 
void sm4_ctx_real_init(struct sm4_init_ctx *cx, bool enc)
{
	if (enc) {
		sm4_setkey_enc(&cx->ctx.sm4, cx->keybuf);
	} else {
		sm4_setkey_dec(&cx->ctx.sm4, cx->keybuf);
	}
}

static int
sm4_ctx_encrypt(PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res)
{
	struct sm4_init_ctx *cx = (struct sm4_init_ctx *) c->ptr;

	if (!cx->is_init)
		sm4_ctx_real_init(cx, true);

	if (dlen == 0)
		return 0;

	if (dlen & 15)
		return PXE_NOTBLOCKSIZE;

	memcpy(res, data, dlen);

	if (cx->mode == MODE_CBC)
	{
		sm4_cbc_encrypt(&cx->ctx.sm4, cx->iv, res, dlen);
		memcpy(cx->iv, res + dlen - 16, 16);
	}
	else {
		sm4_ecb_encrypt(&cx->ctx.sm4, res, dlen);
	}

	return 0;
}

static int
sm4_ctx_decrypt(PX_Cipher *c, const uint8 *data, unsigned dlen, uint8 *res)
{
	struct sm4_init_ctx *cx = (struct sm4_init_ctx *) c->ptr;

	if (!cx->is_init)
		sm4_ctx_real_init(cx, false);

	if (dlen == 0)
		return 0;

	if (dlen & 15)
		return PXE_NOTBLOCKSIZE;

	memcpy(res, data, dlen);

	if (cx->mode == MODE_CBC)
	{
		sm4_cbc_decrypt(&cx->ctx.sm4, cx->iv, res, dlen);
		memcpy(cx->iv, data + dlen - 16, 16);
	}
	else
		sm4_ecb_decrypt(&cx->ctx.sm4, res, dlen);

	return 0;
}

PX_Cipher * 
sm4_load(int mode)
{
	PX_Cipher  *c;
	struct sm4_init_ctx *cx;

	c = palloc0(sizeof *c);

	c->block_size = sm4_ctx_block_size;
	c->key_size = sm4_ctx_key_size;
	c->iv_size = sm4_ctx_iv_size;
	c->init = sm4_ctx_init;
	c->encrypt = sm4_ctx_encrypt;
	c->decrypt = sm4_ctx_decrypt;
	c->free = sm4_init_ctx_free;

	cx = palloc0(sizeof *cx);
	cx->mode = mode;

	c->ptr = cx;
	return c;
}
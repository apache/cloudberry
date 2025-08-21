
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "sha2_int.h"

#define PG_SM3_SHORT_BLOCK_LENGTH		(PG_SM3_BLOCK_LENGTH - 8)

#ifndef WORDS_BIGENDIAN
#ifndef REVERSE32
#define REVERSE32(w,x)	{ \
	uint32 tmp = (w); \
	tmp = (tmp >> 16) | (tmp << 16); \
	(x) = ((tmp & 0xff00ff00UL) >> 8) | ((tmp & 0x00ff00ffUL) << 8); \
}
#endif
#ifndef REVERSE64
#define REVERSE64(w,x)	{ \
	uint64 tmp = (w); \
	tmp = (tmp >> 32) | (tmp << 32); \
	tmp = ((tmp & 0xff00ff00ff00ff00ULL) >> 8) | \
		  ((tmp & 0x00ff00ff00ff00ffULL) << 8); \
	(x) = ((tmp & 0xffff0000ffff0000ULL) >> 16) | \
		  ((tmp & 0x0000ffff0000ffffULL) << 16); \
}
#endif
#endif							/* not bigendian */

/* Initial hash value H for SM3 */
static const uint32 sm3_initial_hash_value[8] = {
	0x7380166fUL,
	0x4914b2b9UL,
	0x172442d7UL,
	0xda8a0600UL,
	0xa96f30bcUL,
	0x163138aaUL,
	0xe38dee4dUL,
	0xb0fb0e4eUL
};


void
pg_sm3_init(pg_sm3_ctx *context)
{
	if (context == NULL)
		return;
	memcpy(context->state, sm3_initial_hash_value, PG_SM3_DIGEST_LENGTH);
	memset(context->buffer, 0, PG_SM3_BLOCK_LENGTH);
	context->bitcount = 0;
}

#define rol32(x, r) (((x)<<(r)) | ((x)>>(32-(r))))

static inline uint32_t SM3_P0(uint32_t X)
{
	return (X ^ (rol32(X, 9)) ^ (rol32(X, 17)));
}

static inline uint32_t SM3_P1(uint32_t X)
{
	return (X ^ (rol32(X, 15)) ^ (rol32(X, 23)));
}

static inline uint32_t SM3_ff(int j, uint32_t x, uint32_t y, uint32_t z)
{
	return j < 16 ? (x ^ y ^ z) : ((x & y) | (x & z) | (y & z));
}

static inline uint32_t SM3_gg(int j, uint32_t x, uint32_t y, uint32_t z)
{
	return j < 16 ? (x ^ y ^ z) : ((x & y) | ((~x) & z));
}

static void
SM3_Transform(pg_sm3_ctx *context, const uint8 *data)
{
	uint32_t a, b, c, d, e, f, g, h;
	uint32_t W[68], W_B[64];
	int		j;

	uint32_t tmp2, *tmp_data;
	uint32_t SS1, SS2, TT1, TT2;
	uint32_t T;

	/* Initialize registers with the prev. intermediate value */
	a = context->state[0];
	b = context->state[1];
	c = context->state[2];
	d = context->state[3];
	e = context->state[4];
	f = context->state[5];
	g = context->state[6];
	h = context->state[7];

	tmp_data = (uint32_t *)data;
	for (j = 0; j <= 15; j++) {
		REVERSE32(tmp_data[j], W[j]);
	}

	for (; j <= 67; j++) {
		tmp2 = W[j - 16] ^ W[j - 9] ^ rol32(W[j - 3], 15);
		W[j] = SM3_P1(tmp2) ^ (rol32(W[j - 13], 7)) ^ W[j - 6];
	}

	for (j = 0; j < 64; j++) {
		W_B[j] = W[j] ^ W[j + 4];
	}

	for (j = 0; j < 64; j++) {
		T = j < 16 ? 0x79cc4519 : 0x7a879d8a;

		SS1 = rol32(rol32(a, 12) + e + rol32(T, (j % 32)), 7);
		SS2 = SS1 ^ rol32(a, 12);
		TT1 = SM3_ff(j, a, b, c) + d + SS2 + W_B[j];
		TT2 = SM3_gg(j, e, f, g) + h + SS1 + W[j];
		d = c;
		c = rol32(b, 9);
		b = a;
		a = TT1;
		h = g;
		g = rol32(f, 19);
		f = e;
		e = SM3_P0(TT2);
	}
	context->state[0] ^= a;
	context->state[1] ^= b;
	context->state[2] ^= c;
	context->state[3] ^= d;
	context->state[4] ^= e;
	context->state[5] ^= f;
	context->state[6] ^= g;
	context->state[7] ^= h;

	/* Clean up */
	a = b = c = d = e = f = g = h = SS1 = SS2 = TT1 = TT2 = T = 0;
}

void
pg_sm3_update(pg_sm3_ctx *context, const uint8 *data, size_t len)
{
	size_t		freespace,
				usedspace;

	/* Calling with no data is valid (we do nothing) */
	if (len == 0)
		return;
    

	usedspace = (context->bitcount >> 3) % PG_SM3_BLOCK_LENGTH;
	if (usedspace > 0)
	{
		/* Calculate how much free space is available in the buffer */
		freespace = PG_SM3_BLOCK_LENGTH - usedspace;

		if (len >= freespace)
		{
			/* Fill the buffer completely and process it */
			memcpy(&context->buffer[usedspace], data, freespace);
			context->bitcount += freespace << 3;
			len -= freespace;
			data += freespace;
			SM3_Transform(context, context->buffer);
		}
		else
		{
			/* The buffer is not yet full */
			memcpy(&context->buffer[usedspace], data, len);
			context->bitcount += len << 3;
			/* Clean up: */
			usedspace = freespace = 0;
			return;
		}
	}
	while (len >= PG_SM3_BLOCK_LENGTH)
	{
		/* Process as many complete blocks as we can */
		SM3_Transform(context, data);
		context->bitcount += PG_SM3_BLOCK_LENGTH << 3;
		len -= PG_SM3_BLOCK_LENGTH;
		data += PG_SM3_BLOCK_LENGTH;
	}
	if (len > 0)
	{
		/* There's left-overs, so save 'em */
		memcpy(context->buffer, data, len);
		context->bitcount += len << 3;
	}
	/* Clean up: */
	usedspace = freespace = 0;
}

static void
SM3_Last(pg_sm3_ctx *context)
{
	unsigned int usedspace;

	usedspace = (context->bitcount >> 3) % PG_SM3_BLOCK_LENGTH;
#ifndef WORDS_BIGENDIAN
	/* Convert FROM host byte order */
	REVERSE64(context->bitcount, context->bitcount);
#endif
	if (usedspace > 0)
	{
		/* Begin padding with a 1 bit: */
		context->buffer[usedspace++] = 0x80;

		if (usedspace <= PG_SM3_SHORT_BLOCK_LENGTH)
		{
			/* Set-up for the last transform: */
			memset(&context->buffer[usedspace], 0, PG_SM3_SHORT_BLOCK_LENGTH - usedspace);
		}
		else
		{
			if (usedspace < PG_SM3_BLOCK_LENGTH)
			{
				memset(&context->buffer[usedspace], 0, PG_SM3_BLOCK_LENGTH - usedspace);
			}
			/* Do second-to-last transform: */
			SM3_Transform(context, context->buffer);

			/* And set-up for the last transform: */
			memset(context->buffer, 0, PG_SM3_SHORT_BLOCK_LENGTH);
		}
	}
	else
	{
		/* Set-up for the last transform: */
		memset(context->buffer, 0, PG_SM3_SHORT_BLOCK_LENGTH);

		/* Begin padding with a 1 bit: */
		*context->buffer = 0x80;
	}
	/* Set the bit count: */
	*(uint64 *) &context->buffer[PG_SM3_SHORT_BLOCK_LENGTH] = context->bitcount;

	/* Final transform: */
	SM3_Transform(context, context->buffer);
}

void
pg_sm3_final(pg_sm3_ctx *context, uint8 *digest)
{
	/* If no digest buffer is passed, we don't bother doing this: */
	if (digest != NULL)
	{
		SM3_Last(context);

#ifndef WORDS_BIGENDIAN
		{
			/* Convert TO host byte order */
			int			j;

			for (j = 0; j < 8; j++)
			{
				REVERSE32(context->state[j], context->state[j]);
			}
		}
#endif
		memcpy(digest, context->state, PG_SM3_DIGEST_LENGTH);
	}

	/* Clean up state data: */
	memset(context, 0, sizeof(pg_sm3_ctx));
}

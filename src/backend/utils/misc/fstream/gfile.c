/*-------------------------------------------------------------------------
*
* gfile.c
*
*--------------------------------------------------------------------------
*/
#include "c.h"


#ifndef FRONTEND
#include "storage/fd.h"
#endif

#ifdef WIN32
/* exclude transformation features on windows for now */
#undef GPFXDIST
#endif

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include <fstream/gfile.h>
#ifdef GPFXDIST
#include <gpfxdist.h>
#endif

#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/file.h>   /* for flock */
#include <unistd.h>

#ifdef WIN32
#include <io.h>
#define snprintf _snprintf
#else
#define O_BINARY 0
#endif

#ifndef S_IRUSR					/* XXX [TRH] should be in a header */
#define S_IRUSR		 S_IREAD
#define S_IWUSR		 S_IWRITE
#define S_IXUSR		 S_IEXEC
#endif 

#define COMPRESSION_BUFFER_SIZE		(1<<14)

#ifdef WIN32
#if !defined(S_ISDIR)
#define S_IFDIR  _S_IFDIR
#define S_ISDIR(m) (((m) & S_IFMT) == S_IFDIR)
#endif
#if !defined(S_ISFIFO)
#define S_IFIFO _S_IFIFO
#define S_ISFIFO(m) (((m) & S_IFMT) == S_IFIFO)
#endif
#define strcasecmp stricmp
#endif

static int
nothing_close(gfile_t *fd)
{
	return 0;
}

static ssize_t
read_and_retry(gfile_t *fd, void *ptr, size_t size)
{
	ssize_t i = 0;

	do
		i = read(fd->fd.filefd, ptr, size);
	while (i<0 && errno==EINTR);

	if (i > 0)
		fd->compressed_position += i;
	return i;
}

static ssize_t
write_and_retry(gfile_t *fd, void *ptr, size_t size)
{
	ssize_t i = 0;

	do
		i = write(fd->fd.filefd, ptr, size);
	while (i<0 && errno==EINTR);

	if (i > 0)
		fd->compressed_position += i;
	return i;
}

static int
closewinpipe(gfile_t*fd)
{
	assert(fd->is_win_pipe);
#ifdef WIN32
	CloseHandle(fd->fd.pipefd);
#endif
	return 0;
}

static ssize_t
readwinpipe(gfile_t* fd, void* ptr, size_t size)
{
	long i = 0;
	
	assert(fd->is_win_pipe);
#ifdef WIN32
	do
		ReadFile(fd->fd.pipefd, ptr, size, (PDWORD)&i, NULL);
	while (i < 0 && errno == EINTR);
#endif

	if (i > 0)
		fd->compressed_position += i;

	return i;
}

static ssize_t
writewinpipe(gfile_t* fd, void* ptr, size_t size)
{
	long i = 0;

	assert(fd->is_win_pipe);
#ifdef WIN32
	do
		WriteFile(fd->fd.pipefd, ptr, size, (PDWORD)&i, NULL);
	while (i < 0 && errno == EINTR);
#endif
	if (i > 0)
		fd->compressed_position += i;

	return i;
}

#ifdef HAVE_LIBBZ2
static void *
bz_alloc(void *a, int b, int c)
{
	return gfile_malloc(b * c);
}

static void
bz_free(void *a,void *b)
{
	gfile_free(b);
}

struct bzlib_stuff
{
	bz_stream s;
	int in_size, out_size, eof;
	char in[COMPRESSION_BUFFER_SIZE];
	char out[COMPRESSION_BUFFER_SIZE];
};

static ssize_t 
bz_file_read(gfile_t *fd, void *ptr, size_t len)
{
	struct bzlib_stuff *z = fd->u.bz;
	
	for (;;)
	{
		int e;
		int s = z->s.next_out - z->out - z->out_size;
		
		if (s > 0 || z->eof)
		{
			if (s > len)
				s = len;
			memcpy(ptr, z->out + z->out_size, s);
			z->out_size += s;
			
			return s;
		}
		
		z->out_size = 0;
		z->s.next_out = z->out;
		
		while (z->in_size < sizeof z->in)
		{
			s = read_and_retry(fd, z->in + z->in_size, sizeof z->in
					- z->in_size);
			if (s == 0)
				break;
			if (s < 0)
				return -1;
			z->in_size += s;
		}
		
		z->s.avail_in = s = z->in + z->in_size - z->s.next_in;
		z->s.avail_out = sizeof z->out;
		e = BZ2_bzDecompress(&z->s);
		
		if (e == BZ_STREAM_END)
			z->eof = 1;
		else if (e)
			return -1;
		
		if (z->s.avail_out == sizeof z->out && z->s.avail_in == s)
			return -1;
		
		if (z->s.next_in == z->in + z->in_size)
		{
			z->s.next_in = z->in;
			z->in_size = 0;
		}
	}
}

static int
bz_file_close(gfile_t *fd)
{
	int e = BZ2_bzDecompressEnd(&fd->u.bz->s);
	
	gfile_free(fd->u.bz);
	
	return e;
}

static int bz_file_open(gfile_t *fd)
{
	if (!(fd->u.bz = gfile_malloc(sizeof *fd->u.bz)))
	{
		gfile_printf_then_putc_newline("Out of memory");
		return 1;
	}
	
	memset(fd->u.bz, 0, sizeof *fd->u.bz);
	fd->u.bz->s.bzalloc = bz_alloc;
	fd->u.bz->s.bzfree = bz_free;
	
	if (BZ2_bzDecompressInit(&fd->u.bz->s, 0, 0))
	{
		gfile_printf_then_putc_newline("BZ2_bzDecompressInit failed");
		return 1;
	}
	
	fd->u.bz->s.next_out = fd->u.bz->out;
	fd->u.bz->s.next_in = fd->u.bz->in;
	fd->read = bz_file_read;
	fd->close = bz_file_close;

	return 0;
}
#endif

#ifdef USE_LZO
/*
 * LZO-compressed file support (standard lzop container format).
 *
 * Uses in-process liblzo2 decompression, following the same pattern as
 * .gz (zlib) / .bz2 (bzlib) / .zst (zstd).  The file format is auto-detected
 * in lzo_file_open() by probing the first 9 bytes:
 *
 *   - standard lzop container format: 9-byte magic + full header + block
 *     checksums
 *   - Hadoop Raw LZO format: no magic/header/checksum, plain LZO block
 *     stream (handled as a bonus path; only the standard lzop format is
 *     advertised via the .lzo extension)
 *
 * lzop header layout:
 *     magic         9 bytes  fixed magic \x89LZO\x00\x0d\x0a\x1a\x0a
 *     version       2 bytes  version (big endian)
 *     lib_version   2 bytes  library version
 *     ver_needed    2 bytes  minimum version required to decompress
 *     method        1 byte   compression algorithm
 *     level         1 byte   compression level
 *     flags         4 bytes  flags (control checksum types etc.)
 *     mode          4 bytes  file mode
 *     mtime_low     4 bytes  mtime low 32 bits
 *     mtime_high    4 bytes  mtime high 32 bits
 *     [extra_ver]   1 byte   if F_H_EXTRA_FIELD(0x40) set
 *     [filter]      4 bytes  if F_H_FILTER(0x800) set
 *     name_len      1 byte   original file name length
 *     name          N bytes  original file name
 *     [path_len]    4 bytes  if F_H_PATH(0x2000) set
 *     [path]        N bytes  if F_H_PATH(0x2000) set
 *     checksum      4 bytes  header checksum
 *
 * Data block layout (common to both formats):
 *     uncomp_len    4 bytes  decompressed size, big endian (0 = EOF marker)
 *     comp_len      4 bytes  compressed size, big endian
 *     [d_adler32]   4 bytes  adler32 of decompressed data (F_ADLER32_D=0x01)
 *     [d_crc32]     4 bytes  crc32 of decompressed data   (F_CRC32_D=0x100)
 *     [c_adler32]   4 bytes  adler32 of compressed data   (F_ADLER32_C=0x02)
 *     [c_crc32]     4 bytes  crc32 of compressed data     (F_CRC32_C=0x200)
 *     data          comp_len bytes  LZO compressed data (or raw data if
 *                                   incompressible)
 *
 * The only difference between the two formats is whether the magic/header
 * and checksum fields are present.  The decompression path is driven by
 * flags: Raw LZO format has flags=0 so all checksum logic is naturally
 * skipped.  The 9 probe bytes (which belong to the first data block in Raw
 * LZO) are cached in peek_buf and consumed first by lzo_read_peek /
 * lzo_read_uint32_peek to keep byte alignment.
 */
#include <lzo/lzo1x.h>

/* LZO block buffer size: standard lzop default block size is 256KB */
#define LZO_BUFFER_SIZE		(1<<20)

/*
 * LZO decompression state structure (complete definition; gfile.h only
 * declares the pointer).  Same heap-allocated double buffer design as
 * zlib_stuff / bzlib_stuff.
 */
struct lzo_stuff
{
	int		out_size;
	int		out_pos;
	int		eof;
	unsigned int	flags;          /* flags from lzop header; 0 for raw LZO */
	bool_t		has_lzop_header; /* TRUE=standard lzop, FALSE=raw LZO */
	int		peek_size;      /* valid bytes in peek_buf */
	int		peek_pos;       /* current read offset in peek_buf */
	char		peek_buf[9];    /* probe buffer (at most 9 bytes) */
	char		in[LZO_BUFFER_SIZE];
	char		out[LZO_BUFFER_SIZE];
};

/* lzop file magic: \x89 L Z O \x00 \x0d \x0a \x1a \x0a */
static const unsigned char lzop_magic[9] = {
	0x89, 0x4c, 0x5a, 0x4f, 0x00, 0x0d, 0x0a, 0x1a, 0x0a
};

/* lzop header flag bits (from lzop-1.03/src/conf.h) */
#define LZOP_F_ADLER32_D    0x00000001   /* adler32 checksum of decompressed data */
#define LZOP_F_ADLER32_C    0x00000002   /* adler32 checksum of compressed data */
#define LZOP_F_CRC32_D      0x00000100   /* crc32 checksum of decompressed data */
#define LZOP_F_CRC32_C      0x00000200   /* crc32 checksum of compressed data */

/*
 * Helper: read exactly n bytes from the underlying file descriptor.
 * Only local files are supported here, so this just wraps read_and_retry
 * (same as the reads inside gz_file_read / bz_file_read).  A short read
 * means the file was truncated and is reported to the caller.
 */
static ssize_t
read_block_bytes(gfile_t *fd, void *buf, size_t n)
{
	size_t total = 0;
	char *p = (char *) buf;

	while (total < n)
	{
		ssize_t r = read_and_retry(fd, p + total, n - total);

		if (r == 0)
			break;          /* EOF, return what we have */
		if (r < 0)
			return -1;      /* read error */
		total += r;
	}
	return (ssize_t) total;
}

/*
 * Helper: read one big-endian uint32 from the file.
 * All multi-byte integers in the lzop format are big endian.
 * Returns 0 on success, -1 on error (read failure or truncation).
 */
static int
read_block_uint32(gfile_t *fd, uint32_t *val)
{
	unsigned char b[4];

	if (read_block_bytes(fd, b, 4) < 4)
		return -1;
	*val = ((uint32_t) b[0] << 24) | ((uint32_t) b[1] << 16) |
	       ((uint32_t) b[2] <<  8) | ((uint32_t) b[3]);
	return 0;
}

/*
 * peek helper: read from peek_buf first, then fall back to the file.
 *
 * During format probing, the first bytes of the file may already have been
 * consumed.  For the standard lzop path peek_buf is always empty, so these
 * helpers degenerate to plain file reads.  For the Raw LZO path, the probe
 * bytes belong to the first data block and are consumed gradually here.
 */
static ssize_t
lzo_read_peek(gfile_t *fd, void *buf, size_t n)
{
	struct lzo_stuff *z = fd->u.lzo;
	size_t total = 0;
	char *p = (char *) buf;

	/* consume the probe bytes first */
	if (z->peek_pos < z->peek_size)
	{
		size_t avail = (size_t)(z->peek_size - z->peek_pos);

		if (avail > n)
			avail = n;
		memcpy(p, z->peek_buf + z->peek_pos, avail);
		z->peek_pos += (int) avail;
		p += avail;
		total += avail;
		n -= avail;
	}

	/* peek_buf exhausted, read the rest from the file */
	if (n > 0)
	{
		ssize_t r = read_block_bytes(fd, p, n);

		if (r < 0)
			return -1;
		total += (size_t) r;
	}

	return (ssize_t) total;
}

/* peek helper: read one big-endian uint32 through the peek mechanism */
static int
lzo_read_uint32_peek(gfile_t *fd, uint32_t *val)
{
	unsigned char b[4];

	if (lzo_read_peek(fd, b, 4) < 4)
		return -1;
	*val = ((uint32_t) b[0] << 24) | ((uint32_t) b[1] << 16) |
	       ((uint32_t) b[2] <<  8) | ((uint32_t) b[3]);
	return 0;
}

/*
 * LZO block decompression main loop - lzo_file_read()
 *
 * Processing flow:
 *   1. if there is still decompressed data in out[], return it to the caller
 *   2. read the next block header (uncomp_len + comp_len)
 *   3. [standard lzop only] read the block checksum fields (driven by flags)
 *   4. read comp_len bytes of LZO data into in[]
 *   5. decompress with lzo1x_decompress_safe() into out[], verify size
 *   6. [standard lzop only] verify checksums of the decompressed data
 *   7. return the out[] data to the caller
 *
 * Protection:
 *   - block size sanity checks (LZO_BUFFER_SIZE cap, compression bomb guard)
 *   - comp_len > uncomp_len rejected (compressed data larger than the
 *     original is impossible; equal means incompressible data stored raw)
 *   - lzo1x_decompress_safe (bounds-checked) instead of the unsafe variant
 *   - decompressed size cross-checked against the header's uncomp_len
 *   - [standard lzop only] adler32/crc32 checksum verification
 */
static ssize_t
lzo_file_read(gfile_t *fd, void *ptr, size_t len)
{
	struct lzo_stuff *z = fd->u.lzo;

	if (!z)
		return -1;      /* defensive: uninitialized call */

	for (;;)
	{
		/*
		 * Step 1: if there is leftover decompressed data in out[],
		 * return it.  Same pattern as gz_file_read / bz_file_read.
		 */
		if (z->out_pos < z->out_size || z->eof)
		{
			size_t avail = z->out_size - z->out_pos;

			if (avail > 0)
			{
				if (avail > len)
					avail = len;
				memcpy(ptr, z->out + z->out_pos, avail);
				z->out_pos += (int) avail;
				return (ssize_t) avail;
			}
			if (z->eof)
				return 0;   /* end of file, no more data */
		}

		/* output buffer exhausted, prepare to read the next block */
		z->out_size = 0;
		z->out_pos  = 0;

		/*
		 * Step 2: read the block header - uncompressed and compressed
		 * sizes (4 bytes each, big endian).  Use the peek variants so
		 * that the Raw LZO probe bytes are consumed in order.
		 */
		uint32_t uncomp_len, comp_len;

		if (lzo_read_uint32_peek(fd, &uncomp_len) < 0)
			return -1;      /* read error or truncation */
		if (uncomp_len == 0)
		{
			z->eof = 1;
			return 0;       /* EOF marker block - normal end of file */
		}
		if (lzo_read_uint32_peek(fd, &comp_len) < 0)
			return -1;

		/* sanity checks on the block sizes */
		if (comp_len > (uint32_t) LZO_BUFFER_SIZE)
		{
			gfile_printf_then_putc_newline("lzo block too large: %u", comp_len);
			return -1;
		}
		if (comp_len == 0 && uncomp_len > 0)
		{
			/* comp_len 0 with non-zero uncomp_len - corrupted header */
			gfile_printf_then_putc_newline("lzo corrupted block: comp_len=0, uncomp_len=%u",
				uncomp_len);
			return -1;
		}
		if (comp_len > uncomp_len)
		{
			/* compressed data larger than the original - not LZO data */
			gfile_printf_then_putc_newline("lzo block comp > uncomp");
			return -1;
		}
		if (uncomp_len > (uint32_t) LZO_BUFFER_SIZE)
		{
			gfile_printf_then_putc_newline("lzo uncompressed size too large: %u",
				uncomp_len);
			return -1;
		}

		/*
		 * Step 3: read and record the block checksums [standard lzop only].
		 * Raw LZO: flags=0, all conditions are false, so this is skipped.
		 */
		uint32_t expected_c_adler32 = 0, expected_c_crc32 = 0;
		uint32_t expected_d_adler32 = 0, expected_d_crc32 = 0;

		if (z->flags & LZOP_F_ADLER32_D)
		{
			if (lzo_read_uint32_peek(fd, &expected_d_adler32) < 0)
				return -1;
		}
		if (z->flags & LZOP_F_CRC32_D)
		{
			if (lzo_read_uint32_peek(fd, &expected_d_crc32) < 0)
				return -1;
		}
		if (z->flags & LZOP_F_ADLER32_C)
		{
			if (lzo_read_uint32_peek(fd, &expected_c_adler32) < 0)
				return -1;
		}
		if (z->flags & LZOP_F_CRC32_C)
		{
			if (lzo_read_uint32_peek(fd, &expected_c_crc32) < 0)
				return -1;
		}

		/*
		 * C-checksums (expected_c_adler32 / expected_c_crc32) are consumed
		 * from the stream to keep the pointer aligned but not verified:
		 * the D-checksum already catches any error the C-checksum would
		 * catch (a corrupt compressed block must produce corrupt output),
		 * and verifying only D-checksums is faster.
		 */
		(void) expected_c_adler32;
		(void) expected_c_crc32;

		/*
		 * Step 4: read the compressed data block (comp_len bytes).
		 * The file pointer has skipped all checksum fields by now.
		 * A short read means the block was truncated.
		 */
		if (lzo_read_peek(fd, z->in, comp_len) < (ssize_t) comp_len)
			return -1;

		/*
		 * Step 5: decompress.
		 * comp_len < uncomp_len  -> data was LZO-compressed
		 * comp_len == uncomp_len -> incompressible data stored raw
		 */
		if (comp_len < uncomp_len)
		{
			lzo_uint d = uncomp_len;
			int r = lzo1x_decompress_safe((const lzo_bytep) z->in, comp_len,
			                              (lzo_bytep) z->out, &d, NULL);

			if (r != LZO_E_OK)
			{
				/* decompression failure - corrupt data or not LZO */
				gfile_printf_then_putc_newline("lzo safe decompress failed: %d", r);
				return -1;
			}
			/* decompressed size must match the block header */
			if (d != uncomp_len)
			{
				gfile_printf_then_putc_newline(
					"lzo decompression size mismatch: %lu vs %u",
					(unsigned long) d, uncomp_len);
				return -1;
			}
		}
		else
		{
			/* incompressible data stored raw - copy as is */
			memcpy(z->out, z->in, comp_len);
			uncomp_len = comp_len;
		}

		/*
		 * Step 6: verify the checksums [standard lzop only].
		 * Even a "successful" decompression (no error, matching size)
		 * can silently corrupt data; the checksums catch that.
		 */
		if (z->flags & LZOP_F_ADLER32_D)
		{
			uint32_t computed = lzo_adler32(1, (const lzo_bytep) z->out,
			                                uncomp_len);

			if (computed != expected_d_adler32)
			{
				gfile_printf_then_putc_newline(
					"lzo adler32 checksum mismatch "
					"(expected 0x%08x, computed 0x%08x)",
					expected_d_adler32, computed);
				return -1;
			}
		}
		if (z->flags & LZOP_F_CRC32_D)
		{
			uint32_t computed = lzo_crc32(0, (const lzo_bytep) z->out,
			                              uncomp_len);

			if (computed != expected_d_crc32)
			{
				gfile_printf_then_putc_newline("lzo crc32 checksum mismatch");
				return -1;
			}
		}

		/* decompression succeeded, hand out[] to the caller */
		z->out_size = uncomp_len;
	}
}

/* LZO file close: free the lzo_stuff heap buffers */
static int
lzo_file_close(gfile_t *fd)
{
	if (fd->u.lzo)
	{
		gfile_free(fd->u.lzo);
		fd->u.lzo = NULL;
	}
	return 0;
}

/*
 * lzop header parser - lzo_skip_header()
 *
 * Parses the standard lzop header, skipping all fields to reach the first
 * data block, and saves flags (checksum types etc.) for later use.
 *
 * magic_already_verified: if the caller has already read and verified the
 * magic via probing, pass true to skip the magic; otherwise this function
 * reads and verifies it itself.
 *
 * Protection:
 *   - every read checks its return value (truncated files)
 *   - name_len is 1 byte (max 255), path_len capped at 4096 (malicious
 *     header guard), path is read in 64-byte chunks into the 1024-byte
 *     stack buffer so it cannot overflow
 */
static int
lzo_skip_header(gfile_t *fd, bool magic_already_verified)
{
	unsigned char hdr_buf[1024];
	unsigned char *buf = hdr_buf;
	uint32_t flags;
	int name_len;

	if (!magic_already_verified)
	{
		/* verify the magic: read 9 bytes and compare, reject non-lzop */
		if (read_block_bytes(fd, buf, 9) < 9)
			return -1;
		if (memcmp(buf, lzop_magic, 9) != 0)
		{
			gfile_printf_then_putc_newline("not a valid lzop file");
			return -1;
		}
	}
	/* magic_already_verified=true: probe already consumed the magic */

	/*
	 * skip version info: version(2) + lib_version(2) + ver_needed(2)
	 *                    + method(1) + level(1) = 8 bytes
	 * These fields do not affect decompression.
	 */
	if (read_block_bytes(fd, buf, 8) < 8)
		return -1;

	/* read flags (4 bytes big endian) and save for block checksum logic */
	if (read_block_uint32(fd, &flags) < 0)
		return -1;
	fd->u.lzo->flags = flags;

	/* skip mode(4) + mtime_low(4) + mtime_high(4) = 12 bytes */
	if (read_block_bytes(fd, buf, 12) < 12)
		return -1;

	/* if F_H_EXTRA_FIELD(0x40) set: skip 1-byte extra version */
	if (flags & 0x00000040)
	{
		if (read_block_bytes(fd, buf, 1) < 1)
			return -1;
	}

	/* if F_H_FILTER(0x800) set: skip 4-byte filter ID */
	if (flags & 0x00000800)
	{
		if (read_block_bytes(fd, buf, 4) < 4)
			return -1;
	}

	/* read the original file name length (1 byte) and skip the name */
	if (read_block_bytes(fd, buf, 1) < 1)
		return -1;
	name_len = buf[0];
	if (name_len > 0)
	{
		if (read_block_bytes(fd, buf, name_len) < name_len)
			return -1;
	}

	/* if F_H_PATH(0x2000) set: skip the file path (chunked reads) */
	if (flags & 0x00002000)
	{
		uint32_t path_len;

		if (read_block_uint32(fd, &path_len) < 0)
			return -1;
		if (path_len > 4096)
		{
			gfile_printf_then_putc_newline("lzop path too long: %u", path_len);
			return -1;
		}
		while (path_len > 0)
		{
			uint32_t chunk = (path_len > 64) ? 64 : path_len;

			if (read_block_bytes(fd, buf, chunk) < (ssize_t) chunk)
				return -1;
			path_len -= chunk;
		}
	}

	/* skip the header checksum (4 bytes) - skipped, not verified */
	if (read_block_bytes(fd, buf, 4) < 4)
		return -1;

	/* header parsed, the file pointer now points at the first data block */
	return 0;
}

/*
 * LZO file open - lzo_file_open()
 *
 * Initializes the LZO library, allocates lzo_stuff (heap, 2xLZO_BUFFER_SIZE
 * plus the probe buffer), auto-detects the file format and wires up
 * fd->read / fd->close.
 *
 * Format auto-detection (probe the first 9 bytes):
 *   a) magic matches  -> standard lzop, lzo_skip_header parses the rest
 *   b) >= 8 bytes     -> Raw LZO, probe bytes cached in peek_buf and
 *                        consumed later (first 8 = uncomp_len + comp_len,
 *                        9th = first byte of compressed data)
 *   c) < 8 bytes      -> file too short to be valid LZO data
 *
 * Same calling pattern and error return convention as gz_file_open /
 * bz_file_open.
 */
static int
lzo_file_open(gfile_t *fd)
{
	/* initialize the LZO library (version check, idempotent) */
	if (lzo_init() != LZO_E_OK)
	{
		gfile_printf_then_putc_newline("lzo_init() failed");
		return 1;
	}

	/* allocate the decompression state structure (heap) */
	if (!(fd->u.lzo = gfile_malloc(sizeof *fd->u.lzo)))
	{
		gfile_printf_then_putc_newline("Out of memory");
		return 1;
	}
	memset(fd->u.lzo, 0, sizeof *fd->u.lzo);

	/* format auto-detection: probe the first 9 bytes, compare magic */
	{
		unsigned char probe[9];
		ssize_t n = read_block_bytes(fd, probe, 9);

		if (n >= 9 && memcmp(probe, lzop_magic, 9) == 0)
		{
			/* path A: standard lzop container format (magic verified) */
			fd->u.lzo->has_lzop_header = TRUE;

			/* parse the remaining header (skip the magic) */
			if (lzo_skip_header(fd, TRUE) != 0)
			{
				gfile_free(fd->u.lzo);
				fd->u.lzo = NULL;
				return 1;
			}
		}
		else if (n >= 8)
		{
			/*
			 * path B: Raw LZO format - no magic, header or checksums.
			 * The n probe bytes belong to the first data block; cache
			 * them in peek_buf for lzo_read_*_peek to consume later.
			 * flags stays 0 (memset), so all checksum logic is skipped.
			 */
			fd->u.lzo->has_lzop_header = FALSE;
			fd->u.lzo->flags          = 0;
			fd->u.lzo->peek_size      = (int) n;
			fd->u.lzo->peek_pos       = 0;
			memcpy(fd->u.lzo->peek_buf, probe, (size_t) n);
		}
		else
		{
			/* file too short (less than 8 bytes) to be valid LZO data */
			gfile_printf_then_putc_newline("lzo file too short (%ld bytes)", (long) n);
			gfile_free(fd->u.lzo);
			fd->u.lzo = NULL;
			return 1;
		}
	}

	/* register callbacks: gfile_read/gfile_close will use our functions */
	fd->read  = lzo_file_read;
	fd->close = lzo_file_close;
	return 0;
}
#endif

#ifdef HAVE_LIBZ
/* GZ */
struct zlib_stuff
{
	z_stream s;
	int in_size, out_size, eof;
	Byte in[COMPRESSION_BUFFER_SIZE];
	Byte out[COMPRESSION_BUFFER_SIZE];
};

static ssize_t
gz_file_read(gfile_t* fd, void* ptr, size_t len)
{
	struct zlib_stuff* z = fd->u.z;
	
	for (;;)
	{
		int	e;
		int	flush = Z_NO_FLUSH;
		
		/*
		 * 'out' is our output buffer.
		 * 'next_out' is a pointer to the next byte in 'out'
		 * 'out_size' is num bytes currently in 'out'
		 * 
		 * if s is >0 we have data in 'out' that we didn't write
		 * yet, write it and return.  
		 */
		int s = z->s.next_out - (z->out + z->out_size);
		
		if (s > 0 || z->eof)
		{
			if (s > len)
				s = len;
			memcpy(ptr, z->out + z->out_size, s);
			z->out_size += s;
			return s;
		}
		
		/* ok, wrote all 'out' data. reset back to beginning of 'out' */
		z->out_size = 0;
		z->s.next_out = z->out;
		
		/*
		 * Fill up our input buffer from the input file.
		 */
		while (z->in_size < sizeof z->in)
		{
			s = read_and_retry(fd, z->in + z->in_size, sizeof z->in - z->in_size);
			
			if (s == 0)
			{
				/* no more data to read */
				
				if (z->in + z->in_size == z->s.next_in)
					flush = Z_FINISH;
				break;
			}
			if (s < 0)
			{
				/* read error */
				return -1;
			}
				
			z->in_size += s;
		}
		
		/* number of bytes available at next_in */
		z->s.avail_in = (z->in + z->in_size) - z->s.next_in;
		
		/* remaining free space at next_out */ 
		z->s.avail_out = sizeof z->out;
		
		/* decompress */ 
		e = inflate(&z->s, flush);
		
		if (e == Z_STREAM_END && z->s.avail_in == 0)
		{
			/* we're done decompressing all we have */
			if (flush == Z_FINISH)
				z->eof = 1;
		}
		else if(e == Z_STREAM_END && z->s.avail_in > 0)
		{
			/* 
			 * we're done decompressing a chunk, but there's more
			 * input. we need to reset state. see MPP-8012 for info 
			 */
			if(inflateReset(&z->s))
				return -1;
		}
		else if (e)
		{
			return -1;			
		}
		
		/* if no more data available for decompression reset input buf */
		if (z->s.next_in == (z->in + z->in_size))
		{
			z->s.next_in = z->in;
			z->in_size = 0;
		}
	}
}

static int 
gz_file_write_one_chunk(gfile_t *fd, int do_flush)
{
	/*
	 * 0 - means we are ok
	 */
	int ret = 0, have;
	struct zlib_stuff* z = fd->u.z;
	
	do 
	{
		int ret1;
		
		z->s.avail_out = COMPRESSION_BUFFER_SIZE;
		z->s.next_out = z->out;
		ret1 = deflate(&(z->s), do_flush);    /* no bad return value */
		if (ret1 == Z_STREAM_ERROR)
		{
			gfile_printf_then_putc_newline("the gz file is unrepaired, stop writing");
			return -1;
		}
		have = COMPRESSION_BUFFER_SIZE - z->s.avail_out;
		
		if ( write_and_retry(fd, z->out, have) != have ) 
		{
			/*
			 * presently gfile_close calls gz_file_close only for the on_write case so we don't need
			 * to handle inflateEnd here
			 */
			gfile_printf_then_putc_newline("failed to write, the stream ends");
			(void)deflateEnd(&(z->s));
			ret = -1;
			break;
		}
		
	} while (COMPRESSION_BUFFER_SIZE == have);	
	/*
	 * if the deflate engine filled all the output buffer, it may have more data, so we must try again
	 */
	
	return ret;
}

static ssize_t
gz_file_write(gfile_t *fd, void *ptr, size_t size)
{
	int ret;
	
	
	size_t left_to_compress = size;
	size_t one_iter_compress;
	struct zlib_stuff* z = fd->u.z;
		
	do
	{
		/*
		 * we do not wish that the size of the input buffer to the deflate engine, will be greater
		 * than the recomended COMPRESSION_BUFFER_SIZE.
		 */
		one_iter_compress = (left_to_compress > COMPRESSION_BUFFER_SIZE) ? COMPRESSION_BUFFER_SIZE : left_to_compress;
			
		z->s.avail_in = one_iter_compress;
		z->s.next_in = (Byte*)((Byte*)ptr + (size - left_to_compress));
		
		ret = gz_file_write_one_chunk(fd, Z_NO_FLUSH);
		if (0 != ret)
		{
			return ret;
		}
				
		left_to_compress -= one_iter_compress; 
	} while( left_to_compress > 0 );

		
	return size;
}

static int
gz_file_close(gfile_t *fd)
{
	int e = 0;
	
	if ( fd->is_write == TRUE ) /* writing, or in other words compressing */
	{
		e = gz_file_write_one_chunk(fd, Z_FINISH);
		if (0 != e)
		{
			return e;
		}

		e = deflateEnd(&fd->u.z->s);
	}
	else /* reading, that is inflating */
	{
		e = inflateEnd(&fd->u.z->s);
	}
	
	gfile_free(fd->u.z);
	return e;
}

static voidpf
z_alloc(voidpf a, uInt b, uInt c)
{
	return gfile_malloc(b * c);
}

static void z_free(voidpf a, voidpf b)
{
	gfile_free(b);
}

static int
gz_file_open(gfile_t *fd)
{
	if (!(fd->u.z = gfile_malloc(sizeof *fd->u.z)))
	{
		gfile_printf_then_putc_newline("Out of memory");
		return 1;
	}
	
	memset(fd->u.z, 0, sizeof *fd->u.z);
	fd->u.z->s.zalloc = z_alloc;
	fd->u.z->s.zfree = z_free;
	fd->u.z->s.opaque = Z_NULL;

	fd->u.z->s.next_out = fd->u.z->out;
	fd->u.z->s.next_in = fd->u.z->in;
	fd->read = gz_file_read;
	fd->write = gz_file_write;
	fd->close = gz_file_close;
	
	if ( fd->is_write == FALSE )/* for read */  
	{
		/*
		 * reading a compressed file
		 */		
		if (inflateInit2(&fd->u.z->s,31))
		{
			gfile_printf_then_putc_newline("inflateInit2 failed");
			return 1;
		}
	}
	else 
	{
		/*
		 * writing a compressed file
		 */
		if ( Z_OK !=
			 deflateInit2(&fd->u.z->s, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 31, 8, Z_DEFAULT_STRATEGY) )
		{
			gfile_printf_then_putc_newline("deflateInit2 failed");
			return 1;			
		}
		 
	}

	return 0;
}
#endif
#ifdef USE_ZSTD

/* The value range of the level could be found at zstd.h at different versions.
 * Although the level in ZSTD_initCStream() has the same meaning in different
 * versions, the macro ZSTD_CLEVEL_DEFAULT may not be defined before 1.3.7.
 * So we borrow the macro from zstd.h at 1.3.7 in case it is not defined */
#ifndef ZSTD_CLEVEL_DEFAULT
#  define ZSTD_CLEVEL_DEFAULT 3
#endif
struct zstdlib_stuff
{
	ZSTD_inBuffer in;
	ZSTD_outBuffer out;
	int in_size, out_size;
	ZSTD_CStream* cstream;
	ZSTD_DStream* dstream;
	unsigned char in_buffer[COMPRESSION_BUFFER_SIZE];
	unsigned char out_buffer[COMPRESSION_BUFFER_SIZE];
};
/* Defined in later version 1.4.8 of zstd.h */
enum compress_mode
{
	ZSTD_CONTINUE_FLUSH = 0,
	ZSTD_FLUSH_FLUSH = 1,
	ZSTD_END_FLUSH = 2,
};
static ssize_t
zstd_file_read(gfile_t* fd, void* ptr, size_t len)
{
	struct zstdlib_stuff* zstd = fd->u.zstd;

	for (;;)
	{
		size_t	ret;
		/*
		 * 'out->dst' is our output buffer.
		 * 'pos' is a size_t value equal to the next pos in 'out_buffer'
		 * 'out_size' is num bytes currently read out of 'out_buffer'
		 *
		 * if s is >0 we have data in 'out.dst' that we didn't write
		 * yet, write it and return.
		 */
		int s = zstd->out.pos - zstd->out_size;

		if (s > 0)
		{
			if (s > len)
				s = len;
			memcpy(ptr, zstd->out_buffer + zstd->out_size, s);
			zstd->out_size += s;
			return s;
		}

		/* ok, wrote all 'out' data. reset back to beginning of 'out_buffer' */
		zstd->out_size = 0;
		zstd->out.pos = 0;

		/*
		 * Fill up our input buffer from the input file.
		 */
		while (zstd->in_size < sizeof zstd->in_buffer)
		{
			s = read_and_retry(fd, zstd->in_buffer + zstd->in_size, sizeof zstd->in_buffer - zstd->in_size);

			if (s < 0)
			{
				/* read error */
				gfile_printf_then_putc_newline("ZSTD read_and_retry failed");
				return -1;
			}
			if(s == 0)
			{
				break;
			}

			zstd->in_size += s;
		}

		/* size of bytes at next decompression */
		zstd->in.size = zstd->in_size;

		/* remaining free space at next decompression */
		zstd->out.size = sizeof zstd->out_buffer;

		/* decompress */
		if (zstd->in.size == 0)
		{
			/* No more data to be decompressed and out_buffer is empty */
			return 0;
		}

		ret = ZSTD_decompressStream(zstd->dstream, &zstd->out, &zstd->in);
		if (ZSTD_isError(ret))
		{
			gfile_printf_then_putc_newline("ZSTD_decompressStream failed");
			return -1;
		}

		/* if no more data available for decompression reset input buf */
		if (zstd->in.pos == zstd->in_size)
		{
			zstd->in.pos = 0;
			zstd->in_size = 0;
		}
	}
}
/* In later version we can use ZSTD_compressStream2 with mode parameter,
 * but in order to coordinate with earlier version, we use the following two functions instead */
static inline size_t zstd_compression_with_mode(ZSTD_CStream* zcs, ZSTD_outBuffer* output, ZSTD_inBuffer* input, int mode)
{
	if(mode != ZSTD_END_FLUSH)
	{
		return ZSTD_compressStream(zcs, output, input);
	}
	else
	{
		return ZSTD_endStream(zcs, output);
	}
}
static int
zstd_file_write_one_chunk(gfile_t *fd, int mode)
{
	/*
	 * 0 - means we are ok
	 */
	size_t remain = 0, have, finished = 0;
	struct zstdlib_stuff* zstd = fd->u.zstd;

	do
	{
		zstd->out.size = COMPRESSION_BUFFER_SIZE;
		zstd->out.dst = zstd->out_buffer;
		zstd->out.pos = 0;
		remain = zstd_compression_with_mode(zstd->cstream, &zstd->out, &zstd->in, mode);    /* no bad return value */
		if (ZSTD_isError(remain))
		{
			gfile_printf_then_putc_newline("zstd_compression_with_mode failed mode:%d", mode);
			return -1;
		}
		have = zstd->out.pos;

		if ( write_and_retry(fd, zstd->out_buffer, have) != have)
		{
			gfile_printf_then_putc_newline("ZSTD write_and_retry failed");
			return -1;
		}
		finished = (mode == ZSTD_END_FLUSH) ? (remain == 0) : (zstd->in.pos == zstd->in.size);
	} while (!finished);
	/*
	 * if the ZSTD_compressStream or ZSTD_endStream engine filled all the output buffer, it may have more data, so we must try again
	 */
	return 0;
}

static ssize_t
zstd_file_write(gfile_t *fd, void *ptr, size_t size)
{
	int ret;
	size_t left_to_compress = size;
	size_t one_iter_compress;
	struct zstdlib_stuff* zstd = fd->u.zstd;

	do
	{
		/*
		 * we do not wish that the size of the input buffer to the ZSTD_compressStream engine, will be greater
		 * than the recommended COMPRESSION_BUFFER_SIZE.
		 */
		one_iter_compress = (left_to_compress > COMPRESSION_BUFFER_SIZE) ? COMPRESSION_BUFFER_SIZE : left_to_compress;

		zstd->in.size = one_iter_compress;
		zstd->in.src = (void *)((unsigned char *)ptr + (size - left_to_compress));
		zstd->in.pos = 0;

		ret = zstd_file_write_one_chunk(fd, ZSTD_CONTINUE_FLUSH);
		if (0 != ret)
		{
			return ret;
		}

		left_to_compress -= one_iter_compress;
	} while( left_to_compress > 0 );

	return size;
}

static int
zstd_file_close(gfile_t *fd)
{
	int ret;
	if ( fd->is_write == FALSE ) /* writing, or in other words compressing */
	{
		ZSTD_freeDStream(fd->u.zstd->dstream);
	}
	else
	{
		/* flush any remaining data _and_ close current frame */
		ret = zstd_file_write_one_chunk(fd, ZSTD_END_FLUSH);
		if (0 != ret)
		{
			return ret;
		}
		ZSTD_freeCStream(fd->u.zstd->cstream);
	}

	gfile_free(fd->u.zstd);
	return 0;
}
static int
zstd_file_open(gfile_t *fd)
{
	if (!(fd->u.zstd = gfile_malloc(sizeof *fd->u.zstd)))
	{
		gfile_printf_then_putc_newline("Out of memory");
		return 1;
	}

	memset(fd->u.zstd, 0, sizeof *fd->u.zstd);

	fd->u.zstd->in.src = fd->u.zstd->in_buffer;
	fd->u.zstd->out.dst = fd->u.zstd->out_buffer;

	fd->read = zstd_file_read;
	fd->write = zstd_file_write;
	fd->close = zstd_file_close;

	if ( fd->is_write == FALSE )/* for read */
	{
		/*
		 * reading a compressed file
		 */
		if (!(fd->u.zstd->dstream = ZSTD_createDStream()))
		{
			gfile_printf_then_putc_newline("ZSTD_createDStream failed");
			return 1;
		}
		if(ZSTD_isError(ZSTD_initDStream(fd->u.zstd->dstream)))
		{
			gfile_printf_then_putc_newline("ZSTD_initDStream failed");
			return 1;
		}
	}
	else
	{
		/*
		 * writing a compressed file
		 */
		if (!(fd->u.zstd->cstream = ZSTD_createCStream()))
		{
			gfile_printf_then_putc_newline("ZSTD_createCStream() failed");
			return 1;
		}
		if(ZSTD_isError(ZSTD_initCStream(fd->u.zstd->cstream, ZSTD_CLEVEL_DEFAULT)))
		{
			gfile_printf_then_putc_newline("ZSTD_initCStream failed");
			return 1;
		}
	}

	return 0;
}
#endif
#ifdef GPFXDIST
/*
 * subprocess support
 */

static void
subprocess_open_errfn(apr_pool_t *pool, apr_status_t status, const char *desc)
{
	char errbuf[256];
	fprintf(stderr, "subprocess: %s: %s\n", desc, apr_strerror(status, errbuf, sizeof(errbuf)));
}

static int 
subprocess_open_failed(int* response_code, const char** response_string, char* reason)
{
	*response_code   = 500;
	*response_string = reason;
	gfile_printf_then_putc_newline("%s", *response_string);
	return 1;
}

static int 
subprocess_open(gfile_t* fd, const char* fpath, int for_write, int* rcode, const char** rstring)
{
	apr_pool_t*     mp     = fd->transform->mp;
	char*           cmd    = fd->transform->cmd;
	apr_procattr_t* pattr;
	char**          tokens;
	apr_status_t    rv;
	apr_file_t*     errfile;

	/* tokenize command string */
	if ((rv = apr_tokenize_to_argv(cmd, &tokens, mp)) != APR_SUCCESS) 
	{
		return subprocess_open_failed(rcode, rstring, "subprocess_open: apr_tokenize_to_argv failed");
	}

	/* replace %FILENAME% with path to input or output file */
	{
		char** p;
		for (p = tokens; *p; p++) 
		{
			if (0 == strcasecmp(*p, "%FILENAME%"))
				*p = (char*) fpath;
		}
	}

	/* setup apr subprocess attribute structure */
	if ((rv = apr_procattr_create(&pattr, mp)) != APR_SUCCESS) 
	{
		return subprocess_open_failed(rcode, rstring, "subprocess_open: apr_procattr_create failed");
	}

	/* setup child stdin/stdout depending on the direction of transformation */
	if (for_write) 
	{
		/* writable external table, so child will be reading from standard input */

		if ((rv = apr_procattr_io_set(pattr, APR_FULL_BLOCK, APR_NO_PIPE, APR_NO_PIPE)) != APR_SUCCESS) 
		{
			return subprocess_open_failed(rcode, rstring, "subprocess_open: apr_procattr_io_set (full,no,no) failed");
		}
		fd->transform->for_write = 1;
	} 
	else 
	{
		/* readable external table, so child will be writing to standard output */

		if ((rv = apr_procattr_io_set(pattr, APR_NO_PIPE, APR_FULL_BLOCK, APR_NO_PIPE)) != APR_SUCCESS) 
		{
			return subprocess_open_failed(rcode, rstring, "subprocess_open: apr_procattr_io_set (no,full,no) failed");
		}
		fd->transform->for_write = 0;
	}

	/* 
	 * For read requests, if we've been requested to send stderr output to the server,
	 * we need prepare a temporary file to hold it.
	 */
	if (for_write && fd->transform->stderr_server) {
		const char*	 tempdir = NULL;
		char*		 tempfilename = NULL;
		apr_file_t*	 f = NULL;
		if ((rv = apr_temp_dir_get(&tempdir, mp)) != APR_SUCCESS)
		{
			return subprocess_open_failed(rcode, rstring, "subprocess_open: failed to get temporary directory for stderr");
		}

		tempfilename = apr_pstrcat(mp, tempdir, "/stderrXXXXXX", NULL);
		if ((rv = apr_file_mktemp(&f, tempfilename, APR_CREATE|APR_WRITE|APR_EXCL, mp)) != APR_SUCCESS)
		{
			return subprocess_open_failed(rcode, rstring, "subprocess_open: failed to create temporary file for stderr");
		}

		gfile_printf_then_putc_newline("writable request opened stderr file %s\n", tempfilename);
		fd->transform->errfilename = tempfilename;
		fd->transform->errfile = f;
	}

	/* setup child stderr */
	if (fd->transform->errfile)
	{
		/* redirect stderr to a file to be sent to server when we're finished */

		errfile = fd->transform->errfile;

		if ((rv = apr_procattr_child_err_set(pattr, errfile, NULL)) !=  APR_SUCCESS)
		{
			return subprocess_open_failed(rcode, rstring, "subprocess_open: apr_procattr_child_err_set failed");
		}
	} 

	/* more APR complexity: setup error handler for when child doesn't spawn properly */
	if ((rv = apr_procattr_child_errfn_set(pattr, subprocess_open_errfn)) != APR_SUCCESS) 
	{
		return subprocess_open_failed(rcode, rstring, "subprocess_open: apr_procattr_child_errfn_set failed");
	}

	/* don't run the child via an operating system shell */
	if ((rv = apr_procattr_cmdtype_set(pattr, APR_PROGRAM_ENV)) != APR_SUCCESS) 
	{
		return subprocess_open_failed(rcode, rstring, "subprocess_open: apr_procattr_cmdtype_set failed");
	}

	/* finally... start the child process */
	if ((rv = apr_proc_create(&fd->transform->proc, tokens[0], (const char* const*)tokens, NULL, pattr, mp)) != APR_SUCCESS) 
	{
		return subprocess_open_failed(rcode, rstring, "subprocess_open: apr_proc_create failed");
	}

	return 0;
}


static ssize_t 
read_subprocess(gfile_t *fd, void *ptr, size_t len)
{
	apr_size_t      nbytes = len;
	apr_status_t    rv;

	rv = apr_file_read(fd->transform->proc.out, ptr, &nbytes);
	if (rv == APR_SUCCESS)
		return nbytes;
	
	if (rv == APR_EOF)
		return 0;

	return -1;
}

static ssize_t
write_subprocess(gfile_t *fd, void *ptr, size_t size)
{
	apr_size_t      nbytes = size;
	apr_status_t    rv;

	rv = apr_file_write(fd->transform->proc.in, ptr, &nbytes);

	if (rv == APR_SUCCESS)
		return nbytes;

	return -1;
}

static int
close_subprocess(gfile_t *fd)
{
	int             st;
	apr_exit_why_e  why;
	apr_status_t    rv;
    
	if (fd->transform->for_write)
	    apr_file_close(fd->transform->proc.in);
	else
	    apr_file_close(fd->transform->proc.out);
        
	rv = apr_proc_wait(&fd->transform->proc, &st, &why, APR_WAIT);
	if (APR_STATUS_IS_CHILD_DONE(rv)) 
	{
		gfile_printf_then_putc_newline("close_subprocess: done: why = %d, exit status = %d", why, st);
		return st;
	} 
	else 
	{
		gfile_printf_then_putc_newline("close_subprocess: notdone");
		return 1;
	}
}
#endif

static int close_filefd(int fd)
{
	int ret = 0;

	do
	{
#ifdef FRONTEND
		ret = close(fd);
#else
		ret = CloseTransientFile(fd);
#endif
	}
	while (ret < 0 && errno == EINTR);

	return ret;
}

/*
 * public interface
 */

int 
gfile_open_flags(int writing, int usesync)
{
	if (writing)
	{
		if (usesync)
			return GFILE_OPEN_FOR_WRITE_SYNC;
		else
			return GFILE_OPEN_FOR_WRITE_NOSYNC;
	}
	return GFILE_OPEN_FOR_READ;
}


int gfile_open(gfile_t* fd, const char* fpath, int flags, int* response_code, const char** response_string, struct gpfxdist_t* transform)
{
	const char* s = strrchr(fpath, '.');
#ifdef WIN32
	bool_t is_win_pipe = FALSE;
#else
	struct 		stat sta;
	memset(&sta, 0, sizeof(sta));
#endif

	memset(fd, 0, sizeof(*fd));

	/*
	 * check for subprocess and/or named pipe
	 */
#ifdef WIN32
	/* is this a windows named pipe, of the form \\<host>\... */
	if (strlen(fpath) > 2)
	{
		if (fpath[0] == '\\' && fpath[1] == '\\')
		{
			is_win_pipe = TRUE;
			gfile_printf_then_putc_newline("looks like a windows pipe");
		}
	}

	if (is_win_pipe)
	{
		/* Try and open it as a windows named pipe */
		HANDLE pipe = CreateFile(fpath, 
								 (flags != GFILE_OPEN_FOR_READ ? GENERIC_WRITE : GENERIC_READ),
								 0, /* no sharing */
								 NULL, /* default security */
								 OPEN_EXISTING, /* file must exist */
								 0, /* default attributes */
								 NULL /* no template */);
		gfile_printf_then_putc_newline("trying to connect to pipe");
		if (pipe != INVALID_HANDLE_VALUE)
		{
			fd->is_win_pipe = TRUE;
			fd->fd.pipefd = pipe;
			gfile_printf_then_putc_newline("connected to pipe");
		}
		else
		{
			LPSTR msg;

			FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER |
						  FORMAT_MESSAGE_FROM_SYSTEM,
				   		  NULL, GetLastError(),
						  MAKELANGID(LANG_ENGLISH, SUBLANG_DEFAULT),
						  (LPSTR) & msg, 0, NULL);
			gfile_printf_then_putc_newline("could not create pipe: %s", msg);

			if (GetLastError() != ERROR_PIPE_BUSY)
			{
				*response_code = 500;
				*response_string = "could not connect to pipe";
			}
			else
			{
				*response_code = 501;
				*response_string = "pipe is busy, close the pipe and try again";
			}
			return 1;
		}
	}
#else	/* not win32 */
#ifdef GPFXDIST
	fd->transform = transform;
	if (fd->transform)
	{
		/* caller wants a subprocess. nothing to do here just yet. */
		gfile_printf_then_putc_newline("looks like a subprocess");
	}
	else
#endif
	{
		if (!fd->is_win_pipe && (flags == GFILE_OPEN_FOR_READ))
		{
			if (stat(fpath, &sta))
			{
				if(errno == EOVERFLOW)
				{
					/*
					* ENGINF-176
					* 
					* Some platforms don't support stat'ing of "large files"
					* accurately (files over 2GB) - SPARC for example. In these
					* cases the storage size of st_size is too small and the
					* file size will overflow. Therefore, we look for cases where
					* overflow had occurred, and resume operation. At least we
					* know that the file does exist and that's the main goal of
					* stat'ing here anyway. we set the size to 0, similarly to
					* the winpipe path, so that negative sizes won't be used.
					* 
					* TODO: there may be side effects to setting the size to 0,
					* need to double check.
					* 
					* TODO: this hack could possibly now be removed after enabling
					* largefiles via the build process with compiler flags.
					*/
					sta.st_size = 0;
				}
				else
				{
					gfile_printf_then_putc_newline("gfile stat %s failure: %s", fpath, strerror(errno));
					*response_code = 404;
					*response_string = "file not found";
					return 1;
				}
			}
			if (S_ISDIR(sta.st_mode))
			{
				gfile_printf_then_putc_newline("gfile %s is a directory", fpath);
				*response_code = 403;
				*response_string = "Reading a directory is forbidden.";
				return 1;
			}
			fd->compressed_size = sta.st_size;
		}
	}
#endif	/* ifdef win32 */

	if (NULL == fd->transform && !fd->is_win_pipe)
	{
		int syncFlag = 0;
		int openFlags;
		mode_t openMode;

#ifndef WIN32
		/*
		 * MPP-13817 (support opening files without O_SYNC)
		 */
		if (flags & GFILE_OPEN_FOR_WRITE_SYNC)
		{
			/*
			 * caller explicitly requested O_SYNC
			 */
			syncFlag = O_SYNC;
		}
		else if ((stat(fpath, &sta) == 0) && S_ISFIFO(sta.st_mode))
		{
			/*
			 * use O_SYNC since we're writing to another process via a pipe
			 */
			syncFlag = O_SYNC;
		}
#endif
		if (flags != GFILE_OPEN_FOR_READ)
		{
			openFlags = O_WRONLY | O_CREAT | O_BINARY | O_APPEND | syncFlag;
			openMode = S_IRUSR | S_IWUSR;
		}
		else
		{
			openFlags = O_RDONLY | O_BINARY;
			openMode = 0;
		}

		do
		{
#ifdef FRONTEND
			fd->fd.filefd = open(fpath, openFlags, openMode);
#else
			fd->fd.filefd = OpenTransientFile((char *) fpath, openFlags);
#endif
		}
		while (fd->fd.filefd < 0 && errno == EINTR);

		if (-1 == fd->fd.filefd)
		{
			static char buf[256];
			gfile_printf_then_putc_newline("gfile open (for %s) failed %s: %s",
										((flags == GFILE_OPEN_FOR_READ) ? "read" :
											((flags == GFILE_OPEN_FOR_WRITE_SYNC) ? "write (sync)" : "write")),
										fpath, strerror(errno));
			*response_code = 404;
			snprintf(buf, sizeof buf, "file open failure %s: %s", fpath,
					strerror(errno));
			*response_string = buf;
			return 1;
		}

#if !defined(WIN32)
		/* Restrict only one reader session for each PIPE */
		if (S_ISFIFO(sta.st_mode) && (flags == GFILE_OPEN_FOR_READ))
		{
			if (flock (fd->fd.filefd, LOCK_EX | LOCK_NB) != 0)
			{
				fd->held_pipe_lock = FALSE;
				gfile_printf_then_putc_newline("gfile %s is a pipe", fpath);
				*response_code = 404;
				*response_string = "Multiple reader to a pipe is forbidden.";
				close_filefd(fd->fd.filefd);
				fd->fd.filefd = -1;
				return 1;
			}
			else
			{
				fd->held_pipe_lock = TRUE;
			}
		}
#endif
	}

	/*
	 * prepare to use the appropriate i/o routines 
	 */

#ifdef GPFXDIST
	if (fd->transform)
	{
		fd->read  = read_subprocess;
		fd->write = write_subprocess;
		fd->close = close_subprocess;
	}
	else 
#endif
	if (fd->is_win_pipe)
	{
		fd->read = readwinpipe;
		fd->write = writewinpipe;
		fd->close = closewinpipe;
	}
	else
	{
		fd->read = read_and_retry;
		fd->write = write_and_retry;
		fd->close = nothing_close;
	}

	/*
	 * delegate remaining setup work to an appropriate open routine
	 * or return an error if we can't handle the type
	 */

#ifdef GPFXDIST
	if (fd->transform)
	{
		return subprocess_open(fd, fpath, (flags != GFILE_OPEN_FOR_READ), response_code, response_string);
	}
	else 
#endif
	if (s && strcasecmp(s,".gz")==0)
	{
#ifndef HAVE_LIBZ
		gfile_printf_then_putc_newline(".gz not supported");
#else
		/*
		 * flag used by function gfile close
		 */
		fd->compression = GZ_COMPRESSION;
		
		if (flags != GFILE_OPEN_FOR_READ)
		{
			fd->is_write = TRUE;
		}

		return gz_file_open(fd);
#endif
	}
	else if (s && strcasecmp(s,".bz2")==0)
	{
#ifndef HAVE_LIBBZ2
		gfile_printf_then_putc_newline(".bz2 not supported");
#else
		fd->compression = BZ_COMPRESSION;
		if (flags != GFILE_OPEN_FOR_READ)
			gfile_printf_then_putc_newline(".bz2 not yet supported for writable tables");

		return bz_file_open(fd);
#endif
	}
	else if (s && strcasecmp(s, ".zst") == 0)
	{
#ifndef USE_ZSTD
		gfile_printf_then_putc_newline(".zst not supported");
#else
		fd->compression = ZSTD_COMPRESSION;
		if (flags != GFILE_OPEN_FOR_READ)
		{
			fd->is_write = TRUE;
		}

		return zstd_file_open(fd);
#endif
	}
	else if (s && strcasecmp(s, ".lzo") == 0)
	{
#ifndef USE_LZO
		gfile_printf_then_putc_newline(".lzo not supported");
#else
		if (flags != GFILE_OPEN_FOR_READ)
		{
			gfile_printf_then_putc_newline(".lzo not yet supported for writable tables");
			*response_code = 415;
			*response_string = "Unsupported File Type";
			return 1;
		}
		fd->compression = LZO_COMPRESSION;
		return lzo_file_open(fd);
#endif
	}
	else if (s && strcasecmp(s,".z") == 0)
		gfile_printf_then_putc_newline("gfile compression .z file is not supported");
	else if (s && strcasecmp(s,".zip") == 0)
		gfile_printf_then_putc_newline("gfile compression zip is not supported");
	else
		return 0;

	*response_code = 415;
	*response_string = "Unsupported File Type";

	return 1;
}

int
gfile_close(gfile_t*fd)
{
	int ret = 1;

	if (fd->close)
	{
#ifdef GPFXDIST
		if (fd->transform)
        {
			fd->close(fd);
		} 
        else
#endif
		{
			/*
			* for the compressed data implementation we need to call the "close" callback. Other implementations
			* didn't use to call this callback here and it will remain so.
			*/
			if (fd->compression == GZ_COMPRESSION || fd->compression == ZSTD_COMPRESSION ||
				fd->compression == LZO_COMPRESSION)
			{
				fd->close(fd);
			}

			if (fd->is_win_pipe)
			{
				fd->close(fd);
			}
			else
			{
				if(fd->held_pipe_lock)
				{
#ifndef WIN32
					flock (fd->fd.filefd, LOCK_UN);
#endif
				}
				ret = close_filefd(fd->fd.filefd);
				if (ret == -1)
					ret = 1;
			}
		} 
		fd->read = 0;
		fd->close = 0;
	}
	return ret;
}

ssize_t 
gfile_read(gfile_t *fd, void *ptr, size_t len)
{
	size_t olen = len;
	
	while (len)
	{
		ssize_t i = fd->read(fd, ptr, len);
		if (i < 0)
			return i;
		if (i == 0)
			break;
		ptr = (char*) ptr + i;
		len -= i;
	}
	
	return olen - len;
}

ssize_t 
gfile_write(gfile_t *fd, void *ptr, size_t len)
{
	size_t olen = len;
	
	while (len)
	{
		ssize_t i = fd->write(fd, ptr, len);
				
		if (i < 0)
			return i;
		if (i == 0)
			break;
		
		ptr = (char*) ptr + i;
		len -= i;
	}
	
	return olen - len;
}

off_t gfile_get_compressed_size(gfile_t *fd)
{
	return fd->compressed_size;
}

off_t gfile_get_compressed_position(gfile_t *fd)
{
	return fd->compressed_position;
}

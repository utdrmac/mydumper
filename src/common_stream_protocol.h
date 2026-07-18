/*
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

        Multiplexed binary stream protocol shared by mydumper and myloader.

    The protocol replaces the legacy textual "\n-- <name> <size>\n" framing.
    It allows several files produced concurrently by dump workers to be
    interleaved over a single pipe, and it does not require the file size to
    be known up front (so files can be streamed directly from worker buffers
    without staging them on disk).

    Wire layout:

        MAGIC (8 bytes, incl. version)
        frame*
        [EOF frame]

    Each frame is:

        1 byte  frame type
        payload (type specific, see below)

      FILE_OPEN : varint stream_id, varint name_len, name bytes,
                  1 byte flags, 1 byte codec
      DATA      : varint stream_id, varint length, <length> raw bytes
      FILE_CLOSE: varint stream_id, varint total_size,
                  1 byte flags, [4 bytes crc32 if MYD_FCLOSE_FLAG_CRC32]
      EOF       : (no payload)

    When flags has MYD_FOPEN_FLAG_COMPRESSED, the DATA payloads for that file
    are compressed with the algorithm named by the codec byte (see enum
    myd_codec) and the FILE_CLOSE total_size/crc32 describe the *uncompressed*
    content, so the reader can verify after decompressing.

    Integers use unsigned LEB128 varints. All multi-byte fixed fields (the
    crc32) are little-endian.

    This module only depends on GLib so it can be unit tested without MySQL.
*/
#ifndef _common_stream_protocol_h
#define _common_stream_protocol_h
#include <glib.h>

#define MYD_STREAM_MAGIC "MYDSTRM2"
#define MYD_STREAM_MAGIC_LEN 8

enum myd_frame_type {
  MYD_FRAME_FILE_OPEN  = 1,
  MYD_FRAME_DATA       = 2,
  MYD_FRAME_FILE_CLOSE = 3,
  MYD_FRAME_EOF        = 4
};

/* FILE_OPEN flags */
#define MYD_FOPEN_FLAG_NONE       0x00
#define MYD_FOPEN_FLAG_COMPRESSED 0x01  /* DATA payload is compressed (see codec) */

/* Compression codec for a file's DATA payloads (FILE_OPEN codec byte). The
   codec is only meaningful when MYD_FOPEN_FLAG_COMPRESSED is set; otherwise it
   is MYD_CODEC_NONE. */
enum myd_codec {
  MYD_CODEC_NONE    = 0,
  MYD_CODEC_DEFLATE = 1, /* raw zlib deflate stream (zlib inflate)        */
  MYD_CODEC_GZIP    = 2, /* gzip-wrapped deflate (zlib inflate, 15+16)    */
  MYD_CODEC_ZSTD    = 3  /* zstd stream (libzstd)                         */
};

/* FILE_CLOSE flags */
#define MYD_FCLOSE_FLAG_NONE  0x00
#define MYD_FCLOSE_FLAG_CRC32 0x01

/* ---- Encoding (append into a binary-safe GString) ---- */
void myd_stream_append_magic(GString *out);
void myd_stream_append_varint(GString *out, guint64 v);
void myd_stream_encode_file_open(GString *out, guint64 stream_id,
                                 const gchar *filename, guint8 flags,
                                 guint8 codec);
/* Emit a DATA frame header only (type + stream_id + length); the caller is
   expected to append exactly `len` payload bytes immediately after. Useful to
   avoid copying large payloads through an intermediate buffer. */
void myd_stream_encode_data_header(GString *out, guint64 stream_id, gsize len);
/* Emit a complete DATA frame (header + payload copied from buf). */
void myd_stream_encode_data(GString *out, guint64 stream_id,
                            const gchar *buf, gsize len);
void myd_stream_encode_file_close(GString *out, guint64 stream_id,
                                  guint64 total_size, gboolean has_crc,
                                  guint32 crc);
void myd_stream_encode_eof(GString *out);

/* Read a varint from buf[*pos..len). On success advances *pos and returns
   TRUE; returns FALSE if the buffer does not (yet) contain a full varint. */
gboolean myd_stream_read_varint(const guchar *buf, gsize len, gsize *pos,
                                guint64 *out);

/* ---- Incremental decoding ---- */
struct myd_stream_callbacks {
  void (*on_file_open)(void *user, guint64 stream_id, const gchar *filename,
                       guint8 flags, guint8 codec);
  void (*on_data)(void *user, guint64 stream_id, const gchar *buf, gsize len);
  void (*on_file_close)(void *user, guint64 stream_id, guint64 total_size,
                        gboolean has_crc, guint32 crc);
  void (*on_eof)(void *user);
};

enum myd_decode_result {
  MYD_DECODE_OK = 0,       /* all supplied bytes were consumed */
  MYD_DECODE_ERROR = -1,   /* malformed stream */
  MYD_DECODE_NOT_BINARY = -2 /* leading bytes are not the binary magic */
};

struct myd_stream_decoder;
struct myd_stream_decoder *myd_stream_decoder_new(
    const struct myd_stream_callbacks *cb, void *user);
int myd_stream_decoder_feed(struct myd_stream_decoder *d, const guchar *buf,
                            gsize len);
gboolean myd_stream_decoder_saw_eof(struct myd_stream_decoder *d);
void myd_stream_decoder_free(struct myd_stream_decoder *d);

#endif

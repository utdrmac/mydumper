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

        Multiplexed binary stream protocol (see common_stream_protocol.h).
*/
#include <string.h>
#include "common_stream_protocol.h"

/* ---------- Encoding ---------- */

void myd_stream_append_varint(GString *out, guint64 v) {
  guint8 byte;
  do {
    byte = v & 0x7f;
    v >>= 7;
    if (v)
      byte |= 0x80;
    g_string_append_len(out, (const gchar *)&byte, 1);
  } while (v);
}

void myd_stream_append_magic(GString *out) {
  g_string_append_len(out, MYD_STREAM_MAGIC, MYD_STREAM_MAGIC_LEN);
}

static void append_u8(GString *out, guint8 v) {
  g_string_append_len(out, (const gchar *)&v, 1);
}

void myd_stream_encode_file_open(GString *out, guint64 stream_id,
                                 const gchar *filename, guint8 flags,
                                 guint8 codec) {
  gsize name_len = filename ? strlen(filename) : 0;
  append_u8(out, MYD_FRAME_FILE_OPEN);
  myd_stream_append_varint(out, stream_id);
  myd_stream_append_varint(out, name_len);
  if (name_len)
    g_string_append_len(out, filename, name_len);
  append_u8(out, flags);
  append_u8(out, codec);
}

void myd_stream_encode_data_header(GString *out, guint64 stream_id, gsize len) {
  append_u8(out, MYD_FRAME_DATA);
  myd_stream_append_varint(out, stream_id);
  myd_stream_append_varint(out, (guint64)len);
}

void myd_stream_encode_data(GString *out, guint64 stream_id, const gchar *buf,
                            gsize len) {
  myd_stream_encode_data_header(out, stream_id, len);
  if (len)
    g_string_append_len(out, buf, len);
}

void myd_stream_encode_file_close(GString *out, guint64 stream_id,
                                  guint64 total_size, gboolean has_crc,
                                  guint32 crc) {
  append_u8(out, MYD_FRAME_FILE_CLOSE);
  myd_stream_append_varint(out, stream_id);
  myd_stream_append_varint(out, total_size);
  append_u8(out, has_crc ? MYD_FCLOSE_FLAG_CRC32 : MYD_FCLOSE_FLAG_NONE);
  if (has_crc) {
    guint8 le[4];
    le[0] = crc & 0xff;
    le[1] = (crc >> 8) & 0xff;
    le[2] = (crc >> 16) & 0xff;
    le[3] = (crc >> 24) & 0xff;
    g_string_append_len(out, (const gchar *)le, 4);
  }
}

void myd_stream_encode_eof(GString *out) { append_u8(out, MYD_FRAME_EOF); }

/* ---------- Varint decode ---------- */

gboolean myd_stream_read_varint(const guchar *buf, gsize len, gsize *pos,
                                guint64 *out) {
  guint64 result = 0;
  guint shift = 0;
  gsize p = *pos;
  while (p < len) {
    guint8 b = buf[p++];
    result |= ((guint64)(b & 0x7f)) << shift;
    if (!(b & 0x80)) {
      *out = result;
      *pos = p;
      return TRUE;
    }
    shift += 7;
    if (shift >= 64)
      return FALSE; /* malformed / overflow: treated as need-more by caller */
  }
  return FALSE; /* need more bytes */
}

/* ---------- Incremental decoder ---------- */

enum decoder_state {
  DS_MAGIC = 0,
  DS_TYPE,
  DS_HEADER,       /* accumulating a (small) frame header in `hdr` */
  DS_DATA_PAYLOAD, /* streaming DATA bytes straight to on_data */
  DS_DONE
};

struct myd_stream_decoder {
  struct myd_stream_callbacks cb;
  void *user;
  enum decoder_state state;
  gsize magic_got;
  guint8 cur_type;
  GByteArray *hdr; /* header bytes for current frame */
  guint64 cur_stream_id;
  guint64 remaining_data; /* bytes left in current DATA payload */
  gboolean saw_eof;
};

struct myd_stream_decoder *myd_stream_decoder_new(
    const struct myd_stream_callbacks *cb, void *user) {
  struct myd_stream_decoder *d = g_new0(struct myd_stream_decoder, 1);
  d->cb = *cb;
  d->user = user;
  d->state = DS_MAGIC;
  d->hdr = g_byte_array_new();
  return d;
}

void myd_stream_decoder_free(struct myd_stream_decoder *d) {
  if (!d)
    return;
  g_byte_array_free(d->hdr, TRUE);
  g_free(d);
}

gboolean myd_stream_decoder_saw_eof(struct myd_stream_decoder *d) {
  return d->saw_eof;
}

/* Try to parse the header accumulated in d->hdr for d->cur_type.
   Returns 1 on complete parse (and acts on it), 0 if more bytes are needed,
   -1 on malformed input. */
static int try_parse_header(struct myd_stream_decoder *d) {
  const guchar *buf = d->hdr->data;
  gsize len = d->hdr->len;
  gsize pos = 0;
  guint64 stream_id = 0;

  switch (d->cur_type) {
  case MYD_FRAME_FILE_OPEN: {
    guint64 name_len = 0;
    if (!myd_stream_read_varint(buf, len, &pos, &stream_id))
      return 0;
    if (!myd_stream_read_varint(buf, len, &pos, &name_len))
      return 0;
    if (len < pos + name_len + 2)
      return 0;
    gchar *name = g_strndup((const gchar *)(buf + pos), name_len);
    guint8 flags = buf[pos + name_len];
    guint8 codec = buf[pos + name_len + 1];
    if (d->cb.on_file_open)
      d->cb.on_file_open(d->user, stream_id, name, flags, codec);
    g_free(name);
    d->state = DS_TYPE;
    g_byte_array_set_size(d->hdr, 0);
    return 1;
  }
  case MYD_FRAME_DATA: {
    guint64 length = 0;
    if (!myd_stream_read_varint(buf, len, &pos, &stream_id))
      return 0;
    if (!myd_stream_read_varint(buf, len, &pos, &length))
      return 0;
    d->cur_stream_id = stream_id;
    d->remaining_data = length;
    g_byte_array_set_size(d->hdr, 0);
    d->state = (length == 0) ? DS_TYPE : DS_DATA_PAYLOAD;
    return 1;
  }
  case MYD_FRAME_FILE_CLOSE: {
    guint64 total_size = 0;
    if (!myd_stream_read_varint(buf, len, &pos, &stream_id))
      return 0;
    if (!myd_stream_read_varint(buf, len, &pos, &total_size))
      return 0;
    if (len < pos + 1)
      return 0;
    guint8 flags = buf[pos++];
    guint32 crc = 0;
    gboolean has_crc = (flags & MYD_FCLOSE_FLAG_CRC32) != 0;
    if (has_crc) {
      if (len < pos + 4)
        return 0;
      crc = (guint32)buf[pos] | ((guint32)buf[pos + 1] << 8) |
            ((guint32)buf[pos + 2] << 16) | ((guint32)buf[pos + 3] << 24);
      pos += 4;
    }
    if (d->cb.on_file_close)
      d->cb.on_file_close(d->user, stream_id, total_size, has_crc, crc);
    d->state = DS_TYPE;
    g_byte_array_set_size(d->hdr, 0);
    return 1;
  }
  default:
    return -1;
  }
}

int myd_stream_decoder_feed(struct myd_stream_decoder *d, const guchar *buf,
                            gsize len) {
  gsize off = 0;
  while (off < len) {
    switch (d->state) {
    case DS_MAGIC: {
      gsize need = MYD_STREAM_MAGIC_LEN - d->magic_got;
      gsize avail = len - off;
      gsize take = avail < need ? avail : need;
      /* Compare incrementally against the expected magic. */
      if (memcmp(&MYD_STREAM_MAGIC[d->magic_got], buf + off, take) != 0)
        return MYD_DECODE_NOT_BINARY;
      d->magic_got += take;
      off += take;
      if (d->magic_got == MYD_STREAM_MAGIC_LEN)
        d->state = DS_TYPE;
      break;
    }
    case DS_TYPE: {
      d->cur_type = buf[off++];
      if (d->cur_type == MYD_FRAME_EOF) {
        d->saw_eof = TRUE;
        if (d->cb.on_eof)
          d->cb.on_eof(d->user);
        d->state = DS_DONE;
        break;
      }
      if (d->cur_type != MYD_FRAME_FILE_OPEN &&
          d->cur_type != MYD_FRAME_DATA &&
          d->cur_type != MYD_FRAME_FILE_CLOSE)
        return MYD_DECODE_ERROR;
      g_byte_array_set_size(d->hdr, 0);
      d->state = DS_HEADER;
      break;
    }
    case DS_HEADER: {
      /* Append one byte at a time and re-attempt the parse. Frame headers are
         tiny so this is cheap, and it lets us stop exactly at the header/body
         boundary for DATA frames. */
      g_byte_array_append(d->hdr, buf + off, 1);
      off++;
      int r = try_parse_header(d);
      if (r < 0)
        return MYD_DECODE_ERROR;
      /* r == 0 -> need more; r == 1 -> state already advanced */
      break;
    }
    case DS_DATA_PAYLOAD: {
      gsize avail = len - off;
      gsize take = (avail < d->remaining_data) ? avail : (gsize)d->remaining_data;
      if (take && d->cb.on_data)
        d->cb.on_data(d->user, d->cur_stream_id, (const gchar *)(buf + off),
                      take);
      off += take;
      d->remaining_data -= take;
      if (d->remaining_data == 0)
        d->state = DS_TYPE;
      break;
    }
    case DS_DONE:
      /* Ignore trailing bytes after EOF. */
      off = len;
      break;
    }
  }
  return MYD_DECODE_OK;
}

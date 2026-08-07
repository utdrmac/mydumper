/*
    Unit test for the multiplexed binary stream protocol.

    Exercises round-trip encode/decode with several interleaved files and a
    range of feed chunk sizes (1 byte, small, whole buffer) to catch frame /
    varint boundary bugs. Also checks non-binary (legacy) detection.

    Depends only on GLib, so it runs in CI without a MySQL server.
*/
#include <glib.h>
#include <string.h>
#include <zlib.h>
#include <zstd.h>
#include "../../src/common_stream_protocol.h"

#define STREAM_CHUNK 65536

struct rebuilt_file {
  gchar *name;
  guint8 flags;
  guint8 codec;
  GString *data;
  gboolean closed;
  guint64 declared_size;
  gboolean has_crc;
  guint32 crc;
};

struct collector {
  GHashTable *files; /* stream_id -> struct rebuilt_file */
  gboolean eof;
  gboolean cancel;
  gint cancel_before_eof;
};

static void on_open(void *user, guint64 sid, const gchar *name, guint8 flags,
                    guint8 codec) {
  struct collector *c = user;
  struct rebuilt_file *f = g_new0(struct rebuilt_file, 1);
  f->name = g_strdup(name);
  f->flags = flags;
  f->codec = codec;
  f->data = g_string_new("");
  g_hash_table_insert(c->files, GINT_TO_POINTER((int)sid), f);
}

static void on_data(void *user, guint64 sid, const gchar *buf, gsize len) {
  struct collector *c = user;
  struct rebuilt_file *f =
      g_hash_table_lookup(c->files, GINT_TO_POINTER((int)sid));
  g_assert_nonnull(f);
  g_string_append_len(f->data, buf, len);
}

static void on_close(void *user, guint64 sid, guint64 total, gboolean has_crc,
                     guint32 crc) {
  struct collector *c = user;
  struct rebuilt_file *f =
      g_hash_table_lookup(c->files, GINT_TO_POINTER((int)sid));
  g_assert_nonnull(f);
  f->closed = TRUE;
  f->declared_size = total;
  f->has_crc = has_crc;
  f->crc = crc;
}

static void on_eof(void *user) {
  struct collector *c = user;
  c->eof = TRUE;
  if (c->cancel)
    c->cancel_before_eof++;
}

static void on_cancel(void *user) {
  struct collector *c = user;
  c->cancel = TRUE;
  if (c->eof)
    c->cancel_before_eof--;
}

/* Build a representative stream: two interleaved files plus a large one. */
static GString *build_stream(GString *big_payload) {
  GString *s = g_string_new("");
  myd_stream_append_magic(s);

  /* Interleave stream 1 and 2. */
  myd_stream_encode_file_open(s, 1, "db.table.00001.sql", MYD_FOPEN_FLAG_NONE,
                              MYD_CODEC_NONE);
  myd_stream_encode_file_open(s, 2, "db.table-schema.sql", MYD_FOPEN_FLAG_NONE,
                              MYD_CODEC_NONE);
  myd_stream_encode_data(s, 1, "INSERT INTO ", 12);
  myd_stream_encode_data(s, 2, "CREATE TABLE ", 13);
  myd_stream_encode_data(s, 1, "t VALUES (1);\n", 14);
  myd_stream_encode_file_close(s, 2, 13, FALSE, 0);
  myd_stream_encode_file_close(s, 1, 12 + 14, TRUE, 0xdeadbeef);

  /* A large single file to exercise multi-chunk DATA payloads. */
  myd_stream_encode_file_open(s, 3, "db.big.00001.dat", MYD_FOPEN_FLAG_NONE,
                              MYD_CODEC_NONE);
  myd_stream_encode_data(s, 3, big_payload->str, big_payload->len);
  myd_stream_encode_file_close(s, 3, big_payload->len, FALSE, 0);

  myd_stream_encode_eof(s);
  return s;
}

static void verify(struct collector *c, GString *big_payload) {
  g_assert_true(c->eof);
  g_assert_cmpuint(g_hash_table_size(c->files), ==, 3);

  struct rebuilt_file *f1 = g_hash_table_lookup(c->files, GINT_TO_POINTER(1));
  struct rebuilt_file *f2 = g_hash_table_lookup(c->files, GINT_TO_POINTER(2));
  struct rebuilt_file *f3 = g_hash_table_lookup(c->files, GINT_TO_POINTER(3));

  g_assert_cmpstr(f1->name, ==, "db.table.00001.sql");
  g_assert_true(f1->closed);
  g_assert_cmpstr(f1->data->str, ==, "INSERT INTO t VALUES (1);\n");
  g_assert_cmpuint(f1->declared_size, ==, f1->data->len);
  g_assert_true(f1->has_crc);
  g_assert_cmphex(f1->crc, ==, 0xdeadbeef);

  g_assert_cmpstr(f2->name, ==, "db.table-schema.sql");
  g_assert_cmpstr(f2->data->str, ==, "CREATE TABLE ");
  g_assert_false(f2->has_crc);

  g_assert_cmpuint(f3->data->len, ==, big_payload->len);
  g_assert_cmpuint(f3->declared_size, ==, big_payload->len);
  g_assert_true(memcmp(f3->data->str, big_payload->str, big_payload->len) == 0);
}

static void free_file(gpointer p) {
  struct rebuilt_file *f = p;
  g_free(f->name);
  g_string_free(f->data, TRUE);
  g_free(f);
}

static void run_with_chunk(GString *stream, GString *big_payload,
                           gsize chunk) {
  struct collector c = {0};
  c.files = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL,
                                  free_file);
  struct myd_stream_callbacks cb = {on_open, on_data, on_close, on_eof, NULL};
  struct myd_stream_decoder *d = myd_stream_decoder_new(&cb, &c);

  gsize off = 0;
  while (off < stream->len) {
    gsize n = MIN(chunk, stream->len - off);
    int r = myd_stream_decoder_feed(d, (const guchar *)stream->str + off, n);
    g_assert_cmpint(r, ==, MYD_DECODE_OK);
    off += n;
  }
  g_assert_true(myd_stream_decoder_saw_eof(d));
  verify(&c, big_payload);

  myd_stream_decoder_free(d);
  g_hash_table_destroy(c.files);
}

static void test_roundtrip(void) {
  GString *big = g_string_new("");
  for (int i = 0; i < 100000; i++)
    g_string_append_c(big, (char)('A' + (i % 26)));

  GString *stream = build_stream(big);

  /* Feed at several granularities to stress frame/varint boundaries. */
  gsize chunks[] = {1, 2, 3, 7, 64, 1000, 100000, stream->len};
  for (guint i = 0; i < G_N_ELEMENTS(chunks); i++)
    run_with_chunk(stream, big, chunks[i]);

  g_string_free(stream, TRUE);
  g_string_free(big, TRUE);
}

static void test_not_binary(void) {
  struct collector c = {0};
  c.files = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL,
                                  free_file);
  struct myd_stream_callbacks cb = {on_open, on_data, on_close, on_eof, NULL};
  struct myd_stream_decoder *d = myd_stream_decoder_new(&cb, &c);

  const char *legacy = "\n-- db.table.00001.sql 42\nINSERT ...";
  int r = myd_stream_decoder_feed(d, (const guchar *)legacy, strlen(legacy));
  g_assert_cmpint(r, ==, MYD_DECODE_NOT_BINARY);

  myd_stream_decoder_free(d);
  g_hash_table_destroy(c.files);
}

static void test_cancel_before_eof(void) {
  GString *s = g_string_new("");
  myd_stream_append_magic(s);
  myd_stream_encode_cancel(s);
  myd_stream_encode_eof(s);

  struct collector c = {0};
  c.files = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL,
                                  free_file);
  struct myd_stream_callbacks cb = {on_open, on_data, on_close, on_eof,
                                    on_cancel};
  struct myd_stream_decoder *d = myd_stream_decoder_new(&cb, &c);

  g_assert_cmpint(myd_stream_decoder_feed(d, (const guchar *)s->str, s->len),
                  ==, MYD_DECODE_OK);
  g_assert_true(c.cancel);
  g_assert_true(c.eof);
  g_assert_cmpint(c.cancel_before_eof, ==, 1);

  myd_stream_decoder_free(d);
  g_hash_table_destroy(c.files);
  g_string_free(s, TRUE);
}

static void test_varint(void) {
  guint64 values[] = {0, 1, 127, 128, 300, 16384, 0xffffffffULL,
                      0x123456789abcdefULL};
  for (guint i = 0; i < G_N_ELEMENTS(values); i++) {
    GString *s = g_string_new("");
    myd_stream_append_varint(s, values[i]);
    gsize pos = 0;
    guint64 out = 0;
    gboolean ok =
        myd_stream_read_varint((const guchar *)s->str, s->len, &pos, &out);
    g_assert_true(ok);
    g_assert_cmpuint(out, ==, values[i]);
    g_assert_cmpuint(pos, ==, s->len);
    g_string_free(s, TRUE);
  }
}

/* Mirrors the compression wire contract used by mydumper/myloader: the DATA
   payload carries deflate-compressed bytes, the FILE_OPEN carries the
   COMPRESSED flag, and FILE_CLOSE size/crc describe the *uncompressed* content.
   The consumer inflates and validates against those. */
static void test_compressed_roundtrip(void) {
  GString *plain = g_string_new("");
  for (int i = 0; i < 50000; i++)
    g_string_append_printf(plain, "INSERT INTO t VALUES (%d,'row-%d');\n", i, i);

  /* deflate */
  uLongf bound = compressBound(plain->len);
  guchar *comp = g_malloc(bound);
  uLongf comp_len = bound;
  g_assert_cmpint(compress(comp, &comp_len, (const Bytef *)plain->str,
                           plain->len), ==, Z_OK);

  uLong seed = crc32(0L, Z_NULL, 0);
  guint32 plain_crc =
      (guint32)crc32(seed, (const Bytef *)plain->str, plain->len);

  GString *stream = g_string_new("");
  myd_stream_append_magic(stream);
  myd_stream_encode_file_open(stream, 1, "db.t.00000.sql",
                              MYD_FOPEN_FLAG_COMPRESSED, MYD_CODEC_DEFLATE);
  myd_stream_encode_data(stream, 1, (const gchar *)comp, comp_len);
  myd_stream_encode_file_close(stream, 1, plain->len, TRUE, plain_crc);
  myd_stream_encode_eof(stream);

  struct collector c = {0};
  c.files = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL,
                                  free_file);
  struct myd_stream_callbacks cb = {on_open, on_data, on_close, on_eof, NULL};
  struct myd_stream_decoder *d = myd_stream_decoder_new(&cb, &c);
  /* Feed 3 bytes at a time to stress boundaries. */
  for (gsize off = 0; off < stream->len; off += 3){
    gsize n = MIN((gsize)3, stream->len - off);
    g_assert_cmpint(
        myd_stream_decoder_feed(d, (const guchar *)stream->str + off, n), ==,
        MYD_DECODE_OK);
  }

  struct rebuilt_file *f = g_hash_table_lookup(c.files, GINT_TO_POINTER(1));
  g_assert_nonnull(f);
  g_assert_true(f->flags & MYD_FOPEN_FLAG_COMPRESSED);
  g_assert_cmpuint(f->codec, ==, MYD_CODEC_DEFLATE);

  /* inflate the received DATA and verify it matches the original. */
  GString *received_comp = f->data;
  uLongf out_len = plain->len;
  guchar *out = g_malloc(out_len);
  g_assert_cmpint(uncompress(out, &out_len,
                             (const Bytef *)received_comp->str,
                             received_comp->len), ==, Z_OK);
  g_assert_cmpuint(out_len, ==, f->declared_size);
  guint32 got = (guint32)crc32(seed, out, out_len);
  g_assert_cmphex(got, ==, f->crc);
  g_assert_true(memcmp(out, plain->str, plain->len) == 0);

  g_free(out);
  myd_stream_decoder_free(d);
  g_hash_table_destroy(c.files);
  g_string_free(stream, TRUE);
  g_free(comp);
  g_string_free(plain, TRUE);
}

static GString *make_payload(void) {
  GString *p = g_string_new("");
  for (int i = 0; i < 50000; i++)
    g_string_append_printf(p, "INSERT INTO t VALUES (%d,'row-%d');\n", i, i);
  return p;
}

/* Mirrors mydumper's gzip codec (deflateInit2 windowBits 15+16, streamed in
   chunks) decoded with myloader's settings (inflateInit2 windowBits 15+32).
   Proves the dump/load window-bit choices are mutually compatible. */
static void test_gzip_codec(void) {
  GString *plain = make_payload();

  z_stream cs = {0};
  g_assert_cmpint(deflateInit2(&cs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8,
                               Z_DEFAULT_STRATEGY), ==, Z_OK);
  GString *comp = g_string_new("");
  gsize off = 0;
  do {
    gsize take = MIN((gsize)STREAM_CHUNK, plain->len - off);
    gboolean last = (off + take >= plain->len);
    cs.next_in = (Bytef *)(plain->str + off);
    cs.avail_in = (uInt)take;
    off += take;
    do {
      guchar out[STREAM_CHUNK];
      cs.next_out = out;
      cs.avail_out = sizeof(out);
      g_assert_cmpint(deflate(&cs, last ? Z_FINISH : Z_NO_FLUSH), !=,
                      Z_STREAM_ERROR);
      g_string_append_len(comp, (gchar *)out, sizeof(out) - cs.avail_out);
    } while (cs.avail_out == 0);
  } while (off < plain->len);
  deflateEnd(&cs);

  z_stream ds = {0};
  g_assert_cmpint(inflateInit2(&ds, 15 + 32), ==, Z_OK);
  GString *got = g_string_new("");
  ds.next_in = (Bytef *)comp->str;
  ds.avail_in = (uInt)comp->len;
  do {
    guchar out[STREAM_CHUNK];
    ds.next_out = out;
    ds.avail_out = sizeof(out);
    int r = inflate(&ds, Z_NO_FLUSH);
    g_assert_true(r == Z_OK || r == Z_STREAM_END);
    g_string_append_len(got, (gchar *)out, sizeof(out) - ds.avail_out);
    if (r == Z_STREAM_END)
      break;
  } while (ds.avail_in > 0);
  inflateEnd(&ds);

  g_assert_cmpuint(got->len, ==, plain->len);
  g_assert_true(memcmp(got->str, plain->str, plain->len) == 0);
  g_assert_cmpuint(comp->len, <, plain->len); /* it actually compressed */

  g_string_free(got, TRUE);
  g_string_free(comp, TRUE);
  g_string_free(plain, TRUE);
}

/* Mirrors mydumper's zstd codec (ZSTD_compressStream2, streamed in chunks)
   decoded with myloader's ZSTD_decompressStream. */
static void test_zstd_codec(void) {
  GString *plain = make_payload();

  ZSTD_CStream *zc = ZSTD_createCStream();
  g_assert_nonnull(zc);
  g_assert_false(ZSTD_isError(ZSTD_initCStream(zc, ZSTD_CLEVEL_DEFAULT)));
  GString *comp = g_string_new("");
  gsize off = 0;
  do {
    gsize take = MIN((gsize)STREAM_CHUNK, plain->len - off);
    gboolean last = (off + take >= plain->len);
    ZSTD_inBuffer in = {plain->str + off, take, 0};
    off += take;
    int done;
    do {
      guchar out[STREAM_CHUNK];
      ZSTD_outBuffer o = {out, sizeof(out), 0};
      size_t rem = ZSTD_compressStream2(zc, &o, &in,
                                        last ? ZSTD_e_end : ZSTD_e_continue);
      g_assert_false(ZSTD_isError(rem));
      g_string_append_len(comp, (gchar *)out, o.pos);
      done = last ? (rem == 0) : (in.pos == in.size);
    } while (!done);
  } while (off < plain->len);
  ZSTD_freeCStream(zc);

  ZSTD_DStream *zd = ZSTD_createDStream();
  g_assert_nonnull(zd);
  ZSTD_initDStream(zd);
  GString *got = g_string_new("");
  ZSTD_inBuffer in = {comp->str, comp->len, 0};
  while (in.pos < in.size) {
    guchar out[STREAM_CHUNK];
    ZSTD_outBuffer o = {out, sizeof(out), 0};
    size_t r = ZSTD_decompressStream(zd, &o, &in);
    g_assert_false(ZSTD_isError(r));
    g_string_append_len(got, (gchar *)out, o.pos);
    if (o.pos == 0 && in.pos == in.size)
      break;
  }
  ZSTD_freeDStream(zd);

  g_assert_cmpuint(got->len, ==, plain->len);
  g_assert_true(memcmp(got->str, plain->str, plain->len) == 0);
  g_assert_cmpuint(comp->len, <, plain->len);

  g_string_free(got, TRUE);
  g_string_free(comp, TRUE);
  g_string_free(plain, TRUE);
}

int main(int argc, char **argv) {
  g_test_init(&argc, &argv, NULL);
  g_test_add_func("/stream_protocol/cancel_before_eof", test_cancel_before_eof);
  g_test_add_func("/stream_protocol/varint", test_varint);
  g_test_add_func("/stream_protocol/roundtrip", test_roundtrip);
  g_test_add_func("/stream_protocol/not_binary", test_not_binary);
  g_test_add_func("/stream_protocol/compressed", test_compressed_roundtrip);
  g_test_add_func("/stream_protocol/gzip_codec", test_gzip_codec);
  g_test_add_func("/stream_protocol/zstd_codec", test_zstd_codec);
  return g_test_run();
}

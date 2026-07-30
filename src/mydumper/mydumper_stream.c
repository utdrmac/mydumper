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

        Authors:    David Ducos, Percona (david dot ducos at percona dot com)
*/

#include <glib/gstdio.h>
#include <sys/file.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <zlib.h>
#include <zstd.h>
#ifdef __linux__
#include <sys/sendfile.h>
#endif
// We need header fcntl.h for open function to build on Alpine. More info in: https://github.com/mydumper/mydumper/issues/1721
#include <fcntl.h>

#include "mydumper.h"
#include "mydumper_global.h"
#include "mydumper_stream.h"
#include "mydumper_file_handler.h"
#include "mydumper_write.h"
#include "mydumper_start_dump.h"
#include "../common_stream_protocol.h"

GThread *stream_thread = NULL;
GThread *metadata_partial_writer_thread = NULL;
gboolean metadata_partial_writer_alive = TRUE;
GAsyncQueue *metadata_partial_queue = NULL;
GAsyncQueue * initial_metadata_lock_queue = NULL;
GAsyncQueue * initial_metadata_queue = NULL;

/* ------------------------------------------------------------------ *
 *  Diskless multiplexed binary stream (Phase 2)                       *
 *                                                                     *
 *  When active (stream_use_binary), dump workers write their data     *
 *  straight into the stream via the m_open/m_write/m_close sink       *
 *  instead of staging files on disk. A single writer thread           *
 *  (process_binary_stream) serialises multiplexed frames to stdout.   *
 *  Producers are throttled by a byte budget rather than a per-file    *
 *  synchronous ACK, which decouples them from stdout throughput while  *
 *  bounding memory.                                                    *
 * ------------------------------------------------------------------ */

gboolean stream_use_binary = FALSE;
static gboolean stream_compress = FALSE; /* in-process deflate of DATA payloads */
guint64 stream_budget_cap = 256ULL * 1024 * 1024; /* default 256 MB in flight */

#define STREAM_DEFLATE_CHUNK 65536

static GAsyncQueue *stream_msg_queue = NULL;
static gint stream_id_counter = 0;
static GHashTable *stream_out_files = NULL;
static GMutex *stream_out_files_mutex = NULL;

static gint64 stream_budget_bytes = 0;
static GMutex *stream_budget_mutex = NULL;
static GCond *stream_budget_cond = NULL;

struct stream_msg {
  guint8 type;         /* MYD_FRAME_* */
  guint64 stream_id;
  gchar *filename;     /* FILE_OPEN (owned) */
  guint8 flags;        /* FILE_OPEN */
  guint8 codec;        /* FILE_OPEN (MYD_CODEC_*) */
  gchar *data;         /* DATA (owned) */
  gsize len;           /* DATA */
  guint64 total_size;  /* FILE_CLOSE */
  gboolean has_crc;    /* FILE_CLOSE */
  guint32 crc;         /* FILE_CLOSE */
};

struct stream_out_file {
  guint64 stream_id;
  gchar *filename;      /* basename */
  guint8 flags;
  guint8 codec;         /* MYD_CODEC_* */
  gboolean open_emitted;
  guint64 bytes;        /* uncompressed bytes */
  uLong crc;            /* crc32 over uncompressed bytes */
  z_stream *zs;         /* deflate/gzip state (zlib) when compressed */
  ZSTD_CStream *zc;     /* zstd state when codec == MYD_CODEC_ZSTD */
};

static void stream_budget_reserve(gsize len){
  g_mutex_lock(stream_budget_mutex);
  while (stream_budget_bytes > 0 &&
         stream_budget_bytes + (gint64)len > (gint64)stream_budget_cap)
    g_cond_wait(stream_budget_cond, stream_budget_mutex);
  stream_budget_bytes += (gint64)len;
  g_mutex_unlock(stream_budget_mutex);
}

static void stream_budget_release(gsize len){
  g_mutex_lock(stream_budget_mutex);
  stream_budget_bytes -= (gint64)len;
  g_cond_broadcast(stream_budget_cond);
  g_mutex_unlock(stream_budget_mutex);
}

static void stream_msg_push(struct stream_msg *m){
  g_async_queue_push(stream_msg_queue, m);
}

/* Portable buffer copy (avoids requiring GLib >= 2.68 for g_memdup2). */
static gchar *stream_dup(const void *src, gsize len){
  gchar *dst = g_malloc(len);
  if (len)
    memcpy(dst, src, len);
  return dst;
}

static void full_write_stdout(const char *buf, gsize len){
  gsize written = 0;
  while (written < len){
    ssize_t r = write(fileno(stdout), buf + written, len - written);
    if (r < 0)
      m_error("Stream failed while writing to stdout: %s", strerror(errno));
    written += (gsize)r;
  }
}

/* Copy a file descriptor to stdout. On Linux this uses sendfile() for a
   zero-copy fast path (falling back to read/write if the destination does not
   support it); elsewhere it uses a portable read/write loop. Returns the total
   number of bytes copied. Used by the legacy (file-backed) stream path. */
static guint64 stream_copy_file_to_stdout(int in_fd, char *buf, guint bufsize,
                                          const char *fname){
  guint64 total = 0;
  int out_fd = fileno(stdout);
#ifdef __linux__
  ssize_t s = sendfile(out_fd, in_fd, NULL, 1 << 20);
  if (s >= 0){
    total += (guint64)s;
    while ((s = sendfile(out_fd, in_fd, NULL, 1 << 20)) > 0)
      total += (guint64)s;
    if (s == 0)
      return total; /* reached EOF via sendfile */
    m_error("Stream failed during transmission of file: %s (%s)", fname,
            strerror(errno));
  }
  /* sendfile unsupported for this destination: rewind and use read/write. */
  if (lseek(in_fd, 0, SEEK_SET) == (off_t)-1)
    m_error("Stream could not rewind file: %s (%s)", fname, strerror(errno));
  total = 0;
#endif
  ssize_t r;
  while ((r = read(in_fd, buf, bufsize)) > 0){
    ssize_t w = 0;
    while (w < r){
      ssize_t x = write(out_fd, buf + w, r - w);
      if (x < 0)
        m_error("Stream failed during transmission of file: %s (%s)", fname,
                strerror(errno));
      w += x;
    }
    total += (guint64)r;
  }
  return total;
}

/* ---- Diskless sink (installed as m_open/m_write/m_close in binary mode) ---- */

static int m_open_stream(char **filename, const char *type){
  (void)type;
  struct stream_out_file *sf = g_new0(struct stream_out_file, 1);
  sf->stream_id = (guint64)g_atomic_int_add(&stream_id_counter, 1) + 1;
  sf->filename = g_path_get_basename(*filename);
  sf->flags = MYD_FOPEN_FLAG_NONE;
  sf->codec = MYD_CODEC_NONE;
  sf->crc = crc32(0L, Z_NULL, 0);
  if (stream_compress){
    sf->flags |= MYD_FOPEN_FLAG_COMPRESSED;
    if (compress_method != NULL && g_ascii_strcasecmp(compress_method, ZSTD) == 0){
      sf->codec = MYD_CODEC_ZSTD;
      sf->zc = ZSTD_createCStream();
      if (sf->zc == NULL)
        m_error("Stream: ZSTD_createCStream failed for %s", sf->filename);
      size_t zr = ZSTD_initCStream(sf->zc, ZSTD_CLEVEL_DEFAULT);
      if (ZSTD_isError(zr))
        m_error("Stream: ZSTD_initCStream failed for %s: %s", sf->filename,
                ZSTD_getErrorName(zr));
    }else{
      /* GZIP (and the bare/default codec) map to gzip-wrapped deflate so the
         wire format is a real gzip stream. */
      sf->codec = MYD_CODEC_GZIP;
      sf->zs = g_new0(z_stream, 1);
      if (deflateInit2(sf->zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, 15 + 16, 8,
                       Z_DEFAULT_STRATEGY) != Z_OK)
        m_error("Stream: deflateInit2 failed for %s", sf->filename);
    }
  }
  g_mutex_lock(stream_out_files_mutex);
  g_hash_table_insert(stream_out_files, GINT_TO_POINTER((gint)sf->stream_id), sf);
  g_mutex_unlock(stream_out_files_mutex);
  trace("Stream(open): %s -> id %" G_GUINT64_FORMAT, sf->filename, sf->stream_id);
  return (int)sf->stream_id;
}

/* Enqueue `produced` compressed bytes from `out` as a DATA frame. */
static void stream_emit_compressed(struct stream_out_file *sf, const guchar *out,
                                   gsize produced){
  if (!produced)
    return;
  stream_budget_reserve(produced);
  struct stream_msg *m = g_new0(struct stream_msg, 1);
  m->type = MYD_FRAME_DATA;
  m->stream_id = sf->stream_id;
  m->data = stream_dup(out, produced);
  m->len = produced;
  stream_msg_push(m);
}

/* Compress `len` bytes from `buf` (owned by caller) through the file's codec and
   enqueue the produced bytes as DATA frames. `finish` flushes the tail of the
   stream at close. Only the owning worker thread touches sf->zs / sf->zc. */
static void stream_compress_emit(struct stream_out_file *sf, const char *buf,
                                 gsize len, gboolean finish){
  if (sf->codec == MYD_CODEC_ZSTD){
    ZSTD_inBuffer in = {buf, len, 0};
    int done;
    do {
      guchar out[STREAM_DEFLATE_CHUNK];
      ZSTD_outBuffer o = {out, sizeof(out), 0};
      size_t rem = ZSTD_compressStream2(sf->zc, &o, &in,
                                        finish ? ZSTD_e_end : ZSTD_e_continue);
      if (ZSTD_isError(rem))
        m_error("Stream: zstd compress failed for %s: %s", sf->filename,
                ZSTD_getErrorName(rem));
      stream_emit_compressed(sf, out, o.pos);
      done = finish ? (rem == 0) : (in.pos == in.size);
    } while (!done);
  }else{
    int flush = finish ? Z_FINISH : Z_NO_FLUSH;
    sf->zs->next_in = (Bytef *)buf;
    sf->zs->avail_in = (uInt)len;
    do {
      guchar out[STREAM_DEFLATE_CHUNK];
      sf->zs->next_out = out;
      sf->zs->avail_out = sizeof(out);
      int ret = deflate(sf->zs, flush);
      if (ret == Z_STREAM_ERROR)
        m_error("Stream: deflate failed for %s", sf->filename);
      stream_emit_compressed(sf, out, sizeof(out) - sf->zs->avail_out);
    } while (sf->zs->avail_out == 0);
  }
}

static ssize_t m_write_stream(int file, const char *buf, gsize len){
  gboolean need_open = FALSE;
  gchar *fname = NULL;
  guint8 flags = 0;
  guint8 codec = MYD_CODEC_NONE;
  guint64 sid = 0;

  g_mutex_lock(stream_out_files_mutex);
  struct stream_out_file *sf =
      g_hash_table_lookup(stream_out_files, GINT_TO_POINTER(file));
  if (sf){
    sid = sf->stream_id;
    if (!sf->open_emitted){
      sf->open_emitted = TRUE;
      need_open = TRUE;
      fname = g_strdup(sf->filename);
      flags = sf->flags;
      codec = sf->codec;
    }
    sf->bytes += len;
    if (len)
      sf->crc = crc32(sf->crc, (const Bytef *)buf, len);
  }
  g_mutex_unlock(stream_out_files_mutex);

  if (!sf){
    g_critical("Stream write to unknown handle %d", file);
    return -1;
  }

  if (need_open){
    struct stream_msg *m = g_new0(struct stream_msg, 1);
    m->type = MYD_FRAME_FILE_OPEN;
    m->stream_id = sid;
    m->filename = fname;
    m->flags = flags;
    m->codec = codec;
    stream_msg_push(m);
  }
  if (len){
    if (sf->codec != MYD_CODEC_NONE){
      stream_compress_emit(sf, buf, len, FALSE);
    }else{
      stream_budget_reserve(len);
      struct stream_msg *m = g_new0(struct stream_msg, 1);
      m->type = MYD_FRAME_DATA;
      m->stream_id = sid;
      m->data = stream_dup(buf, len);
      m->len = len;
      stream_msg_push(m);
    }
  }
  return (ssize_t)len;
}

static int m_close_stream(guint thread_id, int file, gchar *filename,
                          guint64 size, struct db_table *dbt){
  (void)thread_id;
  (void)filename;
  (void)size;
  (void)dbt;
  g_mutex_lock(stream_out_files_mutex);
  struct stream_out_file *sf =
      g_hash_table_lookup(stream_out_files, GINT_TO_POINTER(file));
  if (sf)
    g_hash_table_remove(stream_out_files, GINT_TO_POINTER(file));
  g_mutex_unlock(stream_out_files_mutex);
  if (!sf)
    return 0;

  /* Suppress empty files unless the user asked for them, matching the
     default on-disk behaviour (empty data chunks are not materialised). */
  gboolean produce = sf->open_emitted || sf->bytes > 0 || build_empty_files;
  if (produce){
    if (!sf->open_emitted){
      struct stream_msg *mo = g_new0(struct stream_msg, 1);
      mo->type = MYD_FRAME_FILE_OPEN;
      mo->stream_id = sf->stream_id;
      mo->filename = g_strdup(sf->filename);
      mo->flags = sf->flags;
      mo->codec = sf->codec;
      stream_msg_push(mo);
    }
    /* Flush the compressor: emits the tail of the compressed stream. */
    if (sf->codec != MYD_CODEC_NONE)
      stream_compress_emit(sf, NULL, 0, TRUE);
    struct stream_msg *mc = g_new0(struct stream_msg, 1);
    mc->type = MYD_FRAME_FILE_CLOSE;
    mc->stream_id = sf->stream_id;
    mc->total_size = sf->bytes; /* uncompressed size */
    mc->has_crc = TRUE;
    mc->crc = (guint32)sf->crc; /* crc over uncompressed bytes */
    stream_msg_push(mc);
  }
  if (sf->zs){
    deflateEnd(sf->zs);
    g_free(sf->zs);
  }
  if (sf->zc)
    ZSTD_freeCStream(sf->zc);
  trace("Stream(close): id %" G_GUINT64_FORMAT " %s (%" G_GUINT64_FORMAT " bytes)",
        sf->stream_id, sf->filename, sf->bytes);
  g_free(sf->filename);
  g_free(sf);
  return 0;
}

/* Frame a (small) on-disk file into the binary stream. Used for metadata files
   which are still written to disk via FILE* before being streamed. */
static void stream_binary_push_file(const gchar *filename){
  int fd = open(filename, O_RDONLY);
  if (fd < 0){
    m_error("Stream file failed to open: %s (%s)", filename, strerror(errno));
    return;
  }
  guint64 sid = (guint64)g_atomic_int_add(&stream_id_counter, 1) + 1;
  struct stream_msg *mo = g_new0(struct stream_msg, 1);
  mo->type = MYD_FRAME_FILE_OPEN;
  mo->stream_id = sid;
  mo->filename = g_path_get_basename(filename);
  mo->flags = MYD_FOPEN_FLAG_NONE;
  mo->codec = MYD_CODEC_NONE;
  stream_msg_push(mo);

  guint64 total = 0;
  uLong crc = crc32(0L, Z_NULL, 0);
  gchar *buf = g_malloc(STREAM_BUFFER_SIZE);
  ssize_t r;
  while ((r = read(fd, buf, STREAM_BUFFER_SIZE)) > 0){
    stream_budget_reserve((gsize)r);
    struct stream_msg *md = g_new0(struct stream_msg, 1);
    md->type = MYD_FRAME_DATA;
    md->stream_id = sid;
    md->data = stream_dup(buf, r);
    md->len = (gsize)r;
    stream_msg_push(md);
    total += (guint64)r;
    crc = crc32(crc, (const Bytef *)buf, (guint)r);
  }
  g_free(buf);
  close(fd);

  struct stream_msg *mc = g_new0(struct stream_msg, 1);
  mc->type = MYD_FRAME_FILE_CLOSE;
  mc->stream_id = sid;
  mc->total_size = total;
  mc->has_crc = TRUE;
  mc->crc = (guint32)crc;
  stream_msg_push(mc);

  trace("Deleting %s", filename);
  remove(filename);
}

void *process_binary_stream(void *data){
  (void)data;
  GString *out = g_string_sized_new(64);
  gint64 total_start_time = g_get_monotonic_time();
  guint64 total_size = 0;

  /* Every binary stream begins with the magic so the loader can auto-detect
     the format (and fall back to the legacy parser otherwise). */
  myd_stream_append_magic(out);
  full_write_stdout(out->str, out->len);

  for (;;){
    struct stream_msg *m = g_async_queue_pop(stream_msg_queue);
    if (m->type == MYD_FRAME_EOF){
      g_string_set_size(out, 0);
      myd_stream_encode_eof(out);
      full_write_stdout(out->str, out->len);
      g_free(m);
      break;
    }
    g_string_set_size(out, 0);
    switch (m->type){
    case MYD_FRAME_FILE_OPEN:
      myd_stream_encode_file_open(out, m->stream_id, m->filename, m->flags,
                                  m->codec);
      full_write_stdout(out->str, out->len);
      break;
    case MYD_FRAME_DATA:
      myd_stream_encode_data_header(out, m->stream_id, m->len);
      full_write_stdout(out->str, out->len);
      full_write_stdout(m->data, m->len);
      stream_budget_release(m->len);
      total_size += m->len;
      break;
    case MYD_FRAME_FILE_CLOSE:
      myd_stream_encode_file_close(out, m->stream_id, m->total_size, m->has_crc,
                                   m->crc);
      full_write_stdout(out->str, out->len);
      break;
    default:
      m_error("Unknown stream message type %d", m->type);
    }
    g_free(m->filename);
    g_free(m->data);
    g_free(m);
  }

  g_string_free(out, TRUE);
  GTimeSpan total_diff =
      (g_get_monotonic_time() - total_start_time) / G_TIME_SPAN_SECOND;
  g_message("All data transferred was %" G_GUINT64_FORMAT
            " at a rate of %" G_GINT64_FORMAT " MB/s",
            total_size,
            total_diff != 0 ? (gint64)(total_size / 1024 / 1024 / total_diff)
                            : (gint64)(total_size / 1024 / 1024));
  return NULL;
}

void metadata_partial_queue_push (struct db_table *dbt){
  if (dbt)
    g_async_queue_push(metadata_partial_queue, dbt);
}

guint get_stream_queue_length(){
  if (stream_use_binary)
    return stream_msg_queue ? g_async_queue_length(stream_msg_queue) : 0;
  return stream_queue ? g_async_queue_length(stream_queue) : 0;
}

void stream_queue_push(struct db_table *dbt,gchar *filename){
  if (stream_use_binary){
    /* Only file-based callers (metadata) reach here in binary mode; the bulk
       data path goes through the diskless m_write sink instead. An empty
       filename is the legacy shutdown sentinel and is ignored here. */
    if (filename && strlen(filename) > 0)
      stream_binary_push_file(filename);
    g_free(filename);
    metadata_partial_queue_push(dbt);
    return;
  }
  GAsyncQueue *done = g_async_queue_new();
  g_async_queue_push(stream_queue, new_filename_queue_element(dbt,filename,done));
  g_async_queue_pop(done);
  g_async_queue_unref(done);
  metadata_partial_queue_push(dbt);
}

void *process_stream(void *data){
  (void)data;
  int f=0;
  char *buf=g_new(gchar, STREAM_BUFFER_SIZE);
  guint64 total_size=0;
  // Perf: Use g_get_monotonic_time() instead of GDateTime to eliminate allocations
  gint64 total_start_time = g_get_monotonic_time();
  GTimeSpan diff=0,total_diff=0;
//  gboolean not_compressed = FALSE;
//  guint sz=0;
  ssize_t len=0;
  struct filename_queue_element *sf = NULL;
  for(;;){
    sf = g_async_queue_pop(stream_queue);

    if (strlen(sf->filename) == 0){
      if (sf->done)
        g_async_queue_push(sf->done, GINT_TO_POINTER(1));
      break;
    }
    char *used_filemame=g_path_get_basename(sf->filename);
    len=write(fileno(stdout), "\n-- ", 4);
    len=write(fileno(stdout), used_filemame, strlen(used_filemame));
    len=write(fileno(stdout), " ", 1);
    total_size+=5;
    total_size+=strlen(used_filemame);
    free(used_filemame);
    {
//      g_message("Stream Opening: %s",sf->filename);
      f=open(sf->filename,O_RDONLY);
      if (f < 0){
        m_error("File failed to open: %s (%s)", sf->filename, strerror(errno));
      }else{
/*
      	      if (flock(fileno(f),LOCK_EX)){
          g_async_queue_push(stream_queue,sf);
	  g_message("File not possible to lock %s",sf->filename);
	  continue;
	}
	flock(fileno(f),LOCK_UN);
*/
	if (f < 0){
          g_critical("File failed to open: %s (%s). Retrying", sf->filename, strerror(errno));
          f=open(sf->filename,O_RDONLY);
          if (f < 0){
            m_error("File failed to open: %s (%s). Cancelling",sf->filename, strerror(errno));
          }
        }
        trace("Streaming %s", sf->filename);
        struct stat st;
        fstat(f, &st);
        off_t size = st.st_size;
        
//        g_message("File size of %s is %"G_GINT64_FORMAT, sf->filename, size);
//        g_message("Streaming file %s", sf->filename);
        gchar *c = g_strdup_printf("%" G_GUINT64_FORMAT, size);
        len=write(fileno(stdout), c, strlen(c));
        len=write(fileno(stdout), "\n", 1);
        total_size+=strlen(c) + 1;
        g_free(c);

        // Perf: Use g_get_monotonic_time() - zero allocation timing
        gint64 start_time = g_get_monotonic_time();
        (void) len;
        guint total_len = (guint)stream_copy_file_to_stdout(f, buf, STREAM_BUFFER_SIZE, sf->filename);
//        g_message("Bytes readed of %s is %d", filename, total_len);
        gint64 end_time = g_get_monotonic_time();
        diff = (end_time - start_time) / G_TIME_SPAN_SECOND;
        total_diff = (end_time - total_start_time) / G_TIME_SPAN_SECOND;
        if (diff > 0){
          g_message("File %s transferred in %" G_GINT64_FORMAT " seconds at %" G_GINT64_FORMAT " MB/s | Global: %" G_GINT64_FORMAT " MB/s",sf->filename,diff,total_len/1024/1024/diff,total_diff!=0?total_size/1024/1024/total_diff:total_size/1024/1024);
        }else{
          g_message("File %s transferred | Global: %" G_GINT64_FORMAT "MB/s",sf->filename,total_diff!=0?total_size/1024/1024/total_diff:total_size/1024/1024);
        }
        total_size+=total_len;
        close(f);
      }
    }
    trace("Deleting %s", sf->filename);
    remove(sf->filename);
    if (sf->done)
      g_async_queue_push(sf->done, GINT_TO_POINTER(1));
    g_free(sf->filename);
    g_free(sf);
  }
  // Perf: Zero-allocation final timing
  total_diff = (g_get_monotonic_time() - total_start_time) / G_TIME_SPAN_SECOND;
  g_message("All data transferred was %" G_GINT64_FORMAT " at a rate of %" G_GINT64_FORMAT " MB/s",total_size,total_diff!=0?total_size/1024/1024/total_diff:total_size/1024/1024);
  return NULL;
}



void send_initial_metadata(){
  g_async_queue_push(initial_metadata_queue, GINT_TO_POINTER(1) );
  g_async_queue_pop(initial_metadata_lock_queue);
}

static gchar *make_partial_filename(guint i)
{
  return g_strdup_printf("%s/metadata.partial.%d", dump_directory, i);
}

void *metadata_partial_writer(void *data){
  (void) data;
  struct db_table *dbt=NULL;
  GList *dbt_list = NULL;
  // Perf: Use GHashTable for O(1) deduplication instead of O(n) g_list_find
  GHashTable *dbt_set = g_hash_table_new(g_direct_hash, g_direct_equal);
  GString *output=g_string_sized_new(256);
  guint i=0;
  gchar *filename = NULL;
  GError* gerror = NULL;
  for(i=0;i<num_threads;i++){
    g_async_queue_pop(initial_metadata_queue);
  }
  dbt=g_async_queue_try_pop(metadata_partial_queue);
  while (dbt != NULL ){
    dbt_list=g_list_prepend(dbt_list,dbt);
    g_hash_table_add(dbt_set, dbt);
    dbt=g_async_queue_try_pop(metadata_partial_queue);
  }
  g_string_set_size(output,0);
  g_list_foreach(dbt_list,(GFunc)(&print_dbt_on_metadata_gstring),output);
  filename= make_partial_filename(0);
  g_file_set_contents(filename, output->str,output->len,&gerror);
  stream_queue_push(NULL, filename);
  for(i=0;i<num_threads;i++){
    g_async_queue_push(initial_metadata_lock_queue, GINT_TO_POINTER(1));
  }

  i=1;
  // Perf: Use g_get_monotonic_time() instead of GDateTime
  gint64 prev_time = g_get_monotonic_time();
  GTimeSpan diff=0;
  g_string_set_size(output,0);
  filename=NULL;
  dbt=g_async_queue_timeout_pop(metadata_partial_queue, METADATA_PARTIAL_INTERVAL * 1000000);
  while (metadata_partial_writer_alive){
    // Perf: O(1) hash table lookup instead of O(n) g_list_find
    if (dbt != NULL && !g_hash_table_contains(dbt_set, dbt)){
      dbt_list=g_list_prepend(dbt_list,dbt);
      g_hash_table_add(dbt_set, dbt);
    }
    // Perf: Zero-allocation time check
    gint64 current_time = g_get_monotonic_time();
    diff = (current_time - prev_time) / G_TIME_SPAN_SECOND;
    if (diff > METADATA_PARTIAL_INTERVAL){
      // Perf: O(1) hash table size instead of O(n) g_list_length
      if (g_hash_table_size(dbt_set) > 0){
        filename= make_partial_filename(i);
        i++;
        initialize_config_on_string(output);
        g_list_foreach(dbt_list,(GFunc)(&print_dbt_on_metadata_gstring),output);
        g_file_set_contents(filename,output->str,output->len,&gerror);
        stream_queue_push(NULL, filename);
        filename = NULL;
        g_string_set_size(output,0);
        dbt_list=NULL;
        g_hash_table_remove_all(dbt_set);  // Clear the set when list is cleared
      }
      prev_time = current_time;
    }
    dbt=g_async_queue_timeout_pop(metadata_partial_queue, METADATA_PARTIAL_INTERVAL * 1000000);
  }
  g_hash_table_destroy(dbt_set);
  return NULL;
}

void initialize_stream(){
  initial_metadata_queue = g_async_queue_new();
  initial_metadata_lock_queue = g_async_queue_new();
  metadata_partial_queue = g_async_queue_new();

  /* The diskless binary protocol is the only streaming format. It is disabled
     only for the exec pipe path (--exec-per-thread stages files on disk).
     --compress does NOT disable it: compression is applied in-process on the
     stream (see the compress_method check below) rather than via a
     fork-to-disk pipe. */
  stream_use_binary = stream && !is_pipe_backup();

  if (stream_use_binary){
    /* Tunable in-flight memory budget (backpressure). */
    const gchar *budget_env = g_getenv("MYDUMPER_STREAM_BUDGET_MB");
    if (budget_env){
      guint64 mb = g_ascii_strtoull(budget_env, NULL, 10);
      if (mb)
        stream_budget_cap = mb * 1024 * 1024;
    }
    /* --compress enables in-process (zlib) compression of the stream,
       self-describing via the per-file COMPRESSED flag so the loader handles it
       automatically. The specific codec (GZIP/ZSTD) only affects on-disk files;
       the wire format is DEFLATE. */
    if (compress_method != NULL)
      stream_compress = TRUE;
    stream_msg_queue = g_async_queue_new();
    stream_out_files = g_hash_table_new(g_direct_hash, g_direct_equal);
    stream_out_files_mutex = g_mutex_new();
    stream_budget_mutex = g_mutex_new();
    stream_budget_cond = g_cond_new();
    /* Install the diskless sink: workers stream straight from their buffers. */
    m_open = &m_open_stream;
    m_write = &m_write_stream;
    m_close = &m_close_stream;
    stream_thread = m_thread_new("stream", (GThreadFunc)process_binary_stream, NULL, "Stream thread could not be created");
  }else{
    stream_queue = g_async_queue_new();
    stream_thread = m_thread_new("stream", (GThreadFunc)process_stream, stream_queue, "Stream thread could not be created");
  }
  metadata_partial_writer_thread = m_thread_new("metadata_writer", (GThreadFunc)metadata_partial_writer, NULL, "Metadata partial writer thread could not be created");
}

void wait_stream_to_finish(){
  /*
   * Shutdown ordering matters:
   * - metadata_partial_writer may call stream_queue_push(), which (by default)
   *   waits for an ACK from process_stream.
   * - If we stop process_stream first (by sending the empty-filename sentinel),
   *   metadata_partial_writer can deadlock waiting for an ACK that will never come.
   *
   * So: stop + join metadata writer first, then stop + join stream thread.
   */
  if (metadata_partial_writer_thread != NULL) {
    metadata_partial_writer_alive = FALSE;
    if (metadata_partial_queue != NULL) {
      /* Wake up g_async_queue_timeout_pop() */
      g_async_queue_push(metadata_partial_queue, GINT_TO_POINTER(1));
    }
    g_thread_join(metadata_partial_writer_thread);
    metadata_partial_writer_thread = NULL;
  }

  if (stream_thread != NULL) {
    if (stream_use_binary){
      /* Tell process_binary_stream() to emit EOF and exit. */
      struct stream_msg *m = g_new0(struct stream_msg, 1);
      m->type = MYD_FRAME_EOF;
      g_async_queue_push(stream_msg_queue, m);
    }else{
      /* Tell process_stream() to exit */
      stream_queue_push(NULL, g_strdup(""));
    }
    g_thread_join(stream_thread);
    stream_thread = NULL;
  }
}

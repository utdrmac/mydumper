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

#include <mysql.h>
#include <glib/gstdio.h>
#include <string.h>
#include <unistd.h>
#include <zlib.h>
#include <zstd.h>

#include "myloader.h"
#include "myloader_common.h"
#include "myloader_control_job.h"
#include "myloader_process_filename.h"
#include "myloader_global.h"
#include "myloader_stream_mem_budget.h"
#include "../common_stream_protocol.h"

GThread *stream_thread = NULL;
void *process_stream(struct configuration *stream_conf);
static void *process_stream_legacy(struct configuration *stream_conf,
                                   const guchar *prefill, gsize prefill_len);
static void *process_binary_stream_loader(struct configuration *conf,
                                          const guchar *prefix, gsize prefix_len);

static GMutex *metadata_header_mutex=NULL;
static gboolean metadata_header_done=FALSE;
static GCond *metadata_header_cond= NULL;

/* ------------------------------------------------------------------ *
 *  In-memory streamed-file registry (Phase 3)                         *
 *                                                                     *
 *  When the incoming stream uses the binary protocol, received files  *
 *  are reconstructed in memory instead of being written to disk and   *
 *  read back. Bulk data / schema / .dat files are kept here and served *
 *  to the restore pipeline via myl_open()/fmemopen(); tiny metadata   *
 *  files are still materialised on disk (read via GKeyFile).           *
 * ------------------------------------------------------------------ */

struct stream_mem_file {
  gchar *data;
  gsize len;
};

static gboolean stream_binary_active = FALSE;
static GHashTable *stream_mem_files = NULL; /* basename -> struct stream_mem_file */
static GMutex *stream_mem_mutex = NULL;

/* Queued-file byte budget (--stream-budget-mb, default 512 MiB): charged
   when a completed data/.dat file enters stream_mem_files at FILE_CLOSE, released
   when myl_close() finishes with the buffer. In-flight demux decompression buffers
   are not charged, avoiding self-deadlock on large single-table streams. Schema,
   metadata, and other control-plane files are exempt from the budget. See
   myloader_stream_mem_budget.c. */

gboolean stream_mem_active(void){
  return stream_binary_active;
}

/* Store a completed file's bytes. Takes ownership of data. */
static void stream_mem_put(const gchar *basename, gchar *data, gsize len){
  struct stream_mem_file *mf = g_new0(struct stream_mem_file, 1);
  mf->data = data;
  mf->len = len;
  g_mutex_lock(stream_mem_mutex);
  g_hash_table_insert(stream_mem_files, g_strdup(basename), mf);
  g_mutex_unlock(stream_mem_mutex);
}

/* Steal a stored file if present. Caller owns *data and must g_free it and call
   stream_mem_release_bytes(*len) when done. */
gboolean stream_mem_get(const gchar *basename, gchar **data, gsize *len){
  if (!stream_mem_files)
    return FALSE;
  gboolean found = FALSE;
  g_mutex_lock(stream_mem_mutex);
  struct stream_mem_file *mf = g_hash_table_lookup(stream_mem_files, basename);
  if (mf){
    *data = mf->data;
    *len = mf->len;
    g_hash_table_remove(stream_mem_files, basename);
    found = TRUE;
  }
  g_mutex_unlock(stream_mem_mutex);
  return found;
}

void initialize_stream (struct configuration *c){
  /* In stream mode the backup is read from stdin. If stdin is a terminal, no
     data was piped in. Fail fast with a clear message. An empty pipe/file still yields EOF
     and is handled gracefully by the release guard on the stream thread. */
  if (isatty(fileno(stdin)))
    m_critical("--stream expects the backup on stdin, but stdin is a terminal "
               "(nothing was piped in). Pipe a stream instead, e.g. "
               "`mydumper --stream ... | myloader --stream ...`.");

  stream_mem_budget_init();
  stream_mem_files = g_hash_table_new_full(g_str_hash, g_str_equal, g_free, g_free);
  stream_mem_mutex = g_mutex_new();
  /* Create the metadata-header sync primitives before starting the stream
     thread so it can safely signal them the instant it reaches EOF (avoids a
     latent startup race with the release guard below). */
  metadata_header_mutex=g_mutex_new();
  metadata_header_cond= g_cond_new();
  stream_thread = m_thread_new("myloader_stream",(GThreadFunc)process_stream, c, "Stream thread could not be created");
}

void wait_stream_to_finish(){
  g_thread_join(stream_thread);
}

void wait_stream_to_process_metadata_header(){
  g_mutex_lock(metadata_header_mutex);
  while(!metadata_header_done)
    g_cond_wait (metadata_header_cond, metadata_header_mutex);
  g_mutex_unlock(metadata_header_mutex);
}


void metadata_has_been_processed(){
  g_mutex_lock(metadata_header_mutex);
  metadata_header_done=TRUE;
  g_cond_signal(metadata_header_cond);
  g_mutex_unlock(metadata_header_mutex);
}

/* Guard for a producer that dies before sending the metadata header: the main
   thread blocks in wait_stream_to_process_metadata_header() until
   metadata_has_been_processed() runs (when the metadata.header file is parsed).
   If the stream ends first (eg: broken pipe, mydumper crash, empty stdin) that never
   happens, so the stream thread calls this on every exit path to release the
   waiter and let myloader terminate cleanly instead of hanging on EOF. */
static void release_metadata_header_if_pending(){
  gboolean forced=FALSE;
  g_mutex_lock(metadata_header_mutex);
  if (!metadata_header_done){
    metadata_header_done=TRUE;
    forced=TRUE;
    g_cond_signal(metadata_header_cond);
  }
  g_mutex_unlock(metadata_header_mutex);
  if (forced)
    g_warning("Stream ended before the metadata header was received; the source "
              "may have failed or sent no data. Nothing to restore.");
}



size_t read_stream_line(char *buffer, int c_to_read){
    size_t bytes = fread(buffer, sizeof(char), c_to_read, stdin);
    return bytes;
}

void flush(char *buffer, int from, int to, FILE *file, guint *total_size){
/*  if (to>from){
    char * tmp=g_strndup(&(buffer[from]),to-from);
    g_message("Flushing data %d: %s",to-from, tmp);
    g_free(tmp);
  }
  */
  if (file){ 
    if (write_file(file,&(buffer[from]),to-from+1) != to-from+1) 
      g_critical("Error on writing");
    *total_size=*total_size+(to-from)+1;
  }
}

gboolean has_mydumper_suffix(gchar *line){
  return
    m_filename_has_suffix(line,".dat") ||
    m_filename_has_suffix(line,".sql") ||
    g_strstr_len(line,-1,"metadata.partial") ||
    g_str_has_prefix(line,"metadata");
}

static void *process_stream_legacy(struct configuration *stream_conf,
                                   const guchar *prefill, gsize prefill_len){
  (void) stream_conf;
  set_thread_name("STT");
  char * filename=NULL,*real_filename=NULL,* previous_filename=NULL;
  guint stream_buffer_size=STREAM_BUFFER_SIZE;
  char *buffer=g_new(char, stream_buffer_size);
  FILE *file=NULL;
  guint pos=0,buffer_len=0;
  guint diff=0, i=0, line_from=0, line_end=0; 
  guint initial_pos=0;
  guint total_size=0;
  guint file_size_from_stream=0;
  GString *set_buffer=g_string_new_len("", 1000);
  g_string_set_size(set_buffer,0);
  gboolean writing_set=TRUE;
  gchar *database_name=target_db?g_strdup(target_db):NULL;
  gchar *table_name=NULL;
  for(i=0;i<stream_buffer_size;i++){
    buffer[i]='\0';
  }
  /* Bytes already consumed while auto-detecting the protocol are replayed here
     so the legacy parser sees the full stream from the beginning. */
  if (prefill && prefill_len){
    g_assert(prefill_len < (gsize)stream_buffer_size - 1);
    memcpy(buffer, prefill, prefill_len);
    diff = (guint)prefill_len;
  }
  gchar *new_filename,*new_real_filename,*kind=NULL;
  int num=0;
  while (TRUE){
    // Reads from stdin and fills the buffer from last position
read_more:
    buffer_len=fread(&(buffer[diff]), sizeof(char), stream_buffer_size-1-diff, stdin)+diff;

    if (buffer_len==diff){
      // This menas that there is nothing else to read from stdin
      // so, we need to flush and EXIT.
      flush(buffer,0,buffer_len-1,file, &total_size);
      break;
    }
  
    if (!buffer_len)
      // We read nothing, we have to EXIT
      break;

    if (mysqldump){
      // We have data to process
      // we always start reading from the begining of the buffer
      pos=0;
      diff=0;
      while (pos < buffer_len){

        initial_pos=pos;
        if (buffer[pos] == '\n'){
          // new lines means new file header, new header of file content or new file content
          pos++;

          if (set_buffer->len > 0){
            // SET has been written
            writing_set=FALSE;
            if (file == NULL ){
              if (g_str_has_prefix(&(buffer[line_from]),"--")){
                // after writing the SET and when file is NULL, we should be reading the header of the file
                // we create a temporary filename 
                if (g_str_has_prefix(&(buffer[initial_pos]),"\nUSE ")){
                  gchar ** sp=g_strsplit(&(buffer[initial_pos]), "`", 3);
                  database_name=g_strdup(sp[1]);
                  g_strfreev(sp);
                }else{
                  filename=g_strdup_printf("mydumper_tmp.table_%d.sql",num);
                  num++;

                  real_filename = g_build_filename(directory,filename,NULL);
 
                  file = g_fopen(real_filename, "w");
                  table_name=NULL;
                  kind=NULL;

                  flush(set_buffer->str,0,set_buffer->len -1,file, &total_size);
                  flush(buffer,initial_pos,line_end-1,file, &total_size);
                }
              }

            }else{
              // File content was being written, we might need to flush from initial_pos to line_from
              if (initial_pos < line_from ){
                // flushing from initial_pos to line_from - 1
                flush(buffer,initial_pos,line_from-1,file, &total_size);
              }
              fclose(file);
              file=NULL;
              if (table_name){
                new_filename=g_strdup_printf("%s%s%s%s.sql",database_name?database_name:"",table_name?".":"",table_name?table_name:"",kind?kind:"");
                new_real_filename=g_build_filename(directory,new_filename,NULL);
                g_rename(real_filename,new_real_filename);
                trace("renaming: %s -> %s", real_filename,new_real_filename);
              }else{
                new_filename=g_strdup(filename);
              }
              g_free(filename);
              filename=NULL;
              // sending previous file for processing
              if(!g_str_has_prefix(new_filename,"mydumper_tmp"))
                process_filename_push(new_filename);
              new_filename=NULL;
            }
          }
        }

        // we need to determine the right line_from
        if (initial_pos != pos) {
          line_from=pos-1;
        }else{
          line_from=pos;
        }

        // We process by line to correctly detect the new file
        while (pos < buffer_len && buffer[pos] !='\n' ){
          pos++;
        }

        line_end=pos;    
        // At this point we know:
        // - line_from
        // - line_end

        if (file){
          if ((line_end-line_from < 20) && (buffer[pos] !='\n') && (line_from>=20)){
            // this is not a line, which means pos == buffer_len, so we are at the end of the buffer
            // we need to copy the first 20 chars to the begining of the buffer to get relevant info
            g_message("Copying");
            diff=line_end-line_from ;
            g_strlcpy(buffer,&(buffer[line_from]), line_end-line_from + 1);
            continue;
          }
          // Can we get relevant info?
          if (g_str_has_prefix(&(buffer[line_from]),"CREATE TABLE ") || g_str_has_prefix(&(buffer[line_from]),"/*!50001 CREATE VIEW") || g_str_has_prefix(&(buffer[line_from]),"/*!50001 VIEW")){
            g_free(table_name);
            gchar ** sp=g_strsplit(&(buffer[line_from]), "`", 3);
            table_name=g_strdup(sp[1]);
            g_strfreev(sp);
            kind=g_strdup("-schema");
          } else if (g_str_has_prefix(&(buffer[line_from]),"INSERT INTO ") ){
            g_free(table_name);
            gchar ** sp=g_strsplit(&(buffer[line_from]), "`", 3);
            table_name=g_strdup(sp[1]);
            g_strfreev(sp);
            kind=g_strdup_printf(".000%d",num);
          } 

          char c=buffer[line_end];
          buffer[line_end]='\0';
          buffer[line_end]=c;
          if (buffer[line_end] == '\n'){
            flush(buffer,initial_pos,line_end,file, &total_size);
            pos++;
          }else{
            flush(buffer,initial_pos,line_end-1,file, &total_size);
          }
          continue;
        }else{
          if (writing_set){
            // file was NULL, this must be the header of the mysqldump
            if (buffer[line_end] == '\n'){
              if (!g_str_has_prefix(&(buffer[line_from]),"--") && initial_pos!=line_end){
                g_string_append_len(set_buffer,&(buffer[initial_pos]),line_end-initial_pos+1);
              }
              pos++;        
            }else{
              diff=buffer_len-initial_pos ;
              g_strlcpy(buffer,&(buffer[initial_pos]), diff + 1);
              goto read_more;
            }
          }else{
            pos++;
          }
        }
      }
    }else{
      //mydumper stream

      // We have data to process
      // we always start reading from the begining of the buffer
      pos=0;
      diff=0;
      while (pos < buffer_len){
        initial_pos=pos;
        while (buffer[pos] == '\n')
          // local new lines are ignored at this point, it will be written
          pos++;

        // we need to determine the right line_from
        if (initial_pos != pos) {
          line_from=pos-1;
        }else{
          line_from=pos;
        }

        // We process by line to correctly detect the header
        while (pos < buffer_len && buffer[pos] !='\n' ){
          pos++;
        }

        line_end=pos;

        // At this point we know:
        // - line_from
        // - line_end

        // is it a line?
        if (buffer[line_end] == '\n'){
          // As it is a line we need to detect if it is a header
          if (g_str_has_prefix(&(buffer[line_from]),"\n-- ")){
            // header tag detected 
            if (file != NULL ){
              // Another file was being written, we might need to flush from initial_pos to line_from
              if (initial_pos < line_from ){
                // flushing from initial_pos to line_from - 1
                flush(buffer,initial_pos,line_from-1,file, &total_size);
              }
              // Content of the file are comming from stdin, it is not sharing the backup dir
              if (total_size < file_size_from_stream){
                // The file size reported in the header is not the same that the amount of data written
                // this means that the content of the file has the header tag
                // we need to flush and continue
                flush(buffer,line_from,line_end-1,file, &total_size);
                g_message("Different file size in %s. Should be: %d | Written: %d. But continuing", filename, file_size_from_stream, total_size);
                continue;
              }else if (total_size > file_size_from_stream) {
                // we wrote on the file more data than the file size reported in the header
                m_critical("Different file size in %s. Should be: %d | Written: %d", filename, file_size_from_stream, total_size);
              }else{
                // The amount of data written and the file size reported in the header match!
                total_size=0;
              }
              previous_filename=g_strdup(filename);
              g_free(filename);
            }
            // processing header
            gchar ** sp=g_strsplit(&(buffer[line_from]), " ", 3);
            filename=g_strdup(sp[1]);
            // detecting file size reported on the header
            file_size_from_stream = g_ascii_strtoull(sp[2], NULL, 10);
            g_strfreev(sp);

            // sending previous file for processing
            real_filename = g_build_filename(directory,filename,NULL);
            if (file)
              fclose(file);
            if (previous_filename){
              process_filename_push(previous_filename);
              previous_filename=NULL;
            }
            if (g_file_test(real_filename, G_FILE_TEST_EXISTS)){
              g_warning("Stream Thread: File %s exists in datadir, we are not replacing", real_filename);
              file = NULL;
            }else{
              file = g_fopen(real_filename, "w");
            }
            if (!has_mydumper_suffix(filename)){
              g_debug("Not a mydumper file: %s", filename);
            }
            pos++;
            continue;
          }
          // this was a common line, flushing to disk
          flush(buffer,initial_pos,line_end-1,file, &total_size);
          continue;
        }else{
          // It reached end of buffer
          //
          // this data doesn't end with new line
          // but we need to check if starts with --

          if (line_end-line_from >= 4){
            // In the buffer remains more than 4 chars
            if (g_str_has_prefix(&(buffer[line_from]),"\n-- ")){
              // It could be a header, so we copied to the begining of the buffer
              diff=buffer_len-initial_pos ;
              g_strlcpy(buffer,&(buffer[initial_pos]), diff + 1);
              // diff remains set to do not overwrite the buffer
            }else{
              // it is safe to flush it all the content of the buffer
              flush(buffer,initial_pos,line_end-1,file, &total_size);
              diff=0;
              // the buffer will start empty
            }
          }else{
  
            gchar *tmp=g_strndup(&(buffer[line_from]),line_end-line_from >= 4 ? 4:line_end-line_from);
//          g_message("TMP: |%s| %c", tmp, tmp[0]);
            if  (strlen(tmp)>=1 && g_strstr_len("\n-- ", -1 ,tmp) != NULL ){
              // we need to move to the begining of the buffer and reprocess 
//              g_message("Coping data %d %d: %s", initial_pos,  buffer_len, &(buffer[initial_pos])  );
              diff=buffer_len-initial_pos ;
              g_strlcpy(buffer,&(buffer[initial_pos]), diff + 1);
//              g_message("After copy data: %s | new len should be: %d", buffer , diff);
            }else{
              flush(buffer,initial_pos,line_end-1,file, &total_size);
              diff=0;
            }
            g_free(tmp);
          }
          goto read_more;

        }


        g_error("This should not happen");
      }
    }
  }
  if (mysqldump){
    if (file)
      fclose(file);
    if (filename)
      process_filename_push(filename);
    g_free(filename);
  }else{
    if (file) 
      fclose(file);
    if (filename)
      process_filename_push(filename);
    g_free(filename);
  }
  process_filename_queue_end();
  return NULL;
}

/* ------------------------------------------------------------------ *
 *  Binary multiplexed demux (Phase 3)                                 *
 * ------------------------------------------------------------------ */

struct demux_file {
  gchar *name;
  GString *data;      /* reconstructed (decompressed) content */
  guint8 flags;
  guint8 codec;       /* MYD_CODEC_* */
  gboolean compressed;
  z_stream *zs;       /* zlib inflate state (DEFLATE/GZIP)  */
  ZSTD_DStream *zds;  /* zstd inflate state (ZSTD)          */
};

#define STREAM_INFLATE_CHUNK 65536

struct loader_demux {
  GHashTable *streams; /* stream_id -> struct demux_file */
};

static gboolean stream_lenient_mode = FALSE;
static gboolean stream_producer_cancelled = FALSE;

static void stream_demux_set_lenient(const gchar *reason){
  stream_lenient_mode = TRUE;
  if (reason)
    g_warning("%s", reason);
}

static void demux_on_open(void *user, guint64 sid, const gchar *name,
                          guint8 flags, guint8 codec){
  struct loader_demux *dx = user;
  struct demux_file *df = g_new0(struct demux_file, 1);
  df->name = g_strdup(name);
  df->data = g_string_new("");
  df->flags = flags;
  df->codec = codec;
  if (flags & MYD_FOPEN_FLAG_COMPRESSED){
    df->compressed = TRUE;
    switch (codec){
    case MYD_CODEC_ZSTD:
      df->zds = ZSTD_createDStream();
      if (df->zds == NULL)
        m_critical("Stream: ZSTD_createDStream failed for %s", name);
      ZSTD_initDStream(df->zds);
      break;
    case MYD_CODEC_GZIP:
    case MYD_CODEC_DEFLATE:
    default:
      df->zs = g_new0(z_stream, 1);
      /* windowBits 15+32 auto-detects zlib (DEFLATE) and gzip wrappers. */
      if (inflateInit2(df->zs, 15 + 32) != Z_OK)
        m_critical("Stream: inflateInit2 failed for %s", name);
      break;
    }
  }
  g_hash_table_insert(dx->streams, GINT_TO_POINTER((gint)sid), df);
  trace("Stream(recv open): %s id %" G_GUINT64_FORMAT " codec %u", name, sid,
        codec);
}

/* Decompress bytes into df->data via the file's codec. Budget is charged at
   FILE_CLOSE when the completed file enters the in-memory registry. */
static void demux_inflate_append(struct demux_file *df, const gchar *buf,
                                 gsize len){
  if (df->codec == MYD_CODEC_ZSTD){
    ZSTD_inBuffer in = {buf, len, 0};
    while (in.pos < in.size){
      guchar out[STREAM_INFLATE_CHUNK];
      ZSTD_outBuffer o = {out, sizeof(out), 0};
      size_t ret = ZSTD_decompressStream(df->zds, &o, &in);
      if (ZSTD_isError(ret)){
        if (stream_lenient_mode){
          g_warning("Stream ended before %s finished; skipping partial file",
                    df->name);
          return;
        }
        m_critical("Stream: zstd decompress failed for %s: %s", df->name,
                   ZSTD_getErrorName(ret));
      }
      if (o.pos)
        g_string_append_len(df->data, (gchar *)out, o.pos);
      if (o.pos == 0 && in.pos == in.size)
        break;
    }
    return;
  }
  df->zs->next_in = (Bytef *)buf;
  df->zs->avail_in = (uInt)len;
  do {
    guchar out[STREAM_INFLATE_CHUNK];
    df->zs->next_out = out;
    df->zs->avail_out = sizeof(out);
    int ret = inflate(df->zs, Z_NO_FLUSH);
    if (ret == Z_STREAM_ERROR || ret == Z_DATA_ERROR || ret == Z_MEM_ERROR ||
        ret == Z_NEED_DICT){
      if (stream_lenient_mode){
        g_warning("Stream ended before %s finished; skipping partial file",
                  df->name);
        return;
      }
      m_critical("Stream: inflate failed for %s (%d)", df->name, ret);
    }
    gsize produced = sizeof(out) - df->zs->avail_out;
    if (produced)
      g_string_append_len(df->data, (gchar *)out, produced);
  } while (df->zs->avail_out == 0);
}

static void demux_on_data(void *user, guint64 sid, const gchar *buf,
                          gsize len){
  struct loader_demux *dx = user;
  struct demux_file *df =
      g_hash_table_lookup(dx->streams, GINT_TO_POINTER((gint)sid));
  if (!df){
    g_critical("Stream data for unknown id %" G_GUINT64_FORMAT, sid);
    return;
  }
  if (df->compressed)
    demux_inflate_append(df, buf, len);
  else
    g_string_append_len(df->data, buf, len);
}

static void demux_on_close(void *user, guint64 sid, guint64 total,
                           gboolean has_crc, guint32 crc){
  struct loader_demux *dx = user;
  struct demux_file *df =
      g_hash_table_lookup(dx->streams, GINT_TO_POINTER((gint)sid));
  if (!df){
    g_critical("Stream close for unknown id %" G_GUINT64_FORMAT, sid);
    return;
  }
  g_hash_table_steal(dx->streams, GINT_TO_POINTER((gint)sid));

  if (df->zs){
    inflateEnd(df->zs);
    g_free(df->zs);
    df->zs = NULL;
  }
  if (df->zds){
    ZSTD_freeDStream(df->zds);
    df->zds = NULL;
  }

  if (df->data->len != total)
    m_critical("Stream: size mismatch for %s. Declared: %" G_GUINT64_FORMAT
               " received: %zu", df->name, total, df->data->len);
  if (has_crc){
    uLong seed = crc32(0L, Z_NULL, 0);
    guint32 got = (guint32)crc32(seed, (const Bytef *)df->data->str,
                                 df->data->len);
    if (got != crc)
      m_critical("Stream: checksum mismatch for %s (got %08x expected %08x)",
                 df->name, got, crc);
  }

  gsize len = df->data->len;
  gchar *bytes = g_string_free(df->data, FALSE); /* keep buffer */

  if (g_str_has_prefix(df->name, "metadata")){
    /* Metadata is consumed via GKeyFile from a real path, so materialise it
       on disk (tiny). */
    gchar *path = g_build_filename(directory, df->name, NULL);
    GError *gerror = NULL;
    if (!g_file_set_contents(path, bytes, len, &gerror))
      m_critical("Stream: could not write metadata %s: %s", path,
                 gerror ? gerror->message : "unknown");
    g_free(path);
    g_free(bytes);
  }else{
    /* Bulk data / schema / .dat: served from memory by myl_open / the LOAD
       DATA local-infile handler. Control-plane files are exempt from budget
       backpressure so the demux thread never blocks waiting for loaders. */
    if (!stream_mem_budget_file_exempt(df->name))
      stream_mem_budget_charge(len);
    stream_mem_put(df->name, bytes, len);
  }
  process_filename_push(df->name);
  g_free(df->name);
  g_free(df);
}

static void demux_on_cancel(void *user){
  (void)user;
  stream_producer_cancelled = TRUE;
  stream_lenient_mode = TRUE;
  shutdown_triggered = TRUE;
  g_message("Stream restore stopping: dump cancelled by producer");
}

static void demux_free_pending(gpointer p){
  struct demux_file *df = p;
  if (df->zs){
    inflateEnd(df->zs);
    g_free(df->zs);
  }
  if (df->zds)
    ZSTD_freeDStream(df->zds);
  g_string_free(df->data, TRUE);
  g_free(df->name);
  g_free(df);
}

static void *process_binary_stream_loader(struct configuration *conf,
                                          const guchar *prefix,
                                          gsize prefix_len){
  (void)conf;
  set_thread_name("STT");
  stream_lenient_mode = FALSE;
  stream_producer_cancelled = FALSE;
  struct loader_demux dx;
  dx.streams = g_hash_table_new_full(g_direct_hash, g_direct_equal, NULL,
                                     demux_free_pending);
  struct myd_stream_callbacks cb = {demux_on_open, demux_on_data,
                                    demux_on_close, NULL, demux_on_cancel};
  struct myd_stream_decoder *d = myd_stream_decoder_new(&cb, &dx);

  if (prefix_len){
    int pr = myd_stream_decoder_feed(d, prefix, prefix_len);
    if (pr == MYD_DECODE_ERROR && !stream_lenient_mode)
      m_critical("Corrupted binary stream header");
  }

  guchar *buf = g_malloc(STREAM_BUFFER_SIZE);
  size_t n;
  gboolean stream_saw_eof = FALSE;
  while ((n = fread(buf, 1, STREAM_BUFFER_SIZE, stdin)) > 0){
    if (shutdown_triggered && !stream_lenient_mode)
      stream_demux_set_lenient(NULL);
    int r = myd_stream_decoder_feed(d, buf, n);
    if (r == MYD_DECODE_ERROR){
      if (stream_lenient_mode || shutdown_triggered){
        stream_demux_set_lenient(
            "Stream ended with incomplete frame; discarding partial data");
        break;
      }
      m_critical("Corrupted binary stream");
    }
    if (myd_stream_decoder_saw_eof(d)){
      stream_saw_eof = TRUE;
      break;
    }
  }
  if (n == 0 && !stream_saw_eof){
    stream_demux_set_lenient(NULL);
    if (!stream_producer_cancelled)
      g_warning("Stream producer disconnected unexpectedly");
  }
  if (g_hash_table_size(dx.streams) > 0 && stream_lenient_mode)
    g_warning("Stream ended before %u file(s) finished; skipping partial "
              "file(s)",
              g_hash_table_size(dx.streams));
  g_free(buf);
  myd_stream_decoder_free(d);
  g_hash_table_destroy(dx.streams);
  process_filename_queue_end();
  return NULL;
}

void *process_stream(struct configuration *stream_conf){
  set_thread_name("STT");
  void *ret;
  /* The mysqldump format has no binary magic and is parsed by the legacy
     reader. */
  if (mysqldump){
    ret = process_stream_legacy(stream_conf, NULL, 0);
    release_metadata_header_if_pending();
    return ret;
  }

  /* Auto-detect: peek the leading bytes. If they are the binary magic, use the
     multiplexed decoder; otherwise fall back to the legacy textual parser,
     replaying the peeked bytes. A short/empty read here means the producer sent
     nothing (crash / broken pipe): the legacy parser exits on EOF and the guard
     below unblocks the metadata-header waiter. */
  guchar magic[MYD_STREAM_MAGIC_LEN];
  size_t got = fread(magic, 1, MYD_STREAM_MAGIC_LEN, stdin);
  if (got == MYD_STREAM_MAGIC_LEN &&
      memcmp(magic, MYD_STREAM_MAGIC, MYD_STREAM_MAGIC_LEN) == 0){
    stream_binary_active = TRUE;
    ret = process_binary_stream_loader(stream_conf, magic, got);
    release_metadata_header_if_pending();
    return ret;
  }
  ret = process_stream_legacy(stream_conf, magic, got);
  release_metadata_header_if_pending();
  return ret;
}


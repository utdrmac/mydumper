/*
    Stream memory budget for myloader binary protocol.
*/
#include "myloader_stream_mem_budget.h"
#include <string.h>

guint stream_budget_mb = MYLOADER_STREAM_BUDGET_DEFAULT_MB;
gboolean stream_budget_mb_user_set = FALSE;

static gint64 stream_mem_bytes = 0;
static guint64 stream_mem_cap = 512ULL * 1024 * 1024;
static GMutex *stream_mem_budget_mutex = NULL;
static GCond *stream_mem_budget_cond = NULL;

static void stream_mem_budget_ensure_init(void){
  if (stream_mem_budget_mutex)
    return;
  stream_mem_budget_mutex = g_mutex_new();
  stream_mem_budget_cond = g_cond_new();
}

void stream_mem_budget_init(void){
  stream_mem_cap = (guint64)stream_budget_mb * 1024 * 1024;
  stream_mem_budget_ensure_init();
}

void stream_mem_budget_set_cap_mb(guint mb){
  if (mb == 0)
    mb = MYLOADER_STREAM_BUDGET_DEFAULT_MB;
  stream_budget_mb = mb;
  stream_mem_cap = (guint64)mb * 1024 * 1024;
  stream_mem_budget_ensure_init();
}

guint stream_mem_budget_get_cap_mb(void){
  return stream_budget_mb;
}

guint stream_mem_budget_round_up_pow2_mb(guint mb){
  guint p = 256;
  if (mb <= p)
    return p;
  while (p < mb){
    if (p > G_MAXUINT / 2)
      return G_MAXUINT;
    p <<= 1;
  }
  return p;
}

void stream_mem_budget_adjust_for_dump(guint dump_chunk_size_mb,
                                       guint dump_stream_budget_mb,
                                       guint num_threads,
                                       guint max_threads_per_table){
  guint effective_threads;
  guint cap_mb;
  guint max_concurrent_files;
  guint needed_mb;
  guint recommended_mb;

  if (dump_chunk_size_mb == 0)
    return;

  effective_threads = num_threads < max_threads_per_table ?
      num_threads : max_threads_per_table;
  cap_mb = stream_mem_budget_get_cap_mb();
  max_concurrent_files = cap_mb / dump_chunk_size_mb;

  if (max_concurrent_files >= effective_threads)
    return;

  needed_mb = effective_threads * dump_chunk_size_mb;
  recommended_mb = stream_mem_budget_round_up_pow2_mb(needed_mb);

  if (!stream_budget_mb_user_set){
    guint old_mb = cap_mb;
    stream_mem_budget_set_cap_mb(recommended_mb);
    g_warning("Stream budget raised from %u MB to %u MB (chunk-size=%u MB, "
              "threads=%u) to allow %u concurrent INSERT restores. "
              "mydumper-stream-budget-mb=%u.",
              old_mb, recommended_mb, dump_chunk_size_mb,
              effective_threads, effective_threads, dump_stream_budget_mb);
  }else{
    g_warning("Stream budget %u MB with chunk-size=%u MB allows ~%u concurrent "
              "INSERT restores but %u loader threads are configured. "
              "Use --stream-budget-mb=%u (or higher). "
              "mydumper-stream-budget-mb=%u.",
              cap_mb, dump_chunk_size_mb, max_concurrent_files,
              effective_threads, recommended_mb, dump_stream_budget_mb);
  }
}

gboolean stream_mem_budget_file_exempt(const gchar *name){
  if (name == NULL)
    return FALSE;
  if (g_str_has_prefix(name, "metadata"))
    return TRUE;
  if (strcmp(name, "all-schema-create-tablespace.sql") == 0)
    return TRUE;
  if (g_str_has_suffix(name, "-schema.sql") ||
      g_str_has_suffix(name, "-schema-create.sql") ||
      g_str_has_suffix(name, "-schema-view.sql") ||
      g_str_has_suffix(name, "-schema-sequence.sql") ||
      g_str_has_suffix(name, "-schema-triggers.sql") ||
      g_str_has_suffix(name, "-schema-post.sql"))
    return TRUE;
  return FALSE;
}

void stream_mem_budget_charge(gsize len){
  stream_mem_budget_ensure_init();
  g_mutex_lock(stream_mem_budget_mutex);
  while (stream_mem_bytes > 0 &&
         stream_mem_bytes + (gint64)len > (gint64)stream_mem_cap)
    g_cond_wait(stream_mem_budget_cond, stream_mem_budget_mutex);
  stream_mem_bytes += (gint64)len;
  g_mutex_unlock(stream_mem_budget_mutex);
}

void stream_mem_release_bytes(gsize len){
  if (!stream_mem_budget_mutex)
    return;
  g_mutex_lock(stream_mem_budget_mutex);
  stream_mem_bytes -= (gint64)len;
  g_cond_broadcast(stream_mem_budget_cond);
  g_mutex_unlock(stream_mem_budget_mutex);
}

#ifdef STREAM_MEM_BUDGET_TEST

void stream_mem_budget_init_for_test(guint64 cap_bytes){
  stream_mem_cap = cap_bytes;
  stream_mem_bytes = 0;
  stream_mem_budget_ensure_init();
}

gint64 stream_mem_budget_get_bytes(void){
  return stream_mem_bytes;
}

gboolean stream_mem_budget_try_charge(gsize len){
  stream_mem_budget_ensure_init();
  g_mutex_lock(stream_mem_budget_mutex);
  if (stream_mem_bytes > 0 &&
      stream_mem_bytes + (gint64)len > (gint64)stream_mem_cap){
    g_mutex_unlock(stream_mem_budget_mutex);
    return FALSE;
  }
  stream_mem_bytes += (gint64)len;
  g_mutex_unlock(stream_mem_budget_mutex);
  return TRUE;
}

#endif

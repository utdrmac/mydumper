/*
    Stream memory budget for myloader binary protocol.
*/
#include "stream_mem_budget.h"
#include <string.h>

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

void stream_mem_budget_init_from_env(void){
  const gchar *budget_env = g_getenv("MYLOADER_STREAM_BUDGET_MB");
  if (budget_env){
    guint64 mb = g_ascii_strtoull(budget_env, NULL, 10);
    if (mb)
      stream_mem_cap = mb * 1024 * 1024;
  }
  stream_mem_budget_ensure_init();
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

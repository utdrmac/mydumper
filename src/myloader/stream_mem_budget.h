/*
    Stream memory budget for myloader binary protocol.

    MYLOADER_STREAM_BUDGET_MB (default 512) limits bytes of completed files
    queued in the in-memory registry awaiting loader threads. In-flight
    decompression buffers are not charged; charge happens once at FILE_CLOSE
    for data/.dat files only.
*/
#ifndef _stream_mem_budget_h
#define _stream_mem_budget_h
#include <glib.h>

void stream_mem_budget_init_from_env(void);
void stream_mem_budget_charge(gsize len);
void stream_mem_release_bytes(gsize len);
gboolean stream_mem_budget_file_exempt(const gchar *name);

#ifdef STREAM_MEM_BUDGET_TEST
void stream_mem_budget_init_for_test(guint64 cap_bytes);
gint64 stream_mem_budget_get_bytes(void);
gboolean stream_mem_budget_try_charge(gsize len);
#endif

#endif

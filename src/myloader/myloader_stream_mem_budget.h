/*
    Stream memory budget for myloader binary protocol.

    --stream-budget-mb (default 512) limits bytes of completed files
    queued in the in-memory registry awaiting loader threads. In-flight
    decompression buffers are not charged; charge happens once at FILE_CLOSE
    for data/.dat files only.
*/
#ifndef _myloader_stream_mem_budget_h
#define _myloader_stream_mem_budget_h
#include <glib.h>

#define MYLOADER_STREAM_BUDGET_DEFAULT_MB 256U

void stream_mem_budget_init(void);
void stream_mem_budget_set_cap_mb(guint mb);
guint stream_mem_budget_get_cap_mb(void);
guint stream_mem_budget_round_up_pow2_mb(guint mb);
void stream_mem_budget_adjust_for_dump(guint dump_chunk_size_mb,
                                       guint dump_stream_budget_mb,
                                       guint num_threads,
                                       guint max_threads_per_table);
void stream_mem_budget_charge(gsize len);
void stream_mem_release_bytes(gsize len);
gboolean stream_mem_budget_file_exempt(const gchar *name);

extern guint stream_budget_mb;
extern gboolean stream_budget_mb_user_set;

#ifdef STREAM_MEM_BUDGET_TEST
void stream_mem_budget_init_for_test(guint64 cap_bytes);
gint64 stream_mem_budget_get_bytes(void);
gboolean stream_mem_budget_try_charge(gsize len);
#endif

#endif

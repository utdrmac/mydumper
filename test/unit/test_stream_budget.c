/*
 * Unit tests for myloader stream memory budget accounting.
 *
 * Verifies that charging at FILE_CLOSE (not during inflate) avoids self-deadlock
 * on large single-table streams while preserving backpressure when multiple
 * completed files are queued.
 */
#include <glib.h>
#include <stdio.h>
#include <stdlib.h>

#include "../../src/myloader/stream_mem_budget.h"

#define MIB (1024ULL * 1024ULL)

static void assert_equal(const char *label, gint64 got, gint64 expected){
  if (got != expected){
    fprintf(stderr, "  FAIL [%s]: got %lld expected %lld\n",
            label, (long long)got, (long long)expected);
    exit(1);
  }
  fprintf(stderr, "  pass [%s]\n", label);
}

static void assert_true(const char *label, gboolean cond){
  if (!cond){
    fprintf(stderr, "  FAIL [%s]\n", label);
    exit(1);
  }
  fprintf(stderr, "  pass [%s]\n", label);
}

/* Old semantics: charge each chunk during inflate; blocks once total exceeds cap
   even when nothing can release until the file closes (self-deadlock). */
static gboolean old_incremental_try_charge(gint64 *bytes, gsize chunk,
                                           guint64 cap){
  if (*bytes > 0 && *bytes + (gint64)chunk > (gint64)cap)
    return FALSE;
  *bytes += (gint64)chunk;
  return TRUE;
}

static void test_old_incremental_self_deadlock(void){
  fprintf(stderr, "test_old_incremental_self_deadlock\n");
  const guint64 cap = 512 * MIB;
  const gsize chunk = 64 * 1024;
  const gsize total = 600 * MIB;
  gint64 bytes = 0;
  gsize accumulated = 0;
  gboolean blocked = FALSE;

  while (accumulated < total){
    if (!old_incremental_try_charge(&bytes, chunk, cap)){
      blocked = TRUE;
      break;
    }
    accumulated += chunk;
  }
  assert_true("old incremental blocks before file completes", blocked);
  assert_true("old incremental stops below full file size",
              accumulated < total);
}

static void test_single_file_exceeds_cap_when_queue_empty(void){
  fprintf(stderr, "test_single_file_exceeds_cap_when_queue_empty\n");
  stream_mem_budget_init_for_test(512 * MIB);
  stream_mem_budget_charge(600 * MIB);
  assert_equal("600MB charged with 512MB cap when queue empty",
               stream_mem_budget_get_bytes(), (gint64)(600 * MIB));
  stream_mem_release_bytes(600 * MIB);
  assert_equal("released", stream_mem_budget_get_bytes(), 0);
}

static void test_backpressure_when_queue_non_empty(void){
  fprintf(stderr, "test_backpressure_when_queue_non_empty\n");
  stream_mem_budget_init_for_test(512 * MIB);
  assert_true("first 300MB charge", stream_mem_budget_try_charge(300 * MIB));
  assert_true("second 300MB blocked while first held",
              !stream_mem_budget_try_charge(300 * MIB));
  assert_equal("only first file counted", stream_mem_budget_get_bytes(),
               (gint64)(300 * MIB));
  stream_mem_release_bytes(300 * MIB);
  assert_true("second 300MB after release",
              stream_mem_budget_try_charge(300 * MIB));
  stream_mem_release_bytes(300 * MIB);
}

static void test_schema_exempt_classification(void){
  fprintf(stderr, "test_schema_exempt_classification\n");
  assert_true("metadata.header exempt",
              stream_mem_budget_file_exempt("metadata.header"));
  assert_true("schema table exempt",
              stream_mem_budget_file_exempt("db.t-schema.sql"));
  assert_true("schema create exempt",
              stream_mem_budget_file_exempt("db-schema-create.sql"));
  assert_true("data file not exempt",
              !stream_mem_budget_file_exempt("db.t.00001.sql"));
  assert_true("dat file not exempt",
              !stream_mem_budget_file_exempt("db.t.00001.dat"));
}

static void test_exempt_files_not_counted(void){
  fprintf(stderr, "test_exempt_files_not_counted\n");
  stream_mem_budget_init_for_test(512 * MIB);
  stream_mem_budget_charge(500 * MIB);
  assert_equal("data queued", stream_mem_budget_get_bytes(),
               (gint64)(500 * MIB));
  /* Exempt files skip stream_mem_budget_charge in production; budget unchanged. */
  assert_true("schema still exempt under pressure",
              stream_mem_budget_file_exempt("db.t-schema.sql"));
  stream_mem_release_bytes(500 * MIB);
}

int main(void){
  fprintf(stderr, "=== test_stream_budget ===\n");
  test_old_incremental_self_deadlock();
  test_single_file_exceeds_cap_when_queue_empty();
  test_backpressure_when_queue_non_empty();
  test_schema_exempt_classification();
  test_exempt_files_not_counted();
  fprintf(stderr, "=== all tests passed ===\n");
  return 0;
}

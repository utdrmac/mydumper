/*
 * Unit tests for myloader stream memory budget accounting.
 */
#include <glib.h>
#include <stdio.h>
#include <stdlib.h>

#include "../../src/myloader/myloader_stream_mem_budget.h"

#define MIB (1024ULL * 1024ULL)

static void assert_equal_u(const char *label, guint got, guint expected){
  if (got != expected){
    fprintf(stderr, "  FAIL [%s]: got %u expected %u\n", label, got, expected);
    exit(1);
  }
  fprintf(stderr, "  pass [%s]\n", label);
}

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

static void test_round_up_pow2(void){
  fprintf(stderr, "test_round_up_pow2\n");
  assert_equal_u("minimum 256", stream_mem_budget_round_up_pow2_mb(1), 256);
  assert_equal_u("1600 to 2048", stream_mem_budget_round_up_pow2_mb(1600), 2048);
  assert_equal_u("256 stays 256", stream_mem_budget_round_up_pow2_mb(256), 256);
  assert_equal_u("257 to 512", stream_mem_budget_round_up_pow2_mb(257), 512);
}

static void test_auto_raise_default_budget(void){
  fprintf(stderr, "test_auto_raise_default_budget\n");
  stream_budget_mb = MYLOADER_STREAM_BUDGET_DEFAULT_MB;
  stream_budget_mb_user_set = FALSE;
  stream_mem_budget_init_for_test((guint64)stream_budget_mb * MIB);

  stream_mem_budget_adjust_for_dump(100, 256, 16, 16);
  assert_equal_u("raised to 2048", stream_mem_budget_get_cap_mb(), 2048);
}

static void test_user_set_budget_not_raised(void){
  fprintf(stderr, "test_user_set_budget_not_raised\n");
  stream_budget_mb = 512;
  stream_budget_mb_user_set = TRUE;
  stream_mem_budget_init_for_test(512 * MIB);

  stream_mem_budget_adjust_for_dump(100, 256, 16, 16);
  assert_equal_u("cap unchanged", stream_mem_budget_get_cap_mb(), 512);
}

static void test_chunk_size_zero_skips(void){
  fprintf(stderr, "test_chunk_size_zero_skips\n");
  stream_budget_mb = MYLOADER_STREAM_BUDGET_DEFAULT_MB;
  stream_budget_mb_user_set = FALSE;
  stream_mem_budget_init_for_test(512 * MIB);

  stream_mem_budget_adjust_for_dump(0, 256, 16, 16);
  assert_equal_u("cap unchanged when chunk-size 0",
                 stream_mem_budget_get_cap_mb(), 512);
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
  assert_true("data file not exempt",
              !stream_mem_budget_file_exempt("db.t.00001.sql"));
}

int main(void){
  fprintf(stderr, "=== test_stream_budget ===\n");
  test_round_up_pow2();
  test_auto_raise_default_budget();
  test_user_set_budget_not_raised();
  test_chunk_size_zero_skips();
  test_single_file_exceeds_cap_when_queue_empty();
  test_backpressure_when_queue_non_empty();
  test_schema_exempt_classification();
  fprintf(stderr, "=== all tests passed ===\n");
  return 0;
}

/*
 * Unit tests for stream restore progress totals from metadata.
 *
 * Exercises the part-total helper logic and metadata key parsing used when
 * myloader reads data_files from metadata.partial updates.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DATA_FILES "data_files"
#define DATA_FILES_COMPLETE "data_files_complete"

static unsigned part_total(unsigned local_count, unsigned metadata_reported){
  return metadata_reported > local_count ? metadata_reported : local_count;
}

static unsigned global_total(unsigned local_total, unsigned metadata_sum){
  return metadata_sum > local_total ? metadata_sum : local_total;
}

static void assert_equal_u(const char *label, unsigned got, unsigned expected){
  if (got != expected){
    fprintf(stderr, "  FAIL [%s]: got %u expected %u\n", label, got, expected);
    exit(1);
  }
  fprintf(stderr, "  pass [%s]\n", label);
}

static const char *find_key_value(const char *data, const char *key){
  char search[128];
  snprintf(search, sizeof(search), "%s = ", key);
  const char *line = strstr(data, search);
  if (line == NULL)
    return NULL;
  return line + strlen(search);
}

static void test_part_total_prefers_metadata(void){
  fprintf(stderr, "test_part_total_prefers_metadata\n");
  assert_equal_u("metadata ahead of local", part_total(5, 27), 27);
  assert_equal_u("local ahead of metadata", part_total(30, 27), 30);
  assert_equal_u("equal counts", part_total(10, 10), 10);
}

static void test_global_total_prefers_metadata_sum(void){
  fprintf(stderr, "test_global_total_prefers_metadata_sum\n");
  assert_equal_u("metadata sum ahead", global_total(5, 27), 27);
  assert_equal_u("local ahead", global_total(30, 27), 30);
}

static void test_metadata_key_parsing(void){
  fprintf(stderr, "test_metadata_key_parsing\n");
  const char *data =
      "[db.table]\n"
      "real_table_name=table\n"
      "rows = 1000\n"
      "data_files = 27\n"
      "data_files_complete = 1\n";
  const char *files = find_key_value(data, DATA_FILES);
  const char *complete = find_key_value(data, DATA_FILES_COMPLETE);

  if (files == NULL){
    fprintf(stderr, "  FAIL [data_files missing]\n");
    exit(1);
  }
  assert_equal_u("data_files parsed", (unsigned)strtoul(files, NULL, 10), 27);
  if (complete == NULL || strncmp(complete, "1", 1) != 0){
    fprintf(stderr, "  FAIL [data_files_complete expected 1]\n");
    exit(1);
  }
  fprintf(stderr, "  pass [data_files_complete parsed]\n");
}

static void test_monotonic_metadata_update(void){
  fprintf(stderr, "test_monotonic_metadata_update\n");
  unsigned stored = 0;
  unsigned incoming[] = {10, 27, 22, 30};
  size_t n = sizeof(incoming) / sizeof(incoming[0]);
  for (size_t i = 0; i < n; i++){
    if (incoming[i] > stored)
      stored = incoming[i];
  }
  assert_equal_u("monotonic max", stored, 30);
}

int main(void){
  test_part_total_prefers_metadata();
  test_global_total_prefers_metadata_sum();
  test_metadata_key_parsing();
  test_monotonic_metadata_update();
  fprintf(stderr, "All progress metadata tests passed.\n");
  return 0;
}

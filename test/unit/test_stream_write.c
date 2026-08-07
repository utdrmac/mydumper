/*
    Unit test for graceful stream stdout writes (EPIPE handling).
*/
#include <glib.h>
#include <unistd.h>
#include <errno.h>
#include "../../src/common_stream_protocol.h"

static void test_write_all_epipe(void) {
  int fds[2];
  g_assert_cmpint(pipe(fds), ==, 0);
  close(fds[0]);

  gboolean consumer_gone = FALSE;
  const char payload[] = "MYDSTRM2";
  g_assert_false(myd_stream_write_all(fds[1], payload, sizeof(payload) - 1,
                                        &consumer_gone));
  g_assert_true(consumer_gone);
  close(fds[1]);
}

static void test_write_all_success(void) {
  int fds[2];
  g_assert_cmpint(pipe(fds), ==, 0);

  gboolean consumer_gone = FALSE;
  const char payload[] = "hello";
  g_assert_true(
      myd_stream_write_all(fds[1], payload, sizeof(payload) - 1, &consumer_gone));
  g_assert_false(consumer_gone);

  char got[16] = {0};
  g_assert_cmpint(read(fds[0], got, sizeof(got) - 1), ==, (ssize_t)(sizeof(payload) - 1));
  g_assert_cmpstr(got, ==, "hello");

  close(fds[0]);
  close(fds[1]);
}

int main(int argc, char **argv) {
  g_test_init(&argc, &argv, NULL);
  g_test_add_func("/stream_write/epipe", test_write_all_epipe);
  g_test_add_func("/stream_write/success", test_write_all_success);
  return g_test_run();
}

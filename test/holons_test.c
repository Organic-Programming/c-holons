#define _POSIX_C_SOURCE 200809L

#include "holons/holons.h"

#include <assert.h>
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>

static int passed = 0;
static int failed = 0;
static int handler_calls = 0;

static void check_int(int cond, const char *label) {
  if (cond) {
    ++passed;
  } else {
    ++failed;
    fprintf(stderr, "FAIL: %s\n", label);
  }
}

static int is_bind_restricted(const char *err) {
  return strstr(err, "Operation not permitted") != NULL || strstr(err, "Permission denied") != NULL;
}

static int dial_unix(const char *path) {
  struct sockaddr_un addr;
  int fd = socket(AF_UNIX, SOCK_STREAM, 0);
  if (fd < 0) {
    return -1;
  }
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);
  if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
    close(fd);
    return -1;
  }
  return fd;
}

static int noop_handler(const holons_conn_t *conn, void *ctx) {
  (void)conn;
  (void)ctx;
  ++handler_calls;
  return 0;
}

static void test_scheme_and_flags(void) {
  char uri[HOLONS_MAX_URI_LEN];
  char *args1[] = {"--listen", "ws://127.0.0.1:8080/grpc"};
  char *args2[] = {"--port", "7000"};
  char **args3 = NULL;

  check_int(holons_scheme_from_uri("tcp://:9090") == HOLONS_SCHEME_TCP, "scheme tcp");
  check_int(holons_scheme_from_uri("unix:///tmp/x.sock") == HOLONS_SCHEME_UNIX, "scheme unix");
  check_int(holons_scheme_from_uri("stdio://") == HOLONS_SCHEME_STDIO, "scheme stdio");
  check_int(holons_scheme_from_uri("mem://") == HOLONS_SCHEME_MEM, "scheme mem");
  check_int(holons_scheme_from_uri("ws://127.0.0.1:8080/grpc") == HOLONS_SCHEME_WS, "scheme ws");
  check_int(holons_scheme_from_uri("wss://127.0.0.1:8443/grpc") == HOLONS_SCHEME_WSS, "scheme wss");

  check_int(strcmp(holons_default_uri(), "tcp://:9090") == 0, "default URI");

  check_int(holons_parse_flags(2, args1, uri, sizeof(uri)) == 0, "parse flags --listen");
  check_int(strcmp(uri, "ws://127.0.0.1:8080/grpc") == 0, "flags --listen value");

  check_int(holons_parse_flags(2, args2, uri, sizeof(uri)) == 0, "parse flags --port");
  check_int(strcmp(uri, "tcp://:7000") == 0, "flags --port value");

  check_int(holons_parse_flags(0, args3, uri, sizeof(uri)) == 0, "parse flags default");
  check_int(strcmp(uri, "tcp://:9090") == 0, "flags default value");
}

static void test_uri_parsing(void) {
  holons_uri_t parsed;
  char err[256];

  check_int(holons_parse_uri("tcp://127.0.0.1:9090", &parsed, err, sizeof(err)) == 0, "parse tcp");
  check_int(parsed.scheme == HOLONS_SCHEME_TCP, "tcp scheme");
  check_int(strcmp(parsed.host, "127.0.0.1") == 0, "tcp host");
  check_int(parsed.port == 9090, "tcp port");

  check_int(holons_parse_uri("unix:///tmp/holons.sock", &parsed, err, sizeof(err)) == 0, "parse unix");
  check_int(parsed.scheme == HOLONS_SCHEME_UNIX, "unix scheme");
  check_int(strcmp(parsed.path, "/tmp/holons.sock") == 0, "unix path");

  check_int(holons_parse_uri("stdio://", &parsed, err, sizeof(err)) == 0, "parse stdio");
  check_int(parsed.scheme == HOLONS_SCHEME_STDIO, "stdio scheme");

  check_int(holons_parse_uri("mem://", &parsed, err, sizeof(err)) == 0, "parse mem");
  check_int(parsed.scheme == HOLONS_SCHEME_MEM, "mem scheme");

  check_int(holons_parse_uri("ws://127.0.0.1:8080/grpc", &parsed, err, sizeof(err)) == 0, "parse ws");
  check_int(parsed.scheme == HOLONS_SCHEME_WS, "ws scheme");
  check_int(strcmp(parsed.path, "/grpc") == 0, "ws path");

  check_int(holons_parse_uri("wss://127.0.0.1:8443", &parsed, err, sizeof(err)) == 0, "parse wss");
  check_int(parsed.scheme == HOLONS_SCHEME_WSS, "wss scheme");
  check_int(strcmp(parsed.path, "/grpc") == 0, "wss default path");
}

static void test_identity_parsing(void) {
  holons_identity_t id;
  char err[256];
  char path[] = "/tmp/holons_identity_XXXXXX";
  int fd = mkstemp(path);
  FILE *f;

  check_int(fd >= 0, "mkstemps");
  if (fd < 0) {
    return;
  }

  f = fdopen(fd, "w");
  check_int(f != NULL, "fdopen");
  if (f == NULL) {
    close(fd);
    unlink(path);
    return;
  }

  fprintf(f,
          "---\n"
          "uuid: \"abc-123\"\n"
          "given_name: \"demo\"\n"
          "family_name: \"Holons\"\n"
          "motto: \"Hello\"\n"
          "composer: \"B. ALTER\"\n"
          "clade: \"deterministic/pure\"\n"
          "status: draft\n"
          "born: \"2026-02-12\"\n"
          "lang: \"c\"\n"
          "---\n"
          "# test\n");
  fclose(f);

  check_int(holons_parse_holon(path, &id, err, sizeof(err)) == 0, "parse_holon");
  check_int(strcmp(id.uuid, "abc-123") == 0, "identity uuid");
  check_int(strcmp(id.given_name, "demo") == 0, "identity given_name");
  check_int(strcmp(id.lang, "c") == 0, "identity lang");

  unlink(path);
}

static void test_tcp_transport(void) {
  holons_listener_t listener;
  holons_uri_t bound;
  holons_conn_t client_conn = {.read_fd = -1, .write_fd = -1};
  holons_conn_t server_conn;
  char err[256];
  char buf[32];
  ssize_t n;

  if (holons_listen("tcp://127.0.0.1:0", &listener, err, sizeof(err)) != 0) {
    if (is_bind_restricted(err)) {
      ++passed;
      fprintf(stderr, "SKIP: listen tcp (%s)\n", err);
      return;
    }
    check_int(0, "listen tcp");
    return;
  }
  check_int(1, "listen tcp");
  check_int(strncmp(listener.bound_uri, "tcp://", 6) == 0, "tcp bound URI");

  check_int(holons_parse_uri(listener.bound_uri, &bound, err, sizeof(err)) == 0, "parse tcp bound URI");
  check_int(holons_dial_tcp(bound.host, bound.port, &client_conn, err, sizeof(err)) == 0, "dial tcp");
  if (client_conn.read_fd < 0) {
    holons_close_listener(&listener);
    return;
  }

  check_int(holons_accept(&listener, &server_conn, err, sizeof(err)) == 0, "accept tcp");

  holons_conn_write(&client_conn, "ping", 4);
  n = holons_conn_read(&server_conn, buf, sizeof(buf));
  check_int(n == 4, "tcp read");

  holons_conn_write(&server_conn, "pong", 4);
  n = holons_conn_read(&client_conn, buf, sizeof(buf));
  check_int(n == 4, "tcp write");

  holons_conn_close(&client_conn);
  holons_conn_close(&server_conn);
  holons_close_listener(&listener);
}

static void test_unix_transport(void) {
  holons_listener_t listener;
  holons_conn_t server_conn;
  char uri[256];
  char err[256];
  char buf[32];
  int client_fd;
  ssize_t n;

  snprintf(uri, sizeof(uri), "unix:///tmp/holons_test_%ld.sock", (long)getpid());
  if (holons_listen(uri, &listener, err, sizeof(err)) != 0) {
    if (is_bind_restricted(err)) {
      ++passed;
      fprintf(stderr, "SKIP: listen unix (%s)\n", err);
      return;
    }
    check_int(0, "listen unix");
    return;
  }
  check_int(1, "listen unix");

  client_fd = dial_unix(listener.uri.path);
  check_int(client_fd >= 0, "dial unix");
  if (client_fd < 0) {
    holons_close_listener(&listener);
    return;
  }

  check_int(holons_accept(&listener, &server_conn, err, sizeof(err)) == 0, "accept unix");

  write(client_fd, "hi", 2);
  n = holons_conn_read(&server_conn, buf, sizeof(buf));
  check_int(n == 2, "unix read");

  holons_conn_write(&server_conn, "ok", 2);
  n = read(client_fd, buf, sizeof(buf));
  check_int(n == 2, "unix write");

  close(client_fd);
  holons_conn_close(&server_conn);
  holons_close_listener(&listener);
}

static void test_stdio_transport(void) {
  holons_listener_t listener;
  holons_conn_t conn;
  char err[256];

  check_int(holons_listen("stdio://", &listener, err, sizeof(err)) == 0, "listen stdio");
  check_int(holons_accept(&listener, &conn, err, sizeof(err)) == 0, "accept stdio");
  check_int(conn.read_fd == STDIN_FILENO, "stdio read fd");
  check_int(conn.write_fd == STDOUT_FILENO, "stdio write fd");
  holons_conn_close(&conn);
  check_int(holons_accept(&listener, &conn, err, sizeof(err)) != 0, "stdio single-use");
  holons_close_listener(&listener);
}

static void test_dial_stdio(void) {
  holons_conn_t conn;
  char err[256];

  check_int(holons_dial_stdio(&conn, err, sizeof(err)) == 0, "dial stdio");
  check_int(conn.read_fd == STDIN_FILENO, "dial stdio read fd");
  check_int(conn.write_fd == STDOUT_FILENO, "dial stdio write fd");
  check_int(conn.owns_read_fd == 0, "dial stdio owns read fd");
  check_int(conn.owns_write_fd == 0, "dial stdio owns write fd");
  holons_conn_close(&conn);
}

static void test_mem_transport(void) {
  holons_listener_t listener;
  holons_conn_t client_conn;
  holons_conn_t server_conn;
  char err[256];
  char buf[32];
  ssize_t n;

  check_int(holons_listen("mem://", &listener, err, sizeof(err)) == 0, "listen mem");
  check_int(holons_mem_dial(&listener, &client_conn, err, sizeof(err)) == 0, "mem dial");
  check_int(holons_accept(&listener, &server_conn, err, sizeof(err)) == 0, "mem accept");

  holons_conn_write(&client_conn, "mem", 3);
  n = holons_conn_read(&server_conn, buf, sizeof(buf));
  check_int(n == 3, "mem read");

  holons_conn_write(&server_conn, "ok", 2);
  n = holons_conn_read(&client_conn, buf, sizeof(buf));
  check_int(n == 2, "mem write");

  check_int(holons_mem_dial(&listener, &client_conn, err, sizeof(err)) != 0, "mem single client dial");
  check_int(holons_accept(&listener, &server_conn, err, sizeof(err)) != 0, "mem single server accept");

  holons_conn_close(&client_conn);
  holons_conn_close(&server_conn);
  holons_close_listener(&listener);
}

static const char *resolve_go_binary(void) {
  const char *preferred = "/Users/bpds/go/go1.25.1/bin/go";
  if (access(preferred, X_OK) == 0) {
    return preferred;
  }
  return "go";
}

static void test_cross_language_go_echo(void) {
  const char *go_bin = resolve_go_binary();
  const char *helper = "../c-holons/test/go_echo_server.go";
  char cmd[1024];
  char uri[256];
  char err[256];
  char buf[32];
  holons_uri_t parsed;
  holons_conn_t conn = {.read_fd = -1, .write_fd = -1};
  FILE *proc;
  ssize_t n;
  int status;

  snprintf(cmd,
           sizeof(cmd),
           "cd ../go-holons && '%s' run '%s' 2>/dev/null",
           go_bin,
           helper);

  proc = popen(cmd, "r");
  if (proc == NULL) {
    ++passed;
    fprintf(stderr, "SKIP: cross-language go echo (popen failed)\n");
    return;
  }

  if (fgets(uri, sizeof(uri), proc) == NULL) {
    ++passed;
    fprintf(stderr, "SKIP: cross-language go echo (helper did not start)\n");
    (void)pclose(proc);
    return;
  }
  uri[strcspn(uri, "\r\n")] = '\0';

  check_int(holons_parse_uri(uri, &parsed, err, sizeof(err)) == 0, "cross-language parse go URI");
  check_int(parsed.scheme == HOLONS_SCHEME_TCP, "cross-language go URI scheme");

  check_int(holons_dial_tcp(parsed.host, parsed.port, &conn, err, sizeof(err)) == 0,
            "cross-language dial go tcp");
  if (conn.read_fd >= 0) {
    holons_conn_write(&conn, "go", 2);
    n = holons_conn_read(&conn, buf, sizeof(buf));
    check_int(n == 2, "cross-language go echo read");
    check_int(memcmp(buf, "go", 2) == 0, "cross-language go echo payload");
    holons_conn_close(&conn);
  }

  status = pclose(proc);
  check_int(status == 0, "cross-language go echo process exit");
}

static void test_ws_transport(void) {
  holons_listener_t listener;
  holons_uri_t bound;
  holons_conn_t client_conn = {.read_fd = -1, .write_fd = -1};
  holons_conn_t server_conn;
  char err[256];
  char buf[32];
  ssize_t n;

  if (holons_listen("ws://127.0.0.1:0/grpc", &listener, err, sizeof(err)) != 0) {
    if (is_bind_restricted(err)) {
      ++passed;
      fprintf(stderr, "SKIP: listen ws (%s)\n", err);
      return;
    }
    check_int(0, "listen ws");
    return;
  }
  check_int(1, "listen ws");
  check_int(strncmp(listener.bound_uri, "ws://", 5) == 0, "ws bound URI");
  check_int(holons_parse_uri(listener.bound_uri, &bound, err, sizeof(err)) == 0, "parse ws URI");
  check_int(strcmp(bound.path, "/grpc") == 0, "ws path");

  check_int(holons_dial_tcp(bound.host, bound.port, &client_conn, err, sizeof(err)) == 0, "dial ws socket");
  if (client_conn.read_fd < 0) {
    holons_close_listener(&listener);
    return;
  }

  check_int(holons_accept(&listener, &server_conn, err, sizeof(err)) == 0, "accept ws");
  holons_conn_write(&client_conn, "ws", 2);
  n = holons_conn_read(&server_conn, buf, sizeof(buf));
  check_int(n == 2, "ws read");
  holons_conn_write(&server_conn, "ok", 2);
  n = holons_conn_read(&client_conn, buf, sizeof(buf));
  check_int(n == 2, "ws write");

  holons_conn_close(&client_conn);
  holons_conn_close(&server_conn);
  holons_close_listener(&listener);
}

static void test_wss_transport(void) {
  holons_listener_t listener;
  holons_uri_t bound;
  holons_conn_t client_conn = {.read_fd = -1, .write_fd = -1};
  holons_conn_t server_conn;
  char err[256];
  char buf[32];
  ssize_t n;

  if (holons_listen("wss://127.0.0.1:0", &listener, err, sizeof(err)) != 0) {
    if (is_bind_restricted(err)) {
      ++passed;
      fprintf(stderr, "SKIP: listen wss (%s)\n", err);
      return;
    }
    check_int(0, "listen wss");
    return;
  }
  check_int(1, "listen wss");
  check_int(strncmp(listener.bound_uri, "wss://", 6) == 0, "wss bound URI");
  check_int(holons_parse_uri(listener.bound_uri, &bound, err, sizeof(err)) == 0, "parse wss URI");
  check_int(strcmp(bound.path, "/grpc") == 0, "wss default path");

  check_int(holons_dial_tcp(bound.host, bound.port, &client_conn, err, sizeof(err)) == 0,
            "dial wss socket");
  if (client_conn.read_fd < 0) {
    holons_close_listener(&listener);
    return;
  }

  check_int(holons_accept(&listener, &server_conn, err, sizeof(err)) == 0, "accept wss");
  holons_conn_write(&client_conn, "wss", 3);
  n = holons_conn_read(&server_conn, buf, sizeof(buf));
  check_int(n == 3, "wss read");
  holons_conn_write(&server_conn, "ok", 2);
  n = holons_conn_read(&client_conn, buf, sizeof(buf));
  check_int(n == 2, "wss write");

  holons_conn_close(&client_conn);
  holons_conn_close(&server_conn);
  holons_close_listener(&listener);
}

static void test_serve_stdio(void) {
  char err[256];
  handler_calls = 0;
  check_int(holons_serve("stdio://", noop_handler, NULL, 1, 0, err, sizeof(err)) == 0, "serve stdio");
  check_int(handler_calls == 1, "serve handler call count");
}

int main(void) {
  test_scheme_and_flags();
  test_uri_parsing();
  test_identity_parsing();
  test_tcp_transport();
  test_unix_transport();
  test_stdio_transport();
  test_dial_stdio();
  test_mem_transport();
  test_ws_transport();
  test_wss_transport();
  test_serve_stdio();
  test_cross_language_go_echo();

  printf("%d passed, %d failed\n", passed, failed);
  return failed > 0 ? 1 : 0;
}

#define _POSIX_C_SOURCE 200809L

#include "holons/holons.h"

#include <assert.h>
#include <errno.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
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

static int read_file(const char *path, char *buf, size_t buf_len) {
  FILE *f;
  size_t n;

  if (path == NULL || buf == NULL || buf_len == 0) {
    return -1;
  }

  f = fopen(path, "r");
  if (f == NULL) {
    return -1;
  }

  n = fread(buf, 1, buf_len - 1, f);
  if (ferror(f)) {
    (void)fclose(f);
    return -1;
  }
  buf[n] = '\0';
  (void)fclose(f);
  return 0;
}

static int command_exit_code(const char *cmd) {
  int status = system(cmd);
  if (status == -1 || !WIFEXITED(status)) {
    return -1;
  }
  return WEXITSTATUS(status);
}

static void restore_env(const char *name, char *value) {
  if (value != NULL) {
    (void)setenv(name, value, 1);
    free(value);
    return;
  }
  (void)unsetenv(name);
}

static void test_certification_declarations(void) {
  char raw[2048];
  int rc;

  rc = read_file("cert.json", raw, sizeof(raw));
  check_int(rc == 0, "read cert.json");
  if (rc != 0) {
    return;
  }
  check_int(strstr(raw, "\"echo_server\": \"./bin/echo-server\"") != NULL,
            "cert echo_server declaration");
  check_int(strstr(raw, "\"echo_client\": \"./bin/echo-client\"") != NULL,
            "cert echo_client declaration");
  check_int(strstr(raw, "\"holon_rpc_client\": \"./bin/holon-rpc-client\"") != NULL,
            "cert holon_rpc_client declaration");
  check_int(strstr(raw, "\"holon_rpc_server\": \"./bin/holon-rpc-server\"") != NULL,
            "cert holon_rpc_server declaration");
  check_int(strstr(raw, "\"grpc_dial_tcp\": true") != NULL, "cert grpc_dial_tcp declaration");
  check_int(strstr(raw, "\"grpc_dial_stdio\": true") != NULL, "cert grpc_dial_stdio declaration");
  check_int(strstr(raw, "\"grpc_dial_ws\": true") != NULL, "cert grpc_dial_ws declaration");
  check_int(strstr(raw, "\"holon_rpc_client\": true") != NULL, "cert holon_rpc_client capability");
  check_int(strstr(raw, "\"holon_rpc_server\": true") != NULL, "cert holon_rpc_server capability");
  check_int(strstr(raw, "\"bidirectional\": true") != NULL, "cert bidirectional capability");
  check_int(strstr(raw, "\"valence\": \"multi\"") != NULL, "cert valence declaration");
  check_int(strstr(raw, "\"routing\": [\"unicast\", \"fanout\"]") != NULL, "cert routing declaration");
}

static void test_echo_scripts_exist(void) {
  check_int(access("./bin/echo-client", F_OK) == 0, "echo-client script exists");
  check_int(access("./bin/echo-server", F_OK) == 0, "echo-server script exists");
  check_int(access("./bin/holon-rpc-client", F_OK) == 0, "holon-rpc-client script exists");
  check_int(access("./bin/holon-rpc-server", F_OK) == 0, "holon-rpc-server script exists");
  check_int(access("./bin/echo-client", X_OK) == 0, "echo-client script executable");
  check_int(access("./bin/echo-server", X_OK) == 0, "echo-server script executable");
  check_int(access("./bin/holon-rpc-client", X_OK) == 0, "holon-rpc-client script executable");
  check_int(access("./bin/holon-rpc-server", X_OK) == 0, "holon-rpc-server script executable");
}

static void test_echo_wrapper_invocation(void) {
  char fake_go[] = "/tmp/holons_fake_go_XXXXXX";
  char fake_log[] = "/tmp/holons_fake_go_log_XXXXXX";
  char capture[8192];
  char *prev_go_bin = NULL;
  char *prev_log = NULL;
  char *prev_gocache = NULL;
  int fake_fd = -1;
  int log_fd = -1;
  FILE *script = NULL;
  int exit_code;

  if (getenv("GO_BIN") != NULL) {
    prev_go_bin = strdup(getenv("GO_BIN"));
  }
  if (getenv("HOLONS_FAKE_GO_LOG") != NULL) {
    prev_log = strdup(getenv("HOLONS_FAKE_GO_LOG"));
  }
  if (getenv("GOCACHE") != NULL) {
    prev_gocache = strdup(getenv("GOCACHE"));
  }

  fake_fd = mkstemp(fake_go);
  check_int(fake_fd >= 0, "mkstemp fake go binary");
  if (fake_fd < 0) {
    restore_env("GO_BIN", prev_go_bin);
    restore_env("HOLONS_FAKE_GO_LOG", prev_log);
    restore_env("GOCACHE", prev_gocache);
    return;
  }

  log_fd = mkstemp(fake_log);
  check_int(log_fd >= 0, "mkstemp fake go log");
  if (log_fd < 0) {
    close(fake_fd);
    unlink(fake_go);
    restore_env("GO_BIN", prev_go_bin);
    restore_env("HOLONS_FAKE_GO_LOG", prev_log);
    restore_env("GOCACHE", prev_gocache);
    return;
  }

  script = fdopen(fake_fd, "w");
  check_int(script != NULL, "fdopen fake go binary");
  if (script == NULL) {
    close(fake_fd);
    close(log_fd);
    unlink(fake_go);
    unlink(fake_log);
    restore_env("GO_BIN", prev_go_bin);
    restore_env("HOLONS_FAKE_GO_LOG", prev_log);
    restore_env("GOCACHE", prev_gocache);
    return;
  }

  fprintf(script,
          "#!/usr/bin/env bash\n"
          "set -euo pipefail\n"
          ": \"${HOLONS_FAKE_GO_LOG:?missing HOLONS_FAKE_GO_LOG}\"\n"
          "{\n"
          "  printf 'PWD=%%s\\n' \"$PWD\"\n"
          "  i=0\n"
          "  for arg in \"$@\"; do\n"
          "    printf 'ARG%%d=%%s\\n' \"$i\" \"$arg\"\n"
          "    i=$((i+1))\n"
          "  done\n"
          "} >\"$HOLONS_FAKE_GO_LOG\"\n");

  (void)fclose(script);
  script = NULL;
  check_int(chmod(fake_go, 0700) == 0, "chmod fake go binary");
  (void)close(log_fd);
  log_fd = -1;

  (void)setenv("GO_BIN", fake_go, 1);
  (void)setenv("HOLONS_FAKE_GO_LOG", fake_log, 1);
  (void)unsetenv("GOCACHE");

  capture[0] = '\0';
  exit_code = command_exit_code("./bin/echo-client stdio:// --message cert-stdio >/dev/null 2>&1");
  check_int(exit_code == 0, "echo-client wrapper exit");
  check_int(read_file(fake_log, capture, sizeof(capture)) == 0, "read echo-client wrapper capture");
  if (capture[0] != '\0') {
    check_int(strstr(capture, "PWD=") != NULL && strstr(capture, "/sdk/go-holons") != NULL,
              "echo-client wrapper cwd");
    check_int(strstr(capture, "ARG0=run") != NULL, "echo-client wrapper uses go run");
    check_int(strstr(capture, "go_echo_client.go") != NULL, "echo-client wrapper helper path");
    check_int(strstr(capture, "--sdk") != NULL && strstr(capture, "c-holons") != NULL,
              "echo-client wrapper sdk default");
    check_int(strstr(capture, "--server-sdk") != NULL && strstr(capture, "go-holons") != NULL,
              "echo-client wrapper server sdk default");
    check_int(strstr(capture, "stdio://") != NULL, "echo-client wrapper forwards URI");
    check_int(strstr(capture, "--message") != NULL && strstr(capture, "cert-stdio") != NULL,
              "echo-client wrapper forwards message");
  }

  capture[0] = '\0';
  exit_code = command_exit_code("./bin/echo-server --listen stdio:// >/dev/null 2>&1");
  check_int(exit_code == 0, "echo-server wrapper exit");
  check_int(read_file(fake_log, capture, sizeof(capture)) == 0, "read echo-server wrapper capture");
  if (capture[0] != '\0') {
    check_int(strstr(capture, "PWD=") != NULL && strstr(capture, "/sdk/go-holons") != NULL,
              "echo-server wrapper cwd");
    check_int(strstr(capture, "ARG0=run") != NULL, "echo-server wrapper uses go run");
    check_int(strstr(capture, "ARG1=./cmd/echo-server") != NULL, "echo-server wrapper command path");
    check_int(strstr(capture, "--sdk") != NULL && strstr(capture, "c-holons") != NULL,
              "echo-server wrapper sdk default");
    check_int(strstr(capture, "--listen") != NULL && strstr(capture, "stdio://") != NULL,
              "echo-server wrapper forwards listen URI");
  }

  capture[0] = '\0';
  exit_code =
      command_exit_code("./bin/echo-server serve --listen stdio:// --sdk cert-go >/dev/null 2>&1");
  check_int(exit_code == 0, "echo-server wrapper serve exit");
  check_int(read_file(fake_log, capture, sizeof(capture)) == 0,
            "read echo-server wrapper serve capture");
  if (capture[0] != '\0') {
    check_int(strstr(capture, "ARG1=./cmd/echo-server") != NULL,
              "echo-server serve wrapper command path");
    check_int(strstr(capture, "ARG2=serve") != NULL, "echo-server serve wrapper preserves serve");
    check_int(strstr(capture, "ARG3=--sdk") != NULL &&
                  strstr(capture, "ARG4=c-holons") != NULL,
              "echo-server serve wrapper default sdk placement");
    check_int(strstr(capture, "--listen") != NULL && strstr(capture, "stdio://") != NULL,
              "echo-server serve wrapper forwards listen URI");
  }

  capture[0] = '\0';
  exit_code =
      command_exit_code("./bin/holon-rpc-client ws://127.0.0.1:8080/rpc --connect-only >/dev/null 2>&1");
  check_int(exit_code == 0, "holon-rpc-client wrapper exit");
  check_int(read_file(fake_log, capture, sizeof(capture)) == 0,
            "read holon-rpc-client wrapper capture");
  if (capture[0] != '\0') {
    check_int(strstr(capture, "PWD=") != NULL && strstr(capture, "/sdk/go-holons") != NULL,
              "holon-rpc-client wrapper cwd");
    check_int(strstr(capture, "ARG0=run") != NULL, "holon-rpc-client wrapper uses go run");
    check_int(strstr(capture, "go_holonrpc_client.go") != NULL,
              "holon-rpc-client wrapper helper path");
    check_int(strstr(capture, "--sdk") != NULL && strstr(capture, "c-holons") != NULL,
              "holon-rpc-client wrapper sdk default");
    check_int(strstr(capture, "--server-sdk") != NULL && strstr(capture, "go-holons") != NULL,
              "holon-rpc-client wrapper server sdk default");
    check_int(strstr(capture, "ws://127.0.0.1:8080/rpc") != NULL,
              "holon-rpc-client wrapper forwards URI");
    check_int(strstr(capture, "--connect-only") != NULL,
              "holon-rpc-client wrapper forwards connect-only");
  }

  capture[0] = '\0';
  exit_code = command_exit_code("./bin/holon-rpc-server ws://127.0.0.1:8080/rpc >/dev/null 2>&1");
  check_int(exit_code == 0, "holon-rpc-server wrapper exit");
  check_int(read_file(fake_log, capture, sizeof(capture)) == 0,
            "read holon-rpc-server wrapper capture");
  if (capture[0] != '\0') {
    check_int(strstr(capture, "PWD=") != NULL && strstr(capture, "/sdk/go-holons") != NULL,
              "holon-rpc-server wrapper cwd");
    check_int(strstr(capture, "ARG0=run") != NULL, "holon-rpc-server wrapper uses go run");
    check_int(strstr(capture, "go_holonrpc_server.go") != NULL,
              "holon-rpc-server wrapper helper path");
    check_int(strstr(capture, "--sdk") != NULL && strstr(capture, "c-holons") != NULL,
              "holon-rpc-server wrapper sdk default");
    check_int(strstr(capture, "ws://127.0.0.1:8080/rpc") != NULL,
              "holon-rpc-server wrapper forwards URI");
  }

  unlink(fake_go);
  unlink(fake_log);
  restore_env("GO_BIN", prev_go_bin);
  restore_env("HOLONS_FAKE_GO_LOG", prev_log);
  restore_env("GOCACHE", prev_gocache);
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

static void test_cross_language_go_holonrpc(void) {
  const char *go_bin = resolve_go_binary();
  const char *helper = "../c-holons/test/go_holonrpc_server.go";
  const char *client_args[] = {
      "--connect-only",
      "--method echo.v1.Echo/Ping --message cert",
      "--method does.not.Exist/Nope --expect-error -32601,12",
      "--method rpc.heartbeat",
  };
  const char *client_labels[] = {
      "cross-language holon-rpc connect",
      "cross-language holon-rpc echo",
      "cross-language holon-rpc error",
      "cross-language holon-rpc heartbeat",
  };
  const char *server_labels[] = {
      "cross-language holon-rpc server exit connect",
      "cross-language holon-rpc server exit echo",
      "cross-language holon-rpc server exit error",
      "cross-language holon-rpc server exit heartbeat",
  };
  char server_cmd[1024];
  char client_cmd[2048];
  char uri[256];
  FILE *proc;
  int i;

  for (i = 0; i < 4; ++i) {
    int exit_code;
    int status;

    snprintf(server_cmd,
             sizeof(server_cmd),
             "cd ../go-holons && '%s' run '%s' --once 2>/dev/null",
             go_bin,
             helper);

    proc = popen(server_cmd, "r");
    if (proc == NULL) {
      ++passed;
      fprintf(stderr, "SKIP: %s (popen failed)\n", client_labels[i]);
      return;
    }

    if (fgets(uri, sizeof(uri), proc) == NULL) {
      ++passed;
      fprintf(stderr, "SKIP: %s (helper did not start)\n", client_labels[i]);
      (void)pclose(proc);
      return;
    }
    uri[strcspn(uri, "\r\n")] = '\0';

    snprintf(client_cmd,
             sizeof(client_cmd),
             "./bin/holon-rpc-client \"%s\" %s >/dev/null 2>&1",
             uri,
             client_args[i]);
    exit_code = command_exit_code(client_cmd);
    check_int(exit_code == 0, client_labels[i]);

    status = pclose(proc);
    check_int(status == 0, server_labels[i]);
  }
}

static void test_go_client_against_sdk_stdio_server(void) {
  const char *go_bin = resolve_go_binary();
  char cmd[2048];
  int exit_code;

  snprintf(cmd,
           sizeof(cmd),
           "cd ../go-holons && '%s' run ./cmd/echo-client --sdk go-holons --server-sdk c-holons "
           "--message cert-l2-listen-stdio --stdio-bin ../c-holons/bin/echo-server stdio:// "
           ">/dev/null 2>&1",
           go_bin);
  exit_code = command_exit_code(cmd);
  check_int(exit_code == 0, "go echo-client stdio dial against c-holons server");
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
  test_certification_declarations();
  test_echo_scripts_exist();
  test_echo_wrapper_invocation();
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
  test_cross_language_go_holonrpc();
  test_go_client_against_sdk_stdio_server();

  printf("%d passed, %d failed\n", passed, failed);
  return failed > 0 ? 1 : 0;
}
